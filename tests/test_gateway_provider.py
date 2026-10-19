"""The gateway adapter runs the MCP loop itself, so the loop is ours to test.

The Anthropic adapter hands tool execution to a hosted connector; here the
adapter is the MCP client. Everything that connector did for us -- reading a
tool result, deciding a call was throttled, keeping the transcript in the shape
the next request needs -- is now code in this repo that can be wrong.
"""

import copy
import importlib.util
import json
import os
import sys
import unittest

import httpx
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "gateway_provider",
    Path(__file__).resolve().parents[1] / "scripts" / "sdmx_eval_provider_gateway.py",
)
gateway = importlib.util.module_from_spec(_spec)
sys.modules["gateway_provider"] = gateway
_spec.loader.exec_module(gateway)


class TestRpcBodyParsing(unittest.TestCase):
    """Streamable HTTP may answer with plain JSON or an SSE frame."""

    def test_plain_json_body(self) -> None:
        body = gateway._parse_rpc_body('{"jsonrpc":"2.0","id":1,"result":{"tools":[]}}')
        self.assertEqual(body["result"], {"tools": []})

    def test_sse_framed_body(self) -> None:
        text = 'event: message\ndata: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}\n\n'
        self.assertEqual(gateway._parse_rpc_body(text)["result"], {"ok": True})

    def test_sse_done_sentinel_is_skipped(self) -> None:
        text = 'data: {"jsonrpc":"2.0","id":1,"result":{"ok":true}}\ndata: [DONE]\n'
        self.assertEqual(gateway._parse_rpc_body(text)["result"], {"ok": True})

    def test_unreadable_body_is_an_error_not_an_empty_result(self) -> None:
        # Returning {} here would read downstream as "the MCP has no tools".
        with self.assertRaises(RuntimeError):
            gateway._parse_rpc_body("<html>502 Bad Gateway</html>")


class TestToolSchemaConversion(unittest.TestCase):
    def test_mcp_input_schema_becomes_openai_parameters(self) -> None:
        schema = {"type": "object", "properties": {"flowRef": {"type": "string"}}}
        converted = gateway.mcp_tools_as_openai(
            [{"name": "get_single_observation", "description": "d", "inputSchema": schema}]
        )
        self.assertEqual(converted[0]["type"], "function")
        self.assertEqual(converted[0]["function"]["name"], "get_single_observation")
        self.assertEqual(converted[0]["function"]["parameters"], schema)

    def test_tool_without_a_schema_still_declares_an_object(self) -> None:
        # A bare `{}` makes some gateways reject the whole request, which would
        # fail every case rather than the one tool.
        converted = gateway.mcp_tools_as_openai([{"name": "ping"}])
        self.assertEqual(converted[0]["function"]["parameters"]["type"], "object")


class TestThrottleDetection(unittest.TestCase):
    def test_rate_limited_result_is_throttled(self) -> None:
        self.assertTrue(gateway._was_throttled(json.dumps({"status": "rate_limited"})))

    def test_retryable_flag_is_throttled(self) -> None:
        self.assertTrue(gateway._was_throttled(json.dumps({"retryable": True})))

    def test_absent_data_is_not_throttled(self) -> None:
        payload = {"status": "unresolved_from_official_flows", "retryable": False}
        self.assertFalse(gateway._was_throttled(json.dumps(payload)))

    def test_non_json_result_is_not_throttled(self) -> None:
        self.assertFalse(gateway._was_throttled("value: 4.2"))


class TestToolResultText(unittest.TestCase):
    def test_text_blocks_are_joined(self) -> None:
        result = {"content": [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]}
        self.assertEqual(gateway._tool_result_text(result), "a\nb")

    def test_structured_content_is_used_when_there_is_no_text(self) -> None:
        result = {"structuredContent": {"value": 4.2}}
        self.assertEqual(json.loads(gateway._tool_result_text(result)), {"value": 4.2})


class TestAnswerExtraction(unittest.TestCase):
    def test_bare_json_object(self) -> None:
        parsed = gateway._extract_json_object('{"answer_text":"value: 4.2","claims":{}}')
        self.assertEqual(parsed["answer_text"], "value: 4.2")

    def test_fenced_json_object(self) -> None:
        # Gateway models fence far more readily than the Anthropic connector did.
        parsed = gateway._extract_json_object('```json\n{"answer_text":"x"}\n```')
        self.assertEqual(parsed["answer_text"], "x")

    def test_json_with_surrounding_prose(self) -> None:
        parsed = gateway._extract_json_object('Sure!\n{"answer_text":"x"}\nHope that helps.')
        self.assertEqual(parsed["answer_text"], "x")

    def test_prose_only_yields_nothing(self) -> None:
        self.assertIsNone(gateway._extract_json_object("The value is 4.2."))


class _FakeResponse:
    def __init__(self, body: dict) -> None:
        self.status_code = 200
        self._body = body
        self.text = json.dumps(body)

    def json(self) -> dict:
        return self._body


class _FakeGatewayClient:
    """Replays a scripted sequence of gateway replies and keeps what was sent."""

    def __init__(self, replies: list[dict]) -> None:
        self._replies = list(replies)
        self.sent: list[dict] = []

    def post(self, url, headers=None, json=None):  # noqa: A002 - httpx's name
        # Snapshot the payload. The adapter passes its live `messages` list and
        # keeps appending to it, so holding a reference would let assertions
        # read state from later turns -- a real client serialises at send time.
        self.sent.append(copy.deepcopy(json))
        return _FakeResponse(self._replies.pop(0))

    def close(self) -> None:
        pass


class _FakeMcpSession:
    def __init__(self, results: list[dict]) -> None:
        self._results = list(results)
        self.calls: list[tuple[str, dict]] = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return None

    def list_tools(self):
        return [{"name": "get_single_observation", "description": "d",
                 "inputSchema": {"type": "object", "properties": {}}}]

    def call_tool(self, name, arguments):
        self.calls.append((name, arguments))
        return self._results.pop(0)


def _text_result(payload: dict) -> dict:
    return {"content": [{"type": "text", "text": json.dumps(payload)}]}


def _assistant_tool_call(name: str, args: dict) -> dict:
    return {"choices": [{"message": {"content": "", "tool_calls": [
        {"id": "call_1", "type": "function",
         "function": {"name": name, "arguments": json.dumps(args)}}]}}],
        "usage": {"prompt_tokens": 100, "completion_tokens": 20, "cost": 0.0001}}


def _assistant_answer(text: str) -> dict:
    return {"choices": [{"message": {"content": text}}],
            "usage": {"prompt_tokens": 200, "completion_tokens": 30, "cost": 0.0002}}


class TestRunCaseLoop(unittest.TestCase):
    """The loop end to end, with the gateway and the MCP both stubbed."""

    def setUp(self) -> None:
        self._real_client = gateway.httpx.Client
        self._real_session = gateway.McpSession
        os.environ["TEST_GW_KEY"] = "test-key"

    def tearDown(self) -> None:
        gateway.httpx.Client = self._real_client
        gateway.McpSession = self._real_session
        os.environ.pop("TEST_GW_KEY", None)

    def _run(self, replies, tool_results, **provider_extra):
        self.client = _FakeGatewayClient(replies)
        self.session = _FakeMcpSession(tool_results)
        gateway.httpx.Client = lambda *a, **k: self.client
        gateway.McpSession = lambda *a, **k: self.session
        provider = {"api_key_env": "TEST_GW_KEY", "model": "openai/gpt-6-astra"}
        provider.update(provider_extra)
        return gateway.run_case({
            "provider": provider,
            "mcp": {"url": "https://example.invalid/mcp"},
            "case": {"prompt": "DTP3 coverage for Kenya, 2020, both sexes"},
        })

    def test_tool_call_is_executed_and_fed_back(self) -> None:
        result = self._run(
            [_assistant_tool_call("get_single_observation", {"flowRef": "UNICEF,GLOBAL_DATAFLOW,1.0"}),
             _assistant_answer('{"answer_text":"value: 91; period: 2020",'
                               '"claims":{"value":91,"time_period":"2020",'
                               '"flowRef":"UNICEF,GLOBAL_DATAFLOW,1.0","filters":{"REF_AREA":"KEN"}}}')],
            [_text_result({"value": 91, "time_period": "2020"})],
        )
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["claims"]["value"], 91)
        self.assertEqual(result["claims"]["filters"], {"REF_AREA": "KEN"})
        self.assertEqual(self.session.calls[0][0], "get_single_observation")

        # The second request must carry the tool result back, tied to the call
        # id -- a transcript missing either is rejected by the gateway.
        second = self.client.sent[1]["messages"]
        tool_messages = [m for m in second if m.get("role") == "tool"]
        self.assertEqual(len(tool_messages), 1)
        self.assertEqual(tool_messages[0]["tool_call_id"], "call_1")
        self.assertIn("91", tool_messages[0]["content"])

    def test_trace_records_both_the_call_and_its_result(self) -> None:
        # The grader reads flowRef and filters out of this trace, so a dropped
        # entry silently becomes a routing failure.
        result = self._run(
            [_assistant_tool_call("get_single_observation", {"flowRef": "X"}),
             _assistant_answer('{"answer_text":"value: null","claims":{}}')],
            [_text_result({"status": "unresolved_from_official_flows"})],
        )
        kinds = [entry["type"] for entry in result["tool_trace"]]
        self.assertEqual(kinds, ["mcp_tool_use", "mcp_tool_result"])
        self.assertEqual(result["tool_trace"][0]["input"], {"flowRef": "X"})

    def test_throttled_result_is_retried_before_the_model_sees_it(self) -> None:
        result = self._run(
            [_assistant_tool_call("get_single_observation", {}),
             _assistant_answer('{"answer_text":"value: 91","claims":{"value":91}}')],
            [_text_result({"status": "rate_limited", "retryable": True}),
             _text_result({"value": 91})],
            throttle_retries=1, throttle_backoff_seconds=0,
        )
        self.assertEqual(len(self.session.calls), 2)
        self.assertEqual(result["claims"]["value"], 91)
        # Only the surviving result reaches the transcript.
        self.assertNotIn("rate_limited", json.dumps(self.client.sent[1]["messages"]))

    def test_usage_and_cost_accumulate_across_steps(self) -> None:
        result = self._run(
            [_assistant_tool_call("get_single_observation", {}),
             _assistant_answer('{"answer_text":"value: 91","claims":{"value":91}}')],
            [_text_result({"value": 91})],
        )
        self.assertEqual(result["usage"], {"input_tokens": 300, "output_tokens": 50})
        self.assertAlmostEqual(result["estimated_cost_usd"], 0.0003)

    def test_unparseable_answer_is_a_provider_error_not_a_null_value(self) -> None:
        # A null value grades as a correct abstention on negative cases, so a
        # parse failure that silently becomes null inflates the score.
        result = self._run(
            [_assistant_answer("The DTP3 coverage for Kenya in 2020 was 91 percent.")], [],
        )
        self.assertEqual(result["status"], "provider_error")
        self.assertIsNone(result["claims"]["value"])

    def test_model_id_can_be_overridden_by_environment(self) -> None:
        # How a comparison sweep drives one config across many models.
        os.environ["SDMX_EVAL_MODEL"] = "google/gemini-3-pro"
        try:
            result = self._run(
                [_assistant_answer('{"answer_text":"value: null","claims":{}}')], [],
            )
        finally:
            del os.environ["SDMX_EVAL_MODEL"]
        self.assertEqual(result["model"], "google/gemini-3-pro")
        self.assertEqual(self.client.sent[0]["model"], "google/gemini-3-pro")

    def test_missing_api_key_stops_the_run_with_an_actionable_message(self) -> None:
        gateway.httpx.Client = lambda *a, **k: _FakeGatewayClient([])
        gateway.McpSession = lambda *a, **k: _FakeMcpSession([])
        os.environ.pop("ABSENT_GW_KEY", None)
        with self.assertRaises(SystemExit) as caught:
            gateway.run_case({
                "provider": {"api_key_env": "ABSENT_GW_KEY"},
                "mcp": {"url": "https://example.invalid/mcp"},
                "case": {"prompt": "x"},
            })
        self.assertIn("ABSENT_GW_KEY", str(caught.exception))


if __name__ == "__main__":
    unittest.main()


class TestTerseClaimRecovery(unittest.TestCase):
    """A correct answer stated only in the terse line must still grade.

    One model answered 93 of 500 cases with the right value in answer_text and
    an empty claims object. The grader keys off claims, so all 93 graded as
    unsettled despite being exactly right. Recovery reads back what the model
    already said, and must never go further than that.
    """

    def test_value_and_period_are_read_from_the_terse_line(self) -> None:
        got = gateway._recover_terse_claims("value: 28.7076466874586; period: 2020", {})
        self.assertEqual(got["value"], "28.7076466874586")
        self.assertEqual(got["time_period"], "2020")
        self.assertTrue(got["recovered_from_answer_text"])

    def test_a_claim_the_model_supplied_is_never_overwritten(self) -> None:
        got = gateway._recover_terse_claims("value: 99; period: 1999",
                                            {"value": "42", "time_period": "2020"})
        self.assertEqual(got["value"], "42")
        self.assertEqual(got["time_period"], "2020")
        self.assertNotIn("recovered_from_answer_text", got)

    def test_explicit_null_stays_an_abstention(self) -> None:
        # Negative cases are scored on abstaining, so inventing a value here
        # would turn correct abstentions into wrong answers.
        got = gateway._recover_terse_claims("value: null", {})
        self.assertIsNone(got.get("value"))
        self.assertFalse(got.get("recovered_from_answer_text", False))

    def test_prose_without_the_terse_line_recovers_nothing(self) -> None:
        got = gateway._recover_terse_claims("I could not determine the value.", {})
        self.assertIsNone(got.get("value"))

    def test_recovery_is_flagged_in_the_result(self) -> None:
        self.assertTrue(gateway._recover_terse_claims("value: 5", {})["recovered_from_answer_text"])
        self.assertFalse(
            gateway._recover_terse_claims("value: 5", {"value": "5"})
            .get("recovered_from_answer_text", False))


class TestHarnessLimitsAreNamed(unittest.TestCase):
    """A harness ceiling must never be reported as a wrong answer.

    Measured: with max_tokens at 1200, a reasoning model failed 29 of 63 cases
    having spent its whole budget before writing anything, and the result said
    only "did not contain a parseable JSON object" -- indistinguishable from a
    model that answered badly. That reads as 46% inaccuracy when the real cause
    is a setting. The step ceiling has the same failure mode.
    """

    def setUp(self) -> None:
        self._real_client = gateway.httpx.Client
        self._real_session = gateway.McpSession
        os.environ["TEST_GW_KEY"] = "test-key"

    def tearDown(self) -> None:
        gateway.httpx.Client = self._real_client
        gateway.McpSession = self._real_session
        os.environ.pop("TEST_GW_KEY", None)

    def _run(self, replies, tool_results, **extra):
        client = _FakeGatewayClient(replies)
        gateway.httpx.Client = lambda *a, **k: client
        gateway.McpSession = lambda *a, **k: _FakeMcpSession(tool_results)
        provider = {"api_key_env": "TEST_GW_KEY", "model": "m"}
        provider.update(extra)
        return gateway.run_case({"provider": provider,
                                 "mcp": {"url": "https://example.invalid/mcp"},
                                 "case": {"prompt": "q"}})

    def test_max_tokens_truncation_says_so(self) -> None:
        reply = {"choices": [{"message": {"content": ""}, "finish_reason": "length"}],
                 "usage": {"prompt_tokens": 10, "completion_tokens": 1200}}
        result = self._run([reply], [], max_tokens=1200)
        self.assertEqual(result["status"], "provider_error")
        self.assertTrue(result["truncated"])
        self.assertIn("max_tokens", result["error"])

    def test_step_ceiling_says_so_and_counts_the_calls(self) -> None:
        calls = [_assistant_tool_call("get_single_observation", {}) for _ in range(3)]
        results = [_text_result({"value": 1}) for _ in range(3)]
        result = self._run(calls, results, max_steps=3)
        self.assertEqual(result["status"], "provider_error")
        self.assertTrue(result["steps_exhausted"])
        self.assertIn("step ceiling", result["error"])
        self.assertIn("3 tool calls", result["error"])

    def test_a_genuinely_unparseable_answer_is_still_reported_as_one(self) -> None:
        result = self._run([_assistant_answer("The value is about 90 percent.")], [])
        self.assertEqual(result["status"], "provider_error")
        self.assertFalse(result["truncated"])
        self.assertFalse(result["steps_exhausted"])
        self.assertIn("parseable", result["error"])


class TestJsonMode(unittest.TestCase):
    """Request JSON through the API, not only in prose.

    gemini-2.5-flash-lite returned the correct value inside an explanation of
    why it would not use the requested terse format -- 23 of 27 cases unusable.
    That is instruction-following, not SDMX accuracy, so the adapter can ask
    for JSON through response_format instead.
    """

    def setUp(self) -> None:
        self._real_client = gateway.httpx.Client
        self._real_session = gateway.McpSession
        os.environ["TEST_GW_KEY"] = "test-key"

    def tearDown(self) -> None:
        gateway.httpx.Client = self._real_client
        gateway.McpSession = self._real_session
        os.environ.pop("TEST_GW_KEY", None)

    def _sent(self, **provider_extra):
        client = _FakeGatewayClient([_assistant_answer('{"answer_text":"x","claims":{}}')])
        gateway.httpx.Client = lambda *a, **k: client
        gateway.McpSession = lambda *a, **k: _FakeMcpSession([])
        provider = {"api_key_env": "TEST_GW_KEY", "model": "m"}
        provider.update(provider_extra)
        gateway.run_case({"provider": provider,
                          "mcp": {"url": "https://example.invalid/mcp"},
                          "case": {"prompt": "q"}})
        return client.sent[0]

    def test_default_omits_response_format(self) -> None:
        self.assertNotIn("response_format", self._sent())

    def test_json_mode_is_ignored_while_tools_are_attached(self) -> None:
        # The eval always attaches tools, so this is the real-world path.
        # Constraining the reply to one JSON object made three of seven
        # open-weight models stop calling tools entirely.
        sent = self._sent(json_mode=True)
        self.assertNotIn("response_format", sent)
        self.assertIn("tools", sent)

    def test_json_mode_applies_when_there_are_no_tools(self) -> None:
        self.assertTrue(gateway._json_mode_for(True, []))

    def test_json_mode_suppressed_with_any_tool(self) -> None:
        self.assertFalse(gateway._json_mode_for(True, [{"type": "function"}]))

    def test_off_stays_off_either_way(self) -> None:
        self.assertFalse(gateway._json_mode_for(False, []))
        self.assertFalse(gateway._json_mode_for(False, [{"type": "function"}]))


class _FlakyClient:
    """Returns a scripted sequence of statuses for the same request."""

    def __init__(self, statuses, body=None):
        self.statuses = list(statuses)
        self.body = body or {"choices": [{"message": {"content": "{}"}}], "usage": {}}
        self.attempts = 0

    def post(self, url, headers=None, json=None):  # noqa: A002
        self.attempts += 1
        status = self.statuses.pop(0)
        response = _FakeResponse(self.body)
        response.status_code = status
        return response


class TestGatewayRetry(unittest.TestCase):
    """Infrastructure noise must not land in a model's score.

    A bare 503 from an open-weight provider failed a case mid-run. The request
    was never served, so reissuing it is not a second opinion -- and leaving it
    to fail charges the model for the provider's hiccup, which is random across
    models and would bias a comparison.
    """

    def test_a_transient_503_is_reissued_and_succeeds(self) -> None:
        client = _FlakyClient([503, 200])
        out = gateway._post_with_retry(client, headers={}, payload={}, retries=3, backoff=0)
        self.assertEqual(out.status_code, 200)
        self.assertEqual(client.attempts, 2)

    def test_rate_limiting_is_reissued(self) -> None:
        client = _FlakyClient([429, 429, 200])
        out = gateway._post_with_retry(client, headers={}, payload={}, retries=3, backoff=0)
        self.assertEqual(out.status_code, 200)
        self.assertEqual(client.attempts, 3)

    def test_a_real_rejection_is_not_retried(self) -> None:
        # An unknown model or malformed request returns the same answer however
        # many times it is asked, so retrying only spends money.
        client = _FlakyClient([400, 200])
        out = gateway._post_with_retry(client, headers={}, payload={}, retries=3, backoff=0)
        self.assertEqual(out.status_code, 400)
        self.assertEqual(client.attempts, 1)

    def test_403_is_not_retried(self) -> None:
        client = _FlakyClient([403, 200])
        self.assertEqual(
            gateway._post_with_retry(client, headers={}, payload={}, retries=3, backoff=0).status_code,
            403)
        self.assertEqual(client.attempts, 1)

    def test_retries_are_bounded_and_the_last_status_is_returned(self) -> None:
        client = _FlakyClient([503, 503, 503])
        out = gateway._post_with_retry(client, headers={}, payload={}, retries=2, backoff=0)
        self.assertEqual(out.status_code, 503)
        self.assertEqual(client.attempts, 3)

    def test_a_transport_error_is_reissued(self) -> None:
        calls = {"n": 0}
        body = {"choices": [{"message": {"content": "{}"}}], "usage": {}}

        class Transport:
            def post(self, url, headers=None, json=None):  # noqa: A002
                calls["n"] += 1
                if calls["n"] == 1:
                    raise httpx.ConnectError("reset")
                return _FakeResponse(body)

        out = gateway._post_with_retry(Transport(), headers={}, payload={}, retries=2, backoff=0)
        self.assertEqual(out.status_code, 200)
        self.assertEqual(calls["n"], 2)
