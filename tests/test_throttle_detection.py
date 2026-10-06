"""A throttled tool result must be retried, not passed to the model as absence.

The server labels a 429 as status "rate_limited" with retryable true and a
message saying the query was not answered. In testing the model answered
"value: null" anyway, turning a transport failure into a confident "no data".
The client therefore has to notice and reissue before the model ever sees it.
"""

import importlib.util
import json
import sys
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "provider",
    Path(__file__).resolve().parents[1] / "scripts" / "sdmx_eval_provider_anthropic.py",
)
provider = importlib.util.module_from_spec(_spec)
sys.modules["provider"] = provider
_spec.loader.exec_module(provider)


def _result(payload: dict) -> dict:
    return {"type": "mcp_tool_result",
            "content": [{"type": "text", "text": json.dumps(payload)}]}


class TestTraceWasThrottled(unittest.TestCase):
    def test_rate_limited_is_detected(self) -> None:
        self.assertTrue(provider._trace_was_throttled([
            _result({"status": "rate_limited", "retryable": True, "httpStatus": 429})
        ]))

    def test_upstream_unavailable_is_detected(self) -> None:
        self.assertTrue(provider._trace_was_throttled([
            _result({"status": "upstream_unavailable", "retryable": True, "httpStatus": 503})
        ]))

    def test_retryable_flag_alone_is_enough(self) -> None:
        self.assertTrue(provider._trace_was_throttled([_result({"retryable": True})]))

    def test_resolved_result_is_not_throttled(self) -> None:
        self.assertFalse(provider._trace_was_throttled([
            _result({"status": "resolved", "value": "0.2138"})
        ]))

    def test_genuine_no_data_is_not_throttled(self) -> None:
        """A real miss must still reach the model as a miss."""
        self.assertFalse(provider._trace_was_throttled([
            _result({"status": "unresolved_from_official_flows", "retryable": False})
        ]))

    def test_empty_trace(self) -> None:
        self.assertFalse(provider._trace_was_throttled([]))

    def test_malformed_blocks_do_not_raise(self) -> None:
        for block in ([{"type": "mcp_tool_result", "content": [{"text": "not json"}]}],
                      [{"type": "mcp_tool_use", "input": {}}],
                      [{"type": "mcp_tool_result"}],
                      ["junk"]):
            self.assertFalse(provider._trace_was_throttled(block), block)

    def test_one_throttled_among_several_is_detected(self) -> None:
        self.assertTrue(provider._trace_was_throttled([
            _result({"status": "resolved", "value": "1"}),
            _result({"status": "rate_limited", "retryable": True}),
        ]))


if __name__ == "__main__":
    unittest.main()
