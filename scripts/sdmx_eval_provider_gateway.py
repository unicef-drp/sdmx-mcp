#!/usr/bin/env python3
"""Eval provider for any model reachable through Vercel AI Gateway.

The Anthropic adapter leans on Anthropic's hosted MCP connector: tool calls
execute on Anthropic's side and come back in the response. A gateway has no
such thing -- it is an OpenAI-compatible Chat Completions endpoint and nothing
more -- so this adapter runs the MCP client loop itself:

    list the MCP's tools -> describe them to the model -> model asks for a tool
    -> call the MCP over Streamable HTTP -> feed the result back -> repeat

That difference matters when reading results. The Anthropic adapter measures a
model driving a connector someone else operates; this one measures a model
driving tools through a loop we wrote. A gap between the two is not necessarily
the models differing.

Everything else is held constant deliberately: same manifest, same ground
truth, same grader, same MCP. Only the model and the tool transport change.

Usage -- as a provider `command` in an eval config:

    "provider": {
      "type": "command",
      "name": "gateway",
      "model": "openai/gpt-6-astra",
      "api_key_env": "AI_GATEWAY_API_KEY",
      "command": ["python3", "scripts/sdmx_eval_provider_gateway.py"]
    }

Model ids are `provider/model`, e.g. openai/gpt-6-astra,
anthropic/claude-haiku-4.5, google/gemini-3-pro. SDMX_EVAL_MODEL overrides the
configured model, so one config drives a whole comparison.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from typing import Any

import httpx

GATEWAY_URL = "https://ai-gateway.vercel.sh/v1/chat/completions"
DEFAULT_MODEL = "anthropic/claude-haiku-4.5"
DEFAULT_MAX_TOKENS = 4000
DEFAULT_MAX_STEPS = 12
DEFAULT_TIMEOUT = 120.0

# Mirrors the Anthropic adapter's default so an unconfigured run of either
# adapter asks the same thing of the model.
DEFAULT_SYSTEM_PROMPT = (
    "You are evaluating an SDMX MCP integration. "
    "Use the MCP tools for every case, answer only from the MCP-returned SDMX data, "
    "and do not use training data, external facts, or estimates. "
    "For single-value questions, prefer get_single_observation when the flow and code filters are known, "
    "then answer only if the official MCP result resolves. "
    "Use get_indicator_table for multi-location tables and get_time_series for trend questions. "
    "If the MCP reports unresolved_from_official_flows, no observations, or an ambiguous/non-single-value shape, "
    "abstain with a null value. "
    "Return exactly one JSON object with no markdown fences."
)


# ------------------------------------------------------------------ MCP client


class McpSession:
    """Minimal Streamable HTTP MCP client: initialize, list tools, call them.

    The official SDK is not used here because the adapter must stay a plain
    stdin/stdout subprocess with no event loop of its own, and the three calls
    needed are small.
    """

    def __init__(self, url: str, timeout: float = DEFAULT_TIMEOUT) -> None:
        self.url = url
        self._client = httpx.Client(timeout=timeout)
        self._id = 0
        self._session_id: str | None = None

    def __enter__(self) -> "McpSession":
        self._initialize()
        return self

    def __exit__(self, *exc: object) -> None:
        self._client.close()

    def _headers(self) -> dict[str, str]:
        headers = {
            "Content-Type": "application/json",
            # Streamable HTTP may answer either way; accept both.
            "Accept": "application/json, text/event-stream",
        }
        if self._session_id:
            headers["Mcp-Session-Id"] = self._session_id
        return headers

    def _rpc(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self._id += 1
        response = self._client.post(
            self.url,
            headers=self._headers(),
            json={"jsonrpc": "2.0", "id": self._id, "method": method,
                  "params": params or {}},
        )
        if response.status_code >= 400:
            raise RuntimeError(f"MCP {method} -> HTTP {response.status_code}: {response.text[:400]}")
        session_id = response.headers.get("mcp-session-id")
        if session_id:
            self._session_id = session_id
        payload = _parse_rpc_body(response.text)
        if "error" in payload:
            raise RuntimeError(f"MCP {method} error: {json.dumps(payload['error'])[:400]}")
        return payload.get("result") or {}

    def _initialize(self) -> None:
        self._rpc("initialize", {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "sdmx-eval-gateway", "version": "1.0"},
        })

    def list_tools(self) -> list[dict[str, Any]]:
        return list(self._rpc("tools/list").get("tools") or [])

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        return self._rpc("tools/call", {"name": name, "arguments": arguments})


def _parse_rpc_body(text: str) -> dict[str, Any]:
    """Read a JSON-RPC body that may arrive as JSON or as an SSE frame."""
    stripped = text.strip()
    if stripped.startswith("{"):
        return json.loads(stripped)
    for line in stripped.splitlines():
        if line.startswith("data:"):
            chunk = line[5:].strip()
            if chunk and chunk != "[DONE]":
                return json.loads(chunk)
    raise RuntimeError(f"Unreadable MCP response: {text[:300]}")


def mcp_tools_as_openai(tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Describe MCP tools in the OpenAI function-calling shape."""
    out: list[dict[str, Any]] = []
    for tool in tools:
        schema = tool.get("inputSchema") or tool.get("input_schema") or {
            "type": "object", "properties": {}
        }
        out.append({
            "type": "function",
            "function": {
                "name": str(tool.get("name")),
                "description": (tool.get("description") or "")[:1024],
                "parameters": schema,
            },
        })
    return out


# ------------------------------------------------------------------- the model


def _api_key(provider: dict[str, Any]) -> str:
    name = str(provider.get("api_key_env") or "AI_GATEWAY_API_KEY").strip()
    value = os.getenv(name, "").strip()
    if not value:
        raise SystemExit(
            f"Missing gateway API key in {name}. Create one in the Vercel "
            f"dashboard under AI Gateway, then: export {name}=..."
        )
    return value


def _extract_json_object(text: str) -> dict[str, Any] | None:
    stripped = (text or "").strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass
    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", stripped, re.DOTALL)
    for candidate in ([fenced.group(1)] if fenced else []):
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
    start, end = stripped.find("{"), stripped.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(stripped[start:end + 1])
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return None
    return None


_TERSE_VALUE = re.compile(r"value:\s*([^;\n]+)", re.IGNORECASE)
_TERSE_PERIOD = re.compile(r"period:\s*([^;\n]+)", re.IGNORECASE)


# Gateway statuses worth retrying: the request was never served, so reissuing
# it is not a second opinion. 429 is rate limiting, 5xx and 408 are the
# provider being briefly unavailable -- observed as a bare 503 from an
# open-weight provider mid-run, which failed that case and would have scored
# against the model.
_RETRYABLE_GATEWAY_STATUS = frozenset({408, 429, 500, 502, 503, 504, 529})


def _post_with_retry(
    client: httpx.Client,
    *,
    headers: dict[str, str],
    payload: dict[str, Any],
    retries: int,
    backoff: float,
) -> httpx.Response:
    """POST to the gateway, reissuing only transient failures.

    A 4xx that is not 429 is a real rejection -- an unknown model, a malformed
    request -- and retrying it just spends money to receive the same answer,
    so only the statuses above are reissued. Infrastructure noise must not land
    in a model's score: it is random across models and would silently bias a
    comparison.
    """
    last: httpx.Response | None = None
    for attempt in range(retries + 1):
        try:
            response = client.post(GATEWAY_URL, headers=headers, json=payload)
        except httpx.HTTPError:
            # A transport error never reached the model either.
            if attempt == retries:
                raise
            time.sleep(backoff * (2**attempt))
            continue
        if response.status_code not in _RETRYABLE_GATEWAY_STATUS or attempt == retries:
            return response
        last = response
        time.sleep(backoff * (2**attempt))
    assert last is not None
    return last


def _json_mode_for(json_mode: bool, tools: list[dict[str, Any]]) -> bool:
    """Whether to constrain the reply to a JSON object on this request.

    Never while tools are attached: a reply restricted to one JSON object
    cannot also carry tool_calls, and several models respond by abandoning
    tool use rather than by deferring the JSON. Since this eval always attaches
    tools, the setting is effectively inert -- kept only so a tools-free caller
    can still ask for structured output.
    """
    return bool(json_mode) and not tools


def _recover_terse_claims(answer_text: str, claims: dict[str, Any]) -> dict[str, Any]:
    """Read value and period out of the terse answer when `claims` omitted them.

    The contract asks for two things: the terse "value: <v>; period: <p>" line
    and the structured claims object. Some models supply only the line. The
    grader keys off claims, so a correct answer then grades as unsettled --
    measured at 93 of 500 for one model, every one of them exactly right.

    This reads back what the model already stated in the format it was asked
    for; it never substitutes a tool result or infers a value the model did not
    give. A claims value the model did supply is always left alone, and an
    explicit "value: null" is preserved as an abstention rather than being
    treated as a missing field to fill.
    """
    recovered = dict(claims)
    if recovered.get("value") is None:
        match = _TERSE_VALUE.search(answer_text or "")
        if match:
            token = match.group(1).strip().rstrip(".;")
            if token and token.lower() != "null":
                recovered["value"] = token
                recovered["recovered_from_answer_text"] = True
    if recovered.get("time_period") is None:
        match = _TERSE_PERIOD.search(answer_text or "")
        if match:
            token = match.group(1).strip().rstrip(".;")
            if token and token.lower() != "null":
                recovered["time_period"] = token
    return recovered


def _tool_result_text(result: dict[str, Any]) -> str:
    """Flatten an MCP tool result to the string the model will read."""
    parts: list[str] = []
    for item in result.get("content") or []:
        if isinstance(item, dict) and isinstance(item.get("text"), str):
            parts.append(item["text"])
    if parts:
        return "\n".join(parts)
    return json.dumps(result.get("structuredContent") or result)[:8000]


def _was_throttled(text: str) -> bool:
    """Whether an MCP result reported a retryable upstream failure.

    The server distinguishes a throttled registry from absent data. Models have
    been observed answering with a null value regardless, so the client retries
    before the answer is taken -- same reasoning as the Anthropic adapter.
    """
    try:
        payload = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return False
    if not isinstance(payload, dict):
        return False
    return payload.get("retryable") is True or payload.get("status") in {
        "rate_limited", "upstream_unavailable",
    }


def run_case(payload: dict[str, Any]) -> dict[str, Any]:
    provider = payload.get("provider") or {}
    mcp = payload.get("mcp") or {}
    case = payload.get("case") or {}

    url = str(mcp.get("url") or "").strip()
    if not url:
        raise SystemExit("mcp.url is required.")
    prompt = str(case.get("prompt") or "").strip()
    if not prompt:
        raise SystemExit("Case prompt is required.")

    model = str(os.getenv("SDMX_EVAL_MODEL", "").strip()
                or provider.get("model") or DEFAULT_MODEL).strip()
    max_tokens = int(provider.get("max_tokens") or DEFAULT_MAX_TOKENS)
    max_steps = int(provider.get("max_steps") or DEFAULT_MAX_STEPS)
    # response_format and tools conflict badly. Asking for a JSON object made
    # three of seven open-weight models stop calling tools altogether --
    # deepseek-v4.1-flash 0 calls against 4, glm-5.3-flash 0 against 17,
    # qwen-3-235b 0 against 5 -- because a reply constrained to one JSON object
    # leaves no room to emit tool_calls first. They answered "value: null"
    # immediately and would have scored near zero, which reads as a model that
    # cannot use the MCP rather than a harness that forbade it.
    #
    # It was added for gemini-2.5-flash-lite, which it did not rescue anyway
    # (that model invents dataflow names, a separate problem), so it is off by
    # default and ignored whenever tools are attached -- see _json_mode_for.
    json_mode = bool(provider.get("json_mode"))
    retries = int(provider.get("throttle_retries", 3))
    gateway_retries = int(provider.get("gateway_retries", 3))
    backoff = float(provider.get("throttle_backoff_seconds", 5.0))

    # Held identical to the Anthropic adapter's wording. A comparison run has
    # to differ in the model alone; a reworded contract would make a prompt
    # difference look like a model difference.
    system = str(provider.get("system_prompt") or DEFAULT_SYSTEM_PROMPT).strip()
    contract = (
        ' Return JSON with keys "answer_text" and "claims". '
        'Within "claims", include "value", "time_period", "flowRef", and "filters". '
        'Use null for any field you cannot determine confidently. '
        'For resolved single-value results, answer_text must be terse: "value: <value>; period: <period>". '
        'For no-data or unresolved results, answer_text must be terse: "value: null".'
    )

    client = httpx.Client(timeout=float(provider.get("timeout") or DEFAULT_TIMEOUT))
    headers = {
        "Authorization": f"Bearer {_api_key(provider)}",
        "Content-Type": "application/json",
    }

    tool_trace: list[dict[str, Any]] = []
    usage = {"input_tokens": 0, "output_tokens": 0}
    cost = 0.0

    with McpSession(url) as session:
        tools = mcp_tools_as_openai(session.list_tools())
        messages: list[dict[str, Any]] = [
            {"role": "system", "content": system + contract},
            {"role": "user", "content": prompt},
        ]

        final_text = ""
        truncated = False
        # A model can keep calling tools until the step ceiling and never write
        # an answer. That is a third harness limit, distinct from truncation
        # and from a malformed answer, and it needs its own name or it reads as
        # the model being wrong.
        steps_exhausted = True
        for _ in range(max_steps):
            request: dict[str, Any] = {
                "model": model,
                "max_tokens": max_tokens,
                "messages": messages,
                "tools": tools,
            }
            if _json_mode_for(json_mode, tools):
                request["response_format"] = {"type": "json_object"}
            response = _post_with_retry(
                client, headers=headers, payload=request,
                retries=gateway_retries, backoff=backoff,
            )
            if response.status_code >= 400:
                # The body says what was rejected -- an unknown model id, a
                # budget stop, a malformed tool block. Losing it makes every
                # failure look the same.
                raise RuntimeError(
                    f"Gateway {response.status_code} for model={model!r}: "
                    f"{response.text[:1200]}"
                )
            body = response.json()
            use = body.get("usage") or {}
            usage["input_tokens"] += int(use.get("prompt_tokens") or 0)
            usage["output_tokens"] += int(use.get("completion_tokens") or 0)
            cost += float((use.get("cost") or 0) or 0)

            choice = (body.get("choices") or [{}])[0]
            # "length" means the model ran out of budget mid-thought. That is a
            # harness setting, not a wrong answer, and conflating the two makes
            # a reasoning model look inaccurate when it was simply cut off.
            finish_reason = str(choice.get("finish_reason") or "")
            message = choice.get("message") or {}
            calls = message.get("tool_calls") or []
            messages.append({
                "role": "assistant",
                "content": message.get("content") or "",
                **({"tool_calls": calls} if calls else {}),
            })

            if not calls:
                final_text = message.get("content") or ""
                truncated = finish_reason == "length"
                steps_exhausted = False
                break

            for call in calls:
                fn = call.get("function") or {}
                name = str(fn.get("name") or "")
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except json.JSONDecodeError:
                    args = {}
                tool_trace.append({"type": "mcp_tool_use", "name": name, "input": args})

                text = ""
                for attempt in range(retries + 1):
                    try:
                        text = _tool_result_text(session.call_tool(name, args))
                    except Exception as exc:  # noqa: BLE001 -- report, don't abort
                        text = json.dumps({"status": "tool_error", "message": str(exc)[:300]})
                    if not _was_throttled(text) or attempt == retries:
                        break
                    time.sleep(backoff * (2**attempt))

                tool_trace.append({
                    "type": "mcp_tool_result",
                    "content": [{"type": "text", "text": text}],
                })
                messages.append({
                    "role": "tool",
                    "tool_call_id": call.get("id"),
                    "content": text[:16000],
                })

    client.close()
    parsed = _extract_json_object(final_text)
    claims = (parsed or {}).get("claims")
    if not isinstance(claims, dict):
        claims = {}

    answer_text = (parsed or {}).get("answer_text") or final_text
    claims = _recover_terse_claims(answer_text, claims)
    result = {
        "status": "ok" if parsed else "provider_error",
        "answer_text": answer_text,
        "claims": {
            "value": claims.get("value"),
            "time_period": claims.get("time_period"),
            "flowRef": claims.get("flowRef"),
            "filters": claims.get("filters") if isinstance(claims.get("filters"), dict) else None,
            # Set when value came from the terse answer line rather than the
            # model's own claims object, so a grade can be traced to its source.
            "recovered_from_answer_text": claims.get("recovered_from_answer_text", False),
        },
        "tool_trace": tool_trace,
        "usage": usage,
        "model": model,
        # The gateway reports real cost per request and adds no markup, so this
        # is measured rather than derived from a price table that drifts.
        "estimated_cost_usd": round(cost, 8) if cost else None,
    }
    if not parsed:
        if truncated:
            reason = (
                "Model hit the max_tokens ceiling before producing an answer; raise "
                f"provider.max_tokens (currently {max_tokens}). Reasoning models spend "
                "budget before emitting any text."
            )
        elif steps_exhausted:
            tool_calls = sum(1 for e in tool_trace if e.get("type") == "mcp_tool_use")
            reason = (
                f"Model made {tool_calls} tool calls and reached the step ceiling "
                f"(provider.max_steps = {max_steps}) without writing an answer."
            )
        else:
            reason = "Model response did not contain a parseable JSON object."
        result["error"] = reason
        result["truncated"] = truncated
        result["steps_exhausted"] = steps_exhausted
    return result


def main() -> int:
    payload = json.load(sys.stdin)
    if not isinstance(payload, dict):
        raise SystemExit("Provider payload must be a JSON object.")
    json.dump(run_case(payload), sys.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
