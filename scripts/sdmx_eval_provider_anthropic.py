#!/usr/bin/env python3
import json
import os
import re
import sys
from typing import Any

import httpx


API_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_MODEL = "claude-sonnet-4-20250514"
DEFAULT_MAX_TOKENS = 1200
DEFAULT_ANTHROPIC_VERSION = "2023-06-01"
DEFAULT_MCP_BETA = "mcp-client-2025-04-04"


def _read_payload() -> dict[str, Any]:
    payload = json.load(sys.stdin)
    if not isinstance(payload, dict):
        raise ValueError("Provider payload must be a JSON object.")
    return payload


def _provider_config(payload: dict[str, Any]) -> dict[str, Any]:
    provider = payload.get("provider")
    if not isinstance(provider, dict):
        raise ValueError("Payload must include a provider config object.")
    return provider


def _api_key(provider: dict[str, Any]) -> str:
    env_name = str(provider.get("api_key_env") or "ANTHROPIC_API_KEY").strip()
    value = os.getenv(env_name, "").strip()
    if not value:
        raise ValueError(f"Missing Anthropic API key in environment variable {env_name}.")
    return value


def _mcp_servers(payload: dict[str, Any], provider: dict[str, Any]) -> list[dict[str, Any]]:
    mcp = payload.get("mcp")
    if not isinstance(mcp, dict):
        raise ValueError("Payload must include an mcp config object.")
    url = str(mcp.get("url") or "").strip()
    if not url:
        raise ValueError("mcp.url is required for the Anthropic adapter.")

    server_def: dict[str, Any] = {
        "type": "url",
        "url": url,
        "name": str(mcp.get("server_label") or "sdmx-mcp").strip() or "sdmx-mcp",
        "tool_configuration": {"enabled": True},
    }

    auth_env = str(mcp.get("authorization_token_env") or provider.get("mcp_authorization_token_env") or "").strip()
    if auth_env:
        auth_token = os.getenv(auth_env, "").strip()
        if auth_token:
            server_def["authorization_token"] = auth_token

    return [server_def]


def _system_prompt(payload: dict[str, Any], provider: dict[str, Any]) -> str:
    base = str(
        provider.get("system_prompt")
        or (
            "You are evaluating an SDMX MCP integration. "
            "Use the MCP tools for every case, answer only from the MCP-returned SDMX data, "
            "and do not use training data, external facts, or estimates. "
            "For single-value questions, first resolve the official query with validate_query_scope or "
            "resolve_and_query_data, then answer only if the official MCP result resolves. "
            "Prefer resolve_and_query_data with resultShape='latest_single_value' when the flow and code filters are known. "
            "If the MCP reports unresolved_from_official_flows, no observations, or an ambiguous/non-single-value shape, abstain with a null value. "
            "Return exactly one JSON object with no markdown fences."
        )
    ).strip()
    contract = (
        ' Return JSON with keys "answer_text" and "claims". '
        'Within "claims", include "value", "time_period", "flowRef", and "filters". '
        'Use null for any field you cannot determine confidently. '
        'For resolved single-value results, answer_text must be terse: "value: <value>; period: <period>". '
        'For no-data or unresolved results, answer_text must be terse: "value: null".'
    )
    return base + contract


def _user_message(payload: dict[str, Any]) -> str:
    case = payload.get("case") or {}
    if not isinstance(case, dict):
        raise ValueError("Payload case must be a JSON object.")
    prompt = str(case.get("prompt") or "").strip()
    if not prompt:
        raise ValueError("Case prompt is required.")
    return prompt


def _extract_text(content: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            text = block.get("text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    return "\n".join(parts).strip()


def _extract_tool_trace(content: list[dict[str, Any]]) -> list[dict[str, Any]]:
    trace: list[dict[str, Any]] = []
    for block in content:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")
        if block_type in {"mcp_tool_use", "mcp_tool_result"}:
            trace.append(block)
    return trace


def _extract_json_object(text: str) -> dict[str, Any] | None:
    stripped = text.strip()
    if not stripped:
        return None
    try:
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            return parsed
    except json.JSONDecodeError:
        pass

    fenced = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if fenced:
        try:
            parsed = json.loads(fenced.group(1))
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(text[start : end + 1])
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return None
    return None


def _normalize_result(parsed: dict[str, Any], text: str, raw_response: dict[str, Any], tool_trace: list[dict[str, Any]]) -> dict[str, Any]:
    claims = parsed.get("claims")
    if not isinstance(claims, dict):
        claims = {}
    result = {
        "status": "ok",
        "answer_text": parsed.get("answer_text") if isinstance(parsed.get("answer_text"), str) else text,
        "claims": {
            "value": claims.get("value"),
            "time_period": claims.get("time_period"),
            "flowRef": claims.get("flowRef"),
            "filters": claims.get("filters") if isinstance(claims.get("filters"), dict) else None,
        },
        "tool_trace": tool_trace,
        "raw_response": raw_response,
    }
    return result


def main() -> None:
    payload = _read_payload()
    provider = _provider_config(payload)
    api_key = _api_key(provider)
    model = str(provider.get("model") or DEFAULT_MODEL).strip()
    max_tokens = int(provider.get("max_tokens") or DEFAULT_MAX_TOKENS)
    temperature = provider.get("temperature")
    anthropic_version = str(provider.get("anthropic_version") or DEFAULT_ANTHROPIC_VERSION).strip()
    mcp_beta = str(provider.get("anthropic_beta") or DEFAULT_MCP_BETA).strip()

    body: dict[str, Any] = {
        "model": model,
        "max_tokens": max_tokens,
        "system": _system_prompt(payload, provider),
        "messages": [{"role": "user", "content": _user_message(payload)}],
        "mcp_servers": _mcp_servers(payload, provider),
    }
    if temperature is not None:
        body["temperature"] = temperature

    headers = {
        "x-api-key": api_key,
        "anthropic-version": anthropic_version,
        "anthropic-beta": mcp_beta,
        "content-type": "application/json",
    }

    with httpx.Client(timeout=120.0) as client:
        response = client.post(API_URL, headers=headers, json=body)
    response.raise_for_status()
    raw = response.json()
    content = raw.get("content")
    if not isinstance(content, list):
        raise ValueError("Anthropic response missing content blocks.")

    text = _extract_text(content)
    parsed = _extract_json_object(text)
    tool_trace = _extract_tool_trace(content)

    if not parsed:
        result = {
            "status": "provider_error",
            "answer_text": text,
            "claims": {"value": None, "time_period": None, "flowRef": None, "filters": None},
            "tool_trace": tool_trace,
            "raw_response": raw,
            "error": "Anthropic response did not contain a parseable JSON object.",
        }
    else:
        result = _normalize_result(parsed, text, raw, tool_trace)

    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()
