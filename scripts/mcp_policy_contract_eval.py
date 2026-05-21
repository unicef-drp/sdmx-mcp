#!/usr/bin/env python3
"""Direct MCP contract checks for registry policy behavior.

This runner intentionally does not call an LLM. It validates the deployed MCP
contract that the paid agent eval depends on: strict vs permissive total policy,
visible applied defaults, explicit overrides, and stable explicit-query results.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx


@dataclass(slots=True)
class CheckResult:
    name: str
    status: str
    detail: str


def _load_config(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


async def _call_tool(client: httpx.AsyncClient, url: str, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    response = await client.post(
        url,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        json={
            "jsonrpc": "2.0",
            "id": name,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
    )
    response.raise_for_status()
    envelope = response.json()
    if envelope.get("error"):
        raise AssertionError(envelope["error"])
    result = envelope.get("result") if isinstance(envelope.get("result"), dict) else {}
    structured = result.get("structuredContent")
    if isinstance(structured, dict):
        return structured
    content = result.get("content")
    if isinstance(content, list) and content and isinstance(content[0], dict):
        text = content[0].get("text")
        if isinstance(text, str):
            return json.loads(text)
    raise AssertionError(f"Tool {name} did not return structured JSON.")


def _source(payload: dict[str, Any]) -> dict[str, Any]:
    source = payload.get("source")
    return source if isinstance(source, dict) else {}


def _series_value(payload: dict[str, Any], period: str) -> float | None:
    for item in payload.get("series") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("period")) != period:
            continue
        try:
            return float(str(item.get("value")))
        except (TypeError, ValueError):
            return None
    return None


def _assert_close(actual: float | None, expected: float, tolerance: float) -> None:
    if actual is None:
        raise AssertionError("expected numeric value, got null")
    if not math.isclose(actual, expected, abs_tol=tolerance, rel_tol=0.0):
        raise AssertionError(f"expected {expected} +/- {tolerance}, got {actual}")


async def _run_check(name: str, func: Any) -> CheckResult:
    try:
        detail = await func()
        return CheckResult(name=name, status="pass", detail=str(detail or "ok"))
    except Exception as exc:  # noqa: BLE001 - contract output should capture any failure.
        return CheckResult(name=name, status="fail", detail=str(exc))


async def run_contract(config: dict[str, Any]) -> dict[str, Any]:
    strict_url = str(config.get("strict_url") or "").strip()
    permissive_url = str(config.get("permissive_url") or "").strip()
    if not strict_url:
        raise ValueError("strict_url is required.")

    cme = config.get("cme") if isinstance(config.get("cme"), dict) else {}
    flow_ref = str(cme.get("flowRef") or "UNICEF/CME/1.0")
    base_filters = cme.get("baseFilters") if isinstance(cme.get("baseFilters"), dict) else {
        "REF_AREA": "UNICEF_ESA",
        "INDICATOR": "CME_MRY0T4",
    }
    total_filters = cme.get("totalFilters") if isinstance(cme.get("totalFilters"), dict) else {
        **base_filters,
        "SEX": "_T",
        "WEALTH_QUINTILE": "_T",
    }
    female_filters = cme.get("femaleFilters") if isinstance(cme.get("femaleFilters"), dict) else {
        **base_filters,
        "SEX": "F",
    }
    time_range = str(cme.get("time") or "1990:2024")
    expected_period = str(cme.get("expectedPeriod") or "2024")
    expected_value = float(cme.get("expectedValue") or 50.2)
    tolerance = float(cme.get("tolerance") or 0.5)

    timeout = float(config.get("timeout_seconds") or 90)
    results: list[CheckResult] = []
    explicit_strict: dict[str, Any] | None = None
    explicit_permissive: dict[str, Any] | None = None

    async with httpx.AsyncClient(timeout=timeout) as client:

        async def strict_omitted_totals() -> str:
            payload = await _call_tool(
                client,
                strict_url,
                "get_time_series",
                {"flowRef": flow_ref, "filters": base_filters, "time": time_range, "maxObservations": 100},
            )
            if payload.get("status") != "not_a_single_series":
                raise AssertionError(f"expected not_a_single_series, got {payload.get('status')}")
            if _source(payload).get("appliedDefaults"):
                raise AssertionError(f"strict path applied defaults: {_source(payload).get('appliedDefaults')}")
            return "strict wildcard query rejected as multi-series"

        async def strict_explicit_totals() -> str:
            nonlocal explicit_strict
            payload = await _call_tool(
                client,
                strict_url,
                "get_time_series",
                {"flowRef": flow_ref, "filters": total_filters, "time": time_range, "maxObservations": 100},
            )
            if payload.get("status") != "resolved":
                raise AssertionError(f"expected resolved, got {payload.get('status')}: {payload.get('message')}")
            if int(payload.get("observationCount") or 0) < 30:
                raise AssertionError(f"expected at least 30 observations, got {payload.get('observationCount')}")
            _assert_close(_series_value(payload, expected_period), expected_value, tolerance)
            if _source(payload).get("appliedDefaults"):
                raise AssertionError(f"explicit strict query applied defaults: {_source(payload).get('appliedDefaults')}")
            explicit_strict = payload
            return f"explicit total query resolved for {expected_period}"

        results.append(await _run_check("strict_omitted_totals_rejected", strict_omitted_totals))
        results.append(await _run_check("strict_explicit_totals_resolved", strict_explicit_totals))

        if permissive_url:

            async def permissive_omitted_totals() -> str:
                payload = await _call_tool(
                    client,
                    permissive_url,
                    "get_time_series",
                    {"flowRef": flow_ref, "filters": base_filters, "time": time_range, "maxObservations": 100},
                )
                if payload.get("status") != "resolved":
                    raise AssertionError(f"expected resolved, got {payload.get('status')}: {payload.get('message')}")
                applied = _source(payload).get("appliedDefaults") or {}
                expected = {"SEX": "_T", "WEALTH_QUINTILE": "_T"}
                if applied != expected:
                    raise AssertionError(f"expected appliedDefaults={expected}, got {applied}")
                _assert_close(_series_value(payload, expected_period), expected_value, tolerance)
                return "permissive wildcard query auto-applied totals"

            async def permissive_explicit_override() -> str:
                payload = await _call_tool(
                    client,
                    permissive_url,
                    "get_time_series",
                    {"flowRef": flow_ref, "filters": female_filters, "time": time_range, "maxObservations": 100},
                )
                if payload.get("status") != "resolved":
                    raise AssertionError(f"expected resolved, got {payload.get('status')}: {payload.get('message')}")
                source = _source(payload)
                filters = source.get("filters") if isinstance(source.get("filters"), dict) else {}
                if filters.get("SEX") != "F":
                    raise AssertionError(f"explicit SEX filter was not preserved: {filters}")
                applied = source.get("appliedDefaults") or {}
                if applied.get("SEX") == "_T":
                    raise AssertionError(f"explicit SEX filter was overwritten by defaults: {applied}")
                return "explicit SEX=F preserved"

            async def permissive_explicit_totals_match_strict() -> str:
                nonlocal explicit_permissive
                payload = await _call_tool(
                    client,
                    permissive_url,
                    "get_time_series",
                    {"flowRef": flow_ref, "filters": total_filters, "time": time_range, "maxObservations": 100},
                )
                if payload.get("status") != "resolved":
                    raise AssertionError(f"expected resolved, got {payload.get('status')}: {payload.get('message')}")
                if _source(payload).get("appliedDefaults"):
                    raise AssertionError(f"explicit permissive query applied defaults: {_source(payload).get('appliedDefaults')}")
                explicit_permissive = payload
                strict_series = explicit_strict.get("series") if isinstance(explicit_strict, dict) else None
                if payload.get("series") != strict_series:
                    raise AssertionError("explicit strict and permissive series differ")
                return "explicit query matches strict result"

            results.append(await _run_check("permissive_omitted_totals_resolved", permissive_omitted_totals))
            results.append(await _run_check("permissive_explicit_filter_overrides_default", permissive_explicit_override))
            results.append(await _run_check("explicit_query_matches_between_modes", permissive_explicit_totals_match_strict))

            nutrition = config.get("nutrition_never_apply")
            if isinstance(nutrition, dict) and nutrition.get("flowRef") and nutrition.get("filters"):

                async def permissive_never_apply_age() -> str:
                    payload = await _call_tool(
                        client,
                        permissive_url,
                        "plan_query",
                        {
                            "flowRef": nutrition["flowRef"],
                            "filters": nutrition["filters"],
                            "resultShape": "compact_series",
                            "allowUnboundedTime": True,
                        },
                    )
                    applied = payload.get("appliedDefaults") if isinstance(payload.get("appliedDefaults"), dict) else {}
                    wildcards = payload.get("wildcardDimensions") if isinstance(payload.get("wildcardDimensions"), list) else []
                    if applied.get("AGE") == "_T":
                        raise AssertionError(f"AGE was auto-applied despite never_apply: {applied}")
                    if "AGE" not in wildcards:
                        raise AssertionError(f"expected AGE to remain wildcard, got {wildcards}")
                    return "AGE remains wildcard"

                results.append(await _run_check("permissive_never_apply_age", permissive_never_apply_age))

    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    return {
        "status": "pass" if counts.get("fail", 0) == 0 else "fail",
        "counts": counts,
        "results": [
            {"name": result.name, "status": result.status, "detail": result.detail}
            for result in results
        ],
    }


async def _main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    summary = await run_contract(_load_config(args.config))
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if summary["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
