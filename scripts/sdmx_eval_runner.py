#!/usr/bin/env python3
import argparse
import asyncio
import csv
import io
import itertools
import json
import math
import os
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import quote
from xml.etree import ElementTree as ET

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import server


DEFAULT_OUTPUT_DIR = ROOT / "tmp" / "sdmx_eval"
DEFAULT_MANIFEST_PATH = DEFAULT_OUTPUT_DIR / "cases.jsonl"
DEFAULT_RESPONSES_PATH = DEFAULT_OUTPUT_DIR / "responses.jsonl"
DEFAULT_GRADES_PATH = DEFAULT_OUTPUT_DIR / "grades.jsonl"


@dataclass(frozen=True)
class DimensionValue:
    id: str
    name: str


def _load_json_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Config at {path} must decode to a JSON object.")
    return payload


def _tag_name(element: ET.Element) -> str:
    return element.tag.split("}")[-1]


def _element_text(node: ET.Element, tag_name: str) -> str:
    for elem in node.iter():
        if _tag_name(elem) == tag_name and elem.text:
            text = elem.text.strip()
            if text:
                return text
    return ""


def _config_base_url(config: dict[str, Any]) -> str:
    registry = config.get("registry") or {}
    if isinstance(registry, dict):
        base = registry.get("base_url")
        if isinstance(base, str) and base.strip():
            return base.strip().rstrip("/")
    return server.BASE


def _config_user_agent(config: dict[str, Any]) -> str:
    registry = config.get("registry") or {}
    if isinstance(registry, dict):
        user_agent = registry.get("user_agent")
        if isinstance(user_agent, str) and user_agent.strip():
            return user_agent.strip()
    return "sdmx-eval-runner/0.1"


def _apply_registry_overrides(config: dict[str, Any]) -> None:
    base_url = _config_base_url(config)
    user_agent = _config_user_agent(config)
    if server.BASE != base_url:
        server.BASE = base_url
        server._dataflow_cache.clear()
        server._structure_cache.clear()
        server._dimension_cache.clear()
        server._hierarchical_codelist_cache.clear()
        server._hierarchical_catalog_cache.clear()
    server.USER_AGENT = user_agent


def _time_dimension_id(config: dict[str, Any]) -> str:
    registry = config.get("registry") or {}
    if isinstance(registry, dict):
        dim_id = registry.get("time_dimension_id")
        if isinstance(dim_id, str) and dim_id.strip():
            return dim_id.strip().upper()
    return "TIME_PERIOD"


def _output_path(value: Path | None, default_path: Path) -> Path:
    return value or default_path


def _flow_ref_parts(flow_ref: str) -> tuple[str, str, str]:
    agency, flow_id, version = server._flow_identifiers(flow_ref)
    return agency, flow_id, version


def _quoted_flow_path(flow_ref: str) -> str:
    agency, flow_id, version = _flow_ref_parts(flow_ref)
    return f"{quote(agency)},{quote(flow_id)},{quote(version)}"


def _build_key(dimension_order: list[str], filters: dict[str, str], wildcard_dimensions: set[str]) -> str:
    normalized = {str(key).upper(): str(value) for key, value in filters.items()}
    parts: list[str] = []
    for dimension in dimension_order:
        if dimension in wildcard_dimensions:
            parts.append("")
            continue
        parts.append(normalized.get(dimension, ""))
    return ".".join(parts)


def _cartesian_product(entries: list[tuple[str, list[DimensionValue]]]) -> list[dict[str, DimensionValue]]:
    if not entries:
        return [{}]
    keys = [item[0] for item in entries]
    values = [item[1] for item in entries]
    product = []
    for combo in itertools.product(*values):
        product.append({key: value for key, value in zip(keys, combo)})
    return product


def _jsonl_read(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _jsonl_case_ids(path: Path) -> set[str]:
    return {
        str(item["case_id"])
        for item in _jsonl_read(path)
        if isinstance(item.get("case_id"), str) and item.get("case_id")
    }


def _csv_rows(text: str) -> list[dict[str, Any]]:
    if not text.strip():
        return []
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict[str, Any]] = []
    for row in reader:
        if isinstance(row, dict):
            rows.append({str(key): value for key, value in row.items() if key is not None})
    return rows


def _dimension_specs(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    raw = config.get("dimensions")
    if not isinstance(raw, list) or not raw:
        raise ValueError("Config must include a non-empty 'dimensions' list.")
    specs: dict[str, dict[str, Any]] = {}
    for item in raw:
        if not isinstance(item, dict):
            continue
        dim_id = item.get("id")
        if not isinstance(dim_id, str) or not dim_id.strip():
            raise ValueError("Each dimension entry needs a non-empty 'id'.")
        specs[dim_id.strip().upper()] = item
    return specs


def _wildcard_dimensions(config: dict[str, Any]) -> set[str]:
    raw = config.get("wildcard_dimensions") or []
    if not isinstance(raw, list):
        return set()
    return {str(item).strip().upper() for item in raw if str(item).strip()}


def _prompt_context(
    *,
    case: dict[str, Any],
    flow_name: str,
    flow_id: str,
    flow_ref: str,
    values: dict[str, DimensionValue],
    year: str,
) -> dict[str, str]:
    context: dict[str, str] = {
        "case_id": str(case["case_id"]),
        "flow_ref": flow_ref,
        "flow_id": flow_id,
        "flow_name": flow_name,
        "TIME_PERIOD": year,
        "time_period": year,
        "year": year,
    }
    for dim_id, value in values.items():
        context[dim_id] = value.id
        context[dim_id.lower()] = value.id
        context[f"{dim_id}_id"] = value.id
        context[f"{dim_id.lower()}_id"] = value.id
        context[f"{dim_id}_name"] = value.name
        context[f"{dim_id.lower()}_name"] = value.name
    return context


def _render_prompt(template: str, context: dict[str, str]) -> str:
    try:
        return template.format(**context)
    except KeyError as exc:
        missing = str(exc).strip("'")
        raise ValueError(f"Prompt template references missing placeholder '{missing}'.") from exc


def _infer_value_column(rows: list[dict[str, Any]], hint: str | None = None) -> str | None:
    if not rows:
        return None
    candidate_keys = list(rows[0].keys())
    if hint:
        for key in candidate_keys:
            if key == hint:
                return key
        for key in candidate_keys:
            if hint.upper() in key.upper():
                return key
    for preferred in ("OBS_VALUE", "OBS_VALUE:Observation Value"):
        for key in candidate_keys:
            if key == preferred:
                return key
    for key in candidate_keys:
        if "OBS_VALUE" in key.upper():
            return key
    return None


def _expected_value(rows: list[dict[str, Any]], value_column: str | None) -> dict[str, Any]:
    if not rows:
        return {"status": "no_data", "value": None}
    if not value_column:
        return {"status": "no_value_column", "value": None}
    values = [str(row.get(value_column, "")).strip() for row in rows]
    non_empty = [value for value in values if value]
    if not non_empty:
        return {"status": "empty_values", "value": None}
    unique = sorted(set(non_empty))
    if len(rows) == 1 or len(unique) == 1:
        return {"status": "deterministic", "value": unique[0]}
    return {"status": "multi_row", "value": None, "distinct_values": unique}


def _normalize_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def _decimal_match(expected: Any, actual: Any, tolerance: float) -> bool | None:
    expected_decimal = _normalize_decimal(expected)
    actual_decimal = _normalize_decimal(actual)
    if expected_decimal is None or actual_decimal is None:
        return None
    return math.isclose(float(expected_decimal), float(actual_decimal), abs_tol=tolerance, rel_tol=0.0)


async def _fetch_text(client: httpx.AsyncClient, url: str, user_agent: str) -> str:
    response = await client.get(url, headers={"User-Agent": user_agent})
    response.raise_for_status()
    return response.text


async def _fetch_codelist(client: httpx.AsyncClient, url: str, user_agent: str) -> dict[str, DimensionValue]:
    text = await _fetch_text(client, url, user_agent)
    root = ET.fromstring(text)
    codes: dict[str, DimensionValue] = {}
    for elem in root.iter():
        if _tag_name(elem) != "Code":
            continue
        code_id = (elem.attrib.get("id") or elem.attrib.get("ID") or "").strip()
        if not code_id:
            continue
        codes[code_id] = DimensionValue(id=code_id, name=_element_text(elem, "Name"))
    return codes


def _flow_name(flow_ref: str, payload: dict[str, Any]) -> str:
    flow_id = _flow_ref_parts(flow_ref)[1]
    for flow in server._extract_dataflows(payload):
        current_id = flow.get("id") or flow.get("ID")
        if isinstance(current_id, str) and current_id == flow_id:
            return server._coerce_text(flow.get("name")) or server._coerce_text(flow.get("names"))
    return ""


async def _resolve_dimension_values(
    client: httpx.AsyncClient,
    flow_ref: str,
    payload: dict[str, Any],
    dimension_id: str,
    spec: dict[str, Any],
    user_agent: str,
) -> list[DimensionValue]:
    mode = str(spec.get("mode") or "").strip().lower()
    flow_code_map = server._dimension_code_map(payload, dimension_id)
    if mode == "fixed":
        raw_values = spec.get("values")
        if not isinstance(raw_values, list) or not raw_values:
            raise ValueError(f"Dimension {dimension_id} with mode=fixed needs a non-empty 'values' list.")
        values: list[DimensionValue] = []
        for raw in raw_values:
            code_id = str(raw).strip()
            if not code_id:
                continue
            code = flow_code_map.get(code_id)
            name = server._code_name(code) if code else ""
            values.append(DimensionValue(id=code_id, name=name))
        return values

    if mode == "flow_dimension":
        values = []
        for code_id, code in sorted(flow_code_map.items()):
            values.append(DimensionValue(id=code_id, name=server._code_name(code)))
        max_values = spec.get("max_values")
        if isinstance(max_values, int) and max_values > 0:
            return values[:max_values]
        return values

    if mode == "external_codelist_intersection":
        codelist_url = spec.get("codelist_url")
        if not isinstance(codelist_url, str) or not codelist_url.strip():
            raise ValueError(f"Dimension {dimension_id} with external_codelist_intersection needs 'codelist_url'.")
        external_codes = await _fetch_codelist(client, codelist_url.strip(), user_agent)
        values = []
        for code_id in sorted(code_id for code_id in flow_code_map if code_id in external_codes):
            values.append(external_codes[code_id])
        max_values = spec.get("max_values")
        if isinstance(max_values, int) and max_values > 0:
            return values[:max_values]
        return values

    raise ValueError(
        f"Unsupported mode '{mode}' for dimension {dimension_id}. "
        "Supported modes: fixed, flow_dimension, external_codelist_intersection."
    )


def _resolve_time_values(spec: dict[str, Any]) -> list[str]:
    mode = str(spec.get("mode") or "").strip().lower()
    if mode != "time_range":
        raise ValueError("Time dimension must use mode=time_range.")
    start = spec.get("start")
    end = spec.get("end")
    if start is None or end is None:
        raise ValueError("time_range dimension needs both 'start' and 'end'.")
    start_year = int(str(start))
    end_year = int(str(end))
    if end_year < start_year:
        raise ValueError("time_range end must be >= start.")
    step = int(spec.get("step") or 1)
    return [str(year) for year in range(start_year, end_year + 1, step)]


async def _direct_query_case(
    client: httpx.AsyncClient,
    *,
    flow_ref: str,
    dimension_order: list[str],
    filters: dict[str, str],
    year: str,
    base_url: str,
    user_agent: str,
    wildcard_dimensions: set[str],
    value_column_hint: str | None,
) -> dict[str, Any]:
    key = _build_key(dimension_order, filters, wildcard_dimensions)
    query_url = (
        f"{base_url}/data/{_quoted_flow_path(flow_ref)}/{quote(key, safe='+.')}?"
        f"startPeriod={quote(year)}&endPeriod={quote(year)}&format=csv"
    )
    response = await client.get(query_url, headers={"User-Agent": user_agent})
    text = response.text
    if response.status_code >= 400:
        return {
            "status": "http_error",
            "query_url": query_url,
            "http_status": response.status_code,
            "error": server._parse_sdmx_error(text) or text,
        }
    rows = _csv_rows(text)
    value_column = _infer_value_column(rows, value_column_hint)
    expected = _expected_value(rows, value_column)
    return {
        "status": "resolved",
        "query_url": query_url,
        "http_status": response.status_code,
        "row_count": len(rows),
        "value_column": value_column,
        "expected": expected,
        "rows": rows,
    }


async def build_cases(config: dict[str, Any], manifest_path: Path, case_limit: int | None = None) -> dict[str, Any]:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    _apply_registry_overrides(config)
    dataflows = config.get("dataflows")
    if not isinstance(dataflows, list) or not dataflows:
        raise ValueError("Config must include a non-empty 'dataflows' list.")
    specs = _dimension_specs(config)
    time_dimension_id = _time_dimension_id(config)
    if time_dimension_id not in specs:
        raise ValueError(f"Config dimensions must include the time dimension '{time_dimension_id}'.")
    time_values = _resolve_time_values(specs[time_dimension_id])
    wildcard_dimensions = _wildcard_dimensions(config)
    prompt_template = str(config.get("prompt_template") or "").strip()
    if not prompt_template:
        raise ValueError("Config must include a non-empty 'prompt_template'.")
    value_column_hint = None
    expected_value = config.get("expected_value") or {}
    if isinstance(expected_value, dict):
        hint = expected_value.get("column_hint")
        if isinstance(hint, str) and hint.strip():
            value_column_hint = hint.strip()

    base_url = _config_base_url(config)
    user_agent = _config_user_agent(config)
    concurrency = max(1, int(config.get("concurrency") or 8))

    total_cases = 0
    written_cases = 0
    async with httpx.AsyncClient(timeout=90.0) as client:
        with manifest_path.open("w", encoding="utf-8") as handle:
            semaphore = asyncio.Semaphore(concurrency)

            for flow_ref in dataflows:
                if not isinstance(flow_ref, str) or not flow_ref.strip():
                    continue
                normalized_flow_ref = flow_ref.strip()
                payload = await server.get_flow_structure(normalized_flow_ref)
                dimension_order = await server._dimension_order_for_flow(normalized_flow_ref)
                flow_name = _flow_name(normalized_flow_ref, payload)
                flow_id = _flow_ref_parts(normalized_flow_ref)[1]

                varying_dimensions: list[tuple[str, list[DimensionValue]]] = []
                missing_dimensions: list[str] = []
                for dimension_id in dimension_order:
                    if dimension_id == time_dimension_id or dimension_id in wildcard_dimensions:
                        continue
                    spec = specs.get(dimension_id)
                    if not spec:
                        missing_dimensions.append(dimension_id)
                        continue
                    values = await _resolve_dimension_values(
                        client,
                        normalized_flow_ref,
                        payload,
                        dimension_id,
                        spec,
                        user_agent,
                    )
                    if not values:
                        missing_dimensions.append(dimension_id)
                        continue
                    varying_dimensions.append((dimension_id, values))

                if missing_dimensions:
                    raise ValueError(
                        f"Flow {normalized_flow_ref} has dimensions without config values: {', '.join(missing_dimensions)}"
                    )

                combos = _cartesian_product(varying_dimensions)
                tasks = []
                cases: list[dict[str, Any]] = []
                for selected in combos:
                    for year in time_values:
                        filters = {dimension_id: value.id for dimension_id, value in selected.items()}
                        case_id = f"{normalized_flow_ref}|{year}|{json.dumps(filters, sort_keys=True)}"
                        case = {
                            "case_id": case_id,
                            "flowRef": normalized_flow_ref,
                            "flowID": flow_id,
                            "flowName": flow_name,
                            "dimensionOrder": dimension_order,
                            "filters": filters,
                            "timePeriod": year,
                            "wildcardDimensions": sorted(wildcard_dimensions),
                            "dimensions": {
                                dimension_id: {"id": value.id, "name": value.name}
                                for dimension_id, value in selected.items()
                            },
                        }
                        context = _prompt_context(
                            case=case,
                            flow_name=flow_name,
                            flow_id=flow_id,
                            flow_ref=normalized_flow_ref,
                            values=selected,
                            year=year,
                        )
                        case["prompt"] = _render_prompt(prompt_template, context)
                        cases.append(case)
                        total_cases += 1
                        tasks.append(
                            asyncio.create_task(
                                _bounded_direct_query(
                                    semaphore,
                                    client,
                                    flow_ref=normalized_flow_ref,
                                    dimension_order=dimension_order,
                                    filters=filters,
                                    year=year,
                                    base_url=base_url,
                                    user_agent=user_agent,
                                    wildcard_dimensions=wildcard_dimensions,
                                    value_column_hint=value_column_hint,
                                )
                            )
                        )
                        if case_limit is not None and total_cases >= case_limit:
                            break
                    if case_limit is not None and total_cases >= case_limit:
                        break
                for case, task in zip(cases, tasks):
                    case["ground_truth"] = await task
                    handle.write(json.dumps(case, ensure_ascii=True) + "\n")
                    written_cases += 1
                if case_limit is not None and total_cases >= case_limit:
                    break

    return {"cases_written": written_cases, "manifest_path": str(manifest_path)}


async def _bounded_direct_query(
    semaphore: asyncio.Semaphore,
    client: httpx.AsyncClient,
    **kwargs: Any,
) -> dict[str, Any]:
    async with semaphore:
        return await _direct_query_case(client, **kwargs)


async def _run_command_provider(
    *,
    command: list[str],
    env: dict[str, str],
    payload: dict[str, Any],
) -> dict[str, Any]:
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )
    stdout, stderr = await process.communicate(json.dumps(payload).encode("utf-8"))
    stdout_text = stdout.decode("utf-8").strip()
    stderr_text = stderr.decode("utf-8").strip()
    if process.returncode != 0:
        return {
            "status": "provider_error",
            "provider_exit_code": process.returncode,
            "stderr": stderr_text,
            "stdout": stdout_text,
        }
    if not stdout_text:
        return {"status": "provider_error", "stderr": stderr_text, "stdout": stdout_text, "error": "Empty provider output."}
    try:
        parsed = json.loads(stdout_text)
    except json.JSONDecodeError as exc:
        return {
            "status": "provider_error",
            "stderr": stderr_text,
            "stdout": stdout_text,
            "error": f"Provider output was not valid JSON: {exc}",
        }
    if not isinstance(parsed, dict):
        return {
            "status": "provider_error",
            "stderr": stderr_text,
            "stdout": stdout_text,
            "error": "Provider output must be a JSON object.",
        }
    parsed.setdefault("status", "ok")
    if stderr_text and "stderr" not in parsed:
        parsed["stderr"] = stderr_text
    return parsed


async def run_provider(
    config: dict[str, Any],
    manifest_path: Path,
    responses_path: Path,
    case_limit: int | None = None,
) -> dict[str, Any]:
    _apply_registry_overrides(config)
    provider = config.get("provider")
    if not isinstance(provider, dict):
        raise ValueError("Config must include a 'provider' object for run-provider.")
    provider_type = str(provider.get("type") or "").strip().lower()
    if provider_type != "command":
        raise ValueError("Only provider.type=command is currently supported.")
    command = provider.get("command")
    if not isinstance(command, list) or not command or not all(isinstance(item, str) and item for item in command):
        raise ValueError("provider.command must be a non-empty string list.")
    provider_name = str(provider.get("name") or "command-provider").strip()
    base_env = os.environ.copy()
    extra_env = provider.get("env") or {}
    if isinstance(extra_env, dict):
        for key, value in extra_env.items():
            if isinstance(key, str) and isinstance(value, str):
                base_env[key] = value

    manifest_rows = _jsonl_read(manifest_path)
    completed = _jsonl_case_ids(responses_path)
    pending = [row for row in manifest_rows if row.get("case_id") not in completed]
    if case_limit is not None:
        pending = pending[:case_limit]

    responses_path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with responses_path.open("a", encoding="utf-8") as handle:
        for case in pending:
            payload = {
                "provider_name": provider_name,
                "provider": provider,
                "registry": config.get("registry") or {},
                "mcp": config.get("mcp") or {},
                "case": case,
                "contract": {
                    "required_response_fields": ["answer_text", "claims"],
                    "claims_shape": {
                        "value": "string | number | null",
                        "time_period": "string | null",
                        "flowRef": "string | null",
                        "filters": "object | null",
                    },
                },
            }
            provider_output = await _run_command_provider(command=command, env=base_env, payload=payload)
            result = {
                "case_id": case["case_id"],
                "provider_name": provider_name,
                "status": provider_output.get("status", "ok"),
                "prompt": case.get("prompt"),
                "provider_output": provider_output,
            }
            handle.write(json.dumps(result, ensure_ascii=True) + "\n")
            written += 1
    return {
        "provider_name": provider_name,
        "cases_written": written,
        "responses_path": str(responses_path),
    }


def grade_results(
    manifest_path: Path,
    responses_path: Path,
    grades_path: Path,
    numeric_tolerance: float,
) -> dict[str, Any]:
    manifest_index = {
        str(item["case_id"]): item
        for item in _jsonl_read(manifest_path)
        if isinstance(item.get("case_id"), str)
    }
    responses = _jsonl_read(responses_path)
    grades_path.parent.mkdir(parents=True, exist_ok=True)

    passed = 0
    failed = 0
    manual_review = 0
    with grades_path.open("w", encoding="utf-8") as handle:
        for response in responses:
            case_id = str(response.get("case_id") or "")
            case = manifest_index.get(case_id)
            if not case:
                continue
            provider_output = response.get("provider_output") or {}
            claims = provider_output.get("claims") if isinstance(provider_output, dict) else {}
            if not isinstance(claims, dict):
                claims = {}
            expected_truth = case.get("ground_truth") or {}
            expected = expected_truth.get("expected") if isinstance(expected_truth, dict) else {}
            expected_value = expected.get("value") if isinstance(expected, dict) else None

            value_match = _decimal_match(expected_value, claims.get("value"), numeric_tolerance)
            if value_match is None and expected_value is not None:
                actual_value = claims.get("value")
                if actual_value is not None:
                    value_match = str(expected_value).strip() == str(actual_value).strip()

            time_match = None
            if claims.get("time_period") is not None:
                time_match = str(claims.get("time_period")).strip() == str(case.get("timePeriod")).strip()

            flow_match = None
            if claims.get("flowRef") is not None:
                flow_match = str(claims.get("flowRef")).strip() == str(case.get("flowRef")).strip()

            filter_matches: dict[str, bool] = {}
            claim_filters = claims.get("filters")
            if isinstance(claim_filters, dict):
                for key, expected_filter in dict(case.get("filters") or {}).items():
                    filter_matches[str(key)] = str(claim_filters.get(key, "")).strip() == str(expected_filter).strip()

            expected_status = expected.get("status") if isinstance(expected, dict) else None
            if response.get("status") != "ok":
                overall = "manual_review"
            elif expected_status != "deterministic":
                overall = "manual_review"
            elif value_match is True and (time_match in (True, None)) and (flow_match in (True, None)) and all(filter_matches.values()):
                overall = "pass"
            elif value_match is False or time_match is False or flow_match is False or any(not item for item in filter_matches.values()):
                overall = "fail"
            else:
                overall = "manual_review"

            if overall == "pass":
                passed += 1
            elif overall == "fail":
                failed += 1
            else:
                manual_review += 1

            grade = {
                "case_id": case_id,
                "provider_name": response.get("provider_name"),
                "overall": overall,
                "checks": {
                    "value_match": value_match,
                    "time_match": time_match,
                    "flow_match": flow_match,
                    "filter_matches": filter_matches,
                },
                "expected": {
                    "flowRef": case.get("flowRef"),
                    "filters": case.get("filters"),
                    "timePeriod": case.get("timePeriod"),
                    "expected_value": expected_value,
                    "expected_status": expected_status,
                    "query_url": expected_truth.get("query_url") if isinstance(expected_truth, dict) else None,
                },
                "actual": {
                    "answer_text": provider_output.get("answer_text") if isinstance(provider_output, dict) else None,
                    "claims": claims,
                },
            }
            handle.write(json.dumps(grade, ensure_ascii=True) + "\n")

    return {
        "passed": passed,
        "failed": failed,
        "manual_review": manual_review,
        "grades_path": str(grades_path),
    }


async def _async_main(args: argparse.Namespace) -> None:
    config = _load_json_file(args.config)
    manifest_path = _output_path(args.manifest, DEFAULT_MANIFEST_PATH)
    responses_path = _output_path(args.responses, DEFAULT_RESPONSES_PATH)
    grades_path = _output_path(args.grades, DEFAULT_GRADES_PATH)

    if args.command == "build-cases":
        result = await build_cases(config, manifest_path, case_limit=args.case_limit)
        print(json.dumps(result, indent=2))
        return

    if args.command == "run-provider":
        result = await run_provider(config, manifest_path, responses_path, case_limit=args.case_limit)
        print(json.dumps(result, indent=2))
        return

    if args.command == "grade-results":
        result = grade_results(manifest_path, responses_path, grades_path, numeric_tolerance=args.numeric_tolerance)
        print(json.dumps(result, indent=2))
        return

    raise ValueError(f"Unsupported command: {args.command}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generic SDMX eval harness for case generation, provider runs, and grading.")
    parser.add_argument("command", choices=["build-cases", "run-provider", "grade-results"])
    parser.add_argument("--config", type=Path, required=True, help="Path to the eval config JSON file.")
    parser.add_argument("--manifest", type=Path, default=None, help="Manifest JSONL path.")
    parser.add_argument("--responses", type=Path, default=None, help="Provider responses JSONL path.")
    parser.add_argument("--grades", type=Path, default=None, help="Grades JSONL path.")
    parser.add_argument("--case-limit", type=int, default=None, help="Optional cap on processed cases.")
    parser.add_argument(
        "--numeric-tolerance",
        type=float,
        default=1e-9,
        help="Absolute tolerance for numeric value grading.",
    )
    args = parser.parse_args()
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
