#!/usr/bin/env python3
import argparse
import asyncio
import csv
import io
import json
import sys
from pathlib import Path
from typing import Any
from urllib.parse import quote
from xml.etree import ElementTree as ET

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import server


INDICATOR_CODELIST_URL = f"{server.BASE}/codelist/UNICEF/CL_UNICEF_INDICATOR/1.0"
COUNTRY_CODELIST_URL = f"{server.BASE}/codelist/UNICEF/CL_COUNTRY/1.0"
DEFAULT_OUTPUT_DIR = ROOT / "tmp" / "agent_test_rig"
DEFAULT_MANIFEST_PATH = DEFAULT_OUTPUT_DIR / "unicef_agent_cases.jsonl"
DEFAULT_RESULTS_PATH = DEFAULT_OUTPUT_DIR / "unicef_agent_results.jsonl"


def _tag_name(element: ET.Element) -> str:
    return element.tag.split("}")[-1]


def _element_text(node: ET.Element, tag_name: str) -> str:
    for elem in node.iter():
        if _tag_name(elem) == tag_name and elem.text:
            text = elem.text.strip()
            if text:
                return text
    return ""


def _flow_ref(flow: dict[str, Any]) -> str:
    agency = str(flow.get("agencyID") or flow.get("agencyId") or "UNICEF").strip()
    flow_id = str(flow.get("id") or flow.get("ID") or "").strip()
    version = str(flow.get("version") or "latest").strip() or "latest"
    return f"{agency}/{flow_id}/{version}"


def _flow_name(flow: dict[str, Any]) -> str:
    return server._coerce_text(flow.get("name")) or server._coerce_text(flow.get("names"))


def _quoted_flow_path(flow_ref: str) -> str:
    agency, flow_id, version = server._flow_identifiers(flow_ref)
    return f"{quote(agency)},{quote(flow_id)},{quote(version)}"


def _build_key(dimension_order: list[str], filters: dict[str, str]) -> str:
    parts: list[str] = []
    normalized = {key.upper(): value for key, value in filters.items()}
    for dimension in dimension_order:
        parts.append(normalized.get(dimension, ""))
    return ".".join(parts)


def _case_id(flow_ref: str, indicator_id: str, country_id: str) -> str:
    return f"{flow_ref}|{indicator_id}|{country_id}"


async def _fetch_text(client: httpx.AsyncClient, url: str) -> str:
    response = await client.get(url, headers={"User-Agent": "sdmx-agent-test-rig/0.1"})
    response.raise_for_status()
    return response.text


async def _fetch_codelist(client: httpx.AsyncClient, url: str) -> dict[str, str]:
    text = await _fetch_text(client, url)
    root = ET.fromstring(text)
    codes: dict[str, str] = {}
    for elem in root.iter():
        if _tag_name(elem) != "Code":
            continue
        code_id = (elem.attrib.get("id") or elem.attrib.get("ID") or "").strip()
        if not code_id:
            continue
        codes[code_id] = _element_text(elem, "Name")
    return codes


async def _list_unicef_flows() -> list[dict[str, Any]]:
    payload = await server._cached_dataflows()
    flows = server._extract_scoped_dataflows(payload)
    filtered = []
    for flow in flows:
        agency = str(flow.get("agencyID") or flow.get("agencyId") or "").strip()
        if agency == "UNICEF":
            filtered.append(flow)
    filtered.sort(key=lambda item: str(item.get("id") or item.get("ID") or ""))
    return filtered


async def _inspect_flow(
    flow: dict[str, Any],
    indicator_catalog: dict[str, str],
    country_catalog: dict[str, str],
    semaphore: asyncio.Semaphore,
) -> dict[str, Any] | None:
    async with semaphore:
        flow_ref = _flow_ref(flow)
        try:
            payload = await server.get_flow_structure(flow_ref)
        except Exception as exc:
            return {
                "flowRef": flow_ref,
                "flowName": _flow_name(flow),
                "status": "error",
                "error": str(exc),
            }

    dimension_order = server._dimension_order_from_structure(payload)
    if not dimension_order or "INDICATOR" not in dimension_order or "REF_AREA" not in dimension_order:
        return None

    indicator_codes = server._indicator_codes_from_payload(payload)
    indicator_ids = []
    for code in indicator_codes:
        code_id = server._code_identifier(code)
        if code_id and code_id in indicator_catalog:
            indicator_ids.append(code_id)

    ref_area_codes = server._dimension_code_map(payload, "REF_AREA")
    country_ids = sorted(code_id for code_id in ref_area_codes if code_id in country_catalog)

    if not indicator_ids or not country_ids:
        return None

    return {
        "flowRef": flow_ref,
        "flowID": str(flow.get("id") or flow.get("ID") or ""),
        "flowName": _flow_name(flow),
        "dimensionOrder": dimension_order,
        "indicatorIDs": sorted(set(indicator_ids)),
        "countryIDs": country_ids,
        "status": "ready",
    }


async def _build_manifest(
    manifest_path: Path,
    concurrency: int,
    flow_limit: int | None,
    case_limit: int | None,
) -> dict[str, int]:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    async with httpx.AsyncClient(timeout=60.0) as client:
        indicator_catalog, country_catalog = await asyncio.gather(
            _fetch_codelist(client, INDICATOR_CODELIST_URL),
            _fetch_codelist(client, COUNTRY_CODELIST_URL),
        )

    flows = await _list_unicef_flows()
    if flow_limit is not None:
        flows = flows[:flow_limit]

    semaphore = asyncio.Semaphore(max(1, concurrency))
    inspections = await asyncio.gather(
        *[_inspect_flow(flow, indicator_catalog, country_catalog, semaphore) for flow in flows]
    )

    flow_count = 0
    skipped_flow_count = 0
    case_count = 0
    with manifest_path.open("w", encoding="utf-8") as handle:
        for item in inspections:
            if not item or item.get("status") != "ready":
                skipped_flow_count += 1
                continue
            flow_count += 1
            flow_ref = str(item["flowRef"])
            flow_name = str(item["flowName"])
            dimension_order = list(item["dimensionOrder"])
            for indicator_id in item["indicatorIDs"]:
                for country_id in item["countryIDs"]:
                    case = {
                        "case_id": _case_id(flow_ref, indicator_id, country_id),
                        "flowRef": flow_ref,
                        "flowID": item["flowID"],
                        "flowName": flow_name,
                        "dimensionOrder": dimension_order,
                        "filters": {
                            "INDICATOR": indicator_id,
                            "REF_AREA": country_id,
                        },
                        "indicator": {"id": indicator_id, "name": indicator_catalog.get(indicator_id, "")},
                        "country": {"id": country_id, "name": country_catalog.get(country_id, "")},
                        "startPeriod": "2020",
                        "endPeriod": "2024",
                    }
                    handle.write(json.dumps(case, ensure_ascii=True) + "\n")
                    case_count += 1
                    if case_limit is not None and case_count >= case_limit:
                        return {
                            "flows_considered": len(flows),
                            "flows_ready": flow_count,
                            "flows_skipped": skipped_flow_count,
                            "cases_written": case_count,
                        }
    return {
        "flows_considered": len(flows),
        "flows_ready": flow_count,
        "flows_skipped": skipped_flow_count,
        "cases_written": case_count,
    }


def _load_completed_case_ids(results_path: Path) -> set[str]:
    if not results_path.exists():
        return set()
    completed: set[str] = set()
    with results_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            case_id = payload.get("case_id")
            if isinstance(case_id, str) and case_id:
                completed.add(case_id)
    return completed


def _iter_manifest_cases(manifest_path: Path, completed: set[str], case_limit: int | None) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    with manifest_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            payload = json.loads(line)
            case_id = str(payload.get("case_id") or "")
            if not case_id or case_id in completed:
                continue
            selected.append(payload)
            if case_limit is not None and len(selected) >= case_limit:
                break
    return selected


def _csv_rows(text: str) -> list[dict[str, Any]]:
    if not text.strip():
        return []
    reader = csv.DictReader(io.StringIO(text))
    rows: list[dict[str, Any]] = []
    for row in reader:
        if not isinstance(row, dict):
            continue
        normalized = {str(key): value for key, value in row.items() if key is not None}
        rows.append(normalized)
    return rows


async def _execute_case(client: httpx.AsyncClient, case: dict[str, Any], semaphore: asyncio.Semaphore) -> dict[str, Any]:
    async with semaphore:
        flow_ref = str(case["flowRef"])
        dimension_order = [str(item).upper() for item in case["dimensionOrder"]]
        filters = {str(key).upper(): str(value) for key, value in dict(case["filters"]).items()}
        key = _build_key(dimension_order, filters)
        start_period = str(case["startPeriod"])
        end_period = str(case["endPeriod"])
        query_url = (
            f"{server.BASE}/data/{_quoted_flow_path(flow_ref)}/{quote(key, safe='+.')}?"
            f"startPeriod={quote(start_period)}&endPeriod={quote(end_period)}&format=csv"
        )

        try:
            response = await client.get(query_url, headers={"User-Agent": "sdmx-agent-test-rig/0.1"})
            text = response.text
            status = response.status_code
        except Exception as exc:
            return {
                **case,
                "status": "request_error",
                "queryURL": query_url,
                "error": str(exc),
            }

        if status >= 400:
            return {
                **case,
                "status": "http_error",
                "queryURL": query_url,
                "http_status": status,
                "error": server._parse_sdmx_error(text) or text,
            }

        rows = _csv_rows(text)
        return {
            **case,
            "status": "resolved",
            "queryURL": query_url,
            "http_status": status,
            "observation_count": len(rows),
            "rows": rows,
        }


async def _run_cases(manifest_path: Path, results_path: Path, concurrency: int, case_limit: int | None) -> dict[str, int]:
    results_path.parent.mkdir(parents=True, exist_ok=True)
    completed = _load_completed_case_ids(results_path)
    cases = _iter_manifest_cases(manifest_path, completed, case_limit)
    if not cases:
        return {
            "cases_selected": 0,
            "cases_completed_before_run": len(completed),
            "cases_written": 0,
        }

    semaphore = asyncio.Semaphore(max(1, concurrency))
    written = 0
    async with httpx.AsyncClient(timeout=90.0) as client:
        tasks = [asyncio.create_task(_execute_case(client, case, semaphore)) for case in cases]
        with results_path.open("a", encoding="utf-8") as handle:
            for task in asyncio.as_completed(tasks):
                result = await task
                handle.write(json.dumps(result, ensure_ascii=True) + "\n")
                handle.flush()
                written += 1
                if written % 25 == 0:
                    print(f"Completed {written}/{len(cases)} cases", file=sys.stderr)

    return {
        "cases_selected": len(cases),
        "cases_completed_before_run": len(completed),
        "cases_written": written,
    }


async def _async_main(args: argparse.Namespace) -> None:
    manifest_stats = await _build_manifest(
        manifest_path=args.manifest,
        concurrency=args.concurrency,
        flow_limit=args.flow_limit,
        case_limit=args.manifest_case_limit,
    )
    print(json.dumps({"manifest": manifest_stats, "manifest_path": str(args.manifest)}, indent=2))

    if args.manifest_only:
        return

    run_stats = await _run_cases(
        manifest_path=args.manifest,
        results_path=args.results,
        concurrency=args.concurrency,
        case_limit=args.run_case_limit,
    )
    print(json.dumps({"results": run_stats, "results_path": str(args.results)}, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build and execute a UNICEF SDMX test rig for direct CSV-backed ground truth."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where manifest and result JSONL files are written.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Manifest JSONL path. Defaults to <output-dir>/unicef_agent_cases.jsonl.",
    )
    parser.add_argument(
        "--results",
        type=Path,
        default=None,
        help="Results JSONL path. Defaults to <output-dir>/unicef_agent_results.jsonl.",
    )
    parser.add_argument("--concurrency", type=int, default=8, help="Concurrent HTTP requests.")
    parser.add_argument("--flow-limit", type=int, default=None, help="Optional cap on inspected UNICEF flows.")
    parser.add_argument(
        "--manifest-case-limit",
        type=int,
        default=None,
        help="Optional cap on how many cases are written to the manifest.",
    )
    parser.add_argument(
        "--run-case-limit",
        type=int,
        default=None,
        help="Optional cap on how many unresolved manifest cases are executed in this run.",
    )
    parser.add_argument(
        "--manifest-only",
        action="store_true",
        help="Only build the case manifest and skip direct SDMX execution.",
    )
    args = parser.parse_args()
    args.manifest = args.manifest or args.output_dir / DEFAULT_MANIFEST_PATH.name
    args.results = args.results or args.output_dir / DEFAULT_RESULTS_PATH.name
    asyncio.run(_async_main(args))


if __name__ == "__main__":
    main()
