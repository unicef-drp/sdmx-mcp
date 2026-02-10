#!/usr/bin/env python3
import argparse
import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import server


REGION_REF_AREA = {
    "south asia": "AFG,IND,NPL,PAK,BGD,LKA,BTN,MDV",
}

FLOW_KEYWORDS = [
    ("child mortality", ["CME", "MORTALITY"]),
    ("mortality", ["CME", "MORTALITY"]),
    ("child marriage", ["PT_CM", "CHILD MARRIAGE"]),
    ("marriage", ["PT_CM", "CHILD MARRIAGE"]),
]


def _load_json(value: str | None) -> dict[str, Any]:
    if not value:
        return {}
    text = value.strip()
    if not text:
        return {}
    if text.startswith("@"):
        text = Path(text[1:]).read_text(encoding="utf-8")
    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("Filters must decode to a JSON object.")
    return data


def _read_scenarios(path: str) -> list[dict[str, Any]]:
    file_path = Path(path)
    scenarios: list[dict[str, Any]] = []
    for line in file_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        if isinstance(item, dict):
            scenarios.append(item)
    return scenarios


def _log(verbose: bool, message: str) -> None:
    if verbose:
        print(message, file=sys.stderr)


def _slugify(text: str, max_len: int = 64) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", "-", (text or "").strip().lower()).strip("-")
    if not value:
        value = "case"
    return value[:max_len].rstrip("-")


def _infer_ref_area(question: str) -> str | None:
    text = (question or "").lower()
    for key, value in REGION_REF_AREA.items():
        if key in text:
            return value
    return None


async def _default_dimension_code(
    flow_ref: str,
    dimension: str,
    queries: list[str],
    verbose: bool,
) -> str | None:
    for query in queries:
        try:
            codes = await server.list_codes(flowRef=flow_ref, dimension=dimension, query=query, limit=5)
        except Exception:
            continue
        if codes:
            code_id = codes[0].get("id")
            if isinstance(code_id, str) and code_id:
                _log(verbose, f"Defaulted {dimension}={code_id} based on codelist match for '{query}'.")
                return code_id
    return None


def _flow_score(question: str, flow: dict[str, Any]) -> int:
    text = f"{flow.get('id','')} {flow.get('name','')} {flow.get('description','')}".upper()
    score = 0
    q = (question or "").lower()
    for phrase, tokens in FLOW_KEYWORDS:
        if phrase in q:
            for token in tokens:
                if token in text:
                    score += 5
    if flow.get("agencyID") == "UNICEF":
        score += 2
    if "DRAFT" in text:
        score -= 5
    return score


def _select_flow_from_search(question: str, search: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not search:
        return None
    scored = sorted(search, key=lambda item: _flow_score(question, item), reverse=True)
    return scored[0]


def _filter_flows_by_agency(search: list[dict[str, Any]], agency: str | None) -> list[dict[str, Any]]:
    if not agency:
        return search
    filtered = [item for item in search if item.get("agencyID") == agency]
    return filtered if filtered else search


def _missing_dimensions(dimensions: list[dict[str, Any]], filters: dict[str, Any]) -> list[str]:
    wanted = []
    for dim in dimensions:
        dim_id = dim.get("id")
        if isinstance(dim_id, str) and dim_id not in filters:
            wanted.append(dim_id)
    return wanted


async def _run_case(case: dict[str, Any], verbose: bool = False, journey: bool = False) -> dict[str, Any]:
    question = case.get("question", "").strip()
    if not question:
        raise ValueError("Each case needs a non-empty 'question'.")

    agencies = await server.list_agencies(limit=int(case.get("agency_limit", 100)))
    agency = case.get("agency")
    if agency:
        agencies = [a for a in agencies if a.get("id") == agency]

    top_flows = int(case.get("top_flows", 8))
    search = await server.search_dataflows(query=question, limit=top_flows)
    search = _filter_flows_by_agency(search, agency)
    grouped = await server.list_dataflows_grouped(
        query=question,
        limitPerTheme=int(case.get("limit_per_theme", 10)),
    )

    flow_ref = case.get("flowRef")
    filters = case.get("filters") or {}
    if not flow_ref and search:
        pick = _select_flow_from_search(question, search)
        if pick:
            flow_ref = pick.get("flowRef")
            _log(verbose, f"Selected dataflow: {pick.get('id')} ({flow_ref})")
    indicator_value = None
    if isinstance(filters, dict):
        indicator_value = filters.get("INDICATOR")
    if not flow_ref and indicator_value:
        if isinstance(indicator_value, str):
            indicator_token = indicator_value.replace("+", ",").split(",")[0].strip()
        elif isinstance(indicator_value, (list, tuple)) and indicator_value:
            indicator_token = str(indicator_value[0]).strip()
        else:
            indicator_token = None
        if indicator_token:
            indicator_search = await server.search_dataflows(query=indicator_token, limit=5)
            if indicator_search:
                flow_ref = indicator_search[0].get("flowRef")

    describe = None
    dimensions = None
    data = None
    data_error = None
    resolved_filters: dict[str, Any] | None = None
    indicator_candidates: list[str] = []
    indicator_attempts: list[dict[str, Any]] = []
    if flow_ref:
        describe = await server.describe_flow(flowRef=flow_ref)
        dimensions = await server.list_dimensions(flowRef=flow_ref)

        resolved_filters = dict(filters) if isinstance(filters, dict) else {}
        if journey:
            if "REF_AREA" not in resolved_filters:
                inferred = _infer_ref_area(question)
                if inferred:
                    resolved_filters["REF_AREA"] = inferred
                    _log(verbose, f"Inferred REF_AREA={inferred} from question.")
            if "SEX" not in resolved_filters:
                default_sex = await _default_dimension_code(
                    flow_ref,
                    "SEX",
                    ["total", "both", "all", "total sex", "both sexes"],
                    verbose,
                )
                if default_sex:
                    resolved_filters["SEX"] = default_sex
            if not case.get("lastNObservations") and not (case.get("startPeriod") and case.get("endPeriod")):
                case["lastNObservations"] = 3
                _log(verbose, "Defaulted lastNObservations=3.")
            missing = _missing_dimensions(dimensions, resolved_filters)
            if missing:
                _log(verbose, f"Missing dimensions (left blank): {', '.join(missing)}")

        if resolved_filters and (case.get("lastNObservations") or (case.get("startPeriod") and case.get("endPeriod"))):
            indicator_value = resolved_filters.get("INDICATOR")
            # Primary candidate ranking from question text in journey mode.
            if journey and question:
                try:
                    ranked = await server.find_indicator_candidates(
                        flowRef=flow_ref,
                        query=question,
                        limit=10,
                    )
                    ranked_ids = [
                        item.get("id")
                        for item in ranked
                        if isinstance(item, dict) and item.get("id")
                    ]
                    indicator_candidates.extend(
                        [code for code in ranked_ids if code not in indicator_candidates]
                    )
                except Exception:
                    pass
            if isinstance(indicator_value, str):
                indicator_token = indicator_value.replace("+", ",").split(",")[0].strip()
                if indicator_token:
                    try:
                        ranked = await server.find_indicator_candidates(
                            flowRef=flow_ref,
                            query=case.get("question", ""),
                            limit=5,
                        )
                        ranked_ids = [
                            item.get("id")
                            for item in ranked
                            if isinstance(item, dict) and item.get("id")
                        ]
                        indicator_candidates.extend(
                            [code for code in ranked_ids if code not in indicator_candidates]
                        )
                    except Exception:
                        pass
                    codes = await server.list_codes(
                        flowRef=flow_ref,
                        dimension="INDICATOR",
                        query=indicator_token,
                        limit=5,
                    )
                    code_ids = [
                        item.get("id")
                        for item in codes
                        if isinstance(item, dict) and item.get("id")
                    ]
                    if indicator_token:
                        indicator_candidates.append(indicator_token)
                    indicator_candidates.extend([code for code in code_ids if code not in indicator_candidates])
            if not indicator_candidates and indicator_value:
                if isinstance(indicator_value, (list, tuple)):
                    indicator_candidates.extend([str(item) for item in indicator_value if str(item)])
                elif isinstance(indicator_value, str):
                    indicator_candidates.append(indicator_value)
            if not indicator_candidates and case.get("question"):
                try:
                    ranked = await server.find_indicator_candidates(
                        flowRef=flow_ref,
                        query=case.get("question", ""),
                        limit=5,
                    )
                    ranked_ids = [
                        item.get("id")
                        for item in ranked
                        if isinstance(item, dict) and item.get("id")
                    ]
                    indicator_candidates.extend(
                        [code for code in ranked_ids if code not in indicator_candidates]
                    )
                except Exception:
                    pass
            if not indicator_candidates:
                indicator_candidates.append("")
            for candidate in indicator_candidates:
                try:
                    if candidate:
                        resolved_filters["INDICATOR"] = candidate
                        _log(verbose, f"Trying INDICATOR={candidate}...")
                    data = await server.query_data(
                        flowRef=flow_ref,
                        filters=resolved_filters,
                        startPeriod=case.get("startPeriod"),
                        endPeriod=case.get("endPeriod"),
                        lastNObservations=case.get("lastNObservations"),
                        format=case.get("format", "csv"),
                    )
                    if isinstance(data, dict) and data.get("error"):
                        error_message = data["error"].get("message") if isinstance(data["error"], dict) else None
                        indicator_attempts.append(
                            {"indicator": candidate, "status": "error", "message": error_message}
                        )
                        if verbose:
                            print(
                                f"No data for indicator '{candidate}'. Moving to next match.",
                                file=sys.stderr,
                            )
                        continue
                    raw_csv = data.get("raw_csv") if isinstance(data, dict) else None
                    if isinstance(raw_csv, str):
                        lines = [line for line in raw_csv.splitlines() if line.strip()]
                        if len(lines) <= 1:
                            indicator_attempts.append(
                                {"indicator": candidate, "status": "empty", "message": "No rows returned."}
                            )
                            if verbose:
                                print(
                                    f"No rows for indicator '{candidate}'. Moving to next match.",
                                    file=sys.stderr,
                                )
                            continue
                    indicator_attempts.append({"indicator": candidate, "status": "ok"})
                    break
                except Exception as exc:
                    indicator_attempts.append(
                        {"indicator": candidate, "status": "error", "message": f"{type(exc).__name__}: {exc}"}
                    )
                    if verbose:
                        print(
                            f"Error for indicator '{candidate}' ({type(exc).__name__}). Moving to next match.",
                            file=sys.stderr,
                        )
                    data_error = f"{type(exc).__name__}: {exc}"
                    continue

    return {
        "question": question,
        "agencies": agencies,
        "search_dataflows": search,
        "grouped_dataflows": grouped,
        "selected_flowRef": flow_ref,
        "describe_flow": describe,
        "dimensions": dimensions,
        "resolved_filters": resolved_filters,
        "indicator_candidates": indicator_candidates,
        "indicator_attempts": indicator_attempts,
        "query_data": data,
        "query_error": data_error,
    }


async def _run(args: argparse.Namespace) -> None:
    if args.scenarios:
        cases = _read_scenarios(args.scenarios)
    else:
        question = args.question or input("Question: ").strip()
        cases = [
            {
                "question": question,
                "agency": args.agency,
                "flowRef": args.flow_ref,
                "filters": _load_json(args.filters),
                "startPeriod": args.start_period,
                "endPeriod": args.end_period,
                "lastNObservations": args.last_n,
                "format": args.format,
                "top_flows": args.top_flows,
                "limit_per_theme": args.limit_per_theme,
            }
        ]

    output_dir = Path(args.save_output_dir) if args.save_output_dir else None
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)

    for i, case in enumerate(cases, start=1):
        result = await _run_case(case, verbose=args.verbose, journey=args.journey)
        if len(cases) > 1:
            print(f"# case {i}")
        print(json.dumps(result, indent=2))
        if output_dir:
            question = case.get("question") if isinstance(case, dict) else None
            slug = _slugify(str(question) if question else f"case-{i}")
            out_path = output_dir / f"{i:02d}-{slug}.json"
            out_path.write_text(json.dumps(result, indent=2), encoding="utf-8")
            _log(args.verbose, f"Saved output to {out_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MCP agent test rig for UNICEF SDMX tools (direct tool-call harness)."
    )
    parser.add_argument("--question", help="User question to test.")
    parser.add_argument("--agency", help="Optional agency id filter (e.g., UNICEF).")
    parser.add_argument("--flow-ref", help="Optional explicit flowRef (agency/id/version).")
    parser.add_argument("--filters", help="JSON object or @path/to/file.json with filters.")
    parser.add_argument("--start-period", help="SDMX startPeriod (YYYY or YYYY-MM).")
    parser.add_argument("--end-period", help="SDMX endPeriod (YYYY or YYYY-MM).")
    parser.add_argument("--last-n", type=int, help="Use lastNObservations instead of full period slices.")
    parser.add_argument("--format", default="csv", help="Query format, e.g. csv or sdmx-json.")
    parser.add_argument("--top-flows", type=int, default=8, help="Top flow candidates to return.")
    parser.add_argument(
        "--limit-per-theme",
        type=int,
        default=10,
        help="Max flows per theme bucket for grouped listing.",
    )
    parser.add_argument(
        "--scenarios",
        help="Path to JSONL file with test cases (one JSON object per line).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print progress when iterating through indicator matches.",
    )
    parser.add_argument(
        "--journey",
        action="store_true",
        help="Use agentic journey defaults: infer REF_AREA, choose flow from question, rank indicators by question.",
    )
    parser.add_argument(
        "--save-output-dir",
        help="Optional directory to save one JSON result file per case.",
    )
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
