#!/usr/bin/env python3
import json
import sys


def main() -> None:
    payload = json.load(sys.stdin)
    case = payload.get("case") or {}
    prompt = case.get("prompt") or ""

    # Replace this stub with an actual provider integration.
    # Expected stdout JSON shape:
    # {
    #   "answer_text": "...",
    #   "claims": {
    #     "value": "12.3",
    #     "time_period": "2024",
    #     "flowRef": "AGENCY/FLOW/1.0",
    #     "filters": {"INDICATOR": "...", "REF_AREA": "..."}
    #   },
    #   "tool_trace": [...],
    #   "raw_response": {...}
    # }
    #
    # The grader relies primarily on claims.value, claims.time_period,
    # claims.flowRef, and claims.filters.
    result = {
        "status": "ok",
        "answer_text": f"Provider template received prompt: {prompt}",
        "claims": {
            "value": None,
            "time_period": case.get("timePeriod"),
            "flowRef": case.get("flowRef"),
            "filters": case.get("filters"),
        },
        "tool_trace": [],
        "raw_response": {"note": "Replace scripts/sdmx_eval_provider_template.py with a real provider adapter."},
    }
    json.dump(result, sys.stdout)


if __name__ == "__main__":
    main()
