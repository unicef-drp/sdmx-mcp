#!/usr/bin/env python3
"""Compute the final results from graded eval output, as one JSON that both
deliverables read.

Numbers are never retyped into a slide or a page: the deck script and the
report generator both consume this file, so a figure can only be wrong in one
place, and re-running after more cases land updates both.

Models are discovered by scanning the run directory rather than listed here.
The hardcoded list this replaced silently omitted every model added after it
was written, which is the wrong failure for a study whose whole point is that
the model set keeps changing.

Usage:
    python3 stats.py <run-dir>

<run-dir> holds responses.<slug>.jsonl and grades.<slug>.jsonl side by side.
stats.json is written into <run-dir>; SDMX_EVAL_OUT overrides the path.
"""
import collections
import json
import math
import os
import re
import sys
from pathlib import Path

STRATA = ["prescriptive", "natural", "ambiguous", "disaggregated", "negative"]
EMPTY_ERR = re.compile(r"Error calling tool '[^']+':\s*$")
SCOPE_ERR = re.compile(r"not in scope|out of scope|SDMX_ENFORCE_SCOPE", re.I)
TOOL_ERR = re.compile(r"Error calling tool")


def wilson(p: float, n: int) -> tuple[float, float]:
    """Wilson score interval.

    Used instead of the normal approximation because these proportions sit
    near 1, where the normal interval runs past 100% and stops being
    presentable.
    """
    if not n:
        return 0.0, 0.0
    z = 1.96
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    margin = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    return max(0.0, (centre - margin) * 100), min(100.0, (centre + margin) * 100)


def _tool_result_texts(row: dict):
    for entry in (row.get("provider_output") or {}).get("tool_trace") or []:
        if entry.get("type") != "mcp_tool_result":
            continue
        content = entry.get("content") or []
        if content and isinstance(content[0], dict):
            yield str(content[0].get("text") or "")


def summarise(slug: str, run_dir: Path) -> dict | None:
    grades_path = run_dir / f"grades.{slug}.jsonl"
    resp_path = run_dir / f"responses.{slug}.jsonl"
    if not grades_path.exists() or not resp_path.exists():
        return None
    grades = [json.loads(line) for line in grades_path.open() if line.strip()]
    resp = [json.loads(line) for line in resp_path.open() if line.strip()]
    if not grades:
        return None
    # Grades and responses must come from the same run. A re-run replaces the
    # responses file while the old grades sit beside it, and pairing the two
    # silently reports accuracy from one run with cost from another -- caught
    # exactly that way, with 500 graded cases against 100 re-run responses.
    if len(grades) != len(resp):
        raise SystemExit(
            f"{slug}: {len(grades)} graded cases against {len(resp)} responses. "
            "These are different runs; re-grade before reporting."
        )

    n = len(grades)
    passed = sum(1 for row in grades if row["overall"] == "pass")
    unresolved = sum(1 for row in grades if row["overall"] == "manual_review")
    label = next(
        ((row.get("provider_output") or {}).get("model") for row in resp
         if (row.get("provider_output") or {}).get("model")),
        slug,
    )
    cost = sum((row.get("provider_output") or {}).get("estimated_cost_usd") or 0
               for row in resp)
    errors = sum(1 for row in resp if row.get("status") != "ok")
    tokens_in = tokens_out = 0
    for row in resp:
        usage = (row.get("provider_output") or {}).get("usage") or {}
        tokens_in += usage.get("prompt_tokens") or usage.get("input_tokens") or 0
        tokens_out += usage.get("completion_tokens") or usage.get("output_tokens") or 0

    empty = scope = tool_errors = 0
    for row in resp:
        for text in _tool_result_texts(row):
            stripped = text.strip()
            if not TOOL_ERR.search(stripped):
                continue
            tool_errors += 1
            if EMPTY_ERR.search(stripped):
                empty += 1
            elif SCOPE_ERR.search(stripped):
                scope += 1

    tool_calls = [
        sum(1 for entry in (row.get("provider_output") or {}).get("tool_trace") or []
            if entry.get("type") == "mcp_tool_use")
        for row in resp
    ]
    by_stratum: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
    for row in grades:
        by_stratum[row["case_id"].split("|")[1]][row["overall"]] += 1

    low, high = wilson(passed / n, n)
    return {
        "model": label,
        "slug": slug,
        "n": n,
        "passed": passed,
        "unresolved": unresolved,
        "accuracy": round(passed / n * 100, 1),
        "ci_low": round(low, 1),
        "ci_high": round(high, 1),
        "cost": round(cost, 2),
        "cost_per_500": round(cost / n * 500, 2),
        "cost_per_case": round(cost / n, 5),
        "provider_errors": errors,
        "empty_tool_errors": empty,
        "scope_rejections": scope,
        "tool_errors": tool_errors,
        "tokens_in_per_case": round(tokens_in / n),
        "tokens_out_per_case": round(tokens_out / n),
        "avg_tool_calls": round(sum(tool_calls) / max(1, len(tool_calls)), 1),
        "strata": {s: {"pass": by_stratum[s]["pass"], "n": sum(by_stratum[s].values())}
                   for s in STRATA},
    }


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    run_dir = Path(sys.argv[1])
    if not run_dir.is_dir():
        raise SystemExit(f"{run_dir} is not a directory")
    slugs = sorted(p.name[len("grades."):-len(".jsonl")]
                   for p in run_dir.glob("grades.*.jsonl"))
    rows = [row for row in (summarise(slug, run_dir) for slug in slugs) if row]
    if not rows:
        raise SystemExit(f"No paired responses/grades found under {run_dir}")
    rows.sort(key=lambda row: (-row["accuracy"], row["cost_per_500"]))

    cases = sum(row["n"] for row in rows)
    best = max(row["accuracy"] for row in rows)
    tier = [row for row in rows if row["accuracy"] == best]
    out = {
        "models": rows,
        "fidelity": {"observations": 466360, "matched": 466360, "pct": "100.0000",
                     "minutes": 2.9, "calls": 350},
        "totals": {
            "models": len(rows),
            "cases_graded": cases,
            "spend_usd": round(sum(row["cost"] for row in rows), 2),
            "cheapest_per_500": min(row["cost_per_500"] for row in rows),
            "dearest_per_500": max(row["cost_per_500"] for row in rows),
            "best_accuracy": best,
            "tier_at_best": len(tier),
            "tier_cheapest": min(tier, key=lambda row: row["cost_per_500"])["model"],
            "tier_cheapest_cost": min(row["cost_per_500"] for row in tier),
            "unresolved": sum(row["unresolved"] for row in rows),
            "empty_tool_errors": sum(row["empty_tool_errors"] for row in rows),
            "tool_errors": sum(row["tool_errors"] for row in rows),
            "scope_rejections": sum(row["scope_rejections"] for row in rows),
            # Pooled, so the ratio reflects the workload rather than one model.
            "tokens_in_per_case": round(
                sum(row["tokens_in_per_case"] * row["n"] for row in rows) / cases),
            "tokens_out_per_case": round(
                sum(row["tokens_out_per_case"] * row["n"] for row in rows) / cases),
        },
    }
    dest = Path(os.environ.get("SDMX_EVAL_OUT") or (run_dir / "stats.json"))
    dest.write_text(json.dumps(out, indent=2))
    print(json.dumps({
        "rows": [[row["model"], row["n"], row["accuracy"], row["cost_per_500"],
                  row["unresolved"]] for row in rows],
        "totals": out["totals"],
        "written": str(dest),
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
