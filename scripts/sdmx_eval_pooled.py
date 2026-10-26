#!/usr/bin/env python3
"""Pool the per-stratum pass rates across models, for the difficulty gradient.

The spread per stratum is the finding: models agree where the query is fully
specified and diverge where codes must be resolved. That measure only means
something across models that actually drive the server. A model which never
succeeds in calling a tool scores zero on every stratum, which makes every
range 100 points wide and erases the gradient -- it measures inability to use
tools, not the difficulty of the question.

So models averaging fewer than MIN_TOOL_CALLS tool calls per case are excluded
from the spread, and the exclusion is on tool use rather than on score, decided
before looking at accuracy. Pooled pass rates are reported over every model.
"""
import collections
import json
import os
import sys
from pathlib import Path

# Hardest last: the slide's point is the gradient, not alphabetical order.
# A model averaging fewer than this never got the server to answer, so it
# cannot inform how difficulty varies by question type.
MIN_TOOL_CALLS = 1.0
ORDER = ["negative", "prescriptive", "natural", "disaggregated", "ambiguous"]
LABELS = {"negative": "Negative\n(no data exists)",
          "prescriptive": "Prescriptive\n(tool + codes given)",
          "natural": "Natural\n(names only)",
          "disaggregated": "Disaggregated\n(one breakdown)",
          "ambiguous": "Ambiguous\n(sibling indicators)"}


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit("usage: pooled.py <run-dir>")
    run_dir = Path(sys.argv[1])
    # Only the grades files. A bare *.jsonl glob also matched the responses
    # sitting beside them in a consolidated run directory, which pooled each
    # model twice and silently halved every rate.
    files = sorted(run_dir.glob("grades.*.jsonl"))
    if not files:
        raise SystemExit(f"No grades.*.jsonl in {run_dir}")

    # Mean tool calls per case, used only for the spread exclusion.
    tool_calls: dict[str, float] = {}
    for path in files:
        slug = path.name[len("grades."):-len(".jsonl")]
        resp = run_dir / f"responses.{slug}.jsonl"
        if not resp.exists():
            continue
        calls = cases = 0
        with resp.open() as fh:
            for line in fh:
                if not line.strip():
                    continue
                out = json.loads(line).get("provider_output") or {}
                calls += len(out.get("tool_trace") or [])
                cases += 1
        tool_calls[slug] = calls / cases if cases else 0.0

    pooled: dict[str, list[int]] = collections.defaultdict(lambda: [0, 0])
    per_model: dict[str, dict[str, float]] = {}
    for path in files:
        rows = [json.loads(line) for line in path.open() if line.strip()]
        by: dict[str, collections.Counter] = collections.defaultdict(collections.Counter)
        for row in rows:
            by[row["case_id"].split("|")[1]][row["overall"]] += 1
        model = path.name[len("grades."):-len(".jsonl")]
        per_model[model] = {}
        for stratum in ORDER:
            total = sum(by[stratum].values())
            if not total:
                continue
            ok = by[stratum]["pass"]
            pooled[stratum][0] += ok
            pooled[stratum][1] += total
            per_model[model][stratum] = round(ok / total * 100, 1)

    strata = [{"key": s, "label": LABELS[s],
               "pass": pooled[s][0], "n": pooled[s][1],
               "pct": round(pooled[s][0] / pooled[s][1] * 100, 1)}
              for s in ORDER if pooled[s][1]]

    # The spread per stratum is the real finding: models agree where the query
    # is specified and diverge where it is not.
    engaged = {slug for slug, n in tool_calls.items() if n >= MIN_TOOL_CALLS}
    excluded = sorted(set(per_model) - engaged)
    spread = {}
    for s in ORDER:
        vals = [m[s] for slug, m in per_model.items() if s in m and slug in engaged]
        if vals:
            spread[s] = {"min": min(vals), "max": max(vals),
                         "range": round(max(vals) - min(vals), 1)}

    out = {"models": len(per_model), "cases": sum(p[1] for p in pooled.values()),
           "manifest": "clean (regenerated)", "strata": strata,
           "spread": spread, "per_model": per_model,
           "spread_models": len(engaged), "spread_excluded": excluded,
           "min_tool_calls": MIN_TOOL_CALLS,
           "tool_calls_per_case": {k: round(v, 1) for k, v in sorted(tool_calls.items())}}
    dest = Path(os.environ.get("SDMX_EVAL_OUT_POOLED") or (run_dir / "pooled.json"))
    dest.write_text(json.dumps(out, indent=2))
    print(f"{'stratum':<16}{'pooled':>12}{'rate':>8}{'min':>8}{'max':>8}{'range':>8}")
    print("-" * 60)
    for s in strata:
        sp = spread[s["key"]]
        fraction = "{}/{}".format(s["pass"], s["n"])
        print("{:<16}{:>12}{:>7.1f}%{:>8.1f}{:>8.1f}{:>8.1f}".format(
            s["key"], fraction, s["pct"], sp["min"], sp["max"], sp["range"]))
    print(f"\n{out['models']} models, {out['cases']} graded cases -> {dest}")
    if excluded:
        print(f"spread computed over {len(engaged)} models; excluded for "
              f"<{MIN_TOOL_CALLS} tool calls/case: "
              + ", ".join(f"{e} ({tool_calls[e]:.1f})" for e in excluded))
    return 0


if __name__ == "__main__":
    sys.exit(main())
