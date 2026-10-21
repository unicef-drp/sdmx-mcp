#!/usr/bin/env python3
"""Run one eval manifest across several models and tabulate the comparison.

The point of the sweep is the claim that a cheap model is accurate enough to be
useful against an SDMX registry. That claim needs more than one headline
number, so this reports accuracy per stratum alongside measured cost: a model
that scores well on prescriptive prompts and collapses on ambiguous ones is a
different proposition from one that is merely a little worse everywhere.

Each model is run and graded independently through the normal runner, so a
sweep is exactly equivalent to running the commands by hand -- it only removes
the bookkeeping. Responses and grades are kept per model so any number here can
be traced back to the transcript that produced it.

Usage:
    python3 scripts/sdmx_eval_model_sweep.py \
        --config scripts/sdmx_eval_config.unicef_global.gateway.example.json \
        --manifest tmp/sdmx_eval/run500.jsonl \
        --out-dir tmp/sweep \
        --models openai/gpt-5-nano google/gemini-2.5-flash-lite

Already-finished runs are skipped unless --force, so the sweep is resumable and
re-tabulating costs nothing.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
RUNNER = HERE / "sdmx_eval_runner.py"


def _slug(model: str) -> str:
    return model.replace("/", "-").replace(":", "-")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _stratum(case_id: str) -> str:
    # case_id is "<flowRef>|<stratum>|<period>|<filters json>".
    parts = case_id.split("|")
    return parts[1] if len(parts) > 1 else "unknown"


def _run(args: list[str], model: str) -> None:
    env = dict(os.environ)
    env["SDMX_EVAL_MODEL"] = model
    completed = subprocess.run(args, env=env, capture_output=True, text=True)
    if completed.returncode != 0:
        tail = (completed.stderr or completed.stdout or "").strip()[-1500:]
        raise SystemExit(f"{' '.join(args[:3])} failed for {model}:\n{tail}")


def run_model(
    *, model: str, config: Path, manifest: Path, out_dir: Path, force: bool
) -> tuple[Path, Path]:
    responses = out_dir / f"responses.{_slug(model)}.jsonl"
    grades = out_dir / f"grades.{_slug(model)}.jsonl"
    manifest_n = len(_read_jsonl(manifest))

    rows = _read_jsonl(responses)
    done = len([r for r in rows if r.get("status") == "ok"])
    failed = len(rows) - done
    if force or done < manifest_n:
        print(f"  running {model} ({done}/{manifest_n} ok, {failed} failed)...", flush=True)
        command = [sys.executable, str(RUNNER), "run-provider",
                   "--config", str(config), "--manifest", str(manifest),
                   "--responses", str(responses)]
        # The runner treats any case already in the file as done, errored rows
        # included, so without this a re-run prints "running" and writes
        # nothing. That happened after a network outage left 408 of 500 cases
        # failed: the sweep reported it was re-running them and silently
        # reproduced the same file, and the resulting 16.8% read as the model's
        # score.
        if failed:
            command.append("--retry-failed")
        _run(command, model)
        after = [r for r in _read_jsonl(responses) if r.get("status") == "ok"]
        if len(after) <= done and failed:
            raise SystemExit(
                f"{model}: {failed} cases were failed and the re-run added none "
                f"({done} ok before, {len(after)} after). Investigate before reporting."
            )
    else:
        print(f"  {model}: responses complete, skipping run", flush=True)

    if force or not grades.exists() or len(_read_jsonl(grades)) < manifest_n:
        _run([sys.executable, str(RUNNER), "grade-results",
              "--config", str(config), "--manifest", str(manifest),
              "--responses", str(responses), "--grades", str(grades)], model)
    return responses, grades


def summarise(model: str, responses: Path, grades: Path) -> dict[str, Any]:
    graded = _read_jsonl(grades)
    resp = _read_jsonl(responses)

    # A null value scores as a correct abstention on a negative case, so an
    # unparseable answer must never be counted as one.
    errors = sum(1 for r in resp if r.get("status") != "ok")
    cost = sum((r.get("provider_output") or {}).get("estimated_cost_usd") or 0 for r in resp)
    tool_calls = [
        sum(1 for e in (r.get("provider_output") or {}).get("tool_trace") or []
            if e.get("type") == "mcp_tool_use")
        for r in resp
    ]

    # The runner writes "pass" / "fail" / "manual_review" in `overall`. Reading
    # a verdict that never occurs silently scores every model at zero, so an
    # unrecognised verdict is an error rather than a miscount.
    known = {"pass", "fail", "manual_review"}
    by_stratum: dict[str, dict[str, int]] = {}
    totals = {"pass": 0, "fail": 0, "manual_review": 0}
    for row in graded:
        verdict = str(row.get("overall") or "")
        if verdict not in known:
            raise SystemExit(
                f"Unrecognised grade verdict {verdict!r} for case "
                f"{row.get('case_id')!r}; expected one of {sorted(known)}."
            )
        bucket = by_stratum.setdefault(_stratum(str(row.get("case_id") or "")),
                                       {"pass": 0, "fail": 0, "manual_review": 0})
        bucket[verdict] += 1
        totals[verdict] += 1

    n = len(graded)
    return {
        "model": model,
        "n": n,
        "passed": totals["pass"],
        "failed": totals["fail"],
        "manual_review": totals["manual_review"],
        # Reported two ways on purpose. Strict counts only clean passes;
        # anything the grader could not settle counts against the model, which
        # is the honest headline. Lenient treats manual review as neutral and
        # is the optimistic bound. The gap between them is how much hand
        # checking a result still needs.
        "pass_rate_strict": (totals["pass"] / n * 100) if n else 0.0,
        "pass_rate_lenient": ((totals["pass"] + totals["manual_review"]) / n * 100) if n else 0.0,
        "provider_errors": errors,
        "cost_usd": cost,
        "cost_per_case": (cost / len(resp)) if resp else 0.0,
        "avg_tool_calls": (sum(tool_calls) / len(tool_calls)) if tool_calls else 0.0,
        "by_stratum": by_stratum,
    }


def print_table(summaries: list[dict[str, Any]]) -> None:
    print("\n=== accuracy and cost ===")
    head = f"{'model':<34}{'n':>5}{'pass':>6}{'fail':>6}{'rev':>5}{'strict%':>9}{'lenient%':>10}{'$/run':>9}{'$/case':>9}{'tools':>7}"
    print(head)
    print("-" * len(head))
    for s in sorted(summaries, key=lambda x: -x["pass_rate_strict"]):
        print(f"{s['model']:<34}{s['n']:>5}{s['passed']:>6}{s['failed']:>6}"
              f"{s['manual_review']:>5}{s['pass_rate_strict']:>9.1f}"
              f"{s['pass_rate_lenient']:>10.1f}{s['cost_usd']:>9.2f}"
              f"{s['cost_per_case']:>9.4f}{s['avg_tool_calls']:>7.1f}")

    strata = sorted({k for s in summaries for k in s["by_stratum"]})
    if strata:
        print("\n=== strict pass rate by stratum ===")
        head = f"{'model':<34}" + "".join(f"{s[:11]:>13}" for s in strata)
        print(head)
        print("-" * len(head))
        for s in sorted(summaries, key=lambda x: -x["pass_rate_strict"]):
            cells = ""
            for st in strata:
                b = s["by_stratum"].get(st)
                if not b:
                    cells += f"{'-':>13}"
                else:
                    tot = sum(b.values())
                    cells += f"{(b['pass'] / tot * 100 if tot else 0):>12.1f}%"
            print(f"{s['model']:<34}{cells}")

    flagged = [s for s in summaries if s["provider_errors"]]
    if flagged:
        print("\nprovider errors (excluded from no score; investigate before trusting the row):")
        for s in flagged:
            print(f"  {s['model']}: {s['provider_errors']}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--models", required=True, nargs="+")
    parser.add_argument("--force", action="store_true",
                        help="Re-run and re-grade even where output already exists.")
    parser.add_argument("--summary-json", type=Path,
                        help="Also write the summary as JSON.")
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    summaries = []
    for model in args.models:
        print(f"[{model}]", flush=True)
        responses, grades = run_model(model=model, config=args.config,
                                      manifest=args.manifest, out_dir=args.out_dir,
                                      force=args.force)
        summaries.append(summarise(model, responses, grades))

    print_table(summaries)
    if args.summary_json:
        args.summary_json.write_text(json.dumps(summaries, indent=2) + "\n", encoding="utf-8")
        print(f"\nsummary written to {args.summary_json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
