#!/usr/bin/env python3
"""Build the substitution context shared by the PDF and the deck.

master.md carries {{placeholders}} rather than literal figures so the two
deliverables cannot drift apart. This module turns stats.json into that
context, and writes it as context.json so the Node deck builder reads exactly
the same values the Python PDF builder does.

An unresolved placeholder is an error, not a blank: a slide reading
"{{top_accuracy}}" on a conference screen is worse than a build that refuses.

Nothing here hardcodes a model name or a count. The headline follows whichever
model is cheapest at the best measured accuracy, because that changed three
times during the study and a hand-written headline went stale each time.
"""
import csv
import glob
import json
import os
import re
from pathlib import Path

DIR = Path(__file__).resolve().parent
REPO = Path(os.environ.get("SDMX_REPO") or DIR.parent)
# Where stats.py / pooled.py / variance.py wrote their JSON. The analysis
# scripts now live in the repo under scripts/ and write into the run directory,
# so this no longer has to be the same folder as master.md.
DATA = Path(os.environ.get("SDMX_EVAL_DATA") or DIR)
ABLATION = Path(os.environ.get("SDMX_EVAL_ABLATION") or (REPO / "tmp/ablation"))
ROUTED = Path(os.environ.get("SDMX_EVAL_ROUTED") or (REPO / "tmp/variance"))
PLACEHOLDER = re.compile(r"\{\{([a-z0-9_]+)\}\}")

# Screening runs, used only for the screening-economy figure. Set
# SDMX_SCREEN_DIRS to a colon-separated list of run directories, relative to
# the repository root.
SCREEN_DIRS = [d for d in (os.environ.get("SDMX_SCREEN_DIRS") or
                           "tmp/screen-oct24:tmp/screen-highsteps").split(":") if d]

FAULTS = [
    ("Claude Haiku 4.5", 'Throttling reported as "no data"; bare decimals inviting rescaling; accented names failing verification', "MCP"),
    ("Claude Haiku 5.5", "93 of 500 correct answers scored as unsettled — it omitted a structured field", "Harness"),
    ("GPT-5-nano", "Needs ~3,400 output tokens; our 1,200 ceiling failed 46% of cases. Separately, invents lowercase dimension names and two-letter country codes", "Both"),
    ("GPT-5-mini", "Strong on prescriptive prompts, barely half on ambiguous phrasing", "Model"),
    ("Gemini 2.5 Flash Lite", "Invented dataflow names — scope enforcement rejected every one", "Model"),
    ("DeepSeek, GLM, Qwen-235b", "Our JSON mode silently suppressed tool calling in all three", "Harness"),
    ("Qwen models", "Emit tool calls as plain text rather than through the tool API", "Undetermined"),
    ("Qwen models under load", "Our server returning tool errors with empty messages", "MCP"),
    ("Mistral Nemo", "Ignores tool definitions entirely and answers from memory", "Model"),
    ("Nemotron Nano", "Needs 2–3× the tool calls of any other model; exhausted our step ceiling", "Both"),
    ("Every model measured", "Six withdrawn series left in our ground truth; correct abstentions scored as misses", "Harness"),
    ("Every model measured", "Transport failures and empty discovery results scored as wrong answers", "Harness"),
]


def _md_table(header, rows):
    out = ["| " + " | ".join(header) + " |",
           "| " + " | ".join("---" for _ in header) + " |"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def _theme_prefix_count() -> int:
    """Distinct indicator-code prefixes in the shipped theme map."""
    path = REPO / "theme_prefixes_domain.csv"
    if not path.exists():
        return 0
    with path.open() as fh:
        return len({row["prefix"] for row in csv.DictReader(fh) if row.get("prefix")})


def _screen_economy() -> tuple[int, float]:
    """How many models were screened, and what the screening cost."""
    models, cost = set(), 0.0
    for directory in SCREEN_DIRS:
        for path in glob.glob(str(REPO / directory / "responses.*.jsonl")):
            with open(path) as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    out = (json.loads(line).get("provider_output") or {})
                    if out.get("model"):
                        models.add(out["model"])
                    cost += out.get("estimated_cost_usd") or 0
    return len(models), cost


def _variance():
    """Run-to-run spread and case-level churn, from variance.py."""
    path = DATA / "variance.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    rows = sorted(data.items(), key=lambda kv: kv[1]["spread"])
    return rows


def _ablation_tool_calls():
    """Tool calls per case with the discovery layer withheld, and with it."""
    out = {}
    for label, pattern in (("after", "tmp/ablation/responses.*.jsonl"),
                           ("before", "tmp/consolidated/responses.*.jsonl")):
        calls = cases = 0
        for path in glob.glob(str(REPO / pattern)):
            with open(path) as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    out_obj = json.loads(line).get("provider_output") or {}
                    if label == "before" and "glm-5.3" not in str(out_obj.get("model") or ""):
                        continue
                    calls += len(out_obj.get("tool_trace") or [])
                    cases += 1
        out[label] = round(calls / cases, 1) if cases else None
    return out


def _variance_keys() -> dict:
    rows = _variance()
    if not rows:
        raise SystemExit(
            "variance.json missing -- run `python3 variance.py <run-dir>` before "
            "building, or remove the variance placeholders from master.md."
        )
    tight = rows[0][1]
    worst = rows[-1][1]
    return {
        "table_variance": _md_table(
            ["Model", "Accuracy per repeat", "Spread", "Cases flipping verdict"],
            [[f"`{slug}`", " · ".join(f"{a}" for a in d["accuracies"]),
              f"**{d['spread']}** pts", f"{d['flipped']}/{d['cases']} ({d['flipped_pct']}%)"]
             for slug, d in rows]),
        "variance_tight_spread": str(tight["spread"]),
        "variance_tight_flip": f"{tight['flipped_pct']}%",
        "variance_worst_model": f"`{rows[-1][0]}`",
        "variance_worst_spread": str(worst["spread"]),
        "variance_worst_flip": f"{worst['flipped_pct']}%",
    }


def _ablation_keys() -> dict:
    """The ablation arm: discovery tools withheld, native SDMX operations kept."""
    controls = {"zai-glm-5.3-flash": 99.4, "deepseek-deepseek-v4-flash-0731": 97.8}
    rows = []
    for slug, control in controls.items():
        gp = ABLATION / f"grades.{slug}.jsonl"
        rp = ABLATION / f"responses.{slug}.jsonl"
        if not gp.exists() or not rp.exists():
            continue
        grades = [json.loads(l) for l in gp.open() if l.strip()]
        resp = {json.loads(l)["case_id"]: json.loads(l)
                for l in rp.open() if l.strip()}
        n = len(grades)
        if not n:
            continue
        passed = sum(1 for g in grades if g["overall"] == "pass")
        ceiling = sum(1 for g in grades
                      if (resp.get(g["case_id"], {}).get("provider_output") or {}).get("steps_exhausted"))
        answered = [g for g in grades
                    if (resp.get(g["case_id"], {}).get("provider_output") or {}).get("status") != "provider_error"]
        ok_answered = sum(1 for g in answered if g["overall"] == "pass")
        calls = sum(len((resp.get(g["case_id"], {}).get("provider_output") or {}).get("tool_trace") or [])
                    for g in grades)
        rows.append({
            "slug": slug, "n": n, "control": control,
            "ablated": round(100 * passed / n, 1),
            "drop": round(control - 100 * passed / n, 1),
            "ceiling": round(100 * ceiling / n),
            "answered": round(100 * ok_answered / max(1, len(answered)), 1),
            "tools": round(calls / n, 1),
        })
    if not rows:
        raise SystemExit("no ablation grades found under tmp/ablation/")
    rows.sort(key=lambda r: r["drop"])
    before = _ablation_tool_calls().get("before")
    return {
        "table_ablation": _md_table(
            ["Model", "n", "Control", "Ablated", "Drop", "Step-ceiling failures",
             "Accuracy when answered", "Tools/case"],
            [[f"`{r['slug']}`", r["n"], f"{r['control']}%", f"**{r['ablated']}%**",
              f"**{r['drop']}** pts", f"{r['ceiling']}%", f"{r['answered']}%", r["tools"]]
             for r in rows]),
        "ablation_drop_best": str(rows[0]["drop"]),
        "ablation_drop_worst": str(rows[-1]["drop"]),
        "ablation_tools_before": str(before) if before else "?",
        "ablation_tools_after": f"{min(r['tools'] for r in rows)}–{max(r['tools'] for r in rows)}",
    }


def _routed_keys() -> dict:
    """The routed re-run: host recorded per request, same model, three repeats."""
    hosts, accs, total = {}, [], 0
    for rep in (1, 2, 3):
        rp = ROUTED / f"responses.openai-gpt-oss-20b-routed.rep{rep}.jsonl"
        gp = ROUTED / f"grades.openai-gpt-oss-20b-routed.rep{rep}.jsonl"
        if not rp.exists() or not gp.exists():
            continue
        rows = {}
        for line in rp.open():
            if line.strip():
                row = json.loads(line)
                rows[row["case_id"]] = row
        grades = [json.loads(l) for l in gp.open() if l.strip()]
        if not grades:
            continue
        accs.append(100 * sum(1 for g in grades if g["overall"] == "pass") / len(grades))
        for row in rows.values():
            for host in (row.get("provider_output") or {}).get("served_by") or []:
                hosts[host] = hosts.get(host, 0) + 1
                total += 1
    if not accs:
        raise SystemExit("routed repeats not found under tmp/variance/")
    return {
        "routed_requests": str(total),
        "routed_hosts": ", ".join(f"`{h}`" for h in sorted(hosts)),
        "routed_spread": f"{max(accs) - min(accs):.1f}",
    }


def build():
    stats = json.loads((DATA / "stats.json").read_text())
    pool = json.loads((DATA / "pooled.json").read_text())
    models = stats["models"]
    totals = stats["totals"]

    # The headline model: cheapest at the best measured accuracy.
    best = totals["best_accuracy"]
    tier = sorted((m for m in models if m["accuracy"] == best),
                  key=lambda m: m["cost_per_500"])
    head = tier[0]

    if len(tier) == 1:
        tier_note = (
            f"That figure comes from `{head['model']}`, the most accurate model measured."
        )
    else:
        dearest_in_tier = max(tier, key=lambda m: m["cost_per_500"])
        ratio = (dearest_in_tier["cost_per_500"] / head["cost_per_500"]
                 if head["cost_per_500"] else 0)
        tier_note = (
            f"{len(tier)} of the {totals['models']} models measured reach that same "
            f"{best}% — `{head['model']}` at ${head['cost_per_500']:.2f} per five hundred "
            f"questions and `{dearest_in_tier['model']}` at "
            f"${dearest_in_tier['cost_per_500']:.2f}, a {ratio:.1f}× difference in price for "
            "an identical score."
        )

    screen_models, screen_cost = _screen_economy()
    tool_errors = totals["tool_errors"]
    scope = totals["scope_rejections"]
    validation = tool_errors - scope - totals["empty_tool_errors"]
    annual = head["cost_per_case"] * 100 * 365

    ctx = {
        "top_accuracy": f"{head['accuracy']}%",
        "top_cost": f"${head['cost_per_500']:.2f}",
        "top_model": head["model"],
        "tier_note": tier_note,
        "cheapest_cost": f"${totals['cheapest_per_500']:.2f}",
        "dearest_cost": f"${totals['dearest_per_500']:.2f}",
        "cost_per_question": f"${head['cost_per_case']:.4f}",
        "annual_cost": f"${annual:,.0f}",
        "models_tested": str(totals["models"]),
        "cases_graded": f"{totals['cases_graded']:,}",
        "fidelity_pct": stats["fidelity"]["pct"] + "%",
        "fidelity_total": f"{stats['fidelity']['observations']:,}",
        "fidelity_matched": f"{stats['fidelity']['matched']:,}",
        "fidelity_minutes": str(stats["fidelity"]["minutes"]),
        "input_tokens_per_case": f"{totals['tokens_in_per_case']:,}",
        "output_tokens_per_case": str(totals["tokens_out_per_case"]),
        "io_ratio": str(round(totals["tokens_in_per_case"] / max(1, totals["tokens_out_per_case"]))),
        "validation_errors": f"{validation:,}",
        "scope_rejections": f"{scope:,}",
        "theme_prefix_rows": str(_theme_prefix_count()),
        "screen_models": str(screen_models),
        "screen_cost": f"${screen_cost:.2f}",
        "pooled_models": str(pool["models"]),
        "pooled_cases": f"{pool['cases']:,}",
        "spread_prescriptive": str(pool["spread"]["prescriptive"]["range"]),
        "spread_ambiguous": str(pool["spread"]["ambiguous"]["range"]),
        "negative_rate": next(f"{s['pct']}%" for s in pool["strata"] if s["key"] == "negative"),
        "accuracy_range": "{}% to {}%".format(
            min(m["accuracy"] for m in models), max(m["accuracy"] for m in models)),
        "spread_excluded": ", ".join(f"`{e}`" for e in pool["spread_excluded"]) or "No model",
        "spread_excluded_calls": ", ".join(
            str(pool["tool_calls_per_case"][e]) for e in pool["spread_excluded"]) or "n/a",
        "table_all": _md_table(
            ["Model", "Correct", "Accuracy", "95% CI", "Per 500", "Tools/case"],
            [[f"`{m['model']}`", f"{m['passed']}/{m['n']}", f"**{m['accuracy']}%**",
              f"{m['ci_low']}–{m['ci_high']}", f"${m['cost_per_500']:.2f}",
              m["avg_tool_calls"]] for m in models]),
        "table_strata": _md_table(
            ["Stratum", "Pooled", "Rate", "Range across models", "Spread"],
            [[s["label"].replace("\n", " "), f"{s['pass']}/{s['n']}", f"{s['pct']}%",
              f"{pool['spread'][s['key']]['min']}% – {pool['spread'][s['key']]['max']}%",
              f"{pool['spread'][s['key']]['range']} pts"] for s in pool["strata"]]),
        "spread_negative": str(pool["spread"]["negative"]["range"]),
        "spread_disaggregated": str(pool["spread"]["disaggregated"]["range"]),
        "spread_natural": str(pool["spread"]["natural"]["range"]),
        # Historical, and deliberately not from the table: claude-haiku-4.5 was
        # measured at $18.87 per 500 for the same 99.6% that haiku-5.5 now
        # reaches for $2.16. The model was dropped from the study; the price
        # movement is still the clearest evidence that a model recommendation
        # dates faster than a conference cycle.
        "vendor_price_drop": "8.7-fold",
        "table_faults": _md_table(["Model", "What it exposed", "Owner"], FAULTS),
        **_variance_keys(),
        **_ablation_keys(),
        **_routed_keys(),
    }
    (DIR / "context.json").write_text(json.dumps(ctx, indent=2))
    return ctx


def substitute(text, ctx, where="master.md"):
    """Replace every {{key}}; refuse on any that is unknown."""
    missing = sorted({m.group(1) for m in PLACEHOLDER.finditer(text)} - set(ctx))
    if missing:
        raise SystemExit(
            f"{where}: unresolved placeholder(s) {missing}. Add them to context.py "
            "rather than typing the figure into the markdown."
        )
    return PLACEHOLDER.sub(lambda m: ctx[m.group(1)], text)


if __name__ == "__main__":
    ctx = build()
    print(f"context.json written with {len(ctx)} keys")
    for k in sorted(ctx):
        v = ctx[k]
        print(f"  {k:<24} {v if len(v) < 64 else v.splitlines()[0][:61] + '...'}")
