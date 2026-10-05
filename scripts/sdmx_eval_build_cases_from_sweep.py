#!/usr/bin/env python3
"""Build a stratified agent-eval manifest from the fidelity sweep's ground truth.

Two problems with deriving cases the old way, both of which this fixes.

**The answer key.** build-cases re-queried the registry per case to learn the
expected value. The fidelity sweep already produced an exhaustively verified
answer for every observation in scope -- 466,360 of them, each confirmed
identical between the MCP and a direct API call -- so cases can be drawn from
that instead, with no extra registry traffic and a stronger guarantee.

**What the prompt actually tests.** The existing template hands the agent the
tool name and the exact filter codes:

    Call get_single_observation for <flow> with filters REF_AREA=FRA,
    INDICATOR=CME_MRY0T4, SEX=_T, and time=latest.

An agent that passes that has demonstrated it can relay three codes. It says
nothing about whether it can find the right indicator among 350, pick the right
disaggregation, or decline when the data is not there. This builder emits a mix
of prompt styles so the grade can be read per stratum:

    prescriptive   codes supplied -- the old style, kept as a baseline
    natural        indicator and country by name, agent resolves the codes
    ambiguous      deliberately vague subject wording, one correct answer
    disaggregated  a sex-specific slice where picking the total is wrong
    negative       a cell with no data -- the agent must abstain

Every style except `negative` carries a known-correct value, so the existing
grader scores them unchanged: a wrong answer means the agent resolved to the
wrong series, which is exactly the failure worth measuring.

Usage:
    python3 scripts/sdmx_eval_build_cases_from_sweep.py \
        --ground-truth tmp/fidelity/ground_truth.csv \
        --manifest tmp/sdmx_eval/cases.jsonl \
        --count 500
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

FLOW_REF = "UNICEF/GLOBAL_DATAFLOW/1.0"
FLOW_ID = "GLOBAL_DATAFLOW"
FLOW_NAME = "Cross-sector indicators"
DIM_ORDER = ["REF_AREA", "INDICATOR", "SEX"]
ANNUAL = re.compile(r"\d{4}")
ISO3 = re.compile(r"[A-Z]{3}")

ANSWER_CONTRACT = (
    " Return a single JSON object only. In answer_text, use exactly this terse "
    "format when resolved: value: <value>; period: <period>. If the official MCP "
    "query has no observations or is unresolved, use answer_text: value: null."
)

# Colloquial restatements that still have exactly one right answer. Each maps to
# a single indicator code, never a family.
#
# Prefix patterns were tried and removed. "water and sanitation access" matches
# dozens of WS_* indicators, so pinning one as the expected answer tests whether
# the agent guesses the same one we did, not whether it resolves the phrase
# correctly -- it manufactures failures. An ambiguous case is only fair when a
# competent analyst would land on one specific series.
AMBIGUOUS_HINTS: dict[str, str] = {
    "CME_MRY0T4": "child mortality before age five",
    "CME_MRM0": "newborn deaths in the first month of life",
    "CME_MRY0": "deaths among infants under one year old",
    "IM_DTP3": "DTP3 immunisation coverage",
    "NT_ANT_HAZ_NE2": "child stunting",
    "NT_ANT_WHZ_NE2": "child wasting",
    "DM_POP_TOT": "the total population",
    "DM_LIFE_EXP": "life expectancy at birth",
    "MNCH_SAB": "births attended by skilled health personnel",
    "WS_PPL_W-SM": "safely managed drinking water access",
    "WS_PPL_S-SM": "safely managed sanitation access",
}


def _sdmx_base() -> str:
    base = os.environ.get("SDMX_BASE_URL", "").strip().rstrip("/")
    if not base:
        raise SystemExit("SDMX_BASE_URL must be set (copy .env.example to .env).")
    return base


def _codelist_names(codelist: str) -> dict[str, str]:
    """id -> English label. The path must stay slash-separated; the comma form
    returns HTTP 200 with only the first 50 codes."""
    url = f"{_sdmx_base()}/codelist/{codelist}/?format=sdmx-json&detail=full"
    response = httpx.get(url, timeout=120.0)
    response.raise_for_status()
    out: dict[str, str] = {}
    for code in response.json()["data"]["codelists"][0]["codes"]:
        name = code.get("names", {}).get("en") or code.get("name") or ""
        out[str(code["id"])] = str(name)
    return out


def load_observations(
    path: Path, countries: set[str]
) -> dict[tuple[str, str, str], dict[str, str]]:
    """{(area, indicator, sex): {year: value}} for countries, annual periods."""
    obs: dict[tuple[str, str, str], dict[str, str]] = defaultdict(dict)
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            area = row.get("REF_AREA", "")
            period = row.get("TIME_PERIOD", "")
            if not ISO3.fullmatch(area) or area not in countries:
                continue
            if not ANNUAL.fullmatch(period):
                continue
            key = (area, row.get("INDICATOR", ""), row.get("SEX", "") or "_T")
            obs[key][period] = row.get("OBS_VALUE", "")
    return obs


def _ambiguous_phrase(indicator: str, name: str = "") -> str | None:
    return AMBIGUOUS_HINTS.get(indicator)


def _prompt(style: str, ctx: dict[str, str]) -> str:
    if style == "prescriptive":
        return (
            f"Use the SDMX MCP only. Call get_single_observation for {FLOW_REF} "
            f"with filters REF_AREA={ctx['area']}, INDICATOR={ctx['indicator']}, "
            f"SEX={ctx['sex']}, and time={ctx['year']}." + ANSWER_CONTRACT
        )
    if style == "natural":
        return (
            f"Use the SDMX MCP only. What was \"{ctx['indicator_name']}\" in "
            f"{ctx['area_name']} in {ctx['year']}?" + ANSWER_CONTRACT
        )
    if style == "ambiguous":
        return (
            f"Use the SDMX MCP only. What was {ctx['phrase']} in "
            f"{ctx['area_name']} in {ctx['year']}?" + ANSWER_CONTRACT
        )
    if style == "disaggregated":
        return (
            f"Use the SDMX MCP only. What was \"{ctx['indicator_name']}\" for "
            f"{ctx['sex_name'].lower()} in {ctx['area_name']} in {ctx['year']}? "
            f"Report the {ctx['sex_name'].lower()} figure specifically, not the total."
            + ANSWER_CONTRACT
        )
    if style == "negative":
        return (
            f"Use the SDMX MCP only. What was \"{ctx['indicator_name']}\" in "
            f"{ctx['area_name']} in {ctx['year']}?" + ANSWER_CONTRACT
        )
    raise ValueError(f"unknown style {style!r}")


def _case(
    style: str,
    area: str,
    indicator: str,
    sex: str,
    year: str,
    value: str | None,
    names: dict[str, dict[str, str]],
    phrase: str = "",
) -> dict[str, Any]:
    sex_name = names["sex"].get(sex, sex)
    ctx = {
        "area": area,
        "indicator": indicator,
        "sex": sex,
        "year": year,
        "area_name": names["area"].get(area, area),
        "indicator_name": names["indicator"].get(indicator, indicator),
        "sex_name": sex_name,
        "phrase": phrase,
    }
    negative = style == "negative"
    filters = {"REF_AREA": area, "INDICATOR": indicator, "SEX": sex}
    return {
        "case_id": f"{FLOW_REF}|{style}|{year}|{json.dumps(filters, sort_keys=True)}",
        "flowRef": FLOW_REF,
        "flowID": FLOW_ID,
        "flowName": FLOW_NAME,
        "dimensionOrder": DIM_ORDER,
        "filters": filters,
        "timePeriod": year,
        "lastNObservations": None,
        "queryMode": "time_period",
        "registryProfile": "sparse",
        "caseType": "negative" if negative else "positive",
        "expectedBehavior": "abstain_no_data" if negative else "return_value",
        # Read this to break a grade down by stratum rather than one blended number.
        "promptStyle": style,
        "wildcardDimensions": [],
        "dimensions": {
            "REF_AREA": {"id": area, "name": ctx["area_name"]},
            "INDICATOR": {"id": indicator, "name": ctx["indicator_name"]},
            "SEX": {"id": sex, "name": sex_name},
        },
        "prompt": _prompt(style, ctx),
        "ground_truth": {
            "status": "no_data" if negative else "resolved",
            "source": "fidelity_sweep_ground_truth",
            "resolved_time_periods": [] if negative else [year],
            "expected": {
                "status": "deterministic",
                "value": None if negative else value,
            },
        },
    }


def build(args: argparse.Namespace) -> dict[str, Any]:
    rng = random.Random(args.seed)
    print("[names] fetching codelists")
    area_names = _codelist_names("UNICEF/CL_COUNTRY/latest")
    indicator_names = _codelist_names("UNICEF/CL_UNICEF_INDICATOR/latest")
    names = {
        "area": area_names,
        "indicator": indicator_names,
        "sex": {"_T": "Total", "F": "Female", "M": "Male"},
    }

    print(f"[truth] indexing {args.ground_truth}")
    obs = load_observations(Path(args.ground_truth), set(area_names))
    print(f"[truth] {len(obs):,} series")

    # Series carrying a real F/M split -- the only ones where asking for a
    # sex-specific figure is a meaningful test rather than a trick question.
    by_area_ind: dict[tuple[str, str], set[str]] = defaultdict(set)
    for area, indicator, sex in obs:
        by_area_ind[(area, indicator)].add(sex)
    disagg = [k for k, sexes in by_area_ind.items() if {"F", "M"} <= sexes]

    totals = [k for k in obs if k[2] == "_T"]
    ambiguous_pool = [
        k for k in totals if _ambiguous_phrase(k[1], indicator_names.get(k[1], ""))
    ]

    share = {
        "prescriptive": 0.20,
        "natural": 0.35,
        "ambiguous": 0.15,
        "disaggregated": 0.15,
        "negative": 0.15,
    }
    cases: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(case: dict[str, Any]) -> None:
        if case["case_id"] in seen:
            return
        seen.add(case["case_id"])
        cases.append(case)

    def pick_year(key: tuple[str, str, str]) -> tuple[str, str]:
        years = obs[key]
        year = rng.choice(sorted(years))
        return year, years[year]

    for style in ("prescriptive", "natural"):
        target = int(args.count * share[style])
        for _ in range(target * 4):
            if sum(1 for c in cases if c["promptStyle"] == style) >= target:
                break
            key = rng.choice(totals)
            year, value = pick_year(key)
            add(_case(style, key[0], key[1], key[2], year, value, names))

    target = int(args.count * share["ambiguous"])
    for _ in range(target * 8):
        if sum(1 for c in cases if c["promptStyle"] == "ambiguous") >= target:
            break
        if not ambiguous_pool:
            break
        key = rng.choice(ambiguous_pool)
        year, value = pick_year(key)
        phrase = _ambiguous_phrase(key[1], indicator_names.get(key[1], "")) or ""
        add(_case("ambiguous", key[0], key[1], key[2], year, value, names, phrase))

    target = int(args.count * share["disaggregated"])
    for _ in range(target * 8):
        if sum(1 for c in cases if c["promptStyle"] == "disaggregated") >= target:
            break
        if not disagg:
            break
        area, indicator = rng.choice(disagg)
        sex = rng.choice(["F", "M"])
        key = (area, indicator, sex)
        if key not in obs:
            continue
        year, value = pick_year(key)
        add(_case("disaggregated", area, indicator, sex, year, value, names))

    # Negatives: a real indicator and a real country, but a year that series
    # genuinely lacks. Derived from the same verified data, so "no data" is a
    # fact about the registry rather than an assumption.
    target = int(args.count * share["negative"])
    all_years = [str(y) for y in range(args.start, args.end + 1)]
    for _ in range(target * 20):
        if sum(1 for c in cases if c["promptStyle"] == "negative") >= target:
            break
        key = rng.choice(totals)
        missing = [y for y in all_years if y not in obs[key]]
        if not missing:
            continue
        add(_case("negative", key[0], key[1], key[2], rng.choice(missing), None, names))

    rng.shuffle(cases)
    out = Path(args.manifest)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as handle:
        for case in cases:
            handle.write(json.dumps(case, ensure_ascii=False) + "\n")

    breakdown: dict[str, int] = defaultdict(int)
    for case in cases:
        breakdown[case["promptStyle"]] += 1
    return {
        "cases_written": len(cases),
        "by_prompt_style": dict(sorted(breakdown.items())),
        "manifest_path": str(out),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--ground-truth", default="tmp/fidelity/ground_truth.csv")
    parser.add_argument("--manifest", default="tmp/sdmx_eval/cases.jsonl")
    parser.add_argument("--count", type=int, default=500)
    parser.add_argument("--start", type=int, default=2015)
    parser.add_argument("--end", type=int, default=2024)
    parser.add_argument("--seed", type=int, default=20261005)
    args = parser.parse_args()
    print(json.dumps(build(args), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
