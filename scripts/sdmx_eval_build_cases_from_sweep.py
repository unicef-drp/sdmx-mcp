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

REGISTRY PORTABILITY
--------------------
Nothing here is UNICEF-specific; the defaults simply name the registry it was
written against. Any SDMX 2.1 service with a geography dimension and an
indicator dimension works:

    # ILO-style registry, different dimension names and codelists
    python3 scripts/sdmx_eval_build_cases_from_sweep.py \
        --flow ILO/DF_YI_ALL_EMP_TEMP_SEX_AGE_NB/1.0 \
        --area-dim REF_AREA --indicator-dim INDICATOR \
        --area-codelist ILO/CL_AREA/latest \
        --indicator-codelist ILO/CL_INDICATOR/latest \
        --hints my_registry_hints.json

Flags: --flow, --area-dim, --indicator-dim, --dim-order, --area-codelist,
--indicator-codelist, --area-pattern, --no-area-filter, --hints, --flow-name.

The geography filter assumes ISO3 codes so regional aggregates are excluded
from a country-level test. --area-pattern changes the shape; --no-area-filter
keeps every code, for registries that do not mix aggregates into the geography
codelist.

--hints is the one piece that cannot be defaulted: the ambiguous stratum needs
the colloquial phrasings a domain expert would use, each mapped to the single
code it should resolve to. A phrase that legitimately matches several
indicators tests whether the agent guesses the same one you did, which
manufactures failures -- so map only phrases with one defensible answer, and
omit the file entirely to skip the stratum.

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
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

# Defaults describe the UNICEF registry this was written against. Every one is
# overridable, because nothing here is specific to UNICEF: any SDMX 2.1 registry
# with a flow, a geography dimension and an indicator dimension can be swept and
# evaluated the same way. See --help and REGISTRY PORTABILITY below.
DEFAULT_FLOW_REF = "UNICEF/GLOBAL_DATAFLOW/1.0"
DEFAULT_AREA_CODELIST = "UNICEF/CL_COUNTRY/latest"
DEFAULT_INDICATOR_CODELIST = "UNICEF/CL_UNICEF_INDICATOR/latest"
DEFAULT_AREA_DIM = "REF_AREA"
DEFAULT_INDICATOR_DIM = "INDICATOR"
ANNUAL = re.compile(r"\d{4}")
# Many registries key geography on ISO3. --area-pattern overrides it for those
# that do not; --no-area-filter disables the distinction entirely for registries
# with no aggregate codes mixed into the geography codelist.
DEFAULT_AREA_PATTERN = r"[A-Z]{3}"

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
# Loaded from --hints when given. Inherently registry-specific: these are the
# colloquial phrasings a domain expert would use, mapped to the one code each
# should resolve to. Ship your own; an empty map simply skips the stratum.
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


@dataclass(slots=True)
class Registry:
    """Everything that differs between SDMX registries."""

    flow_ref: str = DEFAULT_FLOW_REF
    area_codelist: str = DEFAULT_AREA_CODELIST
    indicator_codelist: str = DEFAULT_INDICATOR_CODELIST
    area_dim: str = DEFAULT_AREA_DIM
    indicator_dim: str = DEFAULT_INDICATOR_DIM
    area_pattern: str = DEFAULT_AREA_PATTERN
    filter_areas: bool = True
    dim_order: tuple[str, ...] = ()
    flow_id: str = ""
    flow_name: str = ""

    def area_ok(self, area: str, known: set[str]) -> bool:
        if not self.filter_areas:
            return True
        return bool(re.fullmatch(self.area_pattern, area)) and area in known


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


def _flow_name(flow_ref: str) -> str:
    """Human-readable name for a flow, from the registry rather than hardcoded."""
    try:
        agency, flow_id, version = (flow_ref.split("/") + ["", "", ""])[:3]
        url = (
            f"{_sdmx_base()}/dataflow/{agency}/{flow_id}/{version or 'latest'}"
            "/?format=sdmx-json&detail=full&references=none"
        )
        response = httpx.get(url, timeout=60.0)
        response.raise_for_status()
        for flow in response.json()["data"]["dataflows"]:
            if str(flow.get("id")) == flow_id:
                return str(flow.get("names", {}).get("en") or flow.get("name") or flow_id)
    except Exception:  # noqa: BLE001 -- a display label is not worth failing over
        pass
    return flow_id


def load_observations(
    path: Path, countries: set[str], reg: "Registry"
) -> dict[tuple[str, str, str], dict[str, str]]:
    """{(area, indicator, sex): {year: value}} for countries, annual periods."""
    obs: dict[tuple[str, str, str], dict[str, str]] = defaultdict(dict)
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            area = row.get(reg.area_dim, "")
            period = row.get("TIME_PERIOD", "")
            if not reg.area_ok(area, countries):
                continue
            if not ANNUAL.fullmatch(period):
                continue
            key = (area, row.get(reg.indicator_dim, ""), row.get("SEX", "") or "_T")
            obs[key][period] = row.get("OBS_VALUE", "")
    return obs


def _ambiguous_phrase(indicator: str, name: str = "") -> str | None:
    return AMBIGUOUS_HINTS.get(indicator)


def _prompt(style: str, ctx: dict[str, str]) -> str:
    if style == "prescriptive":
        return (
            f"Use the SDMX MCP only. Call get_single_observation for {ctx['flow_ref']} "
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
    reg: "Registry",
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
        "flow_ref": reg.flow_ref,
    }
    negative = style == "negative"
    filters = {reg.area_dim: area, reg.indicator_dim: indicator}
    if sex:
        filters["SEX"] = sex
    return {
        "case_id": f"{reg.flow_ref}|{style}|{year}|{json.dumps(filters, sort_keys=True)}",
        "flowRef": reg.flow_ref,
        "flowID": reg.flow_id,
        "flowName": reg.flow_name,
        "dimensionOrder": list(reg.dim_order),
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
            reg.area_dim: {"id": area, "name": ctx["area_name"]},
            reg.indicator_dim: {"id": indicator, "name": ctx["indicator_name"]},
            **({"SEX": {"id": sex, "name": sex_name}} if sex else {}),
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
    global AMBIGUOUS_HINTS
    if args.hints:
        AMBIGUOUS_HINTS = json.loads(Path(args.hints).read_text(encoding="utf-8"))
        print(f"[hints] {len(AMBIGUOUS_HINTS)} ambiguity phrases from {args.hints}")

    agency, flow_id, _ = (args.flow.split("/") + ["", "", ""])[:3]
    reg = Registry(
        flow_ref=args.flow,
        area_codelist=args.area_codelist,
        indicator_codelist=args.indicator_codelist,
        area_dim=args.area_dim,
        indicator_dim=args.indicator_dim,
        area_pattern=args.area_pattern,
        filter_areas=not args.no_area_filter,
        dim_order=tuple(args.dim_order.split(",")) if args.dim_order else
                  (args.area_dim, args.indicator_dim, "SEX"),
        flow_id=flow_id,
        flow_name=args.flow_name or _flow_name(args.flow),
    )

    print("[names] fetching codelists")
    area_names = _codelist_names(reg.area_codelist)
    indicator_names = _codelist_names(reg.indicator_codelist)
    names = {
        "area": area_names,
        "indicator": indicator_names,
        "sex": {"_T": "Total", "F": "Female", "M": "Male"},
    }

    print(f"[truth] indexing {args.ground_truth}")
    obs = load_observations(Path(args.ground_truth), set(area_names), reg)
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
            add(_case(style, key[0], key[1], key[2], year, value, names, reg))

    target = int(args.count * share["ambiguous"])
    for _ in range(target * 8):
        if sum(1 for c in cases if c["promptStyle"] == "ambiguous") >= target:
            break
        if not ambiguous_pool:
            break
        key = rng.choice(ambiguous_pool)
        year, value = pick_year(key)
        phrase = _ambiguous_phrase(key[1], indicator_names.get(key[1], "")) or ""
        add(_case("ambiguous", key[0], key[1], key[2], year, value, names, reg, phrase))

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
        add(_case("disaggregated", area, indicator, sex, year, value, names, reg))

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
        add(_case("negative", key[0], key[1], key[2], rng.choice(missing), None, names, reg))

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
    reg_group = parser.add_argument_group(
        "registry portability",
        "Defaults target the UNICEF registry. Override these for any other "
        "SDMX 2.1 service; nothing in this script is UNICEF-specific.",
    )
    reg_group.add_argument("--flow", default=DEFAULT_FLOW_REF)
    reg_group.add_argument("--flow-name", default="")
    reg_group.add_argument("--area-codelist", default=DEFAULT_AREA_CODELIST)
    reg_group.add_argument("--indicator-codelist", default=DEFAULT_INDICATOR_CODELIST)
    reg_group.add_argument("--area-dim", default=DEFAULT_AREA_DIM)
    reg_group.add_argument("--indicator-dim", default=DEFAULT_INDICATOR_DIM)
    reg_group.add_argument("--dim-order", default="", help="comma-separated; defaults to area,indicator,SEX")
    reg_group.add_argument("--area-pattern", default=DEFAULT_AREA_PATTERN,
                           help="regex an area code must match to count as a country")
    reg_group.add_argument("--no-area-filter", action="store_true",
                           help="keep every area code, for registries without aggregates mixed in")
    reg_group.add_argument("--hints", default="",
                           help="JSON file mapping indicator code -> colloquial phrase, for ambiguous cases")
    args = parser.parse_args()
    print(json.dumps(build(args), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
