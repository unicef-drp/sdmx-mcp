#!/usr/bin/env python3
"""Build an --area-list for the fidelity sweep from a registry's codelist.

A country-level sweep must exclude regional aggregates, and no rule does that
reliably across registries. This script applies a strategy you choose and then
shows you what it selected and what it dropped, because the failure is silent:
include World and Africa and a "country-level" fidelity claim quietly counts
aggregates; drop a real country and it disappears from the claim.

WHY THERE IS NO AUTOMATIC RULE
------------------------------
Three plausible rules, each measured against UNICEF's CL_COUNTRY (459 codes,
235 of them countries):

  shape (ISO3)      exact here -- 235/235, 0 false positives. Useless under
                    M49, where 004 is Afghanistan and 002 is Africa.
  leaf of hierarchy appealing but wrong here: 441 of 459 codes are leaves,
                    because the hierarchy is shallow (18 parents) and most
                    aggregates have no children either.
  has a parent      inverted between registries. In CL_COUNTRY the aggregates
                    carry parents; in a typical M49 codelist the countries do,
                    sitting under their region.

So pick the strategy that matches your registry, then read the summary. The
counts are the check -- if you expect ~200 countries and get 441, the strategy
is wrong for your data.

Usage:
    # ISO3 registry
    python3 scripts/make_area_list.py --codelist UNICEF/CL_COUNTRY/latest \\
        --pattern '[A-Z]{3}' --out areas.txt

    # M49: countries sit under regions, aggregates do not
    python3 scripts/make_area_list.py --codelist AGENCY/CL_AREA/latest \\
        --with-parent --out m49_countries.txt

    # inspect without writing
    python3 scripts/make_area_list.py --codelist UNICEF/CL_COUNTRY/latest --show 20
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()


def _sdmx_base() -> str:
    base = os.environ.get("SDMX_BASE_URL", "").strip().rstrip("/")
    if not base:
        raise SystemExit("SDMX_BASE_URL must be set (copy .env.example to .env).")
    return base


def fetch_codes(codelist: str) -> list[dict[str, Any]]:
    """Fetch a codelist. The path must stay slash-separated -- the comma form
    the data endpoint uses returns HTTP 200 with only the first 50 codes."""
    url = f"{_sdmx_base()}/codelist/{codelist}/?format=sdmx-json&detail=full"
    response = httpx.get(url, timeout=120.0)
    response.raise_for_status()
    return list(response.json()["data"]["codelists"][0]["codes"])


def select(codes: list[dict[str, Any]], args: argparse.Namespace) -> tuple[set[str], str]:
    ids = {str(c["id"]) for c in codes}
    parents = {str(c["parent"]) for c in codes if c.get("parent")}

    if args.with_parent:
        chosen = {str(c["id"]) for c in codes if c.get("parent")}
        strategy = "has a parent (countries nested under regions)"
    elif args.without_parent:
        chosen = {str(c["id"]) for c in codes if not c.get("parent")}
        strategy = "has no parent"
    elif args.leaves:
        chosen = {i for i in ids if i not in parents}
        strategy = "leaf of the hierarchy (no children)"
    else:
        chosen = set(ids)
        strategy = "all codes"

    if args.pattern:
        before = len(chosen)
        chosen = {i for i in chosen if re.fullmatch(args.pattern, i)}
        strategy += f" + matching /{args.pattern}/ ({before} -> {len(chosen)})"

    if args.exclude:
        drop = {line.strip() for line in Path(args.exclude).read_text().splitlines() if line.strip()}
        chosen -= drop
        strategy += f" - {len(drop)} excluded"

    return chosen, strategy


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--codelist", required=True, help="e.g. UNICEF/CL_COUNTRY/latest")
    parser.add_argument("--out", default="", help="write here; omit to only inspect")
    parser.add_argument("--pattern", default="", help="keep codes matching this regex")
    parser.add_argument("--exclude", default="", help="file of codes to drop")
    parser.add_argument("--show", type=int, default=10, help="sample size in the summary")
    strategy = parser.add_mutually_exclusive_group()
    strategy.add_argument("--with-parent", action="store_true",
                          help="keep codes that have a parent (typical M49 shape)")
    strategy.add_argument("--without-parent", action="store_true",
                          help="keep codes with no parent")
    strategy.add_argument("--leaves", action="store_true",
                          help="keep codes that are nobody's parent")
    args = parser.parse_args()

    codes = fetch_codes(args.codelist)
    names = {str(c["id"]): str(c.get("names", {}).get("en") or c.get("name") or "") for c in codes}
    chosen, strategy_text = select(codes, args)
    dropped = {str(c["id"]) for c in codes} - chosen

    print(f"codelist : {args.codelist}")
    print(f"strategy : {strategy_text}")
    print(f"total    : {len(codes):,}")
    print(f"selected : {len(chosen):,}")
    print(f"dropped  : {len(dropped):,}")
    print()
    print(f"--- selected (first {args.show}) ---")
    for code in sorted(chosen)[: args.show]:
        print(f"  {code:22} {names.get(code, '')[:50]}")
    print(f"--- dropped (first {args.show}) ---")
    for code in sorted(dropped)[: args.show]:
        print(f"  {code:22} {names.get(code, '')[:50]}")
    print()
    print("Check these before trusting the list: an aggregate in the selected "
          "column inflates a country-level claim, and a country in the dropped "
          "column silently disappears from it.")

    if args.out:
        Path(args.out).write_text(
            f"# {len(chosen)} codes from {args.codelist}\n"
            f"# strategy: {strategy_text}\n"
            + "\n".join(f"{code}  # {names.get(code, '')}" for code in sorted(chosen))
            + "\n",
            encoding="utf-8",
        )
        print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
