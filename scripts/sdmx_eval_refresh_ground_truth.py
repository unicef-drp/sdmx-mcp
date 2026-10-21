#!/usr/bin/env python3
"""Re-fetch each manifest case's expected value from the live registry.

Ground truth captured from a sweep ages: UNICEF restated the PV_CHLD_*
child-poverty series between the 5th and the 20th of October 2026 (source
changed from "DHS VII" to "DHS 2018 (IPUMS)", MLI 2018 from 18.5535945892334
to 23.67173993). Every model then "failed" those cases while answering
correctly, capping the prescriptive stratum at 94% for all of them.

Re-fetching costs nothing -- it is direct registry reads, no model calls -- and
lets already-collected responses be re-graded, so staleness is removed without
re-running a single model.

One query per case, bounded concurrency. A case whose value cannot be
re-confirmed is left exactly as it was rather than guessed at.
"""
import asyncio, csv, io, json, sys
from pathlib import Path
import httpx

DEFAULT_BASE = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest"
DEFAULT_CONCURRENCY = 8


def key_for(case):
    order = case["dimensionOrder"]
    filt = case["filters"]
    return ".".join(filt.get(d, "") for d in order)


def _rest_flow_ref(flow_ref: str) -> str:
    """The REST path spells a flowRef with commas; the manifest may use slashes."""
    return str(flow_ref or "").replace("/", ",")


async def fetch(client, sem, case, base):
    period = case.get("timePeriod")
    flow = _rest_flow_ref(case.get("flowRef") or "")
    url = (f"{base}/data/{flow}/{key_for(case)}"
           f"?startPeriod={period}&endPeriod={period}&format=csv&labels=id")
    async with sem:
        try:
            r = await client.get(url, timeout=90)
        except httpx.HTTPError as exc:
            return case, None, f"transport: {exc}"
    if r.status_code == 404:
        return case, [], None           # genuinely no data
    if r.status_code >= 400:
        return case, None, f"HTTP {r.status_code}"
    rows = list(csv.DictReader(io.StringIO(r.text)))
    vals = [row.get("OBS_VALUE", "") for row in rows if row.get("TIME_PERIOD") == str(period)]
    return case, [v for v in vals if v != ""], None


async def run(src: Path, dst: Path, base: str, concurrency: int) -> dict:
    cases = [json.loads(l) for l in src.read_text().splitlines() if l.strip()]
    sem = asyncio.Semaphore(concurrency)
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[fetch(client, sem, c, base) for c in cases])

    changed = unchanged = unresolved = flipped = 0
    out = []
    for case, vals, err in results:
        gt = case.get("ground_truth") or {}
        exp = (gt.get("expected") or {})
        old = exp.get("value")
        if err is not None:
            unresolved += 1
            out.append(case); continue
        # Exactly one observation is required to pin a single value; anything
        # else leaves the case untouched rather than inventing a ground truth.
        if len(vals) == 1:
            new = vals[0]
            if str(new) != str(old):
                exp["value"] = new
                gt["status"] = "resolved"
                gt["resolved_time_periods"] = [str(case.get("timePeriod"))]
                gt["source"] = "live_registry_refresh"
                if case.get("caseType") == "negative":
                    flipped += 1
                changed += 1
            else:
                unchanged += 1
        elif len(vals) == 0:
            if old is not None:
                exp["value"] = None
                gt["status"] = "no_data"
                gt["resolved_time_periods"] = []
                gt["source"] = "live_registry_refresh"
                changed += 1
            else:
                unchanged += 1
        else:
            unresolved += 1
        case["ground_truth"] = gt
        out.append(case)

    dst.write_text("".join(json.dumps(c, ensure_ascii=False) + "\n" for c in out), encoding="utf-8")
    return {"cases": len(cases), "value_changed": changed, "unchanged": unchanged,
            "left_alone": unresolved, "negative_cases_that_now_have_data": flipped,
            "manifest": str(dst)}


def main() -> int:
    import argparse
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--base-url", default=DEFAULT_BASE,
                        help="SDMX REST base URL; override for another registry.")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(
        run(args.manifest, args.out, args.base_url.rstrip("/"), max(1, args.concurrency))
    ), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
