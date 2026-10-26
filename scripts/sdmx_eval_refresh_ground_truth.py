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
import asyncio, collections, csv, io, json, sys
from pathlib import Path
import httpx

DEFAULT_BASE = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest"
DEFAULT_CONCURRENCY = 8
RETRIES = 6
BACKOFF = 2.0


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
    # The registry throttles by client address, hard and without a Retry-After
    # header, so the caller has to pace itself blind: 494 of 500 cases came
    # back 429 and were "left alone", which reads as a clean no-change refresh
    # while actually having checked nothing. Retry on 429 and 5xx, honouring
    # Retry-After when the registry does send it.
    r = None
    for attempt in range(RETRIES + 1):
        async with sem:
            try:
                r = await client.get(url, timeout=90)
            except httpx.HTTPError as exc:
                if attempt == RETRIES:
                    return case, None, f"transport: {exc}"
                await asyncio.sleep(BACKOFF * (2**attempt))
                continue
        if r.status_code != 429 and r.status_code < 500:
            break
        if attempt == RETRIES:
            break
        delay = BACKOFF * (2**attempt)
        try:
            delay = max(delay, float(r.headers.get("Retry-After") or 0))
        except ValueError:
            pass
        await asyncio.sleep(delay)
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

    changed = unchanged = unresolved = flipped = reclassified = 0
    reasons: collections.Counter = collections.Counter()
    out = []
    for case, vals, err in results:
        gt = case.get("ground_truth") or {}
        exp = (gt.get("expected") or {})
        old = exp.get("value")
        if err is not None:
            unresolved += 1
            reasons[err.split(":")[0][:40]] += 1
            out.append(case); continue
        # The registry answered, so this case's ground truth is confirmed
        # against the live registry right now -- whether or not the value
        # moved. Recorded on every answered case, not only changed ones, so
        # the grader can tell "the model abstained and a value demonstrably
        # exists" (a miss) from "the model abstained and we have not checked"
        # (genuinely unresolved). Without it every such case lands in
        # manual_review and has to be verified by hand.
        gt["verified_against_registry"] = True
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
            # The expectation has to follow the ground truth. Rewriting the
            # value to null while leaving caseType "positive" and
            # expectedBehavior "return_value" leaves the case contradicting
            # itself: the grader reads the top-level fields, so a model that
            # correctly abstained scored manual_review rather than pass, and
            # six withdrawn PV_CHLD_* cases silently cost every model a point.
            # Applied even when the value was already null, so a manifest left
            # half-refreshed by an earlier pass is repaired rather than kept.
            if case.get("caseType") != "negative":
                case["caseType"] = "negative"
                case["expectedBehavior"] = "abstain_no_data"
                reclassified += 1
        else:
            unresolved += 1
            reasons[f"ambiguous: {len(vals)} observations"] += 1
        case["ground_truth"] = gt
        out.append(case)

    dst.write_text("".join(json.dumps(c, ensure_ascii=False) + "\n" for c in out), encoding="utf-8")
    return {"cases": len(cases), "value_changed": changed, "unchanged": unchanged,
            "left_alone": unresolved, "negative_cases_that_now_have_data": flipped,
            "reclassified_to_abstain": reclassified,
            "left_alone_reasons": dict(reasons.most_common(8)),
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
