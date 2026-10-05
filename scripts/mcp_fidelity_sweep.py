#!/usr/bin/env python3
"""Exhaustive fidelity sweep: MCP compact tools vs the raw SDMX API.

This runner intentionally does not call an LLM. The question it answers --
"does the MCP return what the API returns?" -- is a property of deterministic
software, and putting a model in the loop could only add false failures (the
model picks the wrong indicator) and false passes (the model is right while the
MCP quietly drops a dimension). The agent-capability question is a separate,
sampled experiment; see scripts/mcp_fidelity_agent_sample.py.

Ground truth comes from one bulk CSV request per flow -- the whole decade of
UNICEF:GLOBAL_DATAFLOW is a single ~116MB download -- so the API side costs ~1
request rather than ~500k. The MCP side is then driven one series at a time.

Key design note: (INDICATOR, REF_AREA, TIME_PERIOD) is NOT unique. Within
GLOBAL_DATAFLOW, 36.2% of such triples carry more than one observation, because
SEX disaggregates them. Adding SEX makes the key exact -- 466,360 keys for
466,360 observations, zero collisions -- so the sweep keys on every dimension
the flow declares and pins all of them on each MCP call, leaving the registry's
auto-apply-total policy no room to resolve a different series than the one under
comparison. The dimension list is read from the DSD at runtime rather than
hardcoded: GLOBAL_DATAFLOW's CSV also carries an AGE column, but AGE is an
attribute there, not a dimension, and filtering on it is rejected outright.

The uniqueness assumption is enforced, not assumed -- if a flow's declared
dimensions fail to separate two observations, the run aborts instead of
comparing one MCP value against an arbitrary one of several API values.

Scope as measured on 2026-10-05: 350 indicators x 235 countries x 10 years is
822,500 possible cells, of which 466,360 carry data (56.7% dense) across 79,266
series.

Two modes, because call count is the binding constraint. The registry throttles
hard: --mode series needs ~79k calls and draws HTTP 429 long before finishing,
even paced at 2/sec. --mode bulk (the default) filters query_data to one
indicator and gets every country, year and disaggregation back in one response
-- 350 calls for the identical comparison, a 226x reduction, under 3 minutes.

Result of the first full run (2026-10-05, --mode bulk --rate 2):
    466,360 / 466,360 observations match. 100.0000%. Zero errors.

Usage:
    # full sweep (default mode), ~3 min
    python3 scripts/mcp_fidelity_sweep.py --rate 2

    # exercise the compact projection instead; expect throttling at scale
    python3 scripts/mcp_fidelity_sweep.py --mode series --limit 2000 --rate 2 --resume

    # drive the deployed server instead of importing it
    python3 scripts/mcp_fidelity_sweep.py --endpoint https://sdmx-mcp.fly.dev/mcp

Exit code is 1 if any observation mismatched, so CI can gate on it.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import math
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

import httpx
from dotenv import load_dotenv

# Same .env the server reads, so the sweep and the MCP always agree on which
# registry they are talking to.
load_dotenv()

DEFAULT_FLOW = "UNICEF/GLOBAL_DATAFLOW/1.0"
DEFAULT_START = 2015
DEFAULT_END = 2024
ANNUAL = re.compile(r"\d{4}")
ISO3 = re.compile(r"[A-Z]{3}")

# Time dimensions are the observation axis, not part of the series key.
TIME_DIMS = {"TIME_PERIOD", "TIME"}


# ---------------------------------------------------------------- ground truth


@dataclass(slots=True)
class Scope:
    flow_ref: str = DEFAULT_FLOW
    start: int = DEFAULT_START
    end: int = DEFAULT_END
    countries_only: bool = True
    annual_only: bool = True


def _sdmx_base() -> str:
    base = os.environ.get("SDMX_BASE_URL", "").strip().rstrip("/")
    if not base:
        raise SystemExit(
            "SDMX_BASE_URL must be set (copy .env.example to .env, or export it)."
        )
    return base


def _flow_path(flow_ref: str) -> str:
    return flow_ref.replace("/", ",")


def ground_truth_url(scope: Scope) -> str:
    return (
        f"{_sdmx_base()}/data/{_flow_path(scope.flow_ref)}/all"
        f"?startPeriod={scope.start}&endPeriod={scope.end}&format=csv&labels=id"
    )


def fetch_ground_truth(scope: Scope, cache: Path, refresh: bool) -> Path:
    """Download the flow's decade as one CSV. Cached -- it is large and static."""
    if cache.exists() and not refresh:
        print(f"[truth] reusing cache {cache} ({cache.stat().st_size:,} bytes)")
        return cache
    url = ground_truth_url(scope)
    print(f"[truth] GET {url}")
    cache.parent.mkdir(parents=True, exist_ok=True)
    tmp = cache.with_suffix(".part")
    started = time.monotonic()
    with httpx.stream("GET", url, timeout=900.0) as response:
        response.raise_for_status()
        with tmp.open("wb") as handle:
            for chunk in response.iter_bytes(1 << 20):
                handle.write(chunk)
    tmp.replace(cache)
    print(
        f"[truth] {cache.stat().st_size:,} bytes in {time.monotonic() - started:.0f}s"
    )
    return cache


def load_country_codes(codelist: str = "UNICEF/CL_COUNTRY/latest") -> set[str]:
    """Fetch the country codelist.

    The path must stay slash-separated. The comma form the data endpoint uses
    (UNICEF,CL_COUNTRY,latest) also returns HTTP 200 here but silently truncates
    to the first 50 codes, which filters the whole sweep down to nothing without
    raising anything.
    """
    url = f"{_sdmx_base()}/codelist/{codelist}/?format=sdmx-json&detail=full"
    response = httpx.get(url, timeout=120.0)
    response.raise_for_status()
    codes = {c["id"] for c in response.json()["data"]["codelists"][0]["codes"]}
    if len(codes) < 200:
        raise SystemExit(
            f"Country codelist looks truncated ({len(codes)} codes from {url}). "
            "Refusing to run: the area filter would silently drop real data."
        )
    return codes


def series_key(row: dict[str, str], dims: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(row.get(dim, "") or "" for dim in dims)


def index_ground_truth(
    path: Path, scope: Scope, countries: set[str], dims: tuple[str, ...]
) -> tuple[dict[tuple[str, ...], dict[str, str]], dict[tuple[str, ...], str]]:
    """Return {series_key: {year: value}} and {series_key: unit}.

    Rows outside the scope (regional aggregates, sub-annual periods) are skipped
    rather than silently folded in -- a monthly observation compared against an
    annual one would read as a fidelity failure that is really a scoping bug.
    """
    values: dict[tuple[str, ...], dict[str, str]] = defaultdict(dict)
    units: dict[tuple[str, ...], str] = {}
    kept = skipped_area = skipped_period = 0
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            area = row.get("REF_AREA", "")
            period = row.get("TIME_PERIOD", "")
            if scope.countries_only and (
                not ISO3.fullmatch(area) or area not in countries
            ):
                skipped_area += 1
                continue
            if scope.annual_only and not ANNUAL.fullmatch(period):
                skipped_period += 1
                continue
            key = series_key(row, dims)
            if period in values[key]:
                # The key must be exact, or one MCP value gets compared against
                # an arbitrary one of several API values and the whole run is
                # meaningless. Fail loudly rather than silently picking one.
                raise SystemExit(
                    f"Series key {dims} is not unique: {key} has multiple rows "
                    f"for {period}. Add the missing dimension before sweeping."
                )
            values[key][period] = row.get("OBS_VALUE", "")
            units.setdefault(key, row.get("UNIT_MEASURE", "") or "")
            kept += 1
    print(
        f"[truth] {kept:,} observations in scope across {len(values):,} series "
        f"(skipped {skipped_area:,} non-country rows, {skipped_period:,} sub-annual)"
    )
    return values, units


# ------------------------------------------------------------------- MCP side


class RateLimiter:
    """Minimum spacing between MCP calls.

    Concurrency alone is not enough. sdmx.data.unicef.org throttles sustained
    request streams, and the MCP surfaces a throttled response as
    status=rate_limited with httpStatus=429. An unpaced 900-series run drew 429
    on 399 of them. Pace the sweep rather than arguing with a shared public
    service; throttled series are not fidelity failures.
    """

    def __init__(self, per_second: float) -> None:
        self._interval = 1.0 / per_second if per_second > 0 else 0.0
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def wait(self) -> None:
        if not self._interval:
            return
        async with self._lock:
            now = asyncio.get_running_loop().time()
            delay = max(self._next - now, 0.0)
            self._next = max(now, self._next) + self._interval
        if delay:
            await asyncio.sleep(delay)


class McpDriver:
    """Calls the MCP either in-process or over Streamable HTTP."""

    def __init__(self, endpoint: str | None) -> None:
        self.endpoint = endpoint
        self._client: Any = None
        self._http: httpx.AsyncClient | None = None
        self._rpc_id = 0

    async def __aenter__(self) -> "McpDriver":
        if self.endpoint:
            self._http = httpx.AsyncClient(timeout=120.0)
        else:
            # server.py lives at the repo root, one level up from scripts/.
            root = str(Path(__file__).resolve().parent.parent)
            if root not in sys.path:
                sys.path.insert(0, root)
            import fastmcp  # noqa: PLC0415 -- optional until in-process mode is used
            import server  # noqa: PLC0415

            self._client = fastmcp.Client(server.mcp)
            await self._client.__aenter__()
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._http is not None:
            await self._http.aclose()
        if self._client is not None:
            await self._client.__aexit__(*exc)

    async def call(self, name: str, arguments: dict[str, Any]) -> Any:
        if self._client is not None:
            result = await self._client.call_tool(name, arguments)
            return _unwrap(result)
        assert self._http is not None
        self._rpc_id += 1
        response = await self._http.post(
            self.endpoint,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            json={
                "jsonrpc": "2.0",
                "id": self._rpc_id,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            },
        )
        response.raise_for_status()
        return _unwrap(response.json())


def _unwrap(result: Any) -> Any:
    """Pull the JSON payload out of whichever envelope the transport used."""
    if isinstance(result, dict):
        inner = result.get("result", result)
        content = inner.get("content") if isinstance(inner, dict) else None
        if isinstance(content, list) and content:
            text = content[0].get("text")
            if isinstance(text, str):
                return json.loads(text)
        if isinstance(inner, dict):
            return inner
        return {}
    data = getattr(result, "data", None)
    if isinstance(data, dict):
        return data
    content = getattr(result, "content", None)
    if content:
        text = getattr(content[0], "text", None)
        if isinstance(text, str):
            return json.loads(text)
    return {}


# ----------------------------------------------------------------- comparison


@dataclass(slots=True)
class Row:
    key: tuple[str, ...]  # positionally aligned with the discovered dimensions
    year: str
    mcp_value: str
    api_value: str
    mcp_unit: str
    api_unit: str
    verdict: str
    note: str = ""

    def as_csv(self) -> list[str]:
        return [
            *self.key, self.year, self.mcp_value, self.api_value,
            self.mcp_unit, self.api_unit, self.verdict, self.note,
        ]


@dataclass(slots=True)
class Tally:
    match: int = 0
    mismatch: int = 0
    missing_in_mcp: int = 0
    extra_in_mcp: int = 0
    error: int = 0
    series_done: int = 0
    notes: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    @property
    def compared(self) -> int:
        return self.match + self.mismatch + self.missing_in_mcp + self.extra_in_mcp


def values_agree(a: str, b: str, rel_tol: float) -> bool:
    """Numeric when both parse, exact string otherwise.

    SDMX serialises the same number different ways ("88" vs "88.0"), so a string
    compare alone would report spurious mismatches.
    """
    if a == b:
        return True
    try:
        fa, fb = float(a), float(b)
    except (TypeError, ValueError):
        return False
    if math.isnan(fa) and math.isnan(fb):
        return True
    return math.isclose(fa, fb, rel_tol=rel_tol, abs_tol=0.0)


async def discover_dimensions(driver: McpDriver, flow_ref: str) -> tuple[str, ...]:
    """Ask the flow which dimensions it actually has.

    Hardcoding these is a trap: GLOBAL_DATAFLOW's CSV carries an AGE column, but
    AGE is an *attribute* there, not a dimension -- filtering on it returns
    "Unknown dimension(s) in filters: AGE". Deriving the key from the DSD keeps
    the sweep correct across flows that disaggregate differently.
    """
    payload = await driver.call("list_dimensions", {"flowRef": flow_ref})
    if isinstance(payload, list):
        items: Any = payload
    else:
        items = payload.get("result") or payload.get("dimensions") or []
    dims = tuple(
        str(d.get("id"))
        for d in items
        if str(d.get("id", "")).upper() not in TIME_DIMS
    )
    if not dims:
        raise SystemExit(f"Could not read dimensions for {flow_ref}: {payload}")
    return dims


async def compare_indicator_bulk(
    driver: McpDriver,
    scope: Scope,
    dims: tuple[str, ...],
    indicator: str,
    truth: dict[tuple[str, ...], dict[str, str]],
    units: dict[tuple[str, ...], str],
    countries: set[str],
    rel_tol: float,
    retries: int = 4,
    backoff: float = 2.0,
    limiter: "RateLimiter | None" = None,
) -> tuple[list[Row], str | None]:
    """Verify every observation for one indicator in a single MCP call.

    The per-series path needs 79,266 calls to cover 466,360 observations, which
    the registry throttles long before it finishes. query_data filtered to one
    indicator returns every country, year and disaggregation for it at once --
    ~350 calls for the whole flow, a 226x reduction, and the comparison is
    exactly as complete.

    The tradeoff is honest and worth stating: this exercises the raw data path,
    not the compact projection that get_time_series applies. Pair it with
    --sample-compact to cover that separately.
    """
    args = {
        "flowRef": scope.flow_ref,
        "filters": {"INDICATOR": indicator},
        "startPeriod": str(scope.start),
        "endPeriod": str(scope.end),
        "format": "csv",
        "labels": "id",
        "maxObs": 100_000,
    }

    payload: Any = {}
    detail = ""
    for attempt in range(retries + 1):
        try:
            if limiter is not None:
                await limiter.wait()
            payload = await driver.call("query_data", args)
        except Exception as exc:  # noqa: BLE001
            detail = f"{type(exc).__name__}: {exc}"
            payload = {}
        else:
            if isinstance(payload, dict) and payload.get("status") == "resolved":
                break
            detail = (
                f"status={payload.get('status')} "
                f"http={payload.get('httpStatus')} "
                f"{str(payload.get('message'))[:120]}"
            ).strip()
            if not payload.get("retryable"):
                break
        if attempt < retries:
            await asyncio.sleep(backoff * (2**attempt))

    if not isinstance(payload, dict) or payload.get("status") != "resolved":
        return [], detail or "unresolved"

    notes = payload.get("notes") or {}
    if notes.get("truncated"):
        # Silently comparing a truncated response would report every dropped
        # observation as MISSING_IN_MCP -- a fabricated fidelity failure.
        return [], f"TRUNCATED at maxObs; totalRows={notes.get('totalRows')}"

    body = payload.get("raw_csv") or ""
    got: dict[tuple[str, ...], dict[str, str]] = defaultdict(dict)
    for row in csv.DictReader(io.StringIO(body)):
        area = row.get("REF_AREA", "")
        period = row.get("TIME_PERIOD", "")
        if scope.countries_only and (not ISO3.fullmatch(area) or area not in countries):
            continue
        if scope.annual_only and not ANNUAL.fullmatch(period):
            continue
        got[series_key(row, dims)][period] = row.get("OBS_VALUE", "")

    expected = {k: v for k, v in truth.items() if k[dims.index("INDICATOR")] == indicator}

    rows: list[Row] = []
    for key in sorted(set(expected) | set(got)):
        want = expected.get(key, {})
        have = got.get(key, {})
        for year in sorted(set(want) | set(have)):
            api_v = want.get(year)
            mcp_v = have.get(year)
            if api_v is not None and mcp_v is not None:
                verdict = "match" if values_agree(mcp_v, api_v, rel_tol) else "MISMATCH"
            elif api_v is not None:
                verdict = "MISSING_IN_MCP"
            else:
                verdict = "EXTRA_IN_MCP"
            rows.append(
                Row(
                    key=key,
                    year=year,
                    mcp_value="" if mcp_v is None else mcp_v,
                    api_value="" if api_v is None else api_v,
                    mcp_unit="",
                    api_unit=units.get(key, ""),
                    verdict=verdict,
                )
            )
    return rows, None


async def compare_series(
    driver: McpDriver,
    scope: Scope,
    dims: tuple[str, ...],
    key: tuple[str, ...],
    truth: dict[str, str],
    truth_unit: str,
    rel_tol: float,
    retries: int = 3,
    backoff: float = 2.0,
    limiter: "RateLimiter | None" = None,
) -> tuple[list[Row], str | None]:
    # Pin every dimension the flow declares, so the registry's auto-apply-total
    # policy cannot resolve to a different series than the one under comparison.
    filters = {dim: val for dim, val in zip(dims, key) if val}

    args = {
        "flowRef": scope.flow_ref,
        "filters": filters,
        "time": f"{scope.start}:{scope.end}",
        "maxObservations": 500,
    }

    # The registry pushes back under concurrency: a series that resolves fine on
    # its own can come back unresolved mid-sweep. Those are transport failures,
    # not fidelity failures, and recording them as the latter would understate
    # the MCP badly -- an early run logged 24% "MCP errors" that were all this.
    payload: Any = {}
    detail = ""
    for attempt in range(retries + 1):
        try:
            if limiter is not None:
                await limiter.wait()
            payload = await driver.call("get_time_series", args)
        except Exception as exc:  # noqa: BLE001 -- one bad series must not kill the run
            detail = f"{type(exc).__name__}: {exc}"
            payload = {}
        else:
            if payload.get("status") == "resolved":
                break
            detail = (
                f"status={payload.get('status')} "
                f"http={payload.get('httpStatus')} "
                f"{str(payload.get('message'))[:120]}"
            ).strip()
            # The server now says whether a failure is transport or real. Only
            # retry the transport kind -- re-asking for data that genuinely is
            # not there just slows the sweep and hides nothing.
            if not payload.get("retryable"):
                break
        if attempt < retries:
            await asyncio.sleep(backoff * (2**attempt))

    if not isinstance(payload, dict) or payload.get("status") != "resolved":
        return [], detail or "unresolved"

    got: dict[str, str] = {}
    unit = ""
    for point in payload.get("series") or []:
        period = str(point.get("period", ""))
        if scope.annual_only and not ANNUAL.fullmatch(period):
            continue
        got[period] = str(point.get("value", ""))
        unit = unit or str(point.get("unit", "") or "")

    rows: list[Row] = []
    for year in sorted(set(truth) | set(got)):
        api_v = truth.get(year)
        mcp_v = got.get(year)
        if api_v is not None and mcp_v is not None:
            verdict = "match" if values_agree(mcp_v, api_v, rel_tol) else "MISMATCH"
        elif api_v is not None:
            verdict = "MISSING_IN_MCP"
        else:
            verdict = "EXTRA_IN_MCP"
        rows.append(
            Row(
                key=key,
                year=year,
                mcp_value="" if mcp_v is None else mcp_v,
                api_value="" if api_v is None else api_v,
                mcp_unit=unit,
                api_unit=truth_unit,
                verdict=verdict,
            )
        )
    return rows, None


# ----------------------------------------------------------------------- main


async def run(args: argparse.Namespace) -> int:
    scope = Scope(
        flow_ref=args.flow,
        start=args.start,
        end=args.end,
        countries_only=not args.include_aggregates,
        annual_only=not args.include_subannual,
    )

    cache = Path(args.cache)
    fetch_ground_truth(scope, cache, args.refresh)
    countries = load_country_codes() if scope.countries_only else set()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tally = Tally()
    sem = asyncio.Semaphore(args.concurrency)
    limiter = RateLimiter(args.rate)

    async with McpDriver(args.endpoint) as driver:
        dims = await discover_dimensions(driver, scope.flow_ref)
        print(f"[dims] series key = {dims}")
        truth, units = index_ground_truth(cache, scope, countries, dims)

        ind_pos = dims.index("INDICATOR")
        if args.mode == "bulk":
            units_label = "indicators"
            work: list[Any] = sorted({k[ind_pos] for k in truth})
        else:
            units_label = "series"
            work = sorted(truth)
        if args.limit:
            work = work[: args.limit]

        done: set[Any] = set()
        if args.resume and out_path.exists():
            with out_path.open(newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    done.add(
                        row["INDICATOR"] if args.mode == "bulk"
                        else tuple(row[d] for d in dims)
                    )
            work = [w for w in work if w not in done]
            print(f"[resume] {len(done):,} recorded; {len(work):,} to go")
        keys = work

        print(
            f"[sweep:{args.mode}] {len(keys):,} {units_label}, "
            f"concurrency {args.concurrency}, "
            f"target {'in-process' if not args.endpoint else args.endpoint}"
        )
        started = time.monotonic()

        writer_handle = out_path.open(
            "a" if (args.resume and done) else "w", newline="", encoding="utf-8"
        )
        writer = csv.writer(writer_handle)
        if not (args.resume and done):
            writer.writerow(
                [
                    *dims, "year", "mcp_value", "api_value",
                    "mcp_unit", "api_unit", "verdict", "note",
                ]
            )

        lock = asyncio.Lock()

        async def worker(key: Any) -> None:
            async with sem:
                if args.mode == "bulk":
                    rows, err = await compare_indicator_bulk(
                        driver, scope, dims, key, truth, units, countries,
                        args.rel_tol, retries=args.retries,
                        backoff=args.backoff, limiter=limiter,
                    )
                else:
                    rows, err = await compare_series(
                        driver, scope, dims, key, truth[key],
                        units.get(key, ""), args.rel_tol,
                        retries=args.retries, backoff=args.backoff, limiter=limiter,
                    )
            async with lock:
                tally.series_done += 1
                if err:
                    tally.error += 1
                    tally.notes[err.split(":")[0]] += 1
                    err_key = (
                        tuple("" if d != "INDICATOR" else key for d in dims)
                        if args.mode == "bulk" else key
                    )
                    writer.writerow([*err_key, "", "", "", "", "", "ERROR", err[:200]])
                for row in rows:
                    if row.verdict == "match":
                        tally.match += 1
                    elif row.verdict == "MISMATCH":
                        tally.mismatch += 1
                    elif row.verdict == "MISSING_IN_MCP":
                        tally.missing_in_mcp += 1
                    else:
                        tally.extra_in_mcp += 1
                    writer.writerow(row.as_csv())
                if tally.series_done % args.progress_every == 0:
                    rate = tally.series_done / max(time.monotonic() - started, 1e-9)
                    remaining = (len(keys) - tally.series_done) / max(rate, 1e-9)
                    writer_handle.flush()
                    print(
                        f"  {tally.series_done:,}/{len(keys):,} {units_label} "
                        f"| {tally.compared:,} obs "
                        f"| mismatch {tally.mismatch:,} "
                        f"| missing {tally.missing_in_mcp:,} "
                        f"| err {tally.error:,} "
                        f"| {rate:.1f} series/s, ~{remaining / 60:.0f}m left",
                        flush=True,
                    )

        await _gather_bounded(worker, keys)
        writer_handle.close()

    elapsed = time.monotonic() - started
    print(f"\n=== fidelity sweep: {scope.flow_ref} {scope.start}-{scope.end} ===")
    print(f"{'indicators' if args.mode == 'bulk' else 'series':18}: {tally.series_done:,}")
    print(f"observations      : {tally.compared:,}")
    print(f"  match           : {tally.match:,}")
    print(f"  MISMATCH        : {tally.mismatch:,}")
    print(f"  MISSING_IN_MCP  : {tally.missing_in_mcp:,}")
    print(f"  EXTRA_IN_MCP    : {tally.extra_in_mcp:,}")
    print(f"errored           : {tally.error:,}")
    if tally.notes:
        print(f"  error kinds     : {dict(tally.notes)}")
    if tally.compared:
        print(f"fidelity          : {100 * tally.match / tally.compared:.4f}%")
    print(f"elapsed           : {elapsed / 60:.1f} min")
    print(f"rows written      : {out_path}")

    return 1 if (tally.mismatch or tally.missing_in_mcp or tally.extra_in_mcp) else 0


async def _gather_bounded(worker: Any, keys: Iterable[tuple[str, ...]]) -> None:
    tasks = [asyncio.create_task(worker(k)) for k in keys]
    for task in asyncio.as_completed(tasks):
        await task


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--mode", choices=["bulk", "series"], default="bulk",
        help="bulk: one query_data call per indicator (~350 calls, default). "
             "series: one get_time_series call per series (~79k calls, exercises "
             "the compact projection but the registry throttles it).",
    )
    parser.add_argument("--flow", default=DEFAULT_FLOW)
    parser.add_argument("--start", type=int, default=DEFAULT_START)
    parser.add_argument("--end", type=int, default=DEFAULT_END)
    parser.add_argument(
        "--endpoint",
        default=None,
        help="MCP Streamable HTTP URL. Omit to import the server in-process.",
    )
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--limit", type=int, default=0, help="first N series only")
    parser.add_argument("--rel-tol", type=float, default=1e-9)
    parser.add_argument("--cache", default="tmp/fidelity/ground_truth.csv")
    parser.add_argument("--out", default="tmp/fidelity/results.csv")
    parser.add_argument("--refresh", action="store_true", help="re-download truth")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--progress-every", type=int, default=250)
    parser.add_argument("--retries", type=int, default=4)
    parser.add_argument("--backoff", type=float, default=2.0)
    parser.add_argument(
        "--rate", type=float, default=6.0,
        help="max MCP calls per second; the registry throttles above this",
    )
    parser.add_argument("--include-aggregates", action="store_true")
    parser.add_argument("--include-subannual", action="store_true")
    args = parser.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
