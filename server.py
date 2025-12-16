import logging
from typing import Any, Optional
from urllib.parse import quote

import httpx
from cachetools import TTLCache

from mcp.server.fastmcp import FastMCP

# IMPORTANT for STDIO servers: do not print() to stdout.
logging.basicConfig(level=logging.INFO)

BASE = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest"

mcp = FastMCP("unicef-sdmx", json_response=True)

# Small caches to keep things fast and reduce load.
_dataflow_cache = TTLCache(maxsize=1, ttl=60 * 60 * 6)  # 6h
_structure_cache = TTLCache(maxsize=256, ttl=60 * 60 * 24)  # 24h
_dimension_cache = TTLCache(maxsize=256, ttl=60 * 60 * 24)  # 24h


async def _get_json(url: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(url, headers={"User-Agent": "unicef-sdmx-mcp/0.1"})
        r.raise_for_status()
        return r.json()


def _dataflow_url() -> str:
    # UNICEF service builder defaults to SDMX 2.1 (XML), but FastMCP expects JSON for parsing.
    return f"{BASE}/dataflow/all/all/latest/?format=sdmx-json&detail=full&references=none"


def _structure_url(flow_ref: str) -> str:
    # Practical approach: fetch dataflow with references=all to pull back related structures (DSD, codelists).
    # UNICEF documentation describes this approach. :contentReference[oaicite:5]{index=5}
    # flow_ref should typically look like: AGENCY:FLOW_ID(VERSION) or similar; keep it simple early.
    return f"{BASE}/dataflow/{_flow_path_for(flow_ref)}/?format=sdmx-json&detail=full&references=all"


def _encode_flow_path(flow_ref: str) -> str:
    parts = [segment for segment in flow_ref.strip("/").split("/") if segment]
    if not parts:
        raise ValueError("flowRef must not be empty.")
    return "/".join(quote(part) for part in parts)


def _coerce_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        for maybe_text in value.values():
            text = _coerce_text(maybe_text)
            if text:
                return text
        return ""
    if isinstance(value, list):
        for item in value:
            text = _coerce_text(item)
            if text:
                return text
        return ""
    return ""


def _extract_dataflows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    flows: list[dict[str, Any]] = []
    data_section = payload.get("data")
    if isinstance(data_section, dict):
        candidates = data_section.get("dataflows")
        if isinstance(candidates, list):
            flows.extend([df for df in candidates if isinstance(df, dict)])
    structure_section = payload.get("structure")
    if isinstance(structure_section, dict):
        dataflows = structure_section.get("dataflows")
        if isinstance(dataflows, dict):
            df_map = dataflows.get("dataflow")
            if isinstance(df_map, dict):
                for df in df_map.values():
                    if isinstance(df, dict):
                        flows.append(df)
        elif isinstance(dataflows, list):
            flows.extend([df for df in dataflows if isinstance(df, dict)])
    return flows


def _flow_ref_for(df_id: str, version: str | None = None, agency: str | None = None) -> str:
    version = version or "latest"
    agency = agency or "all"
    return f"{agency}/{df_id}/{version}"


def _flow_identifiers(flow_ref: str) -> tuple[str, str, str]:
    text = (flow_ref or "").strip()
    if not text:
        raise ValueError("flowRef must not be empty.")

    agency = ""
    df_id = ""
    version = ""

    def _parts_from_delimiter(value: str, delimiter: str) -> tuple[str, str, str]:
        bits = [segment.strip() for segment in value.split(delimiter) if segment.strip()]
        parsed_agency = bits[0] if bits else ""
        parsed_id = bits[1] if len(bits) > 1 else ""
        parsed_version = bits[2] if len(bits) > 2 else ""
        return parsed_agency, parsed_id, parsed_version

    if "," in text:
        agency, df_id, version = _parts_from_delimiter(text, ",")
    elif "/" in text:
        agency, df_id, version = _parts_from_delimiter(text, "/")
    elif ":" in text:
        agency, remainder = text.split(":", 1)
        remainder = remainder.strip()
        if "(" in remainder and remainder.endswith(")"):
            df_id = remainder[: remainder.index("(")].strip()
            version = remainder[remainder.index("(") + 1 : -1].strip()
        else:
            df_id = remainder
    else:
        df_id = text

    if not df_id:
        raise ValueError("flowRef must include a dataflow id.")

    agency = agency or "all"
    version = version or "latest"
    return agency, df_id, version


def _flow_path_for(flow_ref: str) -> str:
    """
    Normalize flow references for SDMX REST paths.
    - Accepts bare flow ids (e.g. "BRAZIL_CO") and expands to all/{id}/latest.
    - Leaves explicit paths (with '/') untouched.
    """
    agency, df_id, version = _flow_identifiers(flow_ref)
    path = "/".join(part for part in (agency, df_id, version) if part)
    return _encode_flow_path(path)


def _data_path_for(flow_ref: str) -> str:
    agency, df_id, version = _flow_identifiers(flow_ref)
    ident = ",".join(part for part in (agency, df_id, version) if part)
    return quote(ident, safe=",")


def _extract_data_structures(payload: dict[str, Any]) -> list[dict[str, Any]]:
    structures: list[dict[str, Any]] = []
    for root_key in ("structure", "data"):
        root = payload.get(root_key)
        if not isinstance(root, dict):
            continue
        ds_container = root.get("dataStructures")
        if isinstance(ds_container, list):
            structures.extend([item for item in ds_container if isinstance(item, dict)])
            continue
        if isinstance(ds_container, dict):
            data_structure = ds_container.get("dataStructure")
            if isinstance(data_structure, dict):
                structures.extend([item for item in data_structure.values() if isinstance(item, dict)])
            elif isinstance(data_structure, list):
                structures.extend([item for item in data_structure if isinstance(item, dict)])
            else:
                structures.extend([item for item in ds_container.values() if isinstance(item, dict)])
    return structures


def _dimension_order_from_structure(payload: dict[str, Any]) -> list[str]:
    for ds in _extract_data_structures(payload):
        components = ds.get("dataStructureComponents")
        if not isinstance(components, dict):
            continue
        dim_list = components.get("dimensionList")
        if not isinstance(dim_list, dict):
            continue
        dims = dim_list.get("dimensions") or dim_list.get("dimension")
        dim_items: list[dict[str, Any]] = []
        if isinstance(dims, list):
            dim_items = [item for item in dims if isinstance(item, dict)]
        elif isinstance(dims, dict):
            dim_items = [item for item in dims.values() if isinstance(item, dict)]
        if not dim_items:
            continue
        dim_items.sort(key=lambda d: d.get("position") or 0)
        ordered: list[str] = []
        for dim in dim_items:
            dim_id = dim.get("id") or dim.get("ID")
            if isinstance(dim_id, str):
                ordered.append(dim_id.upper())
        if ordered:
            return ordered
    return []


def _normalize_selection_values(value: Any) -> str:
    if isinstance(value, str):
        tokens = [part.strip() for part in value.replace("+", ",").split(",") if part.strip()]
        return "+".join(tokens)
    if isinstance(value, (list, tuple, set)):
        tokens = [str(item).strip() for item in value if str(item).strip()]
        return "+".join(tokens)
    return ""


async def _dimension_order_for_flow(flowRef: str) -> list[str]:
    cache_key = flowRef.strip()
    if cache_key in _dimension_cache:
        return _dimension_cache[cache_key]
    payload = await get_flow_structure(flowRef)
    dims = _dimension_order_from_structure(payload)
    if not dims:
        raise ValueError("Unable to determine dimension order for this flow.")
    _dimension_cache[cache_key] = dims
    return dims


def _build_key_from_filters(dimension_order: list[str], filters: dict[str, Any]) -> str:
    if not filters:
        raise ValueError("filters must include at least one dimension.")
    normalized_filters = {str(k).upper(): v for k, v in filters.items()}
    unknown = sorted(k for k in normalized_filters if k not in dimension_order)
    if unknown:
        raise ValueError(
            f"Unknown dimension(s): {', '.join(unknown)}. Available: {', '.join(dimension_order)}"
        )

    parts: list[str] = []
    provided = False
    for dim in dimension_order:
        raw_value = normalized_filters.get(dim)
        selection = _normalize_selection_values(raw_value) if raw_value is not None else ""
        if selection:
            provided = True
        parts.append(selection)

    if not provided:
        raise ValueError("At least one dimension selection must contain a value.")

    return ".".join(parts)


@mcp.resource("sdmx://unicef/dataflows")
async def dataflows_resource() -> dict[str, Any]:
    """Cached SDMX dataflows list (SDMX-JSON)."""
    if "dataflows" not in _dataflow_cache:
        _dataflow_cache["dataflows"] = await _get_json(_dataflow_url())
    return _dataflow_cache["dataflows"]


@mcp.tool()
async def search_dataflows(query: str, limit: int = 10) -> list[dict[str, Any]]:
    """
    Search UNICEF SDMX dataflows by id/name/description.
    Returns lightweight matches with a flowRef you can pass to other tools.
    """
    payload = await dataflows_resource()
    flows = _extract_dataflows(payload)
    matches: list[dict[str, Any]] = []
    q = query.strip().lower()

    for df in flows:
        df_id = df.get("id") or df.get("ID")
        if not isinstance(df_id, str):
            continue
        agency = df.get("agencyID") or df.get("agencyId") or "all"
        name = _coerce_text(df.get("name")) or _coerce_text(df.get("names"))
        desc = _coerce_text(df.get("description")) or _coerce_text(df.get("descriptions"))
        text = f"{df_id} {name} {desc}".lower()
        if q and q not in text:
            continue
        matches.append(
            {
                "id": df_id,
                "agencyID": agency,
                "name": name,
                "description": desc,
                "flowRef": _flow_ref_for(df_id, df.get("version"), agency),
            }
        )
        if len(matches) >= limit:
            break

    return matches


@mcp.tool()
async def get_flow_structure(flowRef: str) -> dict[str, Any]:
    """
    Fetch and cache a flow's structure payload (DSD + codelists via references=all).
    """
    if flowRef not in _structure_cache:
        _structure_cache[flowRef] = await _get_json(_structure_url(flowRef))
    return _structure_cache[flowRef]


@mcp.tool()
async def build_key(flowRef: str, selections: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Build an SDMX key string from human-friendly dimension selections.
    Pass a mapping of dimension names to a single value or list of values.
    """
    if not selections:
        raise ValueError("selections must include at least one dimension.")
    dimension_order = await _dimension_order_for_flow(flowRef)
    key = _build_key_from_filters(dimension_order, selections)
    return {
        "key": key,
        "dimensionOrder": dimension_order,
        "notes": {
            "multipleValues": "Use arrays or comma-separated strings to include multiple codes per dimension.",
            "placeholders": "Dimensions without selections are filled automatically with empty segments.",
        },
    }


@mcp.tool()
async def query_data(
    flowRef: str,
    key: Optional[str] = None,
    startPeriod: Optional[str] = None,
    endPeriod: Optional[str] = None,
    format: str = "sdmx-json",
    maxObs: int = 50_000,
    filters: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Query SDMX data with guardrails.
    - Requires a bounded time window unless caller explicitly accepts the risk.
    - Returns raw SDMX-JSON and a minimal 'query_url' for reproducibility.
    """
    if not (startPeriod and endPeriod):
        raise ValueError("startPeriod and endPeriod are required to prevent unbounded extracts.")

    if filters:
        dimension_order = await _dimension_order_for_flow(flowRef)
        key = _build_key_from_filters(dimension_order, filters)

    if not key:
        raise ValueError("Provide either a key or filters to identify the data slice.")

    # Standard SDMX pattern: /data/{flowRef}/{key}?startPeriod=...&endPeriod=...&format=...
    flow_path = _data_path_for(flowRef)
    url = (
        f"{BASE}/data/{flow_path}/{quote(key)}"
        f"?startPeriod={quote(startPeriod)}&endPeriod={quote(endPeriod)}&format={quote(format)}"
    )

    raw = await _get_json(url)

    # Minimal guardrail: if server returns huge payloads, you can add a response-size check here.
    return {"query_url": url, "raw": raw, "notes": {"maxObs": maxObs}}
