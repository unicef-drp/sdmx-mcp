import logging
import csv
from collections import Counter
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote
from xml.etree import ElementTree as ET

import httpx
from cachetools import TTLCache

from mcp.server.fastmcp import FastMCP

# IMPORTANT for STDIO servers: do not print() to stdout.
logging.basicConfig(level=logging.INFO)

BASE = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest"

# Bind to all interfaces so DNS rebinding protection isn't auto-enabled for localhost-only hosts.
mcp = FastMCP("unicef-sdmx", json_response=True, host="0.0.0.0")

# Starter mapping for common flow id prefixes to human-friendly labels.
FALLBACK_THEME_PREFIX_MAP: dict[str, str] = {
    "PT": "Child Protection",
    "NUTRITION": "Nutrition",
    "EDU": "Education",
    "WASH": "Water, Sanitation and Hygiene",
    "MICS": "Multiple Indicator Cluster Surveys",
    "HIV": "HIV and AIDS",
    "IMM": "Immunization",
    "MCH": "Maternal and Child Health",
}
THEME_PREFIX_CSV = Path(__file__).resolve().parent / "theme_prefixes_domain.csv"

# Small caches to keep things fast and reduce load.
_dataflow_cache = TTLCache(maxsize=1, ttl=60 * 60 * 6)  # 6h
_structure_cache = TTLCache(maxsize=256, ttl=60 * 60 * 24)  # 24h
_dimension_cache = TTLCache(maxsize=256, ttl=60 * 60 * 24)  # 24h


async def _get_json(url: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(url, headers={"User-Agent": "unicef-sdmx-mcp/0.1"})
        r.raise_for_status()
        return r.json()


async def _get_text_with_status(url: str) -> tuple[int, str]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(url, headers={"User-Agent": "unicef-sdmx-mcp/0.1"})
        return r.status_code, r.text


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


def _query_tokens(query: str) -> list[str]:
    tokens = [part.strip().lower() for part in query.replace("/", " ").split() if part.strip()]
    return [token for token in tokens if len(token) >= 3]


def _match_score(text: str, query: str) -> int:
    q = (query or "").strip().lower()
    if not q:
        return 0
    if q in text:
        return len(q.split())
    tokens = _query_tokens(q)
    if not tokens:
        return 0
    score = sum(1 for token in tokens if token in text)
    return score


def _ranked_code_matches(codes: list[dict[str, Any]], query: str, limit: int = 10) -> list[dict[str, Any]]:
    q = (query or "").strip()
    if not q:
        return []
    ranked: list[dict[str, Any]] = []
    for code in codes:
        if not isinstance(code, dict):
            continue
        code_id = code.get("id") or code.get("ID")
        if not isinstance(code_id, str):
            continue
        name = _coerce_text(code.get("name")) or _coerce_text(code.get("names"))
        desc = _coerce_text(code.get("description")) or _coerce_text(code.get("descriptions"))
        text = f"{code_id} {name} {desc}".lower()
        score = _match_score(text, q)
        if score == 0:
            continue
        ranked.append(
            {
                "id": code_id,
                "name": name,
                "description": desc,
                "_score": score,
            }
        )
    ranked.sort(key=lambda item: item.get("_score", 0), reverse=True)
    trimmed = ranked[:limit]
    for item in trimmed:
        item.pop("_score", None)
    return trimmed


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


def _extract_agencies(payload: dict[str, Any]) -> list[dict[str, Any]]:
    agencies: list[dict[str, Any]] = []
    structure_section = payload.get("structure")
    if isinstance(structure_section, dict):
        agency_section = structure_section.get("agencies")
        if isinstance(agency_section, dict):
            agency_schemes = agency_section.get("agencyScheme")
            if isinstance(agency_schemes, list):
                schemes = agency_schemes
            elif isinstance(agency_schemes, dict):
                schemes = list(agency_schemes.values())
            else:
                schemes = []
            for scheme in schemes:
                if not isinstance(scheme, dict):
                    continue
                items = scheme.get("agencies") or scheme.get("agency")
                if isinstance(items, list):
                    for item in items:
                        if isinstance(item, dict):
                            agencies.append(item)
                elif isinstance(items, dict):
                    agencies.extend([item for item in items.values() if isinstance(item, dict)])
    return agencies


def _flow_ref_for(df_id: str, version: str | None = None, agency: str | None = None) -> str:
    version = version or "latest"
    agency = agency or "all"
    return f"{agency}/{df_id}/{version}"


def _infer_theme_hint(df_id: str, name: str, prefix_map: dict[str, str] | None = None) -> dict[str, str]:
    prefix_map = prefix_map or {}
    raw_id = (df_id or "").strip()
    raw_name = (name or "").strip()
    if "_" in raw_id:
        theme_code = raw_id.split("_", 1)[0]
        source = "id-prefix"
    else:
        theme_code = raw_id
        source = "id"
    label = prefix_map.get(theme_code, theme_code)
    if not label and raw_name:
        label = raw_name
        source = "name"
    return {"code": theme_code or raw_id, "label": label or raw_id, "source": source}


def _load_theme_prefix_map_from_csv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    grouped: dict[str, Counter[str]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            prefix = (row.get("prefix") or "").strip()
            domain = (row.get("domain") or "").strip()
            if not prefix or not domain:
                continue
            grouped.setdefault(prefix, Counter())[domain] += 1

    result: dict[str, str] = {}
    for prefix, counts in grouped.items():
        domain = counts.most_common(1)[0][0]
        result[prefix] = domain
    return result


def _theme_prefix_conflicts_from_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    grouped: dict[str, dict[str, Any]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            prefix = (row.get("prefix") or "").strip()
            domain = (row.get("domain") or "").strip()
            example_id = (row.get("example_id") or "").strip()
            example_name = (row.get("example_name") or "").strip()
            if not prefix or not domain:
                continue
            bucket = grouped.setdefault(prefix, {"counts": Counter(), "examples": {}})
            counts: Counter[str] = bucket["counts"]
            counts[domain] += 1
            examples: dict[str, dict[str, str]] = bucket["examples"]
            if domain not in examples and (example_id or example_name):
                examples[domain] = {"id": example_id, "name": example_name}

    conflicts: list[dict[str, Any]] = []
    for prefix, payload in grouped.items():
        counts: Counter[str] = payload["counts"]
        if len(counts) <= 1:
            continue
        domains: list[dict[str, Any]] = []
        examples: dict[str, dict[str, str]] = payload["examples"]
        for domain, count in counts.most_common():
            domains.append(
                {
                    "domain": domain,
                    "count": count,
                    "example": examples.get(domain, {"id": "", "name": ""}),
                }
            )
        conflicts.append({"prefix": prefix, "domains": domains})
    conflicts.sort(key=lambda item: len(item["domains"]), reverse=True)
    return conflicts


def _default_theme_prefix_map() -> dict[str, str]:
    from_csv = _load_theme_prefix_map_from_csv(THEME_PREFIX_CSV)
    if not from_csv:
        return dict(FALLBACK_THEME_PREFIX_MAP)
    merged = dict(FALLBACK_THEME_PREFIX_MAP)
    merged.update(from_csv)
    return merged


DEFAULT_THEME_PREFIX_MAP = _default_theme_prefix_map()


def _theme_code_from_id(df_id: str) -> str:
    raw_id = (df_id or "").strip()
    if "_" in raw_id:
        return raw_id.split("_", 1)[0]
    return raw_id


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


def _extract_codelists(payload: dict[str, Any]) -> list[dict[str, Any]]:
    codelists: list[dict[str, Any]] = []
    for root_key in ("structure", "data"):
        root = payload.get(root_key)
        if not isinstance(root, dict):
            continue
        code_container = root.get("codelists")
        if not code_container:
            continue
        if isinstance(code_container, list):
            codelists.extend([item for item in code_container if isinstance(item, dict)])
            continue
        if isinstance(code_container, dict):
            codelist = code_container.get("codelist")
            if isinstance(codelist, dict):
                codelists.extend([item for item in codelist.values() if isinstance(item, dict)])
            elif isinstance(codelist, list):
                codelists.extend([item for item in codelist if isinstance(item, dict)])
            else:
                codelists.extend([item for item in code_container.values() if isinstance(item, dict)])
    return codelists


def _extract_codelists_from_structures(payload: dict[str, Any]) -> list[dict[str, Any]]:
    codelists: list[dict[str, Any]] = []
    for ds in _extract_data_structures(payload):
        related = ds.get("codelists")
        if isinstance(related, list):
            codelists.extend([item for item in related if isinstance(item, dict)])
        elif isinstance(related, dict):
            codelist = related.get("codelist")
            if isinstance(codelist, dict):
                codelists.extend([item for item in codelist.values() if isinstance(item, dict)])
            elif isinstance(codelist, list):
                codelists.extend([item for item in codelist if isinstance(item, dict)])
            else:
                codelists.extend([item for item in related.values() if isinstance(item, dict)])
    return codelists


def _codelist_key(raw_id: str) -> str:
    text = raw_id.strip()
    if text.startswith("urn:") and "Codelist=" in text:
        text = text.split("Codelist=", 1)[1]
    if ":" in text:
        text = text.split(":", 1)[1]
    if "(" in text:
        text = text.split("(", 1)[0]
    return text


def _parse_sdmx_error(text: str) -> str | None:
    if not text:
        return None
    if "<mes:Error" not in text and "<ErrorMessage" not in text:
        return None
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return None
    for elem in root.iter():
        tag = elem.tag.split("}")[-1]
        if tag == "Text" and elem.text:
            return elem.text.strip()
    return None


def _codelist_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    mapping: dict[str, dict[str, Any]] = {}
    for codelist in _extract_codelists(payload) + _extract_codelists_from_structures(payload):
        list_id = codelist.get("id") or codelist.get("ID")
        if isinstance(list_id, str):
            mapping[list_id] = codelist
            mapping[_codelist_key(list_id)] = codelist
    return mapping


def _dimension_metadata(payload: dict[str, Any]) -> list[dict[str, Any]]:
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
        results: list[dict[str, Any]] = []
        for dim in dim_items:
            dim_id = dim.get("id") or dim.get("ID")
            if not isinstance(dim_id, str):
                continue
            concept = dim.get("conceptIdentity") or {}
            if isinstance(concept, dict):
                concept_id = concept.get("id") or concept.get("ID")
            elif isinstance(concept, str):
                concept_id = concept
            else:
                concept_id = None
            local_rep = dim.get("localRepresentation") or {}
            if isinstance(local_rep, dict):
                enumeration = local_rep.get("enumeration") or {}
                if isinstance(enumeration, dict):
                    codelist_ref = enumeration.get("id") or enumeration.get("ID")
                elif isinstance(enumeration, str):
                    codelist_ref = enumeration
                else:
                    codelist_ref = None
            elif isinstance(local_rep, str):
                codelist_ref = local_rep
            else:
                codelist_ref = None
            results.append(
                {
                    "id": dim_id.upper(),
                    "conceptID": concept_id,
                    "name": _coerce_text(dim.get("name")) or _coerce_text(dim.get("names")),
                    "position": dim.get("position"),
                    "codelist": codelist_ref,
                }
            )
        if results:
            return results
    return []


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
async def list_agencies(limit: int = 50) -> list[dict[str, Any]]:
    """
    List agencies from the UNICEF SDMX service with optional descriptions.
    """
    payload = await dataflows_resource()
    agencies = _extract_agencies(payload)
    if not agencies:
        # Fall back to agencies inferred from dataflows if explicit agency lists are absent.
        flows = _extract_dataflows(payload)
        for df in flows:
            agency = df.get("agencyID") or df.get("agencyId")
            if isinstance(agency, str):
                agencies.append({"id": agency})
    seen: set[str] = set()
    results: list[dict[str, Any]] = []
    for agency in agencies:
        agency_id = agency.get("id") or agency.get("ID")
        if not isinstance(agency_id, str) or agency_id in seen:
            continue
        seen.add(agency_id)
        results.append(
            {
                "id": agency_id,
                "name": _coerce_text(agency.get("name")) or _coerce_text(agency.get("names")),
                "description": _coerce_text(agency.get("description")) or _coerce_text(agency.get("descriptions")),
            }
        )
        if len(results) >= limit:
            break
    return results


@mcp.tool()
async def search_dataflows(query: str, limit: int = 10) -> list[dict[str, Any]]:
    """
    Search UNICEF SDMX dataflows by id/name/description.
    Returns lightweight matches with a flowRef you can pass to other tools.
    """
    payload = await dataflows_resource()
    flows = _extract_dataflows(payload)
    matches: list[dict[str, Any]] = []
    q = query.strip()

    for df in flows:
        df_id = df.get("id") or df.get("ID")
        if not isinstance(df_id, str):
            continue
        agency = df.get("agencyID") or df.get("agencyId") or "all"
        name = _coerce_text(df.get("name")) or _coerce_text(df.get("names"))
        desc = _coerce_text(df.get("description")) or _coerce_text(df.get("descriptions"))
        theme_hint = _infer_theme_hint(df_id, name)
        text = f"{df_id} {name} {desc}".lower()
        score = _match_score(text, q) if q else 0
        if q and score == 0:
            continue
        matches.append(
            {
                "id": df_id,
                "agencyID": agency,
                "name": name,
                "description": desc,
                "themeHint": theme_hint,
                "_score": score,
                "flowRef": _flow_ref_for(df_id, df.get("version"), agency),
            }
        )
    if q:
        matches.sort(key=lambda item: item.get("_score", 0), reverse=True)
    trimmed = matches[:limit]
    for item in trimmed:
        item.pop("_score", None)
    return trimmed


@mcp.tool()
async def list_dataflows_grouped(
    query: str | None = None,
    prefixMap: dict[str, str] | None = None,
    limitPerTheme: int = 50,
) -> list[dict[str, Any]]:
    """
    List dataflows grouped by a theme hint inferred from flow IDs.
    Optionally pass prefixMap to map id prefixes to human-friendly labels.
    """
    payload = await dataflows_resource()
    flows = _extract_dataflows(payload)
    if prefixMap is None:
        prefixMap = DEFAULT_THEME_PREFIX_MAP
    q = (query or "").strip()
    grouped: dict[str, dict[str, Any]] = {}
    for df in flows:
        df_id = df.get("id") or df.get("ID")
        if not isinstance(df_id, str):
            continue
        agency = df.get("agencyID") or df.get("agencyId") or "all"
        name = _coerce_text(df.get("name")) or _coerce_text(df.get("names"))
        desc = _coerce_text(df.get("description")) or _coerce_text(df.get("descriptions"))
        text = f"{df_id} {name} {desc}".lower()
        score = _match_score(text, q) if q else 0
        if q and score == 0:
            continue
        theme_hint = _infer_theme_hint(df_id, name, prefixMap)
        theme_key = theme_hint["code"] or df_id
        if theme_key not in grouped:
            grouped[theme_key] = {
                "themeCode": theme_key,
                "themeLabel": theme_hint.get("label") or theme_key,
                "flows": [],
            }
        bucket = grouped[theme_key]["flows"]
        if len(bucket) >= limitPerTheme:
            continue
        bucket.append(
            {
                "id": df_id,
                "agencyID": agency,
                "name": name,
                "description": desc,
                "flowRef": _flow_ref_for(df_id, df.get("version"), agency),
            }
        )
    return sorted(grouped.values(), key=lambda item: item["themeLabel"])


@mcp.tool()
async def get_default_theme_prefix_map() -> dict[str, str]:
    """
    Return the starter mapping of flow id prefixes to theme labels.
    """
    return DEFAULT_THEME_PREFIX_MAP


@mcp.tool()
async def list_theme_prefixes(limit: int = 50) -> list[dict[str, Any]]:
    """
    Scan dataflows and return common id prefixes with counts and examples.
    """
    payload = await dataflows_resource()
    flows = _extract_dataflows(payload)
    counts: dict[str, dict[str, Any]] = {}
    for df in flows:
        df_id = df.get("id") or df.get("ID")
        if not isinstance(df_id, str):
            continue
        prefix = _theme_code_from_id(df_id)
        if prefix not in counts:
            counts[prefix] = {"prefix": prefix, "count": 0, "examples": []}
        bucket = counts[prefix]
        bucket["count"] += 1
        examples = bucket["examples"]
        if len(examples) < 3:
            name = _coerce_text(df.get("name")) or _coerce_text(df.get("names"))
            examples.append({"id": df_id, "name": name})
    ranked = sorted(counts.values(), key=lambda item: item["count"], reverse=True)
    return ranked[:limit]


@mcp.tool()
async def list_theme_prefix_conflicts(limit: int = 100) -> list[dict[str, Any]]:
    """
    List prefixes that map to multiple domains in theme_prefixes_domain.csv.
    """
    conflicts = _theme_prefix_conflicts_from_csv(THEME_PREFIX_CSV)
    return conflicts[:limit]


@mcp.tool()
async def describe_flow(flowRef: str) -> dict[str, Any]:
    """
    Return a human-friendly summary of a dataflow, including dimension info.
    """
    payload = await get_flow_structure(flowRef)
    flows = _extract_dataflows(payload)
    flow_meta: dict[str, Any] = {}
    agency, df_id, version = _flow_identifiers(flowRef)
    for df in flows:
        df_id_match = df.get("id") or df.get("ID")
        agency_match = df.get("agencyID") or df.get("agencyId") or agency
        if isinstance(df_id_match, str) and df_id_match == df_id and agency_match == agency:
            flow_meta = df
            break
    dims = _dimension_metadata(payload)
    return {
        "id": df_id,
        "agencyID": agency,
        "version": version,
        "name": _coerce_text(flow_meta.get("name")) or _coerce_text(flow_meta.get("names")),
        "description": _coerce_text(flow_meta.get("description")) or _coerce_text(flow_meta.get("descriptions")),
        "dimensions": dims,
    }


@mcp.tool()
async def list_dimensions(flowRef: str) -> list[dict[str, Any]]:
    """
    List ordered dimensions for a flow with codelist references.
    """
    payload = await get_flow_structure(flowRef)
    return _dimension_metadata(payload)


@mcp.tool()
async def list_codes(
    flowRef: str,
    dimension: str,
    query: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    List codes for a specific dimension, optionally filtered by a query string.
    """
    payload = await get_flow_structure(flowRef)
    dims = _dimension_metadata(payload)
    dim_id = dimension.strip().upper()
    target = next((d for d in dims if d.get("id") == dim_id), None)
    if not target:
        raise ValueError(f"Unknown dimension '{dimension}'.")
    codelist_id = target.get("codelist")
    if not codelist_id:
        raise ValueError(f"Dimension '{dimension}' does not have a codelist reference.")
    codelists = _codelist_map(payload)
    key = _codelist_key(codelist_id)
    codelist = codelists.get(codelist_id) or codelists.get(key)
    if not codelist:
        raise ValueError(f"Codelist '{codelist_id}' not found in structure payload.")
    codes = codelist.get("codes") or codelist.get("code") or []
    if isinstance(codes, dict):
        codes = list(codes.values())
    if not isinstance(codes, list):
        codes = []
    q = (query or "").strip().lower()
    results: list[dict[str, Any]] = []
    for code in codes:
        if not isinstance(code, dict):
            continue
        code_id = code.get("id") or code.get("ID")
        if not isinstance(code_id, str):
            continue
        name = _coerce_text(code.get("name")) or _coerce_text(code.get("names"))
        desc = _coerce_text(code.get("description")) or _coerce_text(code.get("descriptions"))
        text = f"{code_id} {name} {desc}".lower()
        if q and q not in text:
            continue
        results.append({"id": code_id, "name": name, "description": desc})
        if len(results) >= limit:
            break
    return results


@mcp.tool()
async def find_indicator_candidates(
    flowRef: str,
    query: str,
    limit: int = 10,
) -> list[dict[str, Any]]:
    """
    Rank indicator codes by matching query text against codelist labels/descriptions.
    """
    payload = await get_flow_structure(flowRef)
    dims = _dimension_metadata(payload)
    target = next((d for d in dims if d.get("id") == "INDICATOR"), None)
    if not target:
        raise ValueError("INDICATOR dimension not found for this flow.")
    codelist_id = target.get("codelist")
    if not codelist_id:
        raise ValueError("INDICATOR dimension does not have a codelist reference.")
    codelists = _codelist_map(payload)
    codelist = codelists.get(codelist_id) or codelists.get(_codelist_key(codelist_id))
    if not codelist:
        raise ValueError(f"Codelist '{codelist_id}' not found in structure payload.")
    codes = codelist.get("codes") or codelist.get("code") or []
    if isinstance(codes, dict):
        codes = list(codes.values())
    if not isinstance(codes, list):
        codes = []
    return _ranked_code_matches(codes, query, limit=limit)


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
    lastNObservations: Optional[int] = None,
) -> dict[str, Any]:
    """
    Query SDMX data with guardrails.
    - Requires a bounded time window unless caller explicitly accepts the risk.
    - Returns raw SDMX-JSON and a minimal 'query_url' for reproducibility.
    """
    if not (startPeriod and endPeriod) and not lastNObservations:
        raise ValueError("Provide start/end periods or lastNObservations to avoid unbounded extracts.")

    if filters:
        dimension_order = await _dimension_order_for_flow(flowRef)
        key = _build_key_from_filters(dimension_order, filters)

    if not key:
        raise ValueError("Provide either a key or filters to identify the data slice.")

    # Standard SDMX pattern: /data/{flowRef}/{key}?startPeriod=...&endPeriod=...&format=...
    flow_path = _data_path_for(flowRef)
    params: list[str] = []
    if startPeriod and endPeriod:
        params.append(f"startPeriod={quote(startPeriod)}")
        params.append(f"endPeriod={quote(endPeriod)}")
    if lastNObservations is not None:
        params.append(f"lastNObservations={int(lastNObservations)}")
    params.append(f"format={quote(format)}")
    url = f"{BASE}/data/{flow_path}/{quote(key, safe='+.')}?{'&'.join(params)}"

    if format.lower() == "csv":
        status, text = await _get_text_with_status(url)
        if status >= 400:
            return {
                "query_url": url,
                "error": {
                    "status": status,
                    "message": _parse_sdmx_error(text),
                    "raw": text,
                },
                "notes": {"maxObs": maxObs, "format": "csv"},
            }
        return {"query_url": url, "raw_csv": text, "notes": {"maxObs": maxObs, "format": "csv"}}

    raw = await _get_json(url)

    # Minimal guardrail: if server returns huge payloads, you can add a response-size check here.
    return {"query_url": url, "raw": raw, "notes": {"maxObs": maxObs, "format": format}}
