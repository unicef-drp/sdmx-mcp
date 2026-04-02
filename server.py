import logging
import csv
import json
import os
from collections import Counter
from dataclasses import asdict, dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote
from xml.etree import ElementTree as ET

import httpx
from cachetools import TTLCache

from fastmcp import FastMCP

# IMPORTANT for STDIO servers: do not print() to stdout.
logging.basicConfig(level=logging.INFO)

REPO_ROOT = Path(__file__).resolve().parent


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env_key = key.strip()
        if not env_key or env_key in os.environ:
            continue
        os.environ[env_key] = value.strip().strip("\"'")


_load_dotenv(REPO_ROOT / ".env")

BASE = os.getenv("SDMX_BASE_URL", "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest")
HTTP_USER_AGENT = os.getenv("SDMX_USER_AGENT", "unicef-sdmx-mcp/0.1")
AGENCY_ALLOWLIST = {item.strip() for item in os.getenv("SDMX_AGENCY_ALLOWLIST", "").split(",") if item.strip()}

mcp = FastMCP("unicef-sdmx")

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
_codelist_cache = TTLCache(maxsize=256, ttl=60 * 60 * 24)  # 24h
_hierarchical_codelist_cache = TTLCache(maxsize=32, ttl=60 * 60 * 24)  # 24h
_hierarchical_codelist_detail_cache = TTLCache(maxsize=32, ttl=60 * 60 * 24)  # 24h
_hierarchical_catalog_cache = TTLCache(maxsize=8, ttl=60 * 60 * 24)  # 24h


@dataclass(slots=True)
class QueryDimensionSource:
    type: str
    id: str


@dataclass(slots=True)
class QueryDimensionPolicyEntry:
    name: str
    role: str
    required_for_retrieval: bool
    priority: int
    preferred_sources: list[QueryDimensionSource] = field(default_factory=list)
    allow_hierarchy_resolution: bool = False
    allow_member_expansion: bool = False


@dataclass(slots=True)
class QueryDimensionPolicyConfig:
    default_query_dimensions: list[QueryDimensionPolicyEntry]


def _env_flag(name: str, default: bool, legacy_names: list[str] | None = None) -> bool:
    candidates = [name] + list(legacy_names or [])
    raw_value: str | None = None
    for candidate in candidates:
        value = os.getenv(candidate)
        if value is None:
            continue
        normalized = value.strip()
        if not normalized:
            continue
        raw_value = normalized
        break
    if raw_value is None:
        return default
    return raw_value.lower() in {"1", "true", "yes", "on"}


async def _get_json(url: str) -> dict[str, Any]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(url, headers={"User-Agent": HTTP_USER_AGENT})
        r.raise_for_status()
        return r.json()


async def _get_text_with_status(url: str) -> tuple[int, str]:
    async with httpx.AsyncClient(timeout=30.0) as client:
        r = await client.get(url, headers={"User-Agent": HTTP_USER_AGENT})
        return r.status_code, r.text


def _default_query_dimension_policy() -> QueryDimensionPolicyConfig:
    return QueryDimensionPolicyConfig(
        default_query_dimensions=[
            QueryDimensionPolicyEntry(
                name="subject",
                role="subject",
                required_for_retrieval=True,
                priority=1,
                preferred_sources=[QueryDimensionSource(type="codelist", id="UNICEF/CL_INDICATOR/latest")],
            ),
            QueryDimensionPolicyEntry(
                name="time",
                role="time",
                required_for_retrieval=True,
                priority=2,
            ),
            QueryDimensionPolicyEntry(
                name="location",
                role="geography",
                required_for_retrieval=True,
                priority=3,
                preferred_sources=[QueryDimensionSource(type="codelist", id="UNICEF/CL_GEO/latest")],
                allow_hierarchy_resolution=True,
                allow_member_expansion=True,
            ),
        ]
    )


def _policy_config_from_dict(payload: dict[str, Any]) -> QueryDimensionPolicyConfig:
    raw_dimensions = payload.get("default_query_dimensions")
    if not isinstance(raw_dimensions, list) or not raw_dimensions:
        raise ValueError("query dimension policy must define a non-empty default_query_dimensions list.")

    dimensions: list[QueryDimensionPolicyEntry] = []
    for item in raw_dimensions:
        if not isinstance(item, dict):
            raise ValueError("query dimension policy entries must be objects.")
        raw_sources = item.get("preferred_sources") or []
        if not isinstance(raw_sources, list):
            raise ValueError("preferred_sources must be a list.")
        sources = [
            QueryDimensionSource(type=str(source.get("type") or ""), id=str(source.get("id") or ""))
            for source in raw_sources
            if isinstance(source, dict) and str(source.get("type") or "").strip() and str(source.get("id") or "").strip()
        ]
        dimensions.append(
            QueryDimensionPolicyEntry(
                name=str(item.get("name") or "").strip(),
                role=str(item.get("role") or "").strip(),
                required_for_retrieval=bool(item.get("required_for_retrieval")),
                priority=int(item.get("priority") or 0),
                preferred_sources=sources,
                allow_hierarchy_resolution=bool(item.get("allow_hierarchy_resolution")),
                allow_member_expansion=bool(item.get("allow_member_expansion")),
            )
        )

    if not all(entry.name for entry in dimensions):
        raise ValueError("each query dimension policy entry must have a name.")
    if len({entry.name for entry in dimensions}) != len(dimensions):
        raise ValueError("query dimension policy entry names must be unique.")
    if len({entry.priority for entry in dimensions}) != len(dimensions):
        raise ValueError("query dimension policy entry priorities must be unique.")
    return QueryDimensionPolicyConfig(default_query_dimensions=dimensions)


@lru_cache(maxsize=1)
def _query_dimension_policy_config() -> QueryDimensionPolicyConfig:
    raw_json = os.getenv("SDMX_QUERY_DIMENSION_POLICY_JSON", "").strip()
    raw_path = os.getenv("SDMX_QUERY_DIMENSION_POLICY_FILE", "").strip()
    default_path = REPO_ROOT / "query_dimension_policy.json"

    if raw_path:
        loaded = json.loads(Path(raw_path).read_text(encoding="utf-8"))
        return _policy_config_from_dict(loaded)
    if raw_json:
        loaded = json.loads(raw_json)
        return _policy_config_from_dict(loaded)
    if default_path.exists():
        loaded = json.loads(default_path.read_text(encoding="utf-8"))
        return _policy_config_from_dict(loaded)
    return _default_query_dimension_policy()


def _query_dimension_policy_payload() -> dict[str, Any]:
    policy = _query_dimension_policy_config()
    return asdict(policy)


def _ordered_query_dimensions() -> list[QueryDimensionPolicyEntry]:
    policy = _query_dimension_policy_config()
    return sorted(policy.default_query_dimensions, key=lambda item: item.priority)


def _default_last_n_observations_enabled() -> bool:
    return _env_flag(
        "SDMX_DEFAULT_LAST_N_OBSERVATIONS",
        True,
        legacy_names=["defaultLastNobservations"],
    )


def _source_scope() -> dict[str, Any]:
    agencies = sorted(AGENCY_ALLOWLIST) if AGENCY_ALLOWLIST else []
    return {
        "allowedAgencies": agencies,
        "policy": "Use only observations returned from these official SDMX flows. If unresolved, do not supplement with external facts.",
    }


def _dataflow_url() -> str:
    # UNICEF service builder defaults to SDMX 2.1 (XML), but FastMCP expects JSON for parsing.
    return f"{BASE}/dataflow/all/all/latest/?format=sdmx-json&detail=full&references=none"


def _structure_url(flow_ref: str) -> str:
    # Practical approach: fetch dataflow with references=all to pull back related structures (DSD, codelists).
    # UNICEF documentation describes this approach. :contentReference[oaicite:5]{index=5}
    # flow_ref should typically look like: AGENCY:FLOW_ID(VERSION) or similar; keep it simple early.
    return f"{BASE}/dataflow/{_flow_path_for(flow_ref)}/?format=sdmx-json&detail=full&references=all"


def _codelist_url(codelist_ref: str) -> str:
    return f"{BASE}/codelist/{_flow_path_for(codelist_ref)}/?format=sdmx-json&detail=full"


def _hierarchical_codelist_url(agency: str, hierarchical_codelist_id: str, version: str = "latest") -> str:
    return f"{BASE}/hierarchicalcodelist/{quote(agency)}/{quote(hierarchical_codelist_id)}/{quote(version)}"


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
    ranked = _scored_code_matches(codes, query)
    trimmed = ranked[:limit]
    for item in trimmed:
        item.pop("_score", None)
    return trimmed


def _scored_code_matches(codes: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
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
    return ranked


def _indicator_codes_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    dims = _dimension_metadata(payload)
    target = next((d for d in dims if d.get("id") == "INDICATOR"), None)
    if not target:
        return []
    codelist_id = target.get("codelist")
    if not isinstance(codelist_id, str) or not codelist_id.strip():
        return []
    codelists = _codelist_map(payload)
    codelist = codelists.get(codelist_id) or codelists.get(_codelist_key(codelist_id))
    if not codelist:
        return []
    return _codelist_codes(codelist)


def _is_cross_sectional_flow(df_id: str, name: str = "", description: str = "") -> bool:
    text = f"{df_id} {name} {description}".lower()
    return any(marker in text for marker in ("cross-sectional", "cross sectional", "cross_sectional"))


def _pick_recommended_flow(candidates: list[dict[str, Any]], query: str) -> dict[str, Any] | None:
    if not candidates:
        return None

    def _rank(item: dict[str, Any]) -> tuple[int, int, int, str]:
        agency = str(item.get("agencyID") or "")
        flow_name = str(item.get("flowName") or "")
        flow_desc = str(item.get("flowDescription") or "")
        flow_id = str(item.get("flowID") or "")
        flow_score = _match_score(f"{flow_id} {flow_name} {flow_desc}".lower(), query)
        cross_penalty = 1 if item.get("isCrossSectional") else 0
        unicef_bonus = 1 if agency == "UNICEF" else 0
        return (flow_score, unicef_bonus, -cross_penalty, flow_id)

    return max(candidates, key=_rank)


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


def _extract_scoped_dataflows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    flows = _extract_dataflows(payload)
    if not AGENCY_ALLOWLIST:
        return flows
    scoped: list[dict[str, Any]] = []
    for df in flows:
        agency = df.get("agencyID") or df.get("agencyId")
        if isinstance(agency, str) and agency in AGENCY_ALLOWLIST:
            scoped.append(df)
    return scoped


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


async def _data_path_for_query(flow_ref: str) -> str:
    """
    Build a data query flow path.
    If version is 'latest', resolve it to a concrete version from dataflow metadata
    because some SDMX /data endpoints reject 'latest' in flowRef.
    """
    agency, df_id, version = _flow_identifiers(flow_ref)
    chosen_agency = agency
    chosen_version = version

    if version.lower() == "latest":
        payload = await _cached_dataflows()
        flows = _extract_scoped_dataflows(payload)
        matches: list[tuple[str, str]] = []
        for df in flows:
            match_id = df.get("id") or df.get("ID")
            if not isinstance(match_id, str) or match_id != df_id:
                continue
            match_agency = df.get("agencyID") or df.get("agencyId") or "all"
            match_version = df.get("version")
            if match_version is None:
                continue
            match_version_text = str(match_version).strip()
            if not match_version_text:
                continue
            matches.append((str(match_agency), match_version_text))

        selected: tuple[str, str] | None = None
        if agency and agency != "all":
            selected = next((item for item in matches if item[0] == agency), None)
        elif len(matches) == 1:
            selected = matches[0]
        elif matches:
            # Stable preference for UNICEF flows when agency is ambiguous.
            selected = next((item for item in matches if item[0] == "UNICEF"), matches[0])

        if selected:
            selected_agency, selected_version = selected
            chosen_agency = agency if agency and agency != "all" else selected_agency
            chosen_version = selected_version

    ident = ",".join(part for part in (chosen_agency, df_id, chosen_version) if part)
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
    if "/" in text:
        parts = [part.strip() for part in text.split("/") if part.strip()]
        if len(parts) >= 2:
            text = parts[1]
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


def _tag_name(element: ET.Element) -> str:
    return element.tag.split("}")[-1]


def _element_text(node: ET.Element, tag_name: str) -> str:
    for elem in node.iter():
        if _tag_name(elem) == tag_name and elem.text:
            text = elem.text.strip()
            if text:
                return text
    return ""


def _hierarchical_ref_id(node: ET.Element) -> str | None:
    for elem in node.iter():
        tag = _tag_name(elem)
        if tag in {"Ref", "CodeRef"}:
            ref_id = elem.attrib.get("id") or elem.attrib.get("ID")
            if ref_id:
                return ref_id.strip()
        if tag == "CodeID" and elem.text and elem.text.strip():
            return elem.text.strip()
    direct = node.attrib.get("id") or node.attrib.get("ID")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    return None


def _walk_hierarchical_codes(node: ET.Element, edges: dict[str, set[str]], parent_ref: str | None = None) -> None:
    tag = _tag_name(node)
    next_parent = parent_ref
    if tag == "HierarchicalCode":
        current_ref = _hierarchical_ref_id(node)
        if parent_ref and current_ref and current_ref != parent_ref:
            edges.setdefault(parent_ref, set()).add(current_ref)
        next_parent = current_ref or parent_ref

    for child in list(node):
        _walk_hierarchical_codes(child, edges, parent_ref=next_parent)


def _hierarchical_edges_from_xml(text: str) -> dict[str, set[str]]:
    if not text.strip():
        return {}
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return {}

    edges: dict[str, set[str]] = {}
    for elem in root.iter():
        if _tag_name(elem) == "Hierarchy":
            _walk_hierarchical_codes(elem, edges)
    return edges


def _hierarchical_catalog_from_xml(text: str) -> list[dict[str, Any]]:
    if not text.strip():
        return []
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []

    results: list[dict[str, Any]] = []
    for elem in root.iter():
        if _tag_name(elem) != "HierarchicalCodelist":
            continue
        agency = elem.attrib.get("agencyID") or elem.attrib.get("agencyId") or elem.attrib.get("agency")
        hierarchy_id = elem.attrib.get("id") or elem.attrib.get("ID")
        version = elem.attrib.get("version") or "latest"
        if not isinstance(hierarchy_id, str) or not hierarchy_id.strip():
            continue
        results.append(
            {
                "agencyID": (agency or "").strip(),
                "id": hierarchy_id.strip(),
                "version": str(version).strip() or "latest",
                "name": _element_text(elem, "Name"),
                "description": _element_text(elem, "Description"),
                "urn": elem.attrib.get("urn") or "",
            }
        )
    return results


def _looks_like_json(text: str) -> bool:
    stripped = text.lstrip()
    return stripped.startswith("{") or stripped.startswith("[")


async def _resolved_flow_details(flowRef: str) -> dict[str, str]:
    requested_agency, flow_id, requested_version = _flow_identifiers(flowRef)
    flow_path = await _data_path_for_query(flowRef)
    decoded = flow_path.replace("%2C", ",")
    agency, _, version = (decoded.split(",") + ["", "", ""])[:3]
    return {
        "requestedFlowRef": flowRef,
        "agencyID": agency or requested_agency,
        "flowID": flow_id,
        "version": version or requested_version,
        "resolvedFlowRef": f"{agency or requested_agency}/{flow_id}/{version or requested_version}",
    }


def _query_context(
    *,
    flow_details: dict[str, str],
    key: str,
    query_url: str,
    dimension_order: list[str] | None,
    format: str,
    labels: str | None,
    startPeriod: str | None,
    endPeriod: str | None,
    lastNObservations: int | None,
    filters: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "sourceScope": _source_scope(),
        "provenance": {
            **flow_details,
            "key": key,
            "dimensionOrder": dimension_order or [],
            "queryURL": query_url,
            "format": format,
            "labels": labels,
            "startPeriod": startPeriod,
            "endPeriod": endPeriod,
            "lastNObservations": lastNObservations,
            "filters": filters or {},
        },
    }


def _resolved_response(
    *,
    flow_details: dict[str, str],
    key: str,
    query_url: str,
    dimension_order: list[str] | None,
    format: str,
    labels: str | None,
    startPeriod: str | None,
    endPeriod: str | None,
    lastNObservations: int | None,
    filters: dict[str, Any] | None,
    maxObs: int,
    raw_csv: str | None = None,
) -> dict[str, Any]:
    payload = _query_context(
        flow_details=flow_details,
        key=key,
        query_url=query_url,
        dimension_order=dimension_order,
        format=format,
        labels=labels,
        startPeriod=startPeriod,
        endPeriod=endPeriod,
        lastNObservations=lastNObservations,
        filters=filters,
    )
    payload.update(
        {
            "status": "resolved",
            "assistant_guidance": "Use only the observations returned here. Cite the agency, flow, key, and query URL when summarizing.",
            "notes": {"maxObs": maxObs, "format": format, "labels": labels, "dataTransportFormat": "csv"},
        }
    )
    if raw_csv is not None:
        payload["raw_csv"] = raw_csv
    return payload


def _unresolved_response(
    *,
    flow_details: dict[str, str],
    key: str,
    query_url: str,
    dimension_order: list[str] | None,
    format: str,
    labels: str | None,
    startPeriod: str | None,
    endPeriod: str | None,
    lastNObservations: int | None,
    filters: dict[str, Any] | None,
    maxObs: int,
    status_code: int | None,
    raw_text: str,
    message: str | None = None,
) -> dict[str, Any]:
    payload = _query_context(
        flow_details=flow_details,
        key=key,
        query_url=query_url,
        dimension_order=dimension_order,
        format=format,
        labels=labels,
        startPeriod=startPeriod,
        endPeriod=endPeriod,
        lastNObservations=lastNObservations,
        filters=filters,
    )
    payload.update(
        {
            "status": "unresolved_from_official_flows",
            "assistant_guidance": "Do not supplement this with non-MCP facts. State that the UNICEF/UNPD flow query did not resolve and report the attempted flow, key, and query URL.",
            "error": {
                "status": status_code,
                "message": message or _parse_sdmx_error(raw_text) or "Query did not resolve from official UNICEF/UNPD flows.",
                "raw": raw_text,
            },
            "notes": {"maxObs": maxObs, "format": format, "labels": labels},
        }
    )
    return payload


def _csv_rows(text: str) -> list[dict[str, Any]]:
    if not text.strip():
        return []
    reader = csv.DictReader(text.splitlines())
    rows: list[dict[str, Any]] = []
    for row in reader:
        if not isinstance(row, dict):
            continue
        rows.append({str(key): value for key, value in row.items() if key is not None})
    return rows


def _find_column(columns: list[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        for column in columns:
            if column == candidate:
                return column
    upper_columns = {column.upper(): column for column in columns}
    for candidate in candidates:
        for upper, original in upper_columns.items():
            if candidate.upper() in upper:
                return original
    return None


def _time_column(rows: list[dict[str, Any]]) -> str | None:
    if not rows:
        return None
    return _find_column(list(rows[0].keys()), ["TIME_PERIOD", "TIME"])


def _value_column(rows: list[dict[str, Any]]) -> str | None:
    if not rows:
        return None
    return _find_column(list(rows[0].keys()), ["OBS_VALUE", "VALUE"])


def _dimension_column(rows: list[dict[str, Any]], dimension_id: str) -> str | None:
    if not rows:
        return None
    return _find_column(list(rows[0].keys()), [dimension_id])


def _row_time_value(row: dict[str, Any], time_column: str | None) -> str:
    if not time_column:
        return ""
    value = row.get(time_column)
    return str(value).strip() if value is not None else ""


def _series_signature(
    row: dict[str, Any],
    *,
    time_column: str | None,
    value_column: str | None,
) -> tuple[tuple[str, str], ...]:
    signature: list[tuple[str, str]] = []
    for key in sorted(row):
        if key in {time_column, value_column}:
            continue
        value = row.get(key)
        signature.append((key, "" if value is None else str(value).strip()))
    return tuple(signature)


def _latest_rows(rows: list[dict[str, Any]], time_column: str | None) -> tuple[str | None, list[dict[str, Any]]]:
    if not rows:
        return None, []
    if not time_column:
        return None, rows
    latest_period = max(_row_time_value(row, time_column) for row in rows)
    return latest_period, [row for row in rows if _row_time_value(row, time_column) == latest_period]


def _query_preview(rows: list[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    return rows[:limit]


def _topline_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    columns = list(rows[0].keys()) if rows else []
    time_column = _time_column(rows)
    value_column = _value_column(rows)
    distinct_counts = {}
    for column in columns:
        distinct_counts[column] = len({str(row.get(column) or "").strip() for row in rows})
    latest_period, latest_rows = _latest_rows(rows, time_column)
    return {
        "rowCount": len(rows),
        "columns": columns,
        "timeColumn": time_column,
        "valueColumn": value_column,
        "latestPeriod": latest_period,
        "latestRowCount": len(latest_rows),
        "distinctCounts": distinct_counts,
        "preview": _query_preview(rows),
    }


def _shape_compact_series(rows: list[dict[str, Any]]) -> dict[str, Any]:
    time_column = _time_column(rows)
    value_column = _value_column(rows)
    return {
        "status": "resolved",
        "shape": "compact_series",
        "series": rows,
        "summary": _topline_summary(rows),
        "timeColumn": time_column,
        "valueColumn": value_column,
    }


def _shape_latest_by_ref_area(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ref_area_column = _dimension_column(rows, "REF_AREA")
    time_column = _time_column(rows)
    value_column = _value_column(rows)
    if not ref_area_column:
        return {
            "status": "shape_not_applicable",
            "shape": "latest_by_ref_area",
            "reason": "REF_AREA column was not present in the returned dataset.",
            "summary": _topline_summary(rows),
        }
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get(ref_area_column) or "").strip(), []).append(row)
    result_rows: list[dict[str, Any]] = []
    for ref_area, members in sorted(grouped.items()):
        latest_period, latest_rows = _latest_rows(members, time_column)
        result_rows.append(
            {
                "refArea": ref_area,
                "latestPeriod": latest_period,
                "rowCountAtLatestPeriod": len(latest_rows),
                "value": latest_rows[0].get(value_column) if value_column and len(latest_rows) == 1 else None,
                "rows": latest_rows[:10],
            }
        )
    return {
        "status": "resolved",
        "shape": "latest_by_ref_area",
        "refAreaColumn": ref_area_column,
        "timeColumn": time_column,
        "valueColumn": value_column,
        "results": result_rows,
        "summary": _topline_summary(rows),
    }


def _shape_latest_single_value(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "status": "no_observations",
            "shape": "latest_single_value",
            "message": "The official query resolved but returned no observation rows.",
        }
    time_column = _time_column(rows)
    value_column = _value_column(rows)
    if not value_column:
        return {
            "status": "no_value_column",
            "shape": "latest_single_value",
            "message": "The returned dataset did not expose an observation value column.",
            "summary": _topline_summary(rows),
        }
    latest_period, latest_rows = _latest_rows(rows, time_column)
    signatures = {_series_signature(row, time_column=time_column, value_column=value_column) for row in latest_rows}
    if len(signatures) > 1:
        return {
            "status": "not_a_single_value",
            "shape": "latest_single_value",
            "message": "The latest period still contains multiple distinct series rows. Narrow more dimensions before answering with one value.",
            "latestPeriod": latest_period,
            "latestRowCount": len(latest_rows),
            "summary": _topline_summary(rows),
            "preview": _query_preview(latest_rows),
        }
    if len(latest_rows) != 1:
        return {
            "status": "not_a_single_value",
            "shape": "latest_single_value",
            "message": "The latest period does not resolve to exactly one observation row.",
            "latestPeriod": latest_period,
            "latestRowCount": len(latest_rows),
            "summary": _topline_summary(rows),
            "preview": _query_preview(latest_rows),
        }
    row = latest_rows[0]
    return {
        "status": "resolved_single_value",
        "shape": "latest_single_value",
        "latestPeriod": latest_period,
        "timeColumn": time_column,
        "valueColumn": value_column,
        "value": row.get(value_column),
        "observation": row,
        "summary": _topline_summary(rows),
    }


def _shape_topline_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "status": "resolved",
        "shape": "topline_summary",
        "summary": _topline_summary(rows),
    }


def _shape_rows(rows: list[dict[str, Any]], shape: str) -> dict[str, Any]:
    normalized = (shape or "").strip().lower()
    if normalized == "compact_series":
        return _shape_compact_series(rows)
    if normalized == "latest_single_value":
        return _shape_latest_single_value(rows)
    if normalized == "latest_by_ref_area":
        return _shape_latest_by_ref_area(rows)
    if normalized == "topline_summary":
        return _shape_topline_summary(rows)
    raise ValueError(
        "Unsupported resultShape. Use one of: compact_series, latest_single_value, latest_by_ref_area, topline_summary."
    )


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


def _selection_tokens(value: Any) -> list[str]:
    normalized = _normalize_selection_values(value)
    if not normalized:
        return []
    return [token for token in normalized.split("+") if token]


def _codelist_codes(codelist: dict[str, Any]) -> list[dict[str, Any]]:
    codes = codelist.get("codes") or codelist.get("code") or []
    if isinstance(codes, dict):
        codes = list(codes.values())
    if not isinstance(codes, list):
        return []
    return [code for code in codes if isinstance(code, dict)]


def _code_identifier(node: Any) -> str | None:
    if isinstance(node, str):
        text = node.strip()
        return text or None
    if not isinstance(node, dict):
        return None
    for key in ("id", "ID", "codeID", "codeId", "codeRef", "value"):
        value = node.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            nested = _code_identifier(value)
            if nested:
                return nested
    return None


def _code_name(code: dict[str, Any]) -> str:
    return _coerce_text(code.get("name")) or _coerce_text(code.get("names"))


def _dimension_code_map(payload: dict[str, Any], dimension_id: str) -> dict[str, dict[str, Any]]:
    dims = _dimension_metadata(payload)
    target = next((d for d in dims if d.get("id") == dimension_id.strip().upper()), None)
    if not target:
        return {}
    codelist_id = target.get("codelist")
    if not isinstance(codelist_id, str) or not codelist_id.strip():
        return {}
    codelists = _codelist_map(payload)
    codelist = codelists.get(codelist_id) or codelists.get(_codelist_key(codelist_id))
    if not codelist:
        return {}
    mapping: dict[str, dict[str, Any]] = {}
    for code in _codelist_codes(codelist):
        code_id = _code_identifier(code)
        if code_id:
            mapping[code_id] = code
    return mapping


def _dimension_codelist(payload: dict[str, Any], dimension_id: str) -> dict[str, Any] | None:
    dims = _dimension_metadata(payload)
    target = next((d for d in dims if d.get("id") == dimension_id.strip().upper()), None)
    if not target:
        return None
    codelist_id = target.get("codelist")
    if not isinstance(codelist_id, str) or not codelist_id.strip():
        return None
    codelists = _codelist_map(payload)
    return codelists.get(codelist_id) or codelists.get(_codelist_key(codelist_id))


def _codelist_meta(payload: dict[str, Any], dimension_id: str) -> dict[str, Any]:
    codelist = _dimension_codelist(payload, dimension_id)
    if not codelist:
        return {}
    codelist_id = str(codelist.get("id") or codelist.get("ID") or "")
    return {
        "id": codelist_id,
        "name": _coerce_text(codelist.get("name")) or _coerce_text(codelist.get("names")),
        "description": _coerce_text(codelist.get("description")) or _coerce_text(codelist.get("descriptions")),
        "key": _codelist_key(codelist_id) if codelist_id else "",
    }


def _ref_area_code_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return _dimension_code_map(payload, "REF_AREA")


def _code_parent_links(codes: dict[str, dict[str, Any]]) -> dict[str, set[str]]:
    edges: dict[str, set[str]] = {}
    for code_id, code in codes.items():
        for key in ("parent", "parentCode", "parentID", "parentId", "parentRef"):
            raw_parent = code.get(key)
            parent_id = _code_identifier(raw_parent)
            if parent_id and parent_id in codes and parent_id != code_id:
                edges.setdefault(parent_id, set()).add(code_id)
    return edges


def _walk_hierarchy_edges(node: Any, valid_ids: set[str], edges: dict[str, set[str]], parent_id: str | None = None) -> None:
    if isinstance(node, list):
        for item in node:
            _walk_hierarchy_edges(item, valid_ids, edges, parent_id=parent_id)
        return

    if not isinstance(node, dict):
        return

    current_id = _code_identifier(node)
    current_valid = current_id if current_id in valid_ids else None
    effective_parent = parent_id
    if parent_id and current_valid and current_valid != parent_id:
        edges.setdefault(parent_id, set()).add(current_valid)
        effective_parent = current_valid
    elif current_valid:
        effective_parent = current_valid

    for key, value in node.items():
        if key in {"id", "ID", "codeID", "codeId", "codeRef", "value", "name", "names", "description", "descriptions"}:
            continue
        child_parent = effective_parent
        if isinstance(value, dict):
            nested_parent = _code_identifier(value)
            if nested_parent in valid_ids:
                child_parent = nested_parent
        _walk_hierarchy_edges(value, valid_ids, edges, parent_id=child_parent)


def _ref_area_hierarchy(payload: dict[str, Any]) -> dict[str, set[str]]:
    codes = _ref_area_code_map(payload)
    if not codes:
        return {}
    valid_ids = set(codes)
    edges = _code_parent_links(codes)
    _walk_hierarchy_edges(payload, valid_ids, edges)
    return edges


async def _official_reporting_region_hierarchy() -> dict[str, set[str]]:
    cache_key = "UNICEF/UNICEF_REPORTING_REGIONS/1.0"
    if cache_key in _hierarchical_codelist_cache:
        return _hierarchical_codelist_cache[cache_key]
    url = _hierarchical_codelist_url("UNICEF", "UNICEF_REPORTING_REGIONS", "1.0")
    status, text = await _get_text_with_status(url)
    if status >= 400:
        return {}
    edges = _hierarchical_edges_from_xml(text)
    if edges:
        _hierarchical_codelist_cache[cache_key] = edges
    return edges


async def _list_hierarchical_codelists_for_agency(agency: str) -> list[dict[str, Any]]:
    cache_key = agency.strip() or "all"
    if cache_key in _hierarchical_catalog_cache:
        return _hierarchical_catalog_cache[cache_key]
    url = _hierarchical_codelist_url(agency or "all", "all", "latest")
    status, text = await _get_text_with_status(url)
    if status >= 400:
        return []
    parsed = _hierarchical_catalog_from_xml(text)
    if parsed:
        _hierarchical_catalog_cache[cache_key] = parsed
    return parsed


async def _get_hierarchical_edges(hierarchy_ref: str) -> dict[str, set[str]]:
    agency, hierarchy_id, version = _flow_identifiers(hierarchy_ref)
    cache_key = f"{agency}/{hierarchy_id}/{version}"
    if cache_key in _hierarchical_codelist_cache:
        return _hierarchical_codelist_cache[cache_key]
    url = _hierarchical_codelist_url(agency, hierarchy_id, version)
    status, text = await _get_text_with_status(url)
    if status >= 400:
        return {}
    edges = _hierarchical_edges_from_xml(text)
    if edges:
        _hierarchical_codelist_cache[cache_key] = edges
    return edges


def _hierarchy_dimension_candidates(
    *,
    hierarchy_id: str,
    hierarchy_name: str,
    dimensions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    text = f"{hierarchy_id} {hierarchy_name}".lower()
    matches: list[dict[str, Any]] = []
    for dim in dimensions:
        dim_id = str(dim.get("id") or "").upper()
        codelist = str(dim.get("codelist") or "")
        codelist_key = _codelist_key(codelist).lower() if codelist else ""
        score = 0
        reasons: list[str] = []
        if dim_id and dim_id.lower() in text:
            score += 20
            reasons.append("hierarchy id/name mentions the dimension")
        if codelist_key and codelist_key in text:
            score += 40
            reasons.append("hierarchy id/name matches the dimension codelist")
        if dim_id == "REF_AREA" and hierarchy_id.upper() == "UNICEF_REPORTING_REGIONS":
            score += 60
            reasons.append("official UNICEF reporting-regions hierarchy is preferred for REF_AREA")
        if score > 0:
            matches.append(
                {
                    "dimension": dim_id,
                    "codelist": codelist,
                    "score": score,
                    "reasons": reasons,
                }
            )
    matches.sort(key=lambda item: int(item.get("score") or 0), reverse=True)
    return matches


async def _structure_hierarchy_summaries(flowRef: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
    flow_details = await _resolved_flow_details(flowRef)
    agency = flow_details.get("agencyID") or "all"
    dimensions = _dimension_metadata(payload)
    catalog = await _list_hierarchical_codelists_for_agency(agency)
    summaries: list[dict[str, Any]] = []
    for item in catalog:
        hierarchy_id = str(item.get("id") or "")
        version = str(item.get("version") or "latest")
        hierarchy_ref = _flow_ref_for(hierarchy_id, version, str(item.get("agencyID") or agency))
        dimension_matches = _hierarchy_dimension_candidates(
            hierarchy_id=hierarchy_id,
            hierarchy_name=str(item.get("name") or ""),
            dimensions=dimensions,
        )
        if not dimension_matches:
            continue
        edges = await _get_hierarchical_edges(hierarchy_ref)
        all_children = {child for children in edges.values() for child in children}
        roots = sorted(code for code in edges if code not in all_children)[:10]
        summaries.append(
            {
                "hierarchyRef": hierarchy_ref,
                "id": hierarchy_id,
                "version": version,
                "name": item.get("name") or "",
                "description": item.get("description") or "",
                "dimensionMatches": dimension_matches[:3],
                "rootCodes": roots,
                "rootCount": len(roots),
            }
        )
    summaries.sort(
        key=lambda item: max((int(match.get("score") or 0) for match in item.get("dimensionMatches", [])), default=0),
        reverse=True,
    )
    return summaries[:10]


async def _dimension_hierarchy_context(flowRef: str, payload: dict[str, Any], dimension_id: str) -> list[dict[str, Any]]:
    summaries = await _structure_hierarchy_summaries(flowRef, payload)
    relevant = []
    for item in summaries:
        matches = item.get("dimensionMatches") or []
        if any(str(match.get("dimension") or "").upper() == dimension_id.upper() for match in matches):
            relevant.append(item)
    return relevant


def _group_likelihood(text: str) -> int:
    lowered = text.lower()
    score = 0
    for token in (
        "region",
        "subregion",
        "country group",
        "countries",
        "group",
        "cluster",
        "category",
        "categories",
        "class",
        "domain",
        "area",
        "programme region",
    ):
        if token in lowered:
            score += 5
    return score


async def _search_reference_candidates(
    flowRef: str,
    query: str,
    dimension: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    payload = await _get_flow_structure(flowRef)
    dimensions = _dimension_metadata(payload)
    wanted_dimension = dimension.strip().upper() if dimension else None
    results: list[dict[str, Any]] = []

    for dim in dimensions:
        dim_id = str(dim.get("id") or "").upper()
        if wanted_dimension and dim_id != wanted_dimension:
            continue
        codelist_meta = _codelist_meta(payload, dim_id)
        code_map = _dimension_code_map(payload, dim_id)
        if codelist_meta:
            codelist_text = " ".join(
                part for part in (
                    codelist_meta.get("id", ""),
                    codelist_meta.get("key", ""),
                    codelist_meta.get("name", ""),
                    codelist_meta.get("description", ""),
                    dim_id,
                    str(dim.get("name") or ""),
                ) if part
            ).lower()
            score = _match_score(codelist_text, query) + _group_likelihood(codelist_text)
            if score > 0:
                results.append(
                    {
                        "kind": "codelist",
                        "dimension": dim_id,
                        "dimensionName": dim.get("name") or "",
                        "id": codelist_meta.get("id") or "",
                        "name": codelist_meta.get("name") or "",
                        "description": codelist_meta.get("description") or "",
                        "score": score,
                        "reason": "matched codelist metadata",
                    }
                )

        hierarchy_context = await _dimension_hierarchy_context(flowRef, payload, dim_id)
        hierarchy_edges: dict[str, dict[str, set[str]]] = {}
        for item in hierarchy_context:
            hierarchy_ref = str(item.get("hierarchyRef") or "")
            if not hierarchy_ref:
                continue
            hierarchy_edges[hierarchy_ref] = await _get_hierarchical_edges(hierarchy_ref)
            text = " ".join(
                part for part in (
                    str(item.get("id") or ""),
                    str(item.get("name") or ""),
                    str(item.get("description") or ""),
                    hierarchy_ref,
                    dim_id,
                ) if part
            ).lower()
            score = _match_score(text, query) + _group_likelihood(text)
            if score > 0:
                results.append(
                    {
                        "kind": "hierarchical_codelist",
                        "dimension": dim_id,
                        "dimensionName": dim.get("name") or "",
                        "id": item.get("id") or "",
                        "name": item.get("name") or "",
                        "description": item.get("description") or "",
                        "hierarchyRef": hierarchy_ref,
                        "score": score,
                        "reason": "matched hierarchical codelist metadata",
                    }
                )

        for code_id, code in code_map.items():
            name = _code_name(code)
            desc = _coerce_text(code.get("description")) or _coerce_text(code.get("descriptions"))
            hierarchy_matches: list[dict[str, Any]] = []
            for item in hierarchy_context:
                hierarchy_ref = str(item.get("hierarchyRef") or "")
                edges = hierarchy_edges.get(hierarchy_ref) or {}
                descendants = _ref_area_descendants(edges, code_id) if edges else []
                if not descendants:
                    continue
                members = _leaf_members(edges, descendants)
                hierarchy_matches.append(
                    {
                        "hierarchyRef": hierarchy_ref,
                        "memberCount": len(members),
                        "memberPreview": members[:10],
                    }
                )
            text = " ".join(
                part for part in (
                    code_id,
                    name,
                    desc,
                    codelist_meta.get("id", ""),
                    codelist_meta.get("name", ""),
                    codelist_meta.get("description", ""),
                    dim_id,
                    str(dim.get("name") or ""),
                ) if part
            ).lower()
            score = _match_score(text, query)
            if hierarchy_matches:
                score += _group_likelihood(text) + 5
            if score <= 0:
                continue
            results.append(
                {
                    "kind": "code",
                    "dimension": dim_id,
                    "dimensionName": dim.get("name") or "",
                    "id": code_id,
                    "name": name,
                    "description": desc,
                    "codelist": codelist_meta.get("id") or "",
                    "codelistName": codelist_meta.get("name") or "",
                    "isAggregate": bool(hierarchy_matches),
                    "hierarchyMatches": hierarchy_matches,
                    "score": score,
                    "reason": "matched code metadata",
                }
            )

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in sorted(results, key=lambda entry: int(entry.get("score") or 0), reverse=True):
        key = (
            str(item.get("kind") or ""),
            str(item.get("dimension") or ""),
            str(item.get("id") or item.get("hierarchyRef") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        item.pop("score", None)
        deduped.append(item)
        if len(deduped) >= limit:
            break
    return deduped


async def _preferred_ref_area_hierarchy(payload: dict[str, Any]) -> tuple[dict[str, set[str]], str]:
    official = await _official_reporting_region_hierarchy()
    if official:
        return official, _hierarchical_codelist_url("UNICEF", "UNICEF_REPORTING_REGIONS", "1.0")
    return _ref_area_hierarchy(payload), "structure-payload-fallback"


def _hierarchy_match_score(
    *,
    hierarchy_id: str,
    hierarchy_name: str,
    requested_code: str,
    code_map: dict[str, dict[str, Any]],
    descendants: list[str],
    dimension_id: str,
    codelist_id: str,
) -> tuple[int, dict[str, Any]]:
    dimension_upper = dimension_id.upper()
    hierarchy_text = f"{hierarchy_id} {hierarchy_name}".lower()
    codelist_key = _codelist_key(codelist_id).lower() if codelist_id else ""
    requested_descendants = [item for item in descendants if item in code_map]
    score = 0
    reasons: list[str] = []

    if requested_descendants:
        score += 100 + len(requested_descendants)
        reasons.append("requested code is present with descendants that intersect the dimension code list")
    if dimension_upper.lower() in hierarchy_text:
        score += 20
        reasons.append("hierarchy id/name mentions the dimension")
    if codelist_key and codelist_key in hierarchy_text:
        score += 30
        reasons.append("hierarchy id/name matches the dimension codelist")
    if "region" in hierarchy_text and ("AREA" in dimension_upper or dimension_upper == "REF_AREA"):
        score += 10
        reasons.append("hierarchy looks geographically relevant to REF_AREA")
    if dimension_upper == "REF_AREA" and hierarchy_id.upper() == "UNICEF_REPORTING_REGIONS":
        score += 40
        reasons.append("official UNICEF reporting-regions hierarchy is preferred for REF_AREA")

    return score, {
        "matchedDescendantCount": len(requested_descendants),
        "matchedDescendants": requested_descendants,
        "reasons": reasons,
    }


def _ref_area_descendants(hierarchy: dict[str, set[str]], root_id: str) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    queue = list(sorted(hierarchy.get(root_id, set())))
    while queue:
        current = queue.pop(0)
        if current in seen or current == root_id:
            continue
        seen.add(current)
        ordered.append(current)
        queue.extend(sorted(hierarchy.get(current, set())))
    return ordered


def _leaf_ref_area_members(hierarchy: dict[str, set[str]], descendants: list[str]) -> list[str]:
    descendant_set = set(descendants)
    leaves: list[str] = []
    for code_id in descendants:
        children = hierarchy.get(code_id, set())
        if not (children & descendant_set):
            leaves.append(code_id)
    return leaves


def _leaf_members(hierarchy: dict[str, set[str]], descendants: list[str]) -> list[str]:
    descendant_set = set(descendants)
    leaves: list[str] = []
    for code_id in descendants:
        children = hierarchy.get(code_id, set())
        if not (children & descendant_set):
            leaves.append(code_id)
    return leaves


def _dimension_meta(payload: dict[str, Any], dimension_id: str) -> dict[str, Any] | None:
    target_id = dimension_id.strip().upper()
    dims = _dimension_metadata(payload)
    return next((d for d in dims if d.get("id") == target_id), None)


def _dimension_codelist_id(payload: dict[str, Any], dimension_id: str) -> str:
    dim = _dimension_meta(payload, dimension_id)
    codelist_id = dim.get("codelist") if isinstance(dim, dict) else None
    if not isinstance(codelist_id, str) or not codelist_id.strip():
        raise ValueError(f"Dimension '{dimension_id}' does not have a codelist reference.")
    return codelist_id


def _canonical_code_id(codes: list[dict[str, Any]], token: str) -> str | None:
    wanted = token.strip()
    if not wanted:
        return None
    wanted_lower = wanted.lower()
    for code in codes:
        code_id = code.get("id") or code.get("ID")
        if isinstance(code_id, str) and code_id.lower() == wanted_lower:
            return code_id
    return None


def _matching_code_label(codes: list[dict[str, Any]], token: str) -> tuple[str, str] | None:
    wanted = token.strip().lower()
    if not wanted:
        return None
    for code in codes:
        code_id = code.get("id") or code.get("ID")
        if not isinstance(code_id, str):
            continue
        name = _coerce_text(code.get("name")) or _coerce_text(code.get("names"))
        if name and name.strip().lower() == wanted:
            return code_id, name
    return None


async def _normalize_filters_to_code_ids(flowRef: str, filters: dict[str, Any]) -> dict[str, Any]:
    payload = await _get_flow_structure(flowRef)
    dims = {str(dim.get("id")): dim for dim in _dimension_metadata(payload) if isinstance(dim.get("id"), str)}
    codelists = _codelist_map(payload)
    normalized_filters: dict[str, Any] = {}

    for raw_dim, raw_value in filters.items():
        dim_id = str(raw_dim).upper()
        dim_meta = dims.get(dim_id)
        if not dim_meta:
            normalized_filters[dim_id] = raw_value
            continue

        codelist_ref = dim_meta.get("codelist")
        if not isinstance(codelist_ref, str) or not codelist_ref.strip():
            normalized_filters[dim_id] = raw_value
            continue

        codelist = codelists.get(codelist_ref) or codelists.get(_codelist_key(codelist_ref))
        if not codelist:
            normalized_filters[dim_id] = raw_value
            continue

        codes = _codelist_codes(codelist)
        if not codes:
            normalized_filters[dim_id] = raw_value
            continue

        canonical_tokens: list[str] = []
        for token in _selection_tokens(raw_value):
            canonical = _canonical_code_id(codes, token)
            if canonical:
                canonical_tokens.append(canonical)
                continue

            label_match = _matching_code_label(codes, token)
            if label_match:
                code_id, label = label_match
                raise ValueError(
                    f"Dimension '{dim_id}' must use code IDs, not labels. "
                    f"Use '{code_id}' instead of '{label}'."
                )

            raise ValueError(
                f"Unknown code ID '{token}' for dimension '{dim_id}'. "
                "Use list_codes to retrieve valid code IDs."
            )

        normalized_filters[dim_id] = "+".join(canonical_tokens)

    return normalized_filters


async def _dimension_order_for_flow(flowRef: str) -> list[str]:
    cache_key = flowRef.strip()
    if cache_key in _dimension_cache:
        return _dimension_cache[cache_key]
    payload = await _get_flow_structure(flowRef)
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


def _normalize_manual_key(key: str, dimension_order: list[str]) -> str:
    """
    Normalize a manually provided SDMX key to the expected dimension count.
    If trailing dimensions are omitted, pad them as empty wildcard segments.
    """
    raw = (key or "").strip()
    if not raw:
        raise ValueError("key must not be empty.")

    parts = raw.split(".")
    expected = len(dimension_order)
    if len(parts) > expected:
        raise ValueError(
            f"Key has too many segments ({len(parts)}). Expected {expected} for dimensions: "
            f"{', '.join(dimension_order)}"
        )
    if len(parts) < expected:
        parts.extend([""] * (expected - len(parts)))
    return ".".join(parts)


async def _query_plan(
    *,
    flowRef: str,
    key: str | None,
    filters: dict[str, Any] | None,
    startPeriod: str | None,
    endPeriod: str | None,
    lastNObservations: int | None,
    format: str,
    labels: str | None,
    resultShape: str | None,
    allowUnboundedTime: bool = False,
) -> dict[str, Any]:
    dimension_order: list[str] | None = None
    normalized_filters: dict[str, Any] | None = None
    wildcard_dimensions: list[str] = []
    effective_format = (format or "csv").strip() or "csv"
    effective_labels = labels.strip() if isinstance(labels, str) and labels.strip() else None
    if effective_format.lower() == "csv" and not effective_labels:
        effective_labels = "name"
    series_like_shapes = {"compact_series", "topline_summary"}
    normalized_shape = (resultShape or "").strip().lower()
    effective_allow_unbounded_time = (
        allowUnboundedTime
        or normalized_shape in series_like_shapes
        or not _default_last_n_observations_enabled()
    )
    effective_last_n = lastNObservations
    if (
        effective_last_n is None
        and not (startPeriod and endPeriod)
        and not effective_allow_unbounded_time
        and _default_last_n_observations_enabled()
    ):
        effective_last_n = 1

    if filters:
        dimension_order = await _dimension_order_for_flow(flowRef)
        normalized_filters = await _normalize_filters_to_code_ids(flowRef, filters)
        key = _build_key_from_filters(dimension_order, normalized_filters)
        wildcard_dimensions = [dim for dim in dimension_order if dim not in normalized_filters]
    elif key:
        dimension_order = await _dimension_order_for_flow(flowRef)
        key = _normalize_manual_key(key, dimension_order)
        wildcard_dimensions = [
            dimension_order[index]
            for index, segment in enumerate(key.split("."))
            if index < len(dimension_order) and not segment.strip()
        ]

    if not key:
        raise ValueError("Provide either a key or filters to identify the data slice.")

    if not (startPeriod and endPeriod) and not effective_last_n and not effective_allow_unbounded_time:
        raise ValueError("Provide start/end periods or lastNObservations to avoid unbounded extracts.")

    flow_details = await _resolved_flow_details(flowRef)
    flow_path = _data_path_for(flow_details["resolvedFlowRef"])
    params: list[str] = []
    if startPeriod and endPeriod:
        params.append(f"startPeriod={quote(startPeriod)}")
        params.append(f"endPeriod={quote(endPeriod)}")
    if effective_last_n is not None and not (startPeriod and endPeriod):
        params.append(f"lastNObservations={int(effective_last_n)}")
    params.append(f"format={quote(effective_format)}")
    if effective_labels:
        params.append(f"labels={quote(effective_labels)}")
    url = f"{BASE}/data/{flow_path}/{quote(key, safe='+.')}?{'&'.join(params)}"
    return {
        "flowDetails": flow_details,
        "dimensionOrder": dimension_order,
        "normalizedFilters": normalized_filters,
        "wildcardDimensions": wildcard_dimensions,
        "key": key,
        "format": effective_format,
        "labels": effective_labels,
        "resultShape": resultShape,
        "queryURL": url,
        "startPeriod": startPeriod,
        "endPeriod": endPeriod,
        "lastNObservations": effective_last_n,
        "allowUnboundedTime": effective_allow_unbounded_time,
    }


def _dataflow_summary(df: dict[str, Any]) -> dict[str, Any] | None:
    df_id = df.get("id") or df.get("ID")
    if not isinstance(df_id, str):
        return None
    agency = df.get("agencyID") or df.get("agencyId") or "all"
    name = _coerce_text(df.get("name")) or _coerce_text(df.get("names"))
    description = _coerce_text(df.get("description")) or _coerce_text(df.get("descriptions"))
    return {
        "id": df_id,
        "agencyID": agency,
        "version": str(df.get("version") or "latest"),
        "name": name,
        "description": description,
        "flowRef": _flow_ref_for(df_id, df.get("version"), agency),
        "themeHint": _infer_theme_hint(df_id, name),
    }


async def _dataflow_summaries() -> list[dict[str, Any]]:
    payload = await _cached_dataflows()
    summaries: list[dict[str, Any]] = []
    for df in _extract_scoped_dataflows(payload):
        item = _dataflow_summary(df)
        if item:
            summaries.append(item)
    return summaries


def _codelist_to_resource_payload(codelist: dict[str, Any]) -> dict[str, Any]:
    codelist_id = str(codelist.get("id") or codelist.get("ID") or "")
    codes: list[dict[str, Any]] = []
    for code in _codelist_codes(codelist):
        code_id = _code_identifier(code)
        if not code_id:
            continue
        codes.append(
            {
                "id": code_id,
                "name": _code_name(code),
                "description": _coerce_text(code.get("description")) or _coerce_text(code.get("descriptions")) or None,
            }
        )
    return {
        "id": codelist_id,
        "name": _coerce_text(codelist.get("name")) or _coerce_text(codelist.get("names")),
        "description": _coerce_text(codelist.get("description")) or _coerce_text(codelist.get("descriptions")) or None,
        "codes": codes,
    }


async def _get_codelist_detail(codelist_ref: str) -> dict[str, Any]:
    cache_key = codelist_ref.strip()
    if cache_key in _codelist_cache:
        return _codelist_cache[cache_key]

    payload = await _get_json(_codelist_url(codelist_ref))
    requested_id = _flow_identifiers(codelist_ref)[1]
    requested_key = _codelist_key(requested_id)
    codelist = None
    for candidate in _extract_codelists(payload):
        candidate_id = candidate.get("id") or candidate.get("ID")
        if not isinstance(candidate_id, str):
            continue
        if candidate_id == requested_id or _codelist_key(candidate_id) == requested_key:
            codelist = candidate
            break
    if not codelist:
        raise ValueError(f"Codelist '{codelist_ref}' was not found.")

    result = _codelist_to_resource_payload(codelist)
    _codelist_cache[cache_key] = result
    return result


def _hierarchical_name_lookup(root: ET.Element) -> dict[str, str]:
    names: dict[str, str] = {}
    for elem in root.iter():
        if _tag_name(elem) != "Code":
            continue
        code_id = _hierarchical_ref_id(elem)
        if not code_id:
            continue
        name = _element_text(elem, "Name")
        if name:
            names[code_id] = name
    return names


def _walk_hierarchical_nodes(
    node: ET.Element,
    nodes: dict[str, dict[str, Any]],
    *,
    parent_id: str | None = None,
    name_lookup: dict[str, str] | None = None,
) -> None:
    if _tag_name(node) == "HierarchicalCode":
        current_id = _hierarchical_ref_id(node)
        if current_id:
            current = nodes.setdefault(
                current_id,
                {
                    "id": current_id,
                    "name": "",
                    "parent_id": parent_id,
                    "children": [],
                },
            )
            if parent_id and current_id != parent_id:
                current["parent_id"] = parent_id
                parent = nodes.setdefault(
                    parent_id,
                    {
                        "id": parent_id,
                        "name": "",
                        "parent_id": None,
                        "children": [],
                    },
                )
                if current_id not in parent["children"]:
                    parent["children"].append(current_id)
            current_name = _element_text(node, "Name") or (name_lookup or {}).get(current_id, "")
            if current_name:
                current["name"] = current_name
            parent_id = current_id

    for child in list(node):
        _walk_hierarchical_nodes(child, nodes, parent_id=parent_id, name_lookup=name_lookup)


def _parse_hierarchical_codelist_detail(text: str) -> list[dict[str, Any]]:
    if not text.strip():
        return []
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []

    name_lookup = _hierarchical_name_lookup(root)
    results: list[dict[str, Any]] = []
    for elem in root.iter():
        if _tag_name(elem) != "HierarchicalCodelist":
            continue
        hierarchy_id = elem.attrib.get("id") or elem.attrib.get("ID")
        if not isinstance(hierarchy_id, str) or not hierarchy_id.strip():
            continue
        nodes: dict[str, dict[str, Any]] = {}
        for child in elem.iter():
            if _tag_name(child) == "Hierarchy":
                _walk_hierarchical_nodes(child, nodes, name_lookup=name_lookup)
        ordered_nodes = sorted(nodes.values(), key=lambda item: item["id"])
        for node in ordered_nodes:
            node["children"] = sorted(node["children"])
        results.append(
            {
                "agencyID": (elem.attrib.get("agencyID") or elem.attrib.get("agencyId") or elem.attrib.get("agency") or "").strip(),
                "id": hierarchy_id.strip(),
                "version": str(elem.attrib.get("version") or "latest").strip() or "latest",
                "name": _element_text(elem, "Name"),
                "description": _element_text(elem, "Description") or None,
                "nodes": ordered_nodes,
            }
        )
    return results


async def _get_hierarchical_codelist_detail(hierarchy_ref: str) -> dict[str, Any]:
    agency, hierarchy_id, version = _flow_identifiers(hierarchy_ref)
    cache_key = f"{agency}/{hierarchy_id}/{version}"
    if cache_key in _hierarchical_codelist_detail_cache:
        return _hierarchical_codelist_detail_cache[cache_key]

    url = _hierarchical_codelist_url(agency, hierarchy_id, version)
    status, text = await _get_text_with_status(url)
    if status >= 400:
        raise ValueError(f"Hierarchical codelist '{hierarchy_ref}' was not found.")

    parsed = _parse_hierarchical_codelist_detail(text)
    match = next(
        (
            item for item in parsed
            if item.get("id") == hierarchy_id and (str(item.get("version") or "latest") == version or version == "latest")
        ),
        None,
    )
    if not match:
        raise ValueError(f"Hierarchical codelist '{hierarchy_ref}' was not found.")
    _hierarchical_codelist_detail_cache[cache_key] = match
    return match


def _find_dimension_from_source(
    payload: dict[str, Any],
    source: QueryDimensionSource,
) -> dict[str, Any] | None:
    dims = _dimension_metadata(payload)
    source_id = source.id.strip().upper()

    if source.type == "dimension":
        return next((dim for dim in dims if str(dim.get("id") or "").upper() == source_id), None)

    if source.type == "codelist":
        source_key = _codelist_key(source.id).upper()
        return next(
            (
                dim for dim in dims
                if _codelist_key(str(dim.get("codelist") or "")).upper() == source_key
            ),
            None,
        )

    return None


def _canonical_token_from_codes(codes: list[dict[str, Any]], token: str) -> tuple[str, str] | None:
    canonical = _canonical_code_id(codes, token)
    if canonical:
        code = next((item for item in codes if (_code_identifier(item) or "") == canonical), {})
        return canonical, _code_name(code)
    label_match = _matching_code_label(codes, token)
    if label_match:
        return label_match
    return None


def _leaf_members_from_nodes(nodes: list[dict[str, Any]], root_id: str) -> list[str]:
    node_map = {str(node.get("id") or ""): node for node in nodes if str(node.get("id") or "")}
    descendants: list[str] = []
    queue = list(node_map.get(root_id, {}).get("children") or [])
    seen: set[str] = set()
    while queue:
        current = queue.pop(0)
        if current in seen:
            continue
        seen.add(current)
        descendants.append(current)
        queue.extend(node_map.get(current, {}).get("children") or [])
    return [item for item in descendants if not (node_map.get(item, {}).get("children") or [])]


def _parse_time_range(raw_time_range: str) -> tuple[str | None, str | None, str]:
    token = raw_time_range.strip()
    if not token or token.lower() in {"latest", "current", "most_recent"}:
        return None, None, "latest"
    if token.lower() in {"all", "full", "trend", "series", "chart", "graph", "table"}:
        return None, None, "all"
    if ":" in token:
        start_period, end_period = (part.strip() for part in token.split(":", 1))
        return (start_period or None), (end_period or None), "range"
    return token, token, "range"


async def _resolve_time_value(
    flow_ref: str,
    payload: dict[str, Any],
    raw_time_range: str,
    policy: QueryDimensionPolicyEntry,
) -> dict[str, Any]:
    start_period, end_period, time_mode = _parse_time_range(raw_time_range)
    dim = _dimension_meta(payload, "TIME_PERIOD")
    if not dim:
        raise ValueError(f"Unable to resolve time dimension for flow '{flow_ref}' using the configured policy.")
    return {
        "name": policy.name,
        "role": policy.role,
        "dimension_id": str(dim.get("id") or ""),
        "value": raw_time_range.strip(),
        "startPeriod": start_period,
        "endPeriod": end_period,
        "timeMode": time_mode,
        "useLatestObservation": time_mode == "latest",
        "useAllObservations": time_mode == "all",
        "source": {"type": "dimension", "id": "TIME_PERIOD"},
        "flowRef": flow_ref,
    }


def _input_aliases_for_policy(policy: QueryDimensionPolicyEntry) -> list[str]:
    aliases = {
        policy.name.strip().lower(),
        policy.role.strip().lower(),
    }
    if policy.role == "subject":
        aliases.add("indicator")
        aliases.add("subject")
    if policy.role == "geography":
        aliases.add("geography")
        aliases.add("location")
        aliases.add("ref_area")
    if policy.role == "time":
        aliases.add("time")
        aliases.add("time_range")
        aliases.add("period")
    return [alias for alias in aliases if alias]


def _input_value_for_policy(provided_inputs: dict[str, str], policy: QueryDimensionPolicyEntry) -> str | None:
    normalized = {str(key).strip().lower(): value for key, value in provided_inputs.items()}
    for alias in _input_aliases_for_policy(policy):
        if alias in normalized:
            return normalized[alias]
    return None


def _same_flow_ref(left: str, right: str) -> bool:
    try:
        left_agency, left_id, left_version = _flow_identifiers(left)
        right_agency, right_id, right_version = _flow_identifiers(right)
    except ValueError:
        return left.strip() == right.strip()
    if left_agency != right_agency or left_id != right_id:
        return False
    if left_version in {"", "latest"} or right_version in {"", "latest"}:
        return True
    return left_version == right_version


def _dimension_for_hierarchical_source(
    payload: dict[str, Any],
    policy: QueryDimensionPolicyEntry,
    hierarchy_source: QueryDimensionSource,
) -> dict[str, Any] | None:
    non_hierarchy_sources = [source for source in policy.preferred_sources if source.type != "hierarchical_codelist"]
    candidates = []
    for source in non_hierarchy_sources:
        dim = _find_dimension_from_source(payload, source)
        if dim and not any(str(existing.get("id") or "") == str(dim.get("id") or "") for existing in candidates):
            candidates.append(dim)
    if len(candidates) == 1:
        return candidates[0]

    summaries = payload.get("hierarchicalCodelists") or []
    requested_ref = hierarchy_source.id.strip()
    for summary in summaries:
        if not isinstance(summary, dict):
            continue
        hierarchy_ref = str(summary.get("hierarchyRef") or "")
        if not hierarchy_ref or not _same_flow_ref(hierarchy_ref, requested_ref):
            continue
        matches = summary.get("dimensionMatches") or []
        for match in matches:
            dimension_id = str(match.get("dimension") or "").upper()
            dim = _dimension_meta(payload, dimension_id)
            if dim:
                return dim
    return candidates[0] if candidates else None


async def _resolve_coded_dimension_value(
    flow_ref: str,
    payload: dict[str, Any],
    raw_value: str,
    policy: QueryDimensionPolicyEntry,
) -> dict[str, Any]:
    tokens = [part.strip() for part in raw_value.replace("+", ",").split(",") if part.strip()]
    if not tokens:
        raise ValueError(f"{policy.name} must not be empty.")

    resolved: list[dict[str, Any]] = []
    for token in tokens:
        matched = False
        for source in policy.preferred_sources:
            dim = (
                _find_dimension_from_source(payload, source)
                if source.type != "hierarchical_codelist"
                else _dimension_for_hierarchical_source(payload, policy, source)
            )
            if not dim:
                continue
            dimension_id = str(dim.get("id") or "")
            codelist = _dimension_codelist(payload, dimension_id)
            if codelist and source.type != "hierarchical_codelist":
                match = _canonical_token_from_codes(_codelist_codes(codelist), token)
                if match:
                    code_id, label = match
                    resolved.append(
                        {
                            "token": token,
                            "match_type": "flat_codelist",
                            "id": code_id,
                            "label": label,
                            "members": [code_id],
                            "source": {"type": source.type, "id": source.id},
                            "dimension_id": dimension_id,
                            "role": policy.role,
                        }
                    )
                    matched = True
                    break

            if source.type != "hierarchical_codelist" or not policy.allow_hierarchy_resolution:
                continue

            hierarchy = await _get_hierarchical_codelist_detail(source.id)
            nodes = hierarchy.get("nodes") or []
            node_map = {str(node.get("id") or "").lower(): node for node in nodes if str(node.get("id") or "")}
            name_map = {
                str(node.get("name") or "").strip().lower(): node
                for node in nodes
                if str(node.get("name") or "").strip()
            }
            node = node_map.get(token.lower()) or name_map.get(token.lower())
            if not node:
                continue
            node_id = str(node.get("id") or "")
            members = [node_id]
            if policy.allow_member_expansion:
                leaf_members = _leaf_members_from_nodes(nodes, node_id)
                if leaf_members:
                    if codelist:
                        valid_codes = {_code_identifier(code) for code in _codelist_codes(codelist)}
                        members = [member for member in leaf_members if member in valid_codes]
                    else:
                        members = leaf_members
                    if not members:
                        members = [node_id]
            resolved.append(
                {
                    "token": token,
                    "match_type": "hierarchy_node",
                    "id": node_id,
                    "label": str(node.get("name") or ""),
                    "members": members,
                    "source": {"type": source.type, "id": source.id},
                    "dimension_id": dimension_id,
                    "role": policy.role,
                }
            )
            matched = True
            break

        if not matched:
            raise ValueError(f"Unable to resolve {policy.name} value '{token}' using the configured policy.")

    dimension_id = str(resolved[0].get("dimension_id") or "")
    flattened_members: list[str] = []
    for item in resolved:
        for member in item.get("members") or []:
            if member not in flattened_members:
                flattened_members.append(member)
    return {
        "name": policy.name,
        "role": policy.role,
        "dimension_id": dimension_id,
        "values": flattened_members,
        "matches": resolved,
        "flowRef": flow_ref,
    }


async def _resolve_query_dimension_inputs(
    flow_ref: str,
    provided_inputs: dict[str, str],
) -> dict[str, Any]:
    payload = await _get_flow_structure(flow_ref)
    resolved: dict[str, Any] = {}
    resolution_order: list[str] = []
    for policy in _ordered_query_dimensions():
        raw_value = _input_value_for_policy(provided_inputs, policy)
        if raw_value is None:
            if policy.required_for_retrieval:
                raise ValueError(f"Missing required input for query dimension '{policy.name}' ({policy.role}).")
            continue
        if policy.role == "time":
            resolved[policy.name] = await _resolve_time_value(flow_ref, payload, raw_value, policy)
        else:
            resolved[policy.name] = await _resolve_coded_dimension_value(flow_ref, payload, raw_value, policy)
        resolution_order.append(policy.name)
    resolved["_resolution_order"] = resolution_order
    return resolved


def _observation_rows_to_resource(
    rows: list[dict[str, Any]],
    *,
    indicator_dimension: str,
    location_dimension: str,
    time_dimension: str,
) -> list[dict[str, Any]]:
    value_column = _value_column(rows)
    observations: list[dict[str, Any]] = []
    for row in rows:
        observations.append(
            {
                "indicator": row.get(indicator_dimension),
                "geography": row.get(location_dimension),
                "time": row.get(time_dimension),
                "value": row.get(value_column) if value_column else None,
            }
        )
    return observations


async def _cached_dataflows() -> dict[str, Any]:
    """Internal cached SDMX dataflows payload for discovery tools."""
    if "dataflows" not in _dataflow_cache:
        _dataflow_cache["dataflows"] = await _get_json(_dataflow_url())
    return _dataflow_cache["dataflows"]


async def _get_flow_structure(flowRef: str) -> dict[str, Any]:
    if flowRef not in _structure_cache:
        _structure_cache[flowRef] = await _get_json(_structure_url(flowRef))
    payload = _structure_cache[flowRef]
    hierarchy_summaries = await _structure_hierarchy_summaries(flowRef, payload)
    enriched = dict(payload)
    enriched["hierarchicalCodelists"] = hierarchy_summaries
    enriched["assistant_guidance"] = (
        "If a selected code looks aggregate, or a query implies a region, category, or country group, "
        "inspect hierarchicalCodelists or use search_reference_candidates / resolve_hierarchy / "
        "resolve_dimension_fallback before retrying with member codes."
    )
    return enriched


async def _list_dimensions_for_flow(flow_ref: str) -> list[dict[str, Any]]:
    payload = await _get_flow_structure(flow_ref)
    return _dimension_metadata(payload)


async def _execute_query_data(
    *,
    flowRef: str,
    key: Optional[str] = None,
    startPeriod: Optional[str] = None,
    endPeriod: Optional[str] = None,
    format: str = "sdmx-json",
    labels: Optional[str] = None,
    maxObs: int = 50_000,
    filters: dict[str, Any] | None = None,
    lastNObservations: Optional[int] = None,
    resultShape: Optional[str] = None,
    allowUnboundedTime: bool = False,
) -> dict[str, Any]:
    requested_format = (format or "csv").strip() or "csv"
    fetch_format = "csv"
    plan = await _query_plan(
        flowRef=flowRef,
        key=key,
        filters=filters,
        startPeriod=startPeriod,
        endPeriod=endPeriod,
        lastNObservations=lastNObservations,
        format=fetch_format,
        labels=labels,
        resultShape=resultShape,
        allowUnboundedTime=allowUnboundedTime,
    )
    flow_details = plan["flowDetails"]
    dimension_order = plan["dimensionOrder"]
    normalized_filters = plan["normalizedFilters"]
    planned_key = plan["key"]
    url = plan["queryURL"]
    effective_format = str(plan["format"] or fetch_format)
    effective_labels = plan["labels"]
    effective_last_n = plan["lastNObservations"]

    status, text = await _get_text_with_status(url)
    if status >= 400:
        return _unresolved_response(
            flow_details=flow_details,
            key=planned_key,
            query_url=url,
            dimension_order=dimension_order,
            format=effective_format,
            labels=effective_labels,
            startPeriod=startPeriod,
            endPeriod=endPeriod,
            lastNObservations=effective_last_n,
            filters=normalized_filters,
            maxObs=maxObs,
            status_code=status,
            raw_text=text,
        )
    payload = _resolved_response(
        flow_details=flow_details,
        key=planned_key,
        query_url=url,
        dimension_order=dimension_order,
        format=effective_format,
        labels=effective_labels,
        startPeriod=startPeriod,
        endPeriod=endPeriod,
        lastNObservations=effective_last_n,
        filters=normalized_filters,
        maxObs=maxObs,
        raw_csv=text,
    )
    if requested_format.lower() != "csv":
        payload["notes"]["requestedFormat"] = requested_format
        payload["notes"]["formatOverride"] = "SDMX data queries are forced to CSV for efficiency and simpler downstream parsing."
    if resultShape:
        rows = _csv_rows(text)
        payload["shaped"] = _shape_rows(rows, resultShape)
    return payload


async def dataflows_resource() -> dict[str, Any]:
    return {"dataflows": await _dataflow_summaries()}


async def dimensions_for_dataflow_resource(dataflow_id: str) -> dict[str, Any]:
    return {
        "dataflow_id": dataflow_id,
        "dimensions": await _list_dimensions_for_flow(dataflow_id),
    }


async def codelist_resource(id: str) -> dict[str, Any]:
    return await _get_codelist_detail(id)


async def hierarchical_codelist_resource(id: str) -> dict[str, Any]:
    return await _get_hierarchical_codelist_detail(id)


def query_dimension_policy_resource() -> dict[str, Any]:
    return _query_dimension_policy_payload()


async def observations_resource(
    dataflow_id: str,
    indicator: str,
    geography: str,
    time_range: str,
) -> dict[str, Any]:
    resolved = await _resolve_query_dimension_inputs(
        dataflow_id,
        {
            "indicator": indicator,
            "geography": geography,
            "time_range": time_range,
        },
    )
    time_resolution = next(
        value for key, value in resolved.items() if key != "_resolution_order" and value.get("role") == "time"
    )
    coded_resolutions = [
        value for key, value in resolved.items() if key != "_resolution_order" and value.get("role") != "time"
    ]
    filters = {}
    for item in coded_resolutions:
        values = item.get("values")
        filters[item["dimension_id"]] = values if values is not None else item.get("value")
    result = await _execute_query_data(
        flowRef=dataflow_id,
        filters=filters,
        startPeriod=time_resolution["startPeriod"],
        endPeriod=time_resolution["endPeriod"],
        lastNObservations=1 if time_resolution.get("useLatestObservation") else None,
        format="csv",
        labels="name",
        allowUnboundedTime=bool(time_resolution.get("useAllObservations")),
    )
    if result.get("status") != "resolved":
        return {
            "dataflow_id": dataflow_id,
            "query": {
                "indicator": indicator,
                "geography": geography,
                "time_range": time_range,
            },
            "resolution": resolved,
            "status": result.get("status"),
            "error": result.get("error"),
        }

    rows = _csv_rows(result.get("raw_csv") or "")
    subject_resolution = next((item for item in coded_resolutions if item.get("role") == "subject"), None)
    geography_resolution = next((item for item in coded_resolutions if item.get("role") == "geography"), None)
    observations = _observation_rows_to_resource(
        rows,
        indicator_dimension=str(subject_resolution.get("dimension_id") if subject_resolution else ""),
        location_dimension=str(geography_resolution.get("dimension_id") if geography_resolution else ""),
        time_dimension=time_resolution["dimension_id"],
    )
    return {
        "dataflow_id": dataflow_id,
        "query": {
            "indicator": indicator,
            "geography": geography,
            "time_range": time_range,
        },
        "resolution": resolved,
        "observations": observations,
        "provenance": result.get("provenance"),
    }


@mcp.tool()
async def list_agencies(limit: int = 50) -> list[dict[str, Any]]:
    """
    List agencies from the UNICEF SDMX service with optional descriptions.
    """
    payload = await _cached_dataflows()
    scoped_flows = _extract_scoped_dataflows(payload)
    scoped_agency_ids = {
        str(df.get("agencyID") or df.get("agencyId"))
        for df in scoped_flows
        if isinstance(df.get("agencyID") or df.get("agencyId"), str)
    }
    agencies = _extract_agencies(payload)
    if scoped_agency_ids:
        agencies = [
            agency
            for agency in agencies
            if isinstance(agency.get("id") or agency.get("ID"), str)
            and str(agency.get("id") or agency.get("ID")) in scoped_agency_ids
        ]
    if not agencies:
        # Fall back to agencies inferred from scoped dataflows.
        for df in scoped_flows:
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
async def plan_topic_query(
    question: str,
    flowQuery: str | None = None,
    indicatorLimit: int = 10,
    flowLimit: int = 200,
) -> dict[str, Any]:
    """
    Plan a topical user question into an indicator-first SDMX workflow.

    Use this as the first tool for questions about a phenomenon or metric such as
    stunting, wasting, mortality, literacy, vaccination, or similar subject-matter topics.
    This tool reads the configured query-dimension policy, highlights the highest-priority
    required dimension, and returns ranked indicator candidates before any flow-level query.
    Do not start with search_dataflows for topical questions unless the user explicitly asks
    for a dataset or dataflow by name.
    """
    q = (question or "").strip()
    if not q:
        raise ValueError("question must not be empty.")

    policy = _sorted_query_dimension_policies()
    prioritized_dimensions = [
        {
            "name": item.name,
            "role": item.role,
            "requiredForRetrieval": item.required_for_retrieval,
            "priority": item.priority,
            "preferredSources": [asdict(source) for source in item.preferred_sources],
            "allowHierarchyResolution": item.allow_hierarchy_resolution,
            "allowMemberExpansion": item.allow_member_expansion,
        }
        for item in policy
    ]

    first_required = next((item for item in policy if item.required_for_retrieval), None)
    indicator_candidates: list[dict[str, Any]] = []
    if first_required and first_required.role == "subject":
        indicator_candidates = await find_indicator_candidates(
            query=q,
            flowQuery=flowQuery,
            limit=indicatorLimit,
            flowLimit=flowLimit,
        )

    recommended_flow_refs: list[str] = []
    for item in indicator_candidates:
        flow_ref = str(item.get("recommendedFlowRef") or "").strip()
        if flow_ref and flow_ref not in recommended_flow_refs:
            recommended_flow_refs.append(flow_ref)

    return {
        "question": q,
        "policyDrivenDiscovery": True,
        "prioritizedDimensions": prioritized_dimensions,
        "firstRequiredDimension": (
            {
                "name": first_required.name,
                "role": first_required.role,
                "priority": first_required.priority,
            }
            if first_required
            else None
        ),
        "indicatorCandidates": indicator_candidates,
        "recommendedFlowRefs": recommended_flow_refs,
        "recommendedSequence": [
            "Use find_indicator_candidates(question) first to resolve the subject/indicator.",
            "Choose a flow from the indicator candidate's recommendedFlowRef or dataflows list.",
            "Only then constrain geography and time with list_codes/search_reference_candidates/expand_ref_area_group.",
            "Call validate_query_scope before query_data or resolve_and_query_data.",
        ],
        "assistant_guidance": (
            "For topical questions, resolve the subject dimension first. "
            "Do not start with search_dataflows unless the user is explicitly asking for a dataset or flow."
        ),
    }


@mcp.tool()
async def search_dataflows(query: str, limit: int = 10) -> list[dict[str, Any]]:
    """
    Search UNICEF SDMX dataflows by id/name/description.
    Returns lightweight matches with a flowRef you can pass to other tools.

    Prefer find_indicator_candidates or plan_topic_query for topical metric questions
    such as stunting, wasting, mortality, immunization, literacy, or similar phenomena.
    Use search_dataflows when the user is asking for a known dataset, domain, or flow,
    not when the main problem is to identify the right indicator first.
    """
    payload = await _cached_dataflows()
    flows = _extract_scoped_dataflows(payload)
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
    payload = await _cached_dataflows()
    flows = _extract_scoped_dataflows(payload)
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
    payload = await _cached_dataflows()
    flows = _extract_scoped_dataflows(payload)
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
    payload = await _get_flow_structure(flowRef)
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
    return await _list_dimensions_for_flow(flowRef)


@mcp.tool()
async def list_codes(
    flowRef: str,
    dimension: str,
    query: str | None = None,
    limit: int = 50,
    includeHierarchyHints: bool = True,
) -> list[dict[str, Any]]:
    """
    List codes for a specific dimension, optionally filtered by a query string.
    Results can be enriched with hierarchy-aware hints so callers can see whether
    a code looks atomic, aggregate, or structurally ambiguous at discovery time.
    """
    payload = await _get_flow_structure(flowRef)
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
    code_map = {
        str(code.get("id") or code.get("ID")): code
        for code in codes
        if isinstance(code, dict) and isinstance(code.get("id") or code.get("ID"), str)
    }
    parent_links = _code_parent_links(code_map)
    reverse_parent_links: dict[str, str] = {}
    for parent_id, children in parent_links.items():
        for child_id in children:
            reverse_parent_links[child_id] = parent_id

    hierarchy_context = await _dimension_hierarchy_context(flowRef, payload, dim_id) if includeHierarchyHints else []
    hierarchy_edges: dict[str, dict[str, set[str]]] = {}
    for item in hierarchy_context:
        hierarchy_ref = str(item.get("hierarchyRef") or "")
        if not hierarchy_ref:
            continue
        hierarchy_edges[hierarchy_ref] = await _get_hierarchical_edges(hierarchy_ref)
    results: list[dict[str, Any]] = []
    for code in codes:
        if not isinstance(code, dict):
            continue
        code_id = code.get("id") or code.get("ID")
        if not isinstance(code_id, str):
            continue
        name = _coerce_text(code.get("name")) or _coerce_text(code.get("names"))
        desc = _coerce_text(code.get("description")) or _coerce_text(code.get("descriptions"))
        codelist_children = sorted(parent_links.get(code_id, set()))
        hierarchy_matches: list[dict[str, Any]] = []
        if includeHierarchyHints:
            for item in hierarchy_context:
                hierarchy_ref = str(item.get("hierarchyRef") or "")
                edges = hierarchy_edges.get(hierarchy_ref) or {}
                descendants = _ref_area_descendants(edges, code_id) if edges else []
                if not descendants:
                    continue
                members = _leaf_members(edges, descendants)
                hierarchy_matches.append(
                    {
                        "hierarchyRef": hierarchy_ref,
                        "name": item.get("name") or "",
                        "memberCount": len(members),
                        "memberPreview": members[:10],
                    }
                )
        hierarchy_text = " ".join(
            f"{match.get('hierarchyRef','')} {match.get('name','')}" for match in hierarchy_matches
        )
        text = f"{code_id} {name} {desc} {hierarchy_text}".lower()
        if q and q not in text:
            continue
        has_children = bool(codelist_children) or bool(hierarchy_matches)
        structural_evidence_available = includeHierarchyHints and (bool(parent_links) or bool(hierarchy_context))
        if has_children:
            kind = "aggregate"
        elif structural_evidence_available:
            kind = "leaf"
        else:
            kind = "unknown"
        results.append(
            {
                "id": code_id,
                "name": name,
                "description": desc,
                "kind": kind,
                "expandable": has_children,
                "hasChildren": has_children,
                "memberCount": max(
                    [len(codelist_children)] + [int(match.get("memberCount") or 0) for match in hierarchy_matches],
                    default=0,
                ),
                "hierarchySource": (hierarchy_matches[0].get("hierarchyRef") if hierarchy_matches else ""),
                "childrenPreview": codelist_children[:10],
                "parentCode": reverse_parent_links.get(code_id) or "",
                "hierarchyMatches": hierarchy_matches if includeHierarchyHints else [],
                "hierarchyHintsIncluded": includeHierarchyHints,
            }
        )
        if len(results) >= limit:
            break
    return results


@mcp.tool()
async def search_reference_candidates(
    flowRef: str,
    query: str,
    dimension: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """
    Search group-like reference structures across ordinary codelists, codes, and hierarchical codelists.
    Use this when a user query implies a region, category, country group, or other aggregate concept.
    """
    q = (query or "").strip()
    if not q:
        return []
    return await _search_reference_candidates(
        flowRef=flowRef,
        query=q,
        dimension=dimension,
        limit=limit,
    )


@mcp.tool()
async def find_indicator_candidates(
    query: str,
    flowRef: str | None = None,
    limit: int = 10,
    flowQuery: str | None = None,
    flowLimit: int = 200,
) -> list[dict[str, Any]]:
    """
    Rank indicator codes by matching query text against codelist labels/descriptions.
    If flowRef is omitted, scan scoped flows and return indicator candidates with matching dataflows.

    This is the preferred first step for topical user questions where the subject/metric
    is not already resolved. For requests such as "stunting in Latin America" or
    "vaccination coverage in West Africa", call this before search_dataflows so the
    subject dimension is resolved before flow selection.
    """
    q = (query or "").strip()
    if not q:
        return []

    if flowRef:
        payload = await _get_flow_structure(flowRef)
        codes = _indicator_codes_from_payload(payload)
        if not codes:
            raise ValueError("INDICATOR dimension not found for this flow.")
        return _ranked_code_matches(codes, q, limit=limit)

    payload = await _cached_dataflows()
    flows = _extract_scoped_dataflows(payload)
    if flowQuery:
        flow_q = flowQuery.strip().lower()
        scored_flows: list[tuple[int, dict[str, Any]]] = []
        for df in flows:
            df_id = str(df.get("id") or df.get("ID") or "")
            name = _coerce_text(df.get("name")) or _coerce_text(df.get("names"))
            desc = _coerce_text(df.get("description")) or _coerce_text(df.get("descriptions"))
            score = _match_score(f"{df_id} {name} {desc}".lower(), flow_q)
            if score > 0:
                scored_flows.append((score, df))
        scored_flows.sort(key=lambda item: item[0], reverse=True)
        flows = [item[1] for item in scored_flows]

    if flowLimit > 0:
        flows = flows[:flowLimit]

    merged: dict[str, dict[str, Any]] = {}

    for df in flows:
        df_id = str(df.get("id") or df.get("ID") or "").strip()
        if not df_id:
            continue
        agency = str(df.get("agencyID") or df.get("agencyId") or "all")
        version = str(df.get("version") or "latest")
        flow_ref = _flow_ref_for(df_id, version, agency)
        flow_name = _coerce_text(df.get("name")) or _coerce_text(df.get("names"))
        flow_desc = _coerce_text(df.get("description")) or _coerce_text(df.get("descriptions"))

        try:
            structure = await _get_flow_structure(flow_ref)
        except Exception:
            continue

        codes = _indicator_codes_from_payload(structure)
        if not codes:
            continue
        for item in _scored_code_matches(codes, q):
            code_id = str(item.get("id") or "").strip()
            if not code_id:
                continue
            bucket = merged.setdefault(
                code_id,
                {
                    "id": code_id,
                    "name": item.get("name") or "",
                    "description": item.get("description") or "",
                    "_score": int(item.get("_score") or 0),
                    "dataflows": [],
                },
            )

            item_score = int(item.get("_score") or 0)
            if item_score > int(bucket.get("_score") or 0):
                bucket["_score"] = item_score
                bucket["name"] = item.get("name") or bucket.get("name") or ""
                bucket["description"] = item.get("description") or bucket.get("description") or ""

            flow_candidates: list[dict[str, Any]] = bucket["dataflows"]
            if any(existing.get("flowRef") == flow_ref for existing in flow_candidates):
                continue
            flow_candidates.append(
                {
                    "flowRef": flow_ref,
                    "agencyID": agency,
                    "flowID": df_id,
                    "flowName": flow_name,
                    "flowDescription": flow_desc,
                    "isCrossSectional": _is_cross_sectional_flow(df_id, flow_name, flow_desc),
                }
            )

    ranked = sorted(
        merged.values(),
        key=lambda item: (int(item.get("_score") or 0), len(item.get("dataflows") or [])),
        reverse=True,
    )[:limit]

    for item in ranked:
        candidates = item.get("dataflows") or []
        recommended = _pick_recommended_flow(candidates, q)
        item["recommendedFlowRef"] = recommended.get("flowRef") if isinstance(recommended, dict) else None
        item.pop("_score", None)

    return ranked


@mcp.tool()
async def search_indicators(
    query: str,
    flowRef: str | None = None,
    limit: int = 10,
    flowQuery: str | None = None,
    flowLimit: int = 200,
) -> list[dict[str, Any]]:
    """
    Alias for find_indicator_candidates with identical behavior and parameters.
    """
    return await find_indicator_candidates(
        query=query,
        flowRef=flowRef,
        limit=limit,
        flowQuery=flowQuery,
        flowLimit=flowLimit,
    )


@mcp.tool()
async def get_flow_structure(flowRef: str) -> dict[str, Any]:
    """
    Fetch and cache a flow's structure payload (DSD + codelists via references=all).
    """
    return await _get_flow_structure(flowRef)


@mcp.tool()
async def build_key(flowRef: str, selections: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Build an SDMX key string from human-friendly dimension selections.
    Pass a mapping of dimension names to a single value or list of values.
    """
    if not selections:
        raise ValueError("selections must include at least one dimension.")
    dimension_order = await _dimension_order_for_flow(flowRef)
    normalized_selections = await _normalize_filters_to_code_ids(flowRef, selections)
    key = _build_key_from_filters(dimension_order, normalized_selections)
    return {
        "key": key,
        "dimensionOrder": dimension_order,
        "notes": {
            "multipleValues": "Use arrays or comma-separated strings to include multiple codes per dimension.",
            "placeholders": "Dimensions without selections are filled automatically with empty segments.",
            "codeIdsOnly": "For codelist-backed dimensions, selections must use code IDs exactly as returned by list_codes.",
        },
    }


@mcp.tool()
async def list_hierarchical_codelists(
    agency: str | None = None,
    query: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """
    List hierarchical codelists for an agency, optionally filtered by text.
    """
    chosen_agency = (agency or "all").strip() or "all"
    items = await _list_hierarchical_codelists_for_agency(chosen_agency)
    if AGENCY_ALLOWLIST and chosen_agency == "all":
        items = [item for item in items if item.get("agencyID") in AGENCY_ALLOWLIST]
    q = (query or "").strip().lower()
    results: list[dict[str, Any]] = []
    for item in items:
        text = f"{item.get('id','')} {item.get('name','')} {item.get('description','')}".lower()
        score = _match_score(text, q) if q else 0
        if q and score == 0:
            continue
        results.append(
            {
                "agencyID": item.get("agencyID"),
                "id": item.get("id"),
                "version": item.get("version"),
                "name": item.get("name"),
                "description": item.get("description"),
                "hierarchyRef": _flow_ref_for(str(item.get("id") or ""), str(item.get("version") or "latest"), str(item.get("agencyID") or "all")),
                "_score": score,
            }
        )
    if q:
        results.sort(key=lambda item: item.get("_score", 0), reverse=True)
    trimmed = results[:limit]
    for item in trimmed:
        item.pop("_score", None)
    return trimmed


@mcp.tool()
async def describe_hierarchical_codelist(hierarchyRef: str) -> dict[str, Any]:
    """
    Describe a hierarchical codelist and expose its immediate roots.
    """
    agency, hierarchy_id, version = _flow_identifiers(hierarchyRef)
    catalog = await _list_hierarchical_codelists_for_agency(agency)
    meta = next(
        (
            item for item in catalog
            if item.get("id") == hierarchy_id and (str(item.get("version") or "latest") == version or version == "latest")
        ),
        {},
    )
    edges = await _get_hierarchical_edges(hierarchyRef)
    all_children = {child for children in edges.values() for child in children}
    roots = sorted(code for code in edges if code not in all_children)
    return {
        "agencyID": agency,
        "id": hierarchy_id,
        "version": version,
        "name": meta.get("name") or "",
        "description": meta.get("description") or "",
        "hierarchyRef": _flow_ref_for(hierarchy_id, version, agency),
        "rootCodes": roots,
        "rootCount": len(roots),
        "edgeCount": sum(len(children) for children in edges.values()),
    }


@mcp.tool()
async def resolve_hierarchy(flowRef: str, dimension: str, code: str) -> dict[str, Any]:
    """
    Resolve the best agency hierarchy for a flow dimension/code.
    Returns resolved, ambiguous, or unresolved so the assistant does not guess.
    """
    payload = await _get_flow_structure(flowRef)
    dim_meta = _dimension_meta(payload, dimension)
    if not dim_meta:
        raise ValueError(f"Unknown dimension '{dimension}'.")
    dimension_id = str(dim_meta.get("id") or "").upper()
    codelist_id = _dimension_codelist_id(payload, dimension_id)
    code_map = _dimension_code_map(payload, dimension_id)
    code_id = code.strip()
    if code_id not in code_map:
        raise ValueError(
            f"Unknown code '{code_id}' for dimension '{dimension_id}'. Use list_codes(flowRef, '{dimension_id}') to retrieve valid codes."
        )

    flow_details = await _resolved_flow_details(flowRef)
    agency = flow_details.get("agencyID") or "all"
    candidates = await _list_hierarchical_codelists_for_agency(agency)
    scored: list[dict[str, Any]] = []
    for candidate in candidates:
        hierarchy_id = str(candidate.get("id") or "")
        version = str(candidate.get("version") or "latest")
        hierarchy_ref = _flow_ref_for(hierarchy_id, version, str(candidate.get("agencyID") or agency))
        edges = await _get_hierarchical_edges(hierarchy_ref)
        if not edges:
            continue
        descendants = [item for item in _ref_area_descendants(edges, code_id) if item in code_map]
        score, detail = _hierarchy_match_score(
            hierarchy_id=hierarchy_id,
            hierarchy_name=str(candidate.get("name") or ""),
            requested_code=code_id,
            code_map=code_map,
            descendants=descendants,
            dimension_id=dimension_id,
            codelist_id=codelist_id,
        )
        if score <= 0:
            continue
        leaf_members = _leaf_members(edges, descendants)
        scored.append(
            {
                "hierarchyRef": hierarchy_ref,
                "agencyID": candidate.get("agencyID"),
                "id": hierarchy_id,
                "version": version,
                "name": candidate.get("name") or "",
                "description": candidate.get("description") or "",
                "score": score,
                "matchedDescendantCount": detail["matchedDescendantCount"],
                "matchedDescendants": detail["matchedDescendants"],
                "matchedLeafMembers": leaf_members,
                "reasons": detail["reasons"],
            }
        )

    scored.sort(key=lambda item: (int(item.get("score") or 0), int(item.get("matchedDescendantCount") or 0)), reverse=True)
    if not scored:
        return {
            "status": "unresolved",
            "flowRef": flow_details.get("resolvedFlowRef"),
            "dimension": dimension_id,
            "code": code_id,
            "assistant_guidance": "No hierarchy could be tied confidently to this dimension/code. Do not guess a hierarchy.",
        }

    top = scored[0]
    same_score = [item for item in scored if item.get("score") == top.get("score")]
    if len(same_score) > 1:
        return {
            "status": "ambiguous",
            "flowRef": flow_details.get("resolvedFlowRef"),
            "dimension": dimension_id,
            "code": code_id,
            "candidates": same_score[:5],
            "assistant_guidance": "Multiple hierarchies plausibly match. Ask the user which hierarchy to use.",
        }

    return {
        "status": "resolved",
        "flowRef": flow_details.get("resolvedFlowRef"),
        "dimension": dimension_id,
        "code": code_id,
        "codelist": codelist_id,
        "hierarchy": top,
        "assistant_guidance": "Use this hierarchy for member expansion and fallback planning.",
    }


@mcp.tool()
async def expand_dimension_group(flowRef: str, dimension: str, code: str) -> dict[str, Any]:
    """
    Expand a dimension code through the best matching agency hierarchy.
    """
    payload = await _get_flow_structure(flowRef)
    dimension_id = dimension.strip().upper()
    code_map = _dimension_code_map(payload, dimension_id)
    if not code_map:
        raise ValueError(f"Unknown dimension '{dimension_id}' or no codelist codes were available.")
    resolution = await resolve_hierarchy(flowRef=flowRef, dimension=dimension_id, code=code)
    if resolution.get("status") != "resolved":
        return resolution
    hierarchy_info = resolution["hierarchy"]
    hierarchy_ref = str(hierarchy_info.get("hierarchyRef"))
    edges = await _get_hierarchical_edges(hierarchy_ref)
    descendants = [item for item in _ref_area_descendants(edges, code) if item in code_map]
    members = _leaf_members(edges, descendants)
    return {
        "status": "resolved",
        "flowRef": resolution.get("flowRef"),
        "dimension": dimension_id,
        "code": code,
        "label": _code_name(code_map.get(code, {})),
        "hierarchy": hierarchy_info,
        "descendants": [{"id": item, "name": _code_name(code_map.get(item, {}))} for item in descendants],
        "members": [{"id": item, "name": _code_name(code_map.get(item, {}))} for item in members],
        "assistant_guidance": (
            "If the aggregate dimension code does not resolve, retry using the member codes and return member-level observations "
            "unless the official flow provides an aggregate."
        ),
    }


@mcp.tool()
async def expand_ref_area_group(flowRef: str, refAreaCode: str) -> dict[str, Any]:
    """
    Expand an aggregate REF_AREA code into its descendant members using hierarchy metadata
    present in the official SDMX structure payload when available.
    """
    result = await expand_dimension_group(flowRef=flowRef, dimension="REF_AREA", code=refAreaCode)
    if result.get("status") != "resolved":
        return result
    hierarchy = result.get("hierarchy") or {}
    descendants = result.get("descendants") or []
    members = result.get("members") or []
    return {
        "refAreaCode": refAreaCode.strip(),
        "label": result.get("label") or "",
        "kind": "aggregate" if descendants else "leaf",
        "hierarchySource": hierarchy.get("hierarchyRef") or "",
        "hierarchyAvailable": True,
        "descendantCount": len(descendants),
        "memberCount": len(members),
        "members": members,
        "descendants": descendants,
        "assistant_guidance": result.get("assistant_guidance"),
    }


@mcp.tool()
async def resolve_dimension_fallback(
    flowRef: str,
    dimension: str,
    code: str,
    filters: dict[str, Any] | None = None,
    startPeriod: Optional[str] = None,
    endPeriod: Optional[str] = None,
    lastNObservations: Optional[int] = None,
    labels: Optional[str] = None,
) -> dict[str, Any]:
    """
    Validate an aggregate dimension code and, when unresolved, return a hierarchy-based retry plan.
    """
    payload = await _get_flow_structure(flowRef)
    dimension_id = dimension.strip().upper()
    code_map = _dimension_code_map(payload, dimension_id)
    if code.strip() not in code_map:
        raise ValueError(
            f"Unknown code '{code}' for dimension '{dimension_id}'. Use list_codes(flowRef, '{dimension_id}') to retrieve valid codes."
        )

    aggregate_filters = dict(filters or {})
    aggregate_filters[dimension_id] = code.strip()
    aggregate_result = await validate_query_scope(
        flowRef=flowRef,
        filters=aggregate_filters,
        startPeriod=startPeriod,
        endPeriod=endPeriod,
        lastNObservations=lastNObservations,
        labels=labels,
    )
    expansion = await expand_dimension_group(flowRef=flowRef, dimension=dimension_id, code=code.strip())

    if aggregate_result.get("status") == "resolved":
        return {
            "status": "aggregate_query_resolved",
            "dimension": dimension_id,
            "aggregate": aggregate_result,
            "group": expansion,
            "assistant_guidance": "The aggregate dimension code resolved from official flows. Use that series directly.",
        }

    if expansion.get("status") == "ambiguous":
        return {
            "status": "ambiguous",
            "dimension": dimension_id,
            "aggregate": aggregate_result,
            "group": expansion,
            "assistant_guidance": "Multiple hierarchies plausibly match. Ask the user which hierarchy to use.",
        }

    if expansion.get("status") != "resolved":
        return {
            "status": "unresolved_no_hierarchy",
            "dimension": dimension_id,
            "aggregate": aggregate_result,
            "group": expansion,
            "assistant_guidance": "No hierarchy could be resolved for this aggregate code. Report the unresolved official query without guessing.",
        }

    member_ids = [item.get("id") for item in expansion.get("members", []) if isinstance(item, dict) and item.get("id")]
    if member_ids:
        retry_filters = dict(filters or {})
        retry_filters[dimension_id] = member_ids
        member_result = await validate_query_scope(
            flowRef=flowRef,
            filters=retry_filters,
            startPeriod=startPeriod,
            endPeriod=endPeriod,
            lastNObservations=lastNObservations,
            labels=labels,
        )
        return {
            "status": "retry_with_members",
            "dimension": dimension_id,
            "reason": aggregate_result.get("error", {}).get("message") or "Aggregate dimension code did not resolve.",
            "aggregate": aggregate_result,
            "group": expansion,
            "retryPlan": {
                "dimension": dimension_id,
                "memberCodes": member_ids,
                "filters": retry_filters,
                "aggregationRecommended": "return member-level rows; do not compute a rolled-up aggregate unless the official flow provides one",
            },
            "memberValidation": member_result,
            "assistant_guidance": (
                "If member validation resolves, return member-level observations only. "
                "Do not synthesize an aggregate unless the official flow publishes one."
            ),
        }

    return {
        "status": "unresolved_no_members",
        "dimension": dimension_id,
        "reason": aggregate_result.get("error", {}).get("message") or "Aggregate dimension code did not resolve.",
        "aggregate": aggregate_result,
        "group": expansion,
        "assistant_guidance": "The hierarchy resolved but no member expansion was available for retry.",
    }


@mcp.tool()
async def resolve_ref_area_fallback(
    flowRef: str,
    refAreaCode: str,
    filters: dict[str, Any] | None = None,
    startPeriod: Optional[str] = None,
    endPeriod: Optional[str] = None,
    lastNObservations: Optional[int] = None,
    labels: Optional[str] = None,
) -> dict[str, Any]:
    """
    Validate an aggregate REF_AREA query and, when it does not resolve, return an official retry plan
    based on hierarchy-derived member REF_AREA codes.
    """
    result = await resolve_dimension_fallback(
        flowRef=flowRef,
        dimension="REF_AREA",
        code=refAreaCode,
        filters=filters,
        startPeriod=startPeriod,
        endPeriod=endPeriod,
        lastNObservations=lastNObservations,
        labels=labels,
    )
    if "group" in result:
        result["refArea"] = result.pop("group")
    retry_plan = result.get("retryPlan")
    if isinstance(retry_plan, dict) and "memberCodes" in retry_plan:
        retry_plan["refAreaCodes"] = retry_plan.pop("memberCodes")
    return result


@mcp.tool()
async def plan_query(
    flowRef: str,
    key: Optional[str] = None,
    filters: dict[str, Any] | None = None,
    startPeriod: Optional[str] = None,
    endPeriod: Optional[str] = None,
    lastNObservations: Optional[int] = None,
    format: str = "csv",
    labels: Optional[str] = "name",
    resultShape: Optional[str] = None,
    allowUnboundedTime: bool = False,
) -> dict[str, Any]:
    """
    Resolve a query into a concrete SDMX URL and highlight wildcard dimensions before execution.
    """
    plan = await _query_plan(
        flowRef=flowRef,
        key=key,
        filters=filters,
        startPeriod=startPeriod,
        endPeriod=endPeriod,
        lastNObservations=lastNObservations,
        format=format,
        labels=labels,
        resultShape=resultShape,
        allowUnboundedTime=allowUnboundedTime,
    )
    return {
        "status": "planned",
        "flowRef": plan["flowDetails"].get("resolvedFlowRef"),
        "key": plan["key"],
        "queryURL": plan["queryURL"],
        "dimensionOrder": plan["dimensionOrder"] or [],
        "filters": plan["normalizedFilters"] or {},
        "wildcardDimensions": plan["wildcardDimensions"],
        "shape": resultShape,
        "notes": {
            "format": format,
            "labels": labels,
            "warning": (
                "Wildcard dimensions mean the query can return multiple series or multiple rows. "
                "Use resultShape='latest_single_value' only when the remaining wildcard dimensions cannot split the result."
            ),
        },
    }


@mcp.tool()
async def validate_query_scope(
    flowRef: str,
    key: Optional[str] = None,
    filters: dict[str, Any] | None = None,
    startPeriod: Optional[str] = None,
    endPeriod: Optional[str] = None,
    lastNObservations: Optional[int] = None,
    labels: Optional[str] = "name",
) -> dict[str, Any]:
    """
    Preflight whether a concrete query resolves from the official UNICEF/UNPD flows.
    This is intended to fail early before any narrative answer is attempted.
    """
    result = await query_data(
        flowRef=flowRef,
        key=key,
        filters=filters,
        startPeriod=startPeriod,
        endPeriod=endPeriod,
        lastNObservations=lastNObservations,
        format="csv",
        labels=labels,
        maxObs=1,
    )
    return {
        "status": result.get("status"),
        "sourceScope": result.get("sourceScope"),
        "provenance": result.get("provenance"),
        "assistant_guidance": result.get("assistant_guidance"),
        "error": result.get("error"),
    }


@mcp.tool()
async def query_data(
    flowRef: str,
    key: Optional[str] = None,
    startPeriod: Optional[str] = None,
    endPeriod: Optional[str] = None,
    format: str = "csv",
    labels: Optional[str] = "name",
    maxObs: int = 50_000,
    filters: dict[str, Any] | None = None,
    lastNObservations: Optional[int] = None,
    resultShape: Optional[str] = None,
    allowUnboundedTime: bool = False,
) -> dict[str, Any]:
    """
    Query SDMX data with guardrails.
    - Requires a bounded time window unless caller explicitly accepts the risk.
    - Returns raw SDMX-JSON and a minimal 'query_url' for reproducibility.
    """
    return await _execute_query_data(
        flowRef=flowRef,
        key=key,
        startPeriod=startPeriod,
        endPeriod=endPeriod,
        format=format,
        labels=labels,
        maxObs=maxObs,
        filters=filters,
        lastNObservations=lastNObservations,
        resultShape=resultShape,
        allowUnboundedTime=allowUnboundedTime,
    )


@mcp.tool()
async def resolve_and_query_data(
    flowRef: str,
    filters: dict[str, Any],
    startPeriod: Optional[str] = None,
    endPeriod: Optional[str] = None,
    lastNObservations: Optional[int] = None,
    labels: Optional[str] = "name",
    resultShape: str = "latest_single_value",
) -> dict[str, Any]:
    """
    Validate a query, try one hierarchy-based aggregate fallback when needed, and then return a shaped result.
    This is intended for common user-facing questions that expect one compact answer rather than raw SDMX payloads.

    Use this only after the subject/indicator dimension has already been resolved to a concrete code
    and a suitable flowRef has already been chosen. Do not use this as the first discovery step for
    topical questions such as stunting or wasting.
    """
    if not filters:
        raise ValueError("filters must include at least one dimension.")
    allow_unbounded_time = (resultShape or "").strip().lower() in {"compact_series", "topline_summary"}

    validation = await validate_query_scope(
        flowRef=flowRef,
        filters=filters,
        startPeriod=startPeriod,
        endPeriod=endPeriod,
        lastNObservations=None if allow_unbounded_time and not (startPeriod and endPeriod) else lastNObservations,
        labels=labels,
    )
    if validation.get("status") == "resolved":
        result = await query_data(
            flowRef=flowRef,
            filters=filters,
            startPeriod=startPeriod,
            endPeriod=endPeriod,
            lastNObservations=None if allow_unbounded_time and not (startPeriod and endPeriod) else lastNObservations,
            format="csv",
            labels=labels,
            resultShape=resultShape,
            allowUnboundedTime=allow_unbounded_time,
        )
        return {
            "status": "resolved",
            "resolutionPath": "direct",
            "validation": validation,
            "result": result,
        }

    single_value_dimensions = []
    for raw_dimension, raw_value in filters.items():
        tokens = _selection_tokens(raw_value)
        if len(tokens) == 1:
            single_value_dimensions.append((str(raw_dimension).upper(), tokens[0]))

    fallback_attempts: list[dict[str, Any]] = []
    for dimension_id, code in single_value_dimensions:
        fallback = await resolve_dimension_fallback(
            flowRef=flowRef,
            dimension=dimension_id,
            code=code,
            filters=filters,
            startPeriod=startPeriod,
            endPeriod=endPeriod,
            lastNObservations=None if allow_unbounded_time and not (startPeriod and endPeriod) else lastNObservations,
            labels=labels,
        )
        fallback_attempts.append(fallback)
        if fallback.get("status") != "retry_with_members":
            continue
        retry_plan = fallback.get("retryPlan") or {}
        retry_filters = retry_plan.get("filters")
        if not isinstance(retry_filters, dict):
            continue
        result = await query_data(
            flowRef=flowRef,
            filters=retry_filters,
            startPeriod=startPeriod,
            endPeriod=endPeriod,
            lastNObservations=None if allow_unbounded_time and not (startPeriod and endPeriod) else lastNObservations,
            format="csv",
            labels=labels,
            resultShape=resultShape,
            allowUnboundedTime=allow_unbounded_time,
        )
        return {
            "status": "resolved_with_fallback",
            "resolutionPath": "member_fallback",
            "validation": validation,
            "fallback": fallback,
            "result": result,
        }

    return {
        "status": "unresolved",
        "resolutionPath": "direct_failed",
        "validation": validation,
        "fallbackAttempts": fallback_attempts,
        "assistant_guidance": (
            "The direct query did not resolve, and no single hierarchy-based fallback path produced a retry plan. "
            "Do not guess a single value."
        ),
    }
