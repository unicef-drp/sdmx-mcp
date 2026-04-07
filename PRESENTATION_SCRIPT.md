# UNICEF SDMX MCP Demo Script

## 1. Opening (30-45s)

Today I am demoing an MCP server over the UNICEF SDMX warehouse.  
The goal is to let an LLM guide users from question to data in a reliable way:
- discover the right dataflow
- inspect dimensions and codelists
- pick the best indicator candidates
- return the requested output format

This is running as a deployed MCP endpoint on Fly.io and connected live in Claude via connector.

## 2. Architecture (45-60s)

The stack is:
- SDMX warehouse: `sdmx.data.unicef.org`
- MCP server: FastMCP (`server.py`)
- Hosted endpoint: `https://sdmx-mcp.fly.dev/mcp`
- Client: Claude connector

Tooling flow:
1. `list_agencies`
2. `search_dataflows` / `list_dataflows_grouped`
3. `describe_flow` + `list_dimensions`
4. `find_indicator_candidates` + `list_codes`
5. `query_data` with `lastNObservations`

## 3. Why It Works Well (30s)

Three design choices made this robust:
- No hardcoded SDMX assumptions: codelists and structures are fetched dynamically.
- Indicator selection is iterative: if one slice has no data, the agent tries next-best candidates.
- Query guardrails: bounded extraction (`lastNObservations` or explicit periods).

## 4. Live Demo Prompts (3-5 min)

### Prompt 1: Mortality
\"Show me the latest under-five mortality rate in South Asia, with a country table and a short narrative summary.\"

What to highlight:
- correct flow selection for child mortality
- inferred South Asia country set
- latest observations pulled
- narrative + table output

### Prompt 2: Nutrition (stunting + wasting)
\"Show me the latest stunting and wasting prevalence for children under 5 in South Asia, side-by-side by country.\"

What to highlight:
- lower-dimensional, cleanly queryable flow
- side-by-side indicator handling
- clear country comparison format

### Prompt 3: Cross-domain comparison
\"Compare immunization coverage and under-five mortality in South Asia for the latest available year, and explain the pattern in plain language.\"

What to highlight:
- multiple flows in one answer
- alignment by country/time
- plain-language interpretation

## 5. Reliability / Regression Story (45s)

The server is designed for repeatable tool-based checks:
- deterministic flow discovery and structure inspection
- explicit query guardrails (`lastNObservations` or bounded periods)
- reproducible requests through returned `query_url` values

This keeps demo behavior stable even when client chat behavior varies.

## 6. Current Scope + Next Steps (45s)

Current scope:
- UNICEF SDMX MCP with practical discovery/query tools
- live connector integration
- demo-ready scenarios

Next steps:
1. Add output renderers (table/chart/map presets).
2. Add stronger cross-flow join logic for comparisons.
3. Add expected-flow assertions in scenario regression tests.
4. Expand prompt libraries by domain (health, nutrition, education, protection).

## 7. Closing (15s)

This shows that an MCP layer over SDMX can make complex statistical systems directly usable in natural language, while still preserving data structure, traceability, and reproducibility.
