# UNICEF SDMX MCP

MCP server for the UNICEF SDMX warehouse (`https://sdmx.data.unicef.org/ws/public/sdmxapi/rest`) built with FastMCP.

This project lets an LLM client do a full guided data journey:
- pick agency
- discover/select dataflow
- inspect dimensions and codelists
- rank candidate indicators
- query data (CSV or SDMX-JSON)

## What This Repo Contains

- `server.py`: MCP tools and SDMX integration logic.
- `scripts/agent_test_rig.py`: direct tool-call harness that simulates an agent workflow.
- `scripts/sdmx_eval_runner.py`: generic SDMX eval harness for case generation, provider execution, and grading.
- `scripts/sdmx_eval_config.example.json`: example config for deterministic case generation and provider runs.
- `scripts/sdmx_eval_provider_anthropic.py`: Anthropic Messages API adapter using Anthropic's MCP connector.
- `scripts/sdmx_eval_provider_template.py`: stdin/stdout adapter template for wiring any LLM+MCP stack into the eval harness.
- `scripts/agent_test_scenarios.example.jsonl`: regression/demo scenarios.
- `scripts/list_theme_prefixes.py`: helper to inspect dataflow ID prefixes.
- `theme_prefixes_domain.csv`: curated prefix-to-domain mapping used for grouping.

## Local Run

```bash
git clone https://github.com/<you>/sdmx_mcp.git
cd sdmx_mcp
UV_CACHE_DIR="$PWD/.uv-cache" uv sync
UV_CACHE_DIR="$PWD/.uv-cache" uv run fastmcp run server.py --transport stdio
```

HTTP mode (local):

```bash
UV_CACHE_DIR="$PWD/.uv-cache" uv run fastmcp run server.py --transport streamable-http --host 127.0.0.1 --port 8000 --path /mcp
```

Test endpoint:

```bash
curl -sS -X POST http://127.0.0.1:8000/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

## Docker

Build and run:

```bash
docker build -t sdmx-mcp:latest .
docker run --rm -p 8000:8000 sdmx-mcp:latest
```

Container command uses:
- `fastmcp run server.py --transport streamable-http --path /mcp`

## Fly.io Deploy

```bash
fly auth login
fly deploy --build-arg UV_CACHE_DIR=/app/.uv-cache
```

Feature-branch draft deploy:

- `unicef` deploys to the main Fly app from `fly.toml`
- `feat/unicef-agent-test-rig` deploys to the separate draft app from `fly.eval.toml`

If the draft app does not exist yet, create it once before relying on GitHub Actions auto-deploys:

```bash
fly launch --no-deploy --copy-config --name sdmx-mcp-eval --config fly.eval.toml
```

Sanity check:

```bash
curl -sS --max-time 20 -X POST https://sdmx-mcp.fly.dev/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

## MCP Tools (Function Catalog)

All tools are defined in `server.py`.

1. `list_agencies(limit=50)`
- Purpose: list agencies discovered from SDMX dataflows.
- Returns: `[{id,name,description}]`.

2. `search_dataflows(query, limit=10)`
- Purpose: keyword search across dataflow ID/name/description.
- Returns: scored matches with `flowRef` and `themeHint`.

3. `list_dataflows_grouped(query=None, prefixMap=None, limitPerTheme=50)`
- Purpose: group dataflows by inferred/curated theme prefix.
- Returns: `[{themeCode,themeLabel,flows:[...]}]`.

4. `get_default_theme_prefix_map()`
- Purpose: return built-in prefix-to-theme map.

5. `list_theme_prefixes(limit=50)`
- Purpose: scan dataflows and report common prefixes with counts/examples.

6. `list_theme_prefix_conflicts(limit=100)`
- Purpose: show prefixes mapping to multiple domains in `theme_prefixes_domain.csv`.

7. `describe_flow(flowRef)`
- Purpose: summary of a flow plus parsed dimension metadata.
- Returns: `{id,agencyID,version,name,description,dimensions}`.

8. `list_dimensions(flowRef)`
- Purpose: ordered dimensions with concept/codelist references.

9. `list_codes(flowRef, dimension, query=None, limit=50, includeHierarchyHints=True)`
- Purpose: list codes for one dimension, optional text filter.
- Behavior: when `includeHierarchyHints=True`, results are enriched with lightweight structure hints such as `kind` (`leaf`, `aggregate`, or `unknown`), `expandable`, `hasChildren`, `memberCount`, `hierarchySource`, `childrenPreview`, `parentCode`, and `hierarchyMatches`.
- Design: this makes `list_codes` the main code-discovery surface, while `expand_dimension_group` remains the explicit expansion step.

10. `search_reference_candidates(flowRef, query, dimension=None, limit=20)`
- Purpose: search group-like reference structures across ordinary codelists, codes, and hierarchical codelists.
- Use when a user query implies a region, category, country group, or other aggregate concept, especially when reference data debt means the grouping lives in a normal codelist rather than a formal hierarchy.

11. `find_indicator_candidates(query, flowRef=None, limit=10, flowQuery=None, flowLimit=200)`
- Purpose: rank `INDICATOR` codes by relevance to user text; when `flowRef` is omitted, scan scoped flows.

12. `search_indicators(query, flowRef=None, limit=10, flowQuery=None, flowLimit=200)`
- Purpose: alias of `find_indicator_candidates`.

13. `get_flow_structure(flowRef)`
- Purpose: fetch/cache structure payload (`references=all`).

14. `build_key(flowRef, selections)`
- Purpose: build SDMX key from dimension selections.
- Notes: supports list/comma syntax for multi-code segments.

15. `list_hierarchical_codelists(agency=None, query=None, limit=50)`
- Purpose: list hierarchical codelists available for an agency.

16. `describe_hierarchical_codelist(hierarchyRef)`
- Purpose: describe one hierarchical codelist and expose root codes.

17. `resolve_hierarchy(flowRef, dimension, code)`
- Purpose: choose the best matching hierarchy for a flow dimension/code.
- Returns: `status` of `resolved`, `ambiguous`, or `unresolved`.
- Behavior: if more than one hierarchy plausibly matches, the MCP returns ambiguity instead of guessing.

18. `expand_dimension_group(flowRef, dimension, code)`
- Purpose: expand an aggregate dimension code into descendant/member codes using the resolved hierarchy.

19. `resolve_dimension_fallback(flowRef, dimension, code, filters=None, startPeriod=None, endPeriod=None, lastNObservations=1, labels=None)`
- Purpose: validate an aggregate dimension query and, if unresolved, return a hierarchy-based retry plan using member codes.

20. `plan_query(flowRef, key=None, filters=None, startPeriod=None, endPeriod=None, lastNObservations=None, format='csv', labels=None, resultShape=None)`
- Purpose: resolve a query into a concrete SDMX URL before execution and show wildcard dimensions that can still split the result.

21. `validate_query_scope(flowRef, key=None, filters=None, startPeriod=None, endPeriod=None, lastNObservations=1, labels=None)`
- Purpose: preflight whether a concrete UNICEF/UNPD query resolves before any narrative answer is attempted.
- Returns: structured source-bound status with `status`, `sourceScope`, `provenance`, optional `error`, and `assistant_guidance`.

22. `query_data(flowRef, key=None, startPeriod=None, endPeriod=None, format='sdmx-json', labels=None, maxObs=50000, filters=None, lastNObservations=None, resultShape=None)`
- Purpose: run data query with bounded extraction guardrails.
- Key behaviors:
  - requires either (`startPeriod` + `endPeriod`) or `lastNObservations`
  - supports `filters` to auto-build key from dimension order
  - leaves unspecified dimensions as empty key segments (`.` wildcard)
  - supports `format='csv'` and returns `raw_csv`
  - supports `resultShape` values `compact_series`, `latest_single_value`, `latest_by_ref_area`, and `topline_summary`
  - supports optional `labels` parameter (for example `labels=both` with CSV)
  - returns explicit `status` of either `resolved` or `unresolved_from_official_flows`
  - when `resultShape='latest_single_value'`, returns explicit non-answer states like `no_observations`, `no_value_column`, or `not_a_single_value` instead of silently picking a row
  - always includes `sourceScope`, `provenance`, and `assistant_guidance`
  - parses SDMX XML error payloads into structured `error.message`
  - on unresolved queries, callers should stop and report the failed official query instead of supplementing with external facts

23. `resolve_and_query_data(flowRef, filters, startPeriod=None, endPeriod=None, lastNObservations=None, labels=None, resultShape='latest_single_value')`
- Purpose: high-level helper for common user questions.
- Behavior: validates the direct query first, then attempts a single hierarchy-based member fallback when an aggregate code does not resolve, and finally returns a shaped result.

## Agent Test Rig

`scripts/agent_test_rig.py` (direct call harness) simulates the intended LLM flow.

Capabilities:
- selects best flow from question
- retries discovery with topic hints when full-sentence flow search is sparse
- infers `REF_AREA` for South Asia
- does not auto-fill non-essential dimensions, leaving unspecified dimensions as SDMX wildcards
- ranks indicator candidates
- iterates indicators until data is found
- logs attempts verbosely
- saves per-case JSON outputs

Single run:

```bash
python scripts/agent_test_rig.py \
  --question "Show me latest child mortality rates in South Asia" \
  --agency UNICEF \
  --journey --verbose \
  --last-n 3 --format csv --labels both
```

Scenario batch:

```bash
python scripts/agent_test_rig.py \
  --scenarios scripts/agent_test_scenarios.example.jsonl \
  --journey --verbose \
  --save-output-dir demo_outputs
```

### Demo Scenarios Included

In `scripts/agent_test_scenarios.example.jsonl`:
- latest under-five mortality in South Asia
- latest stunting and wasting in South Asia
- immunization vs mortality comparison in South Asia

## Generic Eval Harness

`scripts/sdmx_eval_runner.py` is a config-driven evaluation harness for any SDMX registry the server can query.

It supports three steps:
- `build-cases`: deterministically expands configured dimensions and years into a manifest of natural-language prompts plus direct SDMX ground truth.
- `run-provider`: sends each case to a provider adapter over stdin/stdout so you can plug in OpenAI, Anthropic, or any other MCP-capable client without changing the harness.
- `grade-results`: compares structured claims from provider output against the direct SDMX result set.

Example:

```bash
python3 scripts/sdmx_eval_runner.py build-cases \
  --config scripts/sdmx_eval_config.example.json \
  --case-limit 25

python3 scripts/sdmx_eval_runner.py run-provider \
  --config scripts/sdmx_eval_config.example.json \
  --case-limit 25

python3 scripts/sdmx_eval_runner.py grade-results \
  --config scripts/sdmx_eval_config.example.json
```

Anthropic setup:

```bash
export ANTHROPIC_API_KEY=...
python3 scripts/sdmx_eval_runner.py run-provider \
  --config scripts/sdmx_eval_config.example.json \
  --case-limit 25
```

### Config Model

The config file controls:
- `dataflows`: exact flows to test.
- `dimensions`: how each dimension should be expanded.
- `query_mode`: whether cases use explicit `TIME_PERIOD` values or `lastNObservations`.
- `prompt_template`: how deterministic cases become natural-language prompts.
- `provider`: the adapter command that will call an actual model stack.

Supported dimension modes:
- `fixed`: use an explicit list of code IDs.
- `flow_dimension`: use all codes from the flow DSD for that dimension.
- `external_codelist_intersection`: intersect the flow dimension codelist with an external SDMX codelist URL.
- `time_range`: generate yearly cases across a configured start/end range.

Supported query modes:
- `explicit_time_range`: build one case per configured time value from the `TIME_PERIOD` dimension config.
- `last_n_observations`: omit explicit time values and query with `lastNObservations`, which is useful for latest-single-value benchmarks.

### Provider Contract

The provider adapter receives a JSON payload on stdin and must print a JSON object to stdout.

Important output fields:
- `answer_text`: the model's final natural-language answer.
- `claims.value`: the value the model claims is correct.
- `claims.time_period`: the reported year or period.
- `claims.flowRef`: the flow the model believes it used.
- `claims.filters`: the code-level filters the model believes it used.

The grader relies primarily on those structured claims, not on free-form prose alone.

### Anthropic Adapter

`scripts/sdmx_eval_provider_anthropic.py` calls the Anthropic Messages API and passes your MCP server via the `mcp_servers` request field. It expects:
- `ANTHROPIC_API_KEY` in the environment by default
- `mcp.url` in the eval config
- an MCP server reachable over HTTP(S)

The adapter asks Claude to return a single JSON object so grading can stay deterministic. If you need auth on the MCP endpoint, set `mcp.authorization_token_env` in the config and the adapter will forward that bearer token to Anthropic's MCP connector request.

## Connector Setup

### Claude (Connector / Integrations)

If Claude supports remote MCP connectors in your plan/UI:
- add connector URL: `https://sdmx-mcp.fly.dev/mcp`
- test with a first prompt like: "List UNICEF agencies"

### Claude Desktop (config fallback)

`~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "unicef-sdmx": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://sdmx-mcp.fly.dev/mcp"]
    }
  }
}
```

Restart Claude Desktop after editing.

For codelist-backed dimensions, `build_key` and `query_data(filters=...)` must use code IDs (as returned by `list_codes`), not display labels.
If a caller passes a manual `key` with too few segments, `query_data` pads missing trailing dimensions as wildcard segments automatically.

Recommended discovery sequence:
1. `find_indicator_candidates(query)` to find indicator IDs across scoped flows.
2. Use each candidate's `recommendedFlowRef` (or inspect `dataflows`) to choose a flow.
3. `list_codes(flowRef, "GEO", query=...)` and other dimension tools to constrain the slice.
4. If a selected code may be aggregate, call `resolve_hierarchy(...)` or `resolve_dimension_fallback(...)`.
5. `validate_query_scope(flowRef, filters=...)` or `build_key(...)` then `validate_query_scope(...)`.
6. Only if the preflight resolves, call `query_data(flowRef, filters=...)` or `build_key(...)` then `query_data(...)`.

## Troubleshooting

1. `406 Not Acceptable`
- Cause: client `Accept` header mismatch for endpoint/transport.
- Check: use `/mcp` and include `Accept: application/json` for JSON-RPC POST tests.

2. `Missing session ID`
- Some MCP transport/client paths require a full MCP handshake session flow.
- Validate with `tools/list` via known-good client first.

3. `404 Not Found`
- Usually wrong path. Use `/mcp` (not `/`).

4. Data query 404 with SDMX message `No data for data query against the dataflow`
- Query syntax is valid but the selected dimensional slice has no observations.
- Use `find_indicator_candidates` + iterative attempts in the rig.

## Presentation Notes

For a live demo, use the scenario file plus `--save-output-dir` and keep the saved JSON outputs as known-good artifacts.

## Latest Robustness Updates

- Unspecified dimensions are now kept as empty SDMX key segments (`.` wildcard) instead of being auto-filled.
- `query_data` supports optional `labels` for SDMX CSV output (`labels=both` works well for readable demos).
- Journey-mode flow discovery in the test rig now uses fallback topic hints (for example, wasting/stunting -> nutrition).
