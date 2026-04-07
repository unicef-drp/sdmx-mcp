# UNICEF SDMX MCP

MCP server for the UNICEF SDMX warehouse (`https://sdmx.data.unicef.org/ws/public/sdmxapi/rest`) built with FastMCP.

This project lets an LLM client do a full guided data journey:
- pick agency
- discover by subject, location, or time
- select a dataflow through guided indicator-first planning
- inspect dimensions and codelists
- rank candidate indicators
- query data with CSV transport for SDMX `/data` payloads

## What This Repo Contains

- `server.py`: MCP tools and SDMX integration logic.
- `query_dimension_policy.json`: default subject/location/time query-dimension policy.
- `query_dimension_policy.example.json`: example query-dimension policy.
- `discovery_policy.json`: default discovery ranking policy for stopwords and topic-to-flow hints.
- `discovery_policy.example.json`: example discovery ranking policy.
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

Runtime config is loaded from `.env` automatically when present.
Policy config is auto-discovered from `query_dimension_policy.json` when `SDMX_QUERY_DIMENSION_POLICY_FILE` is not set.
Discovery ranking config is auto-discovered from `discovery_policy.json` when `SDMX_DISCOVERY_POLICY_FILE` is not set.

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

Docker installs dependencies with `uv sync --frozen --no-dev --no-cache`, so builds use `uv.lock` rather than resolving floating dependency versions from PyPI.

Container command uses:
- `fastmcp run server.py --transport streamable-http --path /mcp`

## Fly.io Deploy

```bash
fly auth login
fly deploy
```

Fly deploys use the same Dockerfile and locked dependency install. The app may intentionally autostop depending on `fly.toml` settings.

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

## MCP Surface

Tools and resources are defined in `server.py`.

## MCP Resources

The server intentionally exposes only guided discovery resources. Earlier metadata/data resources were removed because some MCP clients expose all resources directly to users and then fail or create redundant tool paths.

Current resources:

- `sdmx://discover/subject` (`Discover by Subject`)
- `sdmx://discover/location` (`Discover by Location`)
- `sdmx://discover/time` (`Discover by Time`)

Use resources for:
- giving the user an intentional entry point
- instructing the client to call `guided_discover(...)`
- showing example prompts and configured policy sources

Use tools for:
- query planning
- metadata discovery
- codelist lookup
- hierarchy inspection
- hierarchy resolution and fallback planning
- shaped retrieval
- comparison, aggregation, or other procedural work

Discovery mode is the user-facing entry path only. The backend still resolves subject, location, and time according to the configured query policy.

## Configuration

Configuration is split into two policy files:

- `query_dimension_policy.json`: what dimensions matter and how to resolve them
- `discovery_policy.json`: how natural-language discovery is scored and routed

This separation is intentional. A registry owner can configure the SDMX semantics independently from search/ranking heuristics.

### Query Dimension Policy

The query-dimension policy defines the retrieval dimensions and their resolution order. The default policy defines:

- `subject`
- `location`
- `time`

Time is special and resolves against the `TIME_PERIOD` dimension directly. Other important query dimensions are expected to point at SDMX codelists or hierarchical codelists through policy configuration.

Override it with either:

- `SDMX_QUERY_DIMENSION_POLICY_JSON`
- `SDMX_QUERY_DIMENSION_POLICY_FILE`
- `SDMX_DEFAULT_LAST_N_OBSERVATIONS`

An example file is included at `query_dimension_policy.example.json`.

Example UNICEF-oriented policy:

```json
{
  "default_query_dimensions": [
    {
      "name": "subject",
      "role": "subject",
      "required_for_retrieval": true,
      "priority": 1,
      "discovery_label": "Discover by Subject",
      "discovery_description": "Start with the phenomenon, metric, or topic and let the system find the best indicator and flow.",
      "example_prompts": [
        "Tell me about stunting in Latin America.",
        "Show me vaccination coverage in West Africa."
      ],
      "preferred_sources": [
        {"type": "codelist", "id": "UNICEF/CL_UNICEF_INDICATOR/1.0"}
      ]
    },
    {
      "name": "location",
      "role": "geography",
      "required_for_retrieval": true,
      "priority": 2,
      "discovery_label": "Discover by Location",
      "discovery_description": "Start with a country, region, or grouping and let the system resolve the right flow and indicator.",
      "example_prompts": [
        "Tell me about stunting in Latin America.",
        "Compare child mortality in South Asia."
      ],
      "preferred_sources": [
        {"type": "codelist", "id": "UNICEF/CL_COUNTRY/1.0"},
        {"type": "hierarchical_codelist", "id": "UNICEF/UNICEF_REPORTING_REGIONS"}
      ],
      "allow_hierarchy_resolution": true,
      "allow_member_expansion": true
    },
    {
      "name": "time",
      "role": "time",
      "required_for_retrieval": true,
      "priority": 3,
      "discovery_label": "Discover by Time",
      "discovery_description": "Start with the period or trend you want and let the system resolve the right subject, flow, and location slice.",
      "example_prompts": [
        "What changed over time for under-five mortality in South Asia?",
        "Show the latest nutrition indicators."
      ]
    }
  ]
}
```

Resolution order follows policy `priority`, not hardcoded dimension names. Non-time dimensions are resolved through the configured code-list or hierarchy sources, and policy source IDs should use the SDMX reference form `agency/id/version` where applicable. `time` is resolved against `TIME_PERIOD`.

The same policy can also drive three user-facing discovery resources:

- `Discover by Subject`
- `Discover by Location`
- `Discover by Time`

Optional policy fields for each query dimension:

- `discovery_enabled`
- `discovery_label`
- `discovery_description`
- `example_prompts`

Discovery mode is the user-facing entry path only. Final query construction still honors policy `priority`.

### Discovery Ranking Policy

The discovery ranking policy controls generic text-scoring behavior. By default the server auto-loads `discovery_policy.json`.

Override it with either:

- `SDMX_DISCOVERY_POLICY_JSON`
- `SDMX_DISCOVERY_POLICY_FILE`

An example file is included at `discovery_policy.example.json`.

Supported fields:

- `query_stopwords`: generic prompt words ignored during candidate scoring, such as `tell`, `show`, or `level`
- `flow_topic_hints`: registry-specific term groups that prefer matching dataflow IDs, such as nutrition terms preferring `NUTRITION`

Example:

```json
{
  "query_stopwords": ["tell", "show", "level", "latest", "table"],
  "flow_topic_hints": [
    {
      "terms": ["bmi", "nutrition", "obesity", "stunting", "wasting"],
      "preferred_flow_markers": ["NUTRITION"]
    },
    {
      "terms": ["education", "learning", "school"],
      "preferred_flow_markers": ["EDUCATION"]
    }
  ]
}
```

`preferred_flow_markers` are matched against flow IDs and flow refs, so a marker like `NUTRITION` matches `UNICEF/NUTRITION/1.0`.

### Data Transport

All SDMX `/data` calls are fetched as CSV for efficiency and simpler downstream parsing. This applies to `query_data`, `validate_query_scope`, `resolve_and_query_data`, and `guided_discover`.

Structure and discovery tools still return JSON objects because SDMX structures, codelists, and hierarchy metadata are JSON-shaped in this server.

If a caller requests a non-CSV data format, the response records the requested format and the CSV override in `notes`.

Time inputs accepted by the query policy can be:

- an explicit range like `2004:2024`, which becomes `startPeriod` and `endPeriod`
- a single period like `2024`, which becomes `startPeriod=2024&endPeriod=2024`
- `latest`, which uses `lastNObservations=1`
- `all`, `trend`, `series`, `chart`, `graph`, or `table`, which omit both time parameters and `lastNObservations`

## Tool Reference

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

13. `guided_discover(question, discoveryMode, flowQuery=None, indicatorLimit=10, flowLimit=200, labels='name', resultShape='topline_summary')`
- Purpose: run the full guided discovery workflow from a user-facing entry mode of `subject`, `location`, or `time`.
- Behavior: uses the chosen discovery mode as the visible entry path, then resolves subject/location/time according to the configured backend policy before validating and executing the query.

14. `get_flow_structure(flowRef)`
- Purpose: fetch/cache structure payload (`references=all`).

15. `build_key(flowRef, selections)`
- Purpose: build SDMX key from dimension selections.
- Notes: supports list/comma syntax for multi-code segments.

16. `list_hierarchical_codelists(agency=None, query=None, limit=50)`
- Purpose: list hierarchical codelists available for an agency.

17. `describe_hierarchical_codelist(hierarchyRef)`
- Purpose: describe one hierarchical codelist and expose root codes.

18. `resolve_hierarchy(flowRef, dimension, code)`
- Purpose: choose the best matching hierarchy for a flow dimension/code.
- Returns: `status` of `resolved`, `ambiguous`, or `unresolved`.
- Behavior: if more than one hierarchy plausibly matches, the MCP returns ambiguity instead of guessing.

19. `expand_dimension_group(flowRef, dimension, code)`
- Purpose: expand an aggregate dimension code into descendant/member codes using the resolved hierarchy.

20. `resolve_dimension_fallback(flowRef, dimension, code, filters=None, startPeriod=None, endPeriod=None, lastNObservations=None, labels=None)`
- Purpose: validate an aggregate dimension query and, if unresolved, return a hierarchy-based retry plan using member codes.

21. `plan_query(flowRef, key=None, filters=None, startPeriod=None, endPeriod=None, lastNObservations=None, format='csv', labels='name', resultShape=None)`
- Purpose: resolve a query into a concrete SDMX URL before execution and show wildcard dimensions that can still split the result.

22. `validate_query_scope(flowRef, key=None, filters=None, startPeriod=None, endPeriod=None, lastNObservations=None, labels='name')`
- Purpose: preflight whether a concrete UNICEF/UNPD query resolves before any narrative answer is attempted.
- Returns: structured source-bound status with `status`, `sourceScope`, `provenance`, optional `error`, and `assistant_guidance`.

23. `query_data(flowRef, key=None, startPeriod=None, endPeriod=None, format='csv', labels='name', maxObs=50000, filters=None, lastNObservations=None, resultShape=None)`
- Purpose: run data query with bounded extraction guardrails.
- Key behaviors:
  - defaults to `lastNObservations=1` when no explicit `startPeriod` and `endPeriod` are provided and `SDMX_DEFAULT_LAST_N_OBSERVATIONS=true`
  - when `SDMX_DEFAULT_LAST_N_OBSERVATIONS=false`, unqualified requests default to all time periods unless the caller explicitly asks for latest data or provides a range
  - does not inject `startPeriod`, `endPeriod`, or `lastNObservations` for full-series requests such as `resultShape='compact_series'` or `resultShape='topline_summary'`
  - supports `filters` to auto-build key from dimension order
  - leaves unspecified dimensions as empty key segments (`.` wildcard)
  - always fetches SDMX `/data` payloads as CSV for efficiency and simpler downstream parsing
  - returns `raw_csv` for data queries; structure and discovery tools still return JSON objects
  - if a caller passes a non-CSV `format`, the response notes record the requested format and the CSV override
  - supports `resultShape` values `compact_series`, `latest_single_value`, `latest_by_ref_area`, and `topline_summary`
  - defaults to `labels='name'` for CSV readability
  - returns explicit `status` of either `resolved` or `unresolved_from_official_flows`
  - when `resultShape='latest_single_value'`, returns explicit non-answer states like `no_observations`, `no_value_column`, or `not_a_single_value` instead of silently picking a row
  - always includes `sourceScope`, `provenance`, and `assistant_guidance`
  - parses SDMX XML error payloads into structured `error.message`
  - on unresolved queries, callers should stop and report the failed official query instead of supplementing with external facts

24. `resolve_and_query_data(flowRef, filters, startPeriod=None, endPeriod=None, lastNObservations=None, labels='name', resultShape='latest_single_value')`
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
- `registry_profile`: whether the registry/dataflows should be tested as `dense` or `sparse`.
- `test_mode`: whether to build `positive`, `negative`, or `mixed` case sets.
- `dataflows`: exact flows to test.
- `dimensions`: how each dimension should be expanded.
- `query_mode`: whether cases use explicit `TIME_PERIOD` values or `lastNObservations`.
- `negative_case_options`: how sparse/negative cases should be generated.
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

Supported registry profiles:
- `dense`: use when most valid intersections are expected to resolve.
- `sparse`: use when many syntactically valid intersections are expected to return no observations.

Supported test modes:
- `positive`: benchmark value correctness on resolvable cases.
- `negative`: benchmark abstention/non-hallucination on deliberately empty cases.
- `mixed`: generate both positive and negative cases in one manifest.

Supported negative-case strategies:
- `swap_dimension_value`: replace one configured dimension value with another valid code and keep the mutated case only when the direct SDMX query returns no observations.
- `shift_year`: for explicit time-range cases, move the requested year and keep the mutated case only when the direct SDMX query returns no observations.

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
