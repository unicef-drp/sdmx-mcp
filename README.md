# SDMX MCP

Generic MCP server for SDMX registries built with FastMCP. Set `SDMX_BASE_URL` to your SDMX REST API and configure registry-specific policies from the included examples.

This project lets an LLM client do a full guided data journey:
- pick agency
- discover by subject, location, or time
- select a dataflow through guided indicator-first planning
- inspect dimensions and codelists
- rank candidate indicators
- query data with CSV transport for SDMX `/data` payloads

## What This Repo Contains

- `server.py`: MCP tools and SDMX integration logic.
- `query_dimension_policy.example.json`: example subject/location/time query-dimension policy.
- `discovery_policy.example.json`: example discovery ranking policy for stopwords and topic-to-flow hints.
- `scripts/mcp_fidelity_sweep.py`: exhaustive MCP-vs-API comparison. No LLM; proves the pipe is faithful.
- `scripts/make_area_list.py`: builds an area allowlist from a registry codelist, for registries where no pattern separates countries from aggregates (M49).
- `scripts/sdmx_eval_build_cases_from_sweep.py`: stratified agent-eval cases drawn from the sweep's verified ground truth.
- `scripts/agent_test_rig.py`: direct tool-call harness that simulates an agent workflow.
- `scripts/sdmx_eval_runner.py`: generic SDMX eval harness for case generation, provider execution, and grading.
- `scripts/sdmx_eval_model_sweep.py`: runs one manifest across many models and tabulates accuracy against measured cost, per stratum. Resumable.
- `scripts/sdmx_eval_refresh_ground_truth.py`: re-reads expected values from the live registry. Run it immediately before grading, every time.
- `scripts/sdmx_eval_normalise_claims.py`: applies claim recovery to response files already collected, so a harness fix can be applied retroactively.
- `scripts/sdmx_eval_consolidate.py`: gathers runs from several directories into one, refusing to pair grades with responses from a different run.
- `scripts/sdmx_eval_stats.py`: per-model accuracy, Wilson intervals, measured cost, token profile and tool-error classification.
- `scripts/sdmx_eval_pooled.py`: per-stratum pass rates and the spread across models — the difficulty gradient.
- `scripts/sdmx_eval_variance.py`: how far a figure moves across identical repeat runs, and which serving host answered.
- `scripts/sdmx_eval_config.example.json`: example config for deterministic case generation and provider runs.
- `scripts/sdmx_eval_provider_anthropic.py`: Anthropic Messages API adapter using Anthropic's MCP connector.
- `scripts/sdmx_eval_provider_template.py`: stdin/stdout adapter template for wiring any LLM+MCP stack into the eval harness.
- `scripts/list_theme_prefixes.py`: helper to inspect dataflow ID prefixes.
- `theme_prefixes_domain.csv`: optional example prefix-to-domain mapping for grouping. Set `SDMX_THEME_PREFIX_CSV` to use it.
- `paper/`: source for the written report and the deck, built from one markdown file so the two cannot disagree. See `paper/README.md`.

## Local Run

```bash
git clone https://github.com/<you>/sdmx_mcp.git
cd sdmx_mcp
UV_CACHE_DIR="$PWD/.uv-cache" uv sync
UV_CACHE_DIR="$PWD/.uv-cache" uv run fastmcp run server.py --transport stdio
```

Runtime config is loaded from `.env` automatically when present.
Main ships only example policy files. Copy `query_dimension_policy.example.json` to `query_dimension_policy.json`, copy `discovery_policy.example.json` to `discovery_policy.json`, or set the corresponding environment variables when you want active registry-specific policies.

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

Generic `main` intentionally does not auto-deploy on push. The included `.github/workflows/fly-deploy.yml` workflow is manual-only (`workflow_dispatch`) so downstream branches can opt into deployment after setting their own Fly app/config.

Sanity check:

```bash
curl -sS --max-time 20 -X POST https://<your-app>.fly.dev/mcp \
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
The generic `main` branch includes `.example.json` versions only. Active policy files should be provided by downstream branches or deployment environment variables.

### Scope and payload environment variables

| variable | default | effect |
|---|---|---|
| `SDMX_ENFORCE_SCOPE` | `false` | Reject an out-of-scope `flowRef` server-side rather than merely discouraging it in a prompt. Enforced at every data and metadata entry point, so it cannot be bypassed by skipping discovery. |
| `SDMX_DATAFLOW_ID_ALLOW_REGEX` | unset | Restrict the server to matching flows, e.g. `^GLOBAL_DATAFLOW$`. |
| `SDMX_INDICATOR_FLOW_LIMIT` | `3` | Flows listed per candidate by `find_indicator_candidates`. `0` disables trimming. |

Pinning an eval deployment to a single flow is the clearest use of the first two. Scoped to `GLOBAL_DATAFLOW`, one `find_indicator_candidates` call returns ~1,300 tokens instead of ~16,000, recommends the right flow, and an agent cannot wander into a flow that lacks the data.

`SDMX_INDICATOR_FLOW_LIMIT` exists because these flows share one `INDICATOR` codelist, so **codelist membership matches every indicator against every flow**. The untrimmed `dataflows` array came back byte-identical for all candidates and made up 95% of a 57KB payload. The trimmed response adds `dataflowsTotal` and a note that membership is not evidence the flow holds data — prefer `recommendedFlowRef`.

This is a payload-shape issue, not a metadata gap. Only 4 of ~42 UNICEF flows declare an `INDICATOR` content constraint, so better upstream metadata would make the list *accurate*; it would not stop it being emitted once per candidate.

### Upstream failures vs absent data

A throttled registry and a flow with no matching observations are different answers, and callers can tell them apart:

| upstream | `status` | `retryable` |
|---|---|---|
| 429 | `rate_limited` | `true` |
| 5xx, 408, 425 | `upstream_unavailable` | `true` |
| 404, 400, other | `unresolved_from_official_flows` | `false` |

Retryable responses carry `httpStatus` and a message that explicitly denies absence of data, so an agent reports "could not complete, retry" rather than "no data exists". Sustained request streams against a public registry will hit 429 — pace them.

> [!WARNING]
> **A model will not act on `retryable` by itself.** In testing, agents received
> `status: "rate_limited"`, `retryable: true` and the message *"this does not mean
> the data is absent"*, and answered `value: null` regardless — collapsing a
> transport failure back into "no data". For a user that is the worst outcome: a
> transient 429 surfacing as a confident "no data for Liechtenstein".
>
> The *data* path labels this correctly. The **discovery** path did not, until
> October 2026: `find_indicator_candidates` sweeps every scoped flow and skipped
> the ones it could not read, which is right when one flow of many is broken. In
> a total registry outage every fetch failed, every failure was skipped, and the
> tool returned an empty list with `isError: false`. A model asked for "total
> population" was told, in effect, that the registry holds no such indicator —
> and abstained. Discovery now raises when nothing was readable *and* the cause
> was the transport, while still skipping a single unreadable flow.
>
> Beyond that, any client built on this MCP should do **both**:
>
> 1. **Retry in the client**, with backoff, before the model sees the result.
>    `scripts/sdmx_eval_provider_anthropic.py` does this via `throttle_retries`
>    and `throttle_backoff_seconds`. Where tool calls run inside a hosted
>    connector the individual call cannot be retried, so reissue the request.
> 2. **Say it in the system prompt** — never report null for a retryable status —
>    as a second line of defence for when a retry budget is exhausted.
>
> Note this also affects anyone else querying the same public registry: your
> traffic and theirs share the limit, so a quiet deployment can still be
> throttled by someone else's bulk job.



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

> [!IMPORTANT]
> **Writing this policy is a deployment step, not an optimisation.** Which
> dimension carries the subject role, which the geography, which the time, and
> which tokens to ignore when scoring a query, are properties of *your*
> registry and of the questions your office actually receives. They cannot ship
> with useful universal defaults, which is why the repository carries an example
> rather than a working file — and why `main` does not carry one at all.
>
> The server does **not** fail without one — it falls back to a built-in
> default, and that is the problem. The default declares the three roles with
> **no dimension and no codelist bound to any of them**, and **zero** flow
> topic hints. So an unconfigured deployment starts, answers, and quietly
> operates without the resolution layer, which is the part worth the most:
> withholding those tools in a controlled ablation cost the best model tested
> 45 accuracy points and the next 95. Nothing errors. You simply get much worse
> answers than the server is capable of.
>
> It is a small piece of work — a single JSON file, an afternoon with somebody
> who knows the registry's conventions — but it is not optional, and a
> deployment that skips it will conclude the tooling does not work rather than
> that it was never configured. Plan it alongside pointing `SDMX_BASE_URL` at
> your endpoint and declaring which dataflows are in scope.

Example policy:

```json
{
  "auto_apply_total": {
    "enabled": false,
    "dimensions": ["SEX", "WEALTH_QUINTILE", "RESIDENCE"],
    "never_apply": ["INDICATOR", "REF_AREA", "AGE"]
  },
  "default_query_dimensions": [
    {
      "name": "subject",
      "role": "subject",
      "required_for_retrieval": true,
      "priority": 1,
      "discovery_label": "Discover by Subject",
      "discovery_description": "Start with the phenomenon, metric, or topic and let the system find the best indicator and flow.",
      "example_prompts": [
        "Tell me about an indicator in a region.",
        "Show me education coverage for a country group."
      ],
      "preferred_sources": [
        {"type": "codelist", "id": "AGENCY/CL_INDICATOR/1.0"}
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
        "Tell me about an indicator in a region.",
        "Compare an indicator across a country group."
      ],
      "preferred_sources": [
        {"type": "codelist", "id": "AGENCY/CL_REF_AREA/1.0"},
        {"type": "hierarchical_codelist", "id": "AGENCY/REPORTING_REGIONS/1.0"}
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
        "What changed over time for an indicator in a region?",
        "Show the latest values for this topic."
      ]
    }
  ]
}
```

Resolution order follows policy `priority`, not hardcoded dimension names. Non-time dimensions are resolved through the configured code-list or hierarchy sources, and policy source IDs should use the SDMX reference form `agency/id/version` where applicable. `time` is resolved against `TIME_PERIOD`.

`auto_apply_total` lets a registry owner opt in to filling omitted dimensions with `_T`. It is disabled by default. When enabled, the server only applies `_T` to listed dimensions that are present in the selected flow and whose codelist actually contains `_T`; explicit user filters always win. `never_apply` blocks dimensions even if they are also listed in `dimensions`. `TIME_PERIOD` is always treated as special query-parameter time and never receives `_T`.

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

### Units on discovery results

`find_indicator_candidates` / `search_indicators` annotate each candidate with
`unit` (and `unitMultiplier` where the registry sets one):

```
CME_MRM0   Neonatal mortality rate   unit='Deaths per 1,000 live births'
CME_TMM0   Neonatal deaths           unit='Number of deaths'
```

This is the strongest signal for separating sibling indicators whose labels sit
close together — a rate from a count, a percentage from a number affected.
Without it an agent has to choose an indicator, query it, and only then discover
from the observation that it picked the wrong one.

`UNIT_MEASURE` is an observation-level attribute in SDMX, not a dimension, so it
cannot be read from structure. It is resolved with one bulk data probe per flow,
cached for 24h, and is best-effort: a failed probe omits the unit rather than
failing discovery. Pass `includeUnits=false` to skip it.

Set `SDMX_UNIT_PROBE_AREAS` to a few well-populated area codes to keep the probe
small (measured: 27KB/90 rows versus 1.48MB/5152 rows unrestricted, same
coverage). Any indicator those areas do not cover is re-probed unrestricted, so
correctness does not depend on the list. `SDMX_DISCOVERY_UNITS=false` disables
annotation entirely.

**What measurement shows.** A controlled A/B on 75 ambiguous-phrasing cases —
two servers identical but for `SDMX_DISCOVERY_UNITS`, same cases, same models —
found no statistically significant effect either way:

| model | units off | units on | exact McNemar |
|---|---|---|---|
| claude-haiku-4.5-class | 74/75 | 75/75 | p = 1.00 |
| a weaker cheap model | 54/75 | **49/75** | p = 0.36 |

The weaker model got *worse*, and 11 of its 12 regressions were abstentions —
it answered `value: null` on cases it had previously answered correctly. The
extra field appears to give a model with weak tool-use something more to
second-guess.

So units are justified on the grounds that they expose real metadata the agent
could otherwise only obtain by querying, not on a demonstrated accuracy gain.
If you deploy for a model that abstains readily, measure before assuming this
helps, and use `SDMX_DISCOVERY_UNITS=false` if it does not.

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

`preferred_flow_markers` are matched against flow IDs and flow refs, so a marker like `NUTRITION` matches `AGENCY/NUTRITION/1.0`.

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
- Purpose: fetch/cache structure payload. By default, structure requests use `references=descendants` to retrieve the DSD, codelists, and concepts without unrelated registry artifacts. Override with `SDMX_STRUCTURE_REFERENCES` if a registry requires another SDMX references mode.

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
- Purpose: preflight whether a concrete SDMX query resolves before any narrative answer is attempted.
- Returns: structured source-bound status with `status`, `sourceScope`, `provenance`, optional `error`, and `assistant_guidance`.

23. `query_data(flowRef, key=None, startPeriod=None, endPeriod=None, format='csv', labels='name', maxObs=50000, filters=None, lastNObservations=None, resultShape=None)`
- `labels='both'` returns code and label together in SDMX CSV, which reads well in demos.
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

25. `get_single_observation(flowRef, filters=None, subject=None, location=None, time='latest', extraFilters=None, verbose=False)`
- Purpose: compact exact-value tool for one indicator, one location, and one period/latest value.
- Returns: `status`, `value`, `period`, `unit` (resolved label), `refArea`, `refAreaName`, `indicator`, `indicatorName`, and compact `source`; no raw CSV.
- `verbose=False` (default): compact — strips filter-dimension fields beyond core context (e.g. `sex`, `age`).
- `verbose=True`: includes all resolved coded filter dimensions and their labels.

26. `get_indicator_table(flowRef, filters=None, subject=None, locations=None, location=None, time='latest', extraFilters=None, maxRows=200, verbose=False)`
- Purpose: compact latest-by-location table for multi-country, group, comparison, ranking, or table requests.
- Each row: `refArea`, `refAreaName`, `INDICATOR`, `indicatorName`, `value`, `period`, `unit` (resolved label), `rowCountAtLatestPeriod`.
- `verbose=False` (default): coded dimensions constant across all rows (e.g. `SEX=_T` everywhere) are dropped — they carry no information for this result set. Provenance attributes (UNIT_MULTIPLIER, OBS_CONF, …) are dropped except `DATA_SOURCE`, which is always kept.
- `verbose=True`: all coded dimensions (constant or varying) and all non-empty attributes, each with a companion `*Name` label field.
- Single-location and multi-location queries produce identical row schemas for the same flow and mode.

27. `get_time_series(flowRef, filters=None, subject=None, location=None, time='all', extraFilters=None, maxObservations=500, verbose=False)`
- Purpose: compact period/value series for trend, change-over-time, chart, or historical requests.
- Each point: `period`, `value`, `unit` (resolved label). Top-level context: `refArea`, `refAreaName`, `indicator`, `indicatorName`.
- `verbose=False` (default): compact — omits injected filter-dimension fields beyond core context.
- `verbose=True`: includes all resolved coded filter dimensions.

#### DSD-driven label resolution

All three compact tools resolve codes to human-readable labels from the flow's DSD rather than relying on the SDMX endpoint's `labels=name` parameter. This works correctly when components such as `UNIT_MEASURE` live in `attributeList` rather than `dimensionList`, applies the same resolution logic to every coded component, and falls back to the raw code string on any resolution failure (never `null`). Codelists are discovered from the DSD structure — no codelist IDs are hardcoded.

## Fidelity Sweep

`scripts/mcp_fidelity_sweep.py` answers one question exhaustively: **does the MCP return what a direct SDMX API call returns?**

It deliberately does not call an LLM. That question is a property of deterministic software, and a model in the loop can only add false failures (it picks the wrong indicator) and false passes (it is right while the MCP drops a dimension). Agent capability is a separate, sampled experiment — see [Agent Eval](#agent-eval).

Ground truth is one bulk CSV per flow, so the API side costs about one request rather than one per observation.

```bash
# full sweep, ~3 minutes
python3 scripts/mcp_fidelity_sweep.py --rate 2

# exercise the compact projection instead of the raw data path
python3 scripts/mcp_fidelity_sweep.py --mode series --limit 2000 --rate 2 --resume
```

Two modes, because call count is the binding constraint — the registry throttles hard:

| mode | calls | covers |
|---|---|---|
| `bulk` (default) | ~350 | raw data path, one `query_data` per indicator |
| `series` | ~79,000 | the compact projection `get_time_series` applies |

Output is a CSV with one row per observation: the flow's dimensions, year, `mcp_value`, `api_value`, both units, and a verdict of `match` / `MISMATCH` / `MISSING_IN_MCP` / `EXTRA_IN_MCP` / `ERROR`. Exit code is 1 on any mismatch, so CI can gate on it. `--resume` checkpoints.

Result against UNICEF's `GLOBAL_DATAFLOW` for countries, annual 2015–2024: **466,360 / 466,360 observations match (100.0000%)** across 350 indicators and 235 countries.

### Area selection

A country-level sweep must exclude regional aggregates, and **no rule does that reliably across registries**:

| rule | against UNICEF `CL_COUNTRY` (459 codes, 235 countries) |
|---|---|
| ISO3 shape (default) | exact here. Useless under **M49**, where `004` is Afghanistan and `002` is Africa |
| leaf of hierarchy | wrong here — 441 of 459 are leaves, the hierarchy being only 18 parents deep |
| has a parent | inverted between registries: here aggregates carry parents, under M49 countries do |

Membership is authoritative; the pattern is an optional extra filter.

```bash
# M49: select by membership, disable the shape check
--area-codelist AGENCY/CL_COUNTRY_ONLY/latest --area-pattern ''
--area-list m49_countries.txt --area-pattern ''
```

`scripts/make_area_list.py` builds such a list from any registry's codelist and prints what it selected versus dropped — check both columns, because the failure is silent. An aggregate in the selected column inflates a country-level claim; a country in the dropped column disappears from it.

```bash
python3 scripts/make_area_list.py --codelist UNICEF/CL_COUNTRY/latest \
    --pattern '[A-Z]{3}' --out areas.txt
```

## Agent Eval

Where the fidelity sweep tests the pipe, this tests whether an agent can *use* it — find the right indicator among hundreds, pick the right disaggregation, and decline when the data is absent.

`scripts/sdmx_eval_build_cases_from_sweep.py` builds a stratified manifest from the sweep's verified ground truth, so the answer key needs no extra registry traffic. Each case carries `promptStyle`, and the grade should be read per stratum — blending them hides both halves.

| style | tests |
|---|---|
| `prescriptive` | codes supplied — a baseline, not a capability measure |
| `natural` | indicator and country by name, agent resolves the codes |
| `ambiguous` | colloquial phrasing with one defensible answer |
| `disaggregated` | a sex-specific slice where picking the total is wrong |
| `negative` | a cell with no data — the agent must abstain |

```bash
python3 scripts/sdmx_eval_build_cases_from_sweep.py \
  --ground-truth tmp/fidelity/ground_truth.csv \
  --manifest tmp/sdmx_eval/cases.jsonl --count 500

python3 scripts/sdmx_eval_runner.py run-provider \
  --config "$CONFIG" --manifest tmp/sdmx_eval/cases.jsonl \
  --responses tmp/sdmx_eval/responses.jsonl --retry-failed

python3 scripts/sdmx_eval_runner.py grade-results \
  --config "$CONFIG" --manifest tmp/sdmx_eval/cases.jsonl \
  --responses tmp/sdmx_eval/responses.jsonl --grades tmp/sdmx_eval/grades.jsonl
```

`--retry-failed` re-runs only errored cases, dropping the stale records first. Without it an errored case counts as done, so one transient 429 poisons it until the file is deleted by hand.

**Grading scores retrieval, not reporting style.** A filter matches on code or label (`San Marino` and `SMR` both pass), an omitted dimension is unknown rather than wrong, and rounding sets a `notes` field instead of failing. A `trace_match` check confirms the agent actually queried the expected series — a right number reached some other way is not a pass, and a negative case must have looked before abstaining.

**`--hints` is registry-specific and cannot be defaulted.** It maps a colloquial phrase to the single code it should resolve to. A phrase matching several indicators tests whether the agent guesses the same one you did, which manufactures failures — map only phrases with one defensible answer, or omit the file to skip the stratum.

### Registry portability

Nothing in either rig is UNICEF-specific; the defaults just name the registry they were written against.

```bash
python3 scripts/sdmx_eval_build_cases_from_sweep.py \
    --flow AGENCY/SOME_FLOW/1.0 \
    --area-dim REF_AREA --indicator-dim INDICATOR \
    --area-codelist AGENCY/CL_AREA/latest \
    --indicator-codelist AGENCY/CL_INDICATOR/latest \
    --hints my_registry_hints.json
```

## Agent Test Rig

`scripts/agent_test_rig.py` (direct call harness) simulates the intended LLM flow.

Capabilities:
- builds direct SDMX manifest cases from configured indicator and reference-area codelists
- filters inspected flows by optional agency
- does not auto-fill non-essential dimensions, leaving unspecified dimensions as SDMX wildcards
- logs attempts verbosely
- saves per-case JSON outputs

Single run:

```bash
python scripts/agent_test_rig.py \
  --agency <AGENCY> \
  --indicator-codelist-url https://<registry>/codelist/<AGENCY>/<INDICATOR_CODELIST>/1.0 \
  --country-codelist-url https://<registry>/codelist/<AGENCY>/<REF_AREA_CODELIST>/1.0 \
  --manifest-only
```

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

UNICEF global dataflow smoke/eval config:

```bash
python3 scripts/sdmx_eval_runner.py build-cases \
  --config scripts/sdmx_eval_config.unicef_global.example.json \
  --manifest tmp/sdmx_eval/unicef_global_cases.jsonl \
  --case-limit 12

export ANTHROPIC_API_KEY=...
python3 scripts/sdmx_eval_runner.py run-provider \
  --config scripts/sdmx_eval_config.unicef_global.example.json \
  --manifest tmp/sdmx_eval/unicef_global_cases.jsonl \
  --responses tmp/sdmx_eval/unicef_global_responses.jsonl \
  --case-limit 12

python3 scripts/sdmx_eval_runner.py grade-results \
  --config scripts/sdmx_eval_config.unicef_global.example.json \
  --manifest tmp/sdmx_eval/unicef_global_cases.jsonl \
  --responses tmp/sdmx_eval/unicef_global_responses.jsonl \
  --grades tmp/sdmx_eval/unicef_global_grades.jsonl
```

This config targets `UNICEF/GLOBAL_DATAFLOW/1.0`, the flow behind:

```text
https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/data/UNICEF,GLOBAL_DATAFLOW,1.0/all?format=csv&labels=name
```

It builds direct SDMX ground truth first, then asks an MCP-connected agent to answer the same prompts through the configured MCP endpoint. The UNICEF config instructs the agent to use compact routing tools:

- `get_single_observation` for one country, one indicator, and one period/latest.
- `get_indicator_table` for multi-country, regional, comparison, or table-shaped requests.
- `get_time_series` for trend, history, or chart-shaped requests.

The grader checks the agent's structured claims for the numeric value, time period, flow reference, code filters, required MCP tool use, and concise answer shape so failures expose either retrieval errors, routing errors, or hallucinated claims.

Policy contract eval:

```bash
python3 scripts/mcp_policy_contract_eval.py \
  --config scripts/mcp_policy_contract.example.json
```

This direct MCP eval does not call an LLM or spend Anthropic tokens. It compares strict and permissive deployments for the `auto_apply_total` contract: strict wildcard queries stay multi-series, explicit total queries resolve, permissive wildcard queries surface `appliedDefaults`, explicit user filters override defaults, and explicit queries match across both modes.

Anthropic setup:

```bash
export ANTHROPIC_API_KEY=...
python3 scripts/sdmx_eval_runner.py run-provider \
  --config scripts/sdmx_eval_config.example.json \
  --case-limit 25
```

GitHub Actions setup:

- Add `ANTHROPIC_API_KEY` as a repository or environment secret.
- Run **UNICEF Global MCP Eval** manually from the Actions tab.
- Use `target_ref=unicef` when evaluating the UNICEF branch.
- Start with a small `case_limit` such as `4`, then run the full manifest once the smoke test passes.
- Check the **Summarize grades** step for pass/fail counts, token usage, and estimated cost.
- Download the `unicef-global-eval` artifact to inspect the generated manifest, raw provider responses, and grade records.

The workflow is intentionally `workflow_dispatch` only. It does not run on `pull_request` or public fork events, and its `GITHUB_TOKEN` permissions are limited to `contents: read`. Public users cannot spend your Anthropic key just by opening a PR. Anyone with enough repository permission to manually run workflows could run this job with the configured secret, so keep write/workflow access limited and prefer an environment-protected secret if you need an approval gate for eval runs.

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

Diagnostic fields, for attributing a failure rather than guessing at it:
- `served_by`: the inference host(s) that answered, one per step. For an
  open-weight model the numerical precision, the sampling defaults and the
  **tool-call parser** all belong to the host rather than to the weights, so
  without this a cluster of failures cannot be told apart from a bad route.
- `truncated`, `steps_exhausted`: harness ceilings hit, each distinct from a
  wrong answer and from a malformed one.
- `finish_reason`, `reasoning_tail` (failures only): where the answer went when
  there is no answer. A reasoning model can finish normally having written its
  response into a reasoning channel and left `content` empty — worth ruling out
  before concluding the model produced nothing.

### Anthropic Adapter

`scripts/sdmx_eval_provider_anthropic.py` calls the Anthropic Messages API and passes your MCP server via the `mcp_servers` request field. It expects:
- `ANTHROPIC_API_KEY` in the environment by default
- `mcp.url` in the eval config
- an MCP server reachable over HTTP(S)

The adapter asks Claude to return a single JSON object so grading can stay deterministic. If you need auth on the MCP endpoint, set `mcp.authorization_token_env` in the config and the adapter will forward that bearer token to Anthropic's MCP connector request.

### Refresh ground truth before grading

A manifest's expected values are a snapshot of the registry at the moment the
sweep ran, and a live statistical registry revises data in weeks, not years.
Measured: a manifest built on 5 October 2026 and graded on the 20th scored
every model at 94% on the prescriptive stratum, because UNICEF had restated the
`PV_CHLD_*` child-poverty family in that window (source "DHS VII" to
"DHS 2018 (IPUMS)", one series from 18.5535945892334 to 23.67173993) and
withdrawn four series entirely. Models were penalised twice: once for reporting
the current value, once for correctly answering `null` where data no longer
existed.

Refreshing costs nothing — registry reads, no model calls — so run it
immediately before grading rather than trusting the manifest's snapshot:

```bash
python3 scripts/sdmx_eval_refresh_ground_truth.py \
  --manifest tmp/eval/manifest.jsonl \
  --out tmp/eval/manifest.fresh.jsonl
```

Then grade against the refreshed manifest. Responses already collected can be
re-graded without re-running a single model, so this fixes staleness
retroactively.

It is deliberately conservative: a value is rewritten only when exactly one
observation pins it. A period returning several observations, or a fetch that
errors, leaves the case exactly as it was rather than inventing ground truth —
a transport failure must never read as "the data is gone".

**Three things it does that are easy to miss.**

*Withdrawal flips the expectation, not just the value.* When a series is gone
the expected value becomes null **and** the case's `caseType` becomes
`negative` with `expectedBehavior: abstain_no_data`. Nulling the value alone
left the case contradicting itself — the grader reads the top-level fields — so
a model that correctly abstained scored `manual_review`, a verdict nobody goes
back to review. Six withdrawn `PV_CHLD_*` cases sat that way and cost every
measured model a point. This is applied even when the value was already null,
so a manifest an earlier pass left half-refreshed gets repaired.

*It retries, because the registry throttles without saying so.* Limits are per
client address, hard, and carry **no `Retry-After` and no rate-limit headers**,
so a client cannot pace itself except by guessing. Without backoff, 494 of 500
cases came back 429 and were reported as "left alone" beneath a summary reading
`value_changed: 0` — a refresh that checked almost nothing looked exactly like
one that confirmed everything. The summary now includes
`left_alone_reasons`, so you can see *why* cases were skipped.

*Every answered case is stamped* `verified_against_registry`, whether or not
its value moved. The grader needs that to tell "the model abstained and a value
demonstrably exists" from "the model abstained and nobody checked". A throttled
case is never stamped.

Summary fields: `value_changed`, `unchanged`, `left_alone`,
`left_alone_reasons`, `reclassified_to_abstain`, and
`negative_cases_that_now_have_data` (those are no longer negative cases and
need regenerating).

### Grading an abstention

A model answering `null` where the manifest expects a value is either a miss or
a correct refusal against stale ground truth, and the grader cannot tell which
by itself. It used to defer every such case to `manual_review`; 55 accumulated
across seven models, and checking them by hand against the registry found 54
genuine misses and one withdrawn series that refreshing had already handled.

So the rule is narrow: an abstention is a **failure** when the ground truth
carries `verified_against_registry` from the refresh pass, and still
`manual_review` when it does not. Failing an abstention against truth that has
quietly gone stale is the original error, and it is worse than deferring.

### Retry transient failures before grading

A provider error is not a wrong answer, and grading it as one is
indistinguishable from incompetence in the final table. Measured: a 500-case
run showed 116 empty responses from one model; a single `--retry-failed` pass
recovered 70 of them and moved that model from 68.4% to 84.4%. Across nine
models, 474 outstanding provider errors were pending at one point.

Re-run the same sweep command: each model with errored rows gets
`--retry-failed`, complete models are skipped, and everything is re-graded.
Then confirm the grades were actually rewritten — a sweep that runs the retry
but dies before grading leaves stale verdicts beside improved responses, which
reads as "the retry did nothing".

### Gateway Adapter (multi-model)

`scripts/sdmx_eval_provider_gateway.py` runs the same eval against any model
reachable through [Vercel AI Gateway](https://vercel.com/docs/ai-gateway), so
one manifest can be scored across providers. It expects:
- `AI_GATEWAY_API_KEY` in the environment (create the key in the Vercel
  dashboard under AI Gateway)
- `mcp.url` in the eval config
- a model id in `provider/model` form, e.g. `openai/gpt-6-astra`,
  `google/gemini-3-pro`, `anthropic/claude-haiku-4.5`

Start from `scripts/sdmx_eval_config.unicef_global.gateway.example.json`:

```bash
export AI_GATEWAY_API_KEY=...
python3 scripts/sdmx_eval_runner.py run \
  --config scripts/sdmx_eval_config.unicef_global.gateway.example.json \
  --manifest tmp/eval/manifest.jsonl \
  --responses tmp/eval/responses.gpt.jsonl
```

`SDMX_EVAL_MODEL` overrides the configured model, so one config drives a whole
comparison:

```bash
for model in openai/gpt-6-astra google/gemini-3-pro anthropic/claude-haiku-4.5; do
  SDMX_EVAL_MODEL="$model" python3 scripts/sdmx_eval_runner.py run \
    --config scripts/sdmx_eval_config.unicef_global.gateway.example.json \
    --manifest tmp/eval/manifest.jsonl \
    --responses "tmp/eval/responses.${model//\//-}.jsonl"
done
```

**One caveat when comparing against Anthropic-adapter results.** The Anthropic
adapter uses Anthropic's *hosted* MCP connector: tool calls execute on
Anthropic's side. A gateway is a plain Chat Completions endpoint with no such
facility, so this adapter runs the MCP client loop locally — it lists the
server's tools, converts their schemas to OpenAI function definitions, executes
each requested call over Streamable HTTP, and feeds the result back. The
system prompt, contract wording, manifest, ground truth and grader are held
identical, but the tool transport is not. A score gap between the two adapters
is not purely a model difference.

Cost is read from the gateway's own per-request `usage.cost` rather than from a
local price table, since the gateway bills at list price with no markup.

**`provider.deny_tools`** withholds named tools from the model. It exists for
ablation: strip the curated discovery and resolution tools, leave the ones
corresponding to native SDMX REST operations (`get_flow_structure`,
`list_dimensions`, `list_codes`, `get_single_observation`, `query_data`,
`get_time_series`), and measure what the guided layer is worth. It refuses to
run if the denylist would leave no tools at all.

Measured on the resolution strata: the best model tested fell from 99.4% to
54.8% and the next from 97.8% to 2.7%, with tool calls per question rising from
5.3 to between 35 and 98 and a third to a half of cases exhausting the step
ceiling. Treat that as a **lower bound** on the discovery layer's value rather
than an estimate — the ablated arm still receives structured tool schemas, the
validation layer explaining each malformed request, scope enforcement and
caching, and never has to build a URL or parse SDMX-JSON. A model working
against the raw REST interface has none of that.

If you set `deny_tools`, strip tool names out of `provider.system_prompt` too.
Routing advice naming tools the model cannot see measures prompt/tool mismatch
rather than the missing layer.

### Model Sweep (many models, one manifest)

`scripts/sdmx_eval_model_sweep.py` runs one manifest across several models and
tabulates accuracy against measured cost, per stratum. Each model is run and
graded through the normal runner, so a sweep is exactly equivalent to issuing
the commands by hand — it only removes the bookkeeping, and every number still
traces back to the transcript that produced it.

```bash
python3 scripts/sdmx_eval_model_sweep.py \
  --config scripts/sdmx_eval_config.unicef_global.gateway.example.json \
  --manifest tmp/eval/manifest.fresh.jsonl \
  --out-dir tmp/sweep \
  --summary-json tmp/sweep/summary.json \
  --models zai/glm-5.3-flash deepseek/deepseek-v4.1-flash anthropic/claude-haiku-5.5
```

Models are discovered from the run directory by the downstream analysis rather
than listed in it, and responses and grades are kept per model.

**It is resumable, and that is load-bearing.** Re-running the same command
retries each model's errored rows with `--retry-failed`, skips models that are
already complete, and re-grades. Three behaviours exist because each one cost a
published-looking number:

- **The runner treats any case already in the file as done, errored rows
  included.** Without `--retry-failed` a re-run prints "running" and writes
  nothing. After a network outage left 408 of 500 cases failed, a sweep reported
  it was re-running them, reproduced the same file, and the resulting **16.8%**
  read as the model's score. The sweep now raises if a retry adds no cases.
- **A torn final line is tolerated; corruption is not.** The runner appends from
  several workers, so reading immediately after it exits can catch the last line
  mid-write — which once ended a sweep with a bare `JSONDecodeError` after seven
  models had been measured, on a file that was valid moments later. A malformed
  *last* line is skipped as a partial append; a malformed line with others after
  it is real corruption and stops the sweep, naming the file and line.
- **`--retry-failed` rewrites the responses file atomically.** Records reach
  1.7MB, so rebuilding in place leaves a long window where the file is half
  written; one 500-case run came back with a record torn across two lines. The
  rewrite now writes a sibling and renames.

**Screen before you commit.** Capability at tool use varies far more than price
does, and the gap does not appear on easy questions. Screening 11 models on a
74-case manifest cost **$4.52** and separated models that drive the server from
ones that cannot — one of which never successfully called a tool at all, and
answered population questions from memory instead. Spend a full run only on
what survives.

`scripts/sdmx_eval_normalise_claims.py` applies the terse-answer claim recovery
to response files already collected, so a harness improvement can be applied
retroactively without re-running a model.

### Analysis layer

Four scripts turn graded output into the numbers a report can quote. They take
a run directory, discover the models in it by scanning rather than from a list,
and write their JSON into that directory.

```bash
# Gather runs; later sources supersede earlier ones, so list a retry after the
# original it replaces.
python3 scripts/sdmx_eval_consolidate.py tmp/final tmp/sweep tmp/sweep-retried

python3 scripts/sdmx_eval_stats.py    tmp/final     # -> tmp/final/stats.json
python3 scripts/sdmx_eval_pooled.py   tmp/final     # -> tmp/final/pooled.json
python3 scripts/sdmx_eval_variance.py tmp/repeats   # -> tmp/repeats/variance.json
```

`stats.py` reports accuracy with a **Wilson** interval rather than the normal
approximation, because these proportions sit near 1 where the normal interval
runs past 100%. It also reports the token profile, since input dominates by
roughly 35:1 on this workload and a per-token price list therefore misleads
about cost per task.

`pooled.py` reports the per-stratum spread across models, which is the finding
worth having: models agree where a query is fully specified and diverge where
codes must be resolved. **It excludes models averaging under one tool call per
case**, decided on tool use rather than on score — a model that never gets the
server to answer records zero on every stratum, which makes every range the
full 100 points and erases the gradient. Pooled pass rates include every model,
and the exclusion is reported in the output.

`variance.py` reports the accuracy spread across repeats *and* the share of
individual cases that change verdict, because a stable headline can sit on top
of unstable answers. It also tabulates pass rate by serving host where a model
was served by more than one.

**Two refusals worth knowing about.** All of them stop rather than average when
a model's grades and responses have different lengths — that pairing reports
accuracy from one run and cost from another, and it happened once with 500
graded cases against 100 re-run responses. `consolidate.py` additionally
refuses a file whose rows contain duplicate `case_id`s, since those would be
graded twice.

### Repeat a run before publishing a number

A single run is an estimate. Measured over three identical runs of the same 148
cases: competent models vary by **0.7 to 2.0 points**, and that is a reasonable
error bar to carry against any single figure. One model varied by **10.1
points**, with **half its cases returning a different verdict** between
identical runs — a far more disqualifying property than its headline score, and
invisible to any single run at any sample size.

Report both the accuracy spread *and* the share of individual cases that change
verdict. A stable headline can sit on top of unstable answers: one model held a
1.4-point spread while 4.7% of its cases churned, three times the rate of the
most stable model measured.

Run-to-run variance tracked model competence rather than open-weight versus
commercial licensing — a first-party commercial model and the best open-weight
model returned identical 0.7-point spreads. And when the serving host was
recorded for every request of the least stable model, all 444 went to the same
host, so its instability is within-host rather than an artefact of the gateway
switching between them.


## Connector Setup

### Claude (Connector / Integrations)

If Claude supports remote MCP connectors in your plan/UI:
- add connector URL: `https://<your-app>.fly.dev/mcp`
- test with a first prompt like: "List agencies"

### Claude Desktop (config fallback)

`~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "sdmx": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "https://<your-app>.fly.dev/mcp"]
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
- Use `find_indicator_candidates` and inspect the returned candidate flows, or configure stronger `flow_topic_hints` in `discovery_policy.json`.
