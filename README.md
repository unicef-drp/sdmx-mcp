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

9. `list_codes(flowRef, dimension, query=None, limit=50)`
- Purpose: list codes for one dimension, optional text filter.

10. `find_indicator_candidates(flowRef, query, limit=10)`
- Purpose: rank `INDICATOR` codes by relevance to user text.

11. `get_flow_structure(flowRef)`
- Purpose: fetch/cache structure payload (`references=all`).

12. `build_key(flowRef, selections)`
- Purpose: build SDMX key from dimension selections.
- Notes: supports list/comma syntax for multi-code segments.

13. `query_data(flowRef, key=None, startPeriod=None, endPeriod=None, format='sdmx-json', labels=None, maxObs=50000, filters=None, lastNObservations=None)`
- Purpose: run data query with bounded extraction guardrails.
- Key behaviors:
  - requires either (`startPeriod` + `endPeriod`) or `lastNObservations`
  - supports `filters` to auto-build key from dimension order
  - leaves unspecified dimensions as empty key segments (`.` wildcard)
  - supports `format='csv'` and returns `raw_csv`
  - supports optional `labels` parameter (for example `labels=both` with CSV)
  - parses SDMX XML error payloads into structured `error.message`

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
