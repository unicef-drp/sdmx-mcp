# SDMX MCP

MCP server for SDMX registries, built with FastMCP. 

You can try it out over the UNICEF Data Warehouse: https://sdmx-mcp.fly.dev/ (NOTE: this is not an official UNICEF MCP release and should not be used as a replacement for actually pulling the datatsets using the warehosue APIs: https://sdmx.data.unicef.org/webservice/data.html)

The server is registry-agnostic: users can point it at any SDMX REST endpoint and optionally scope which dataflows are exposed (for example, exclude draft flows).

## What This Repo Contains

- `server.py`: MCP tools and SDMX integration logic.
- `scripts/list_theme_prefixes.py`: helper to inspect dataflow ID prefixes.
- `theme_prefixes_domain.csv`: optional prefix-to-domain mapping used for grouped flow discovery.
- `.env.example`: starter environment settings for local, Docker, and Fly deployments.

## Configuration

Set environment variables before running:

- `SDMX_BASE_URL`: SDMX REST base URL.
  - Example: `https://example.org/ws/public/sdmxapi/rest`
- `SDMX_MCP_NAME`: MCP server name exposed to clients. Default: `sdmx-mcp`.
- `SDMX_USER_AGENT`: outbound HTTP user agent. Default: `sdmx-mcp/0.1`.
- `SDMX_THEME_PREFIX_CSV`: optional path to a custom prefix mapping CSV.

Optional scoping controls:

- `SDMX_EXCLUDE_DRAFT=true`: hide flows that appear to be draft.
- `SDMX_AGENCY_ALLOWLIST=AGENCY1,AGENCY2`: only expose listed agencies.
- `SDMX_DATAFLOW_ID_ALLOW_PREFIXES=PROD_,CORE_`: only flow IDs starting with these prefixes.
- `SDMX_DATAFLOW_ID_DENY_PREFIXES=DRAFT_,TMP_`: exclude flow IDs with these prefixes.
- `SDMX_DATAFLOW_ID_ALLOW_REGEX=...`: regex allow filter for flow IDs.
- `SDMX_DATAFLOW_ID_DENY_REGEX=...`: regex deny filter for flow IDs.
- `SDMX_ENFORCE_SCOPE=true|false`: enforce scope even for explicit `flowRef` calls.
  - Default: automatically `true` when any scope filter is configured.

## Local Run (Testing)

```bash
git clone https://github.com/<you>/sdmx_mcp.git
cd sdmx_mcp
UV_CACHE_DIR="$PWD/.uv-cache" uv sync

cp .env.example .env
set -a; source .env; set +a

UV_CACHE_DIR="$PWD/.uv-cache" uv run fastmcp run server.py --transport stdio
```

HTTP mode:

```bash
UV_CACHE_DIR="$PWD/.uv-cache" uv run fastmcp run server.py --transport streamable-http --host 127.0.0.1 --port 8000 --path /mcp
```

Quick check:

```bash
curl -sS -X POST http://127.0.0.1:8000/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

## Docker

Build:

```bash
docker build -t sdmx-mcp:latest .
```

Run with your registry config:

```bash
docker run --rm -p 8000:8000 \
  --env-file .env \
  sdmx-mcp:latest
```

## Theme Mapping Workflow

Prefix grouping is a convenience heuristic, not an SDMX standard.

- Many UNICEF flow IDs use stable prefixes (for example `CME_*`, `PT_*`), so grouping by prefix works well there.
- Other registries may use different naming conventions, mixed conventions, or no meaningful prefix at all.
- If prefixes are not meaningful for your registry, you can still use the MCP normally and ignore theme mapping features.

How prefix grouping works in this server:

- It extracts a theme code from the flow ID (text before the first `_`, otherwise full ID).
- It optionally maps that code to a friendly label using `SDMX_THEME_PREFIX_CSV`.
- This affects grouped discovery tools (`list_dataflows_grouped`, prefix-listing helpers), not core SDMX querying.

Generate an editable template for `SDMX_THEME_PREFIX_CSV`:

```bash
python scripts/list_theme_prefixes.py --limit 200 --format theme-map-template > theme_prefixes_domain.csv
```

Then fill the `domain` column and point the server to it:

```bash
export SDMX_THEME_PREFIX_CSV=theme_prefixes_domain.csv
```

## Deploy Anywhere

This project does not require Fly.io. Deploy it anywhere that can run a container or Python process.

Container runtime requirements:

- Expose HTTP on port `8000`.
- Start command:
  - `fastmcp run server.py --transport streamable-http --host 0.0.0.0 --port 8000 --path /mcp`
- Provide environment variables (at minimum `SDMX_BASE_URL`).

Then validate with a JSON-RPC `tools/list` request to `/mcp`.

## Fly.io (Quick Default Option)

`fly.toml` is included for quick deployment, but it is optional.

Deploy:

```bash
fly auth login
fly secrets set SDMX_BASE_URL="https://example.org/ws/public/sdmxapi/rest"
fly deploy
```

Optional scope controls:

```bash
fly secrets set SDMX_EXCLUDE_DRAFT=true
fly secrets set SDMX_DATAFLOW_ID_DENY_PREFIXES="DRAFT_,TMP_"
```

Validate:

```bash
curl -sS --max-time 20 -X POST https://<your-app>.fly.dev/mcp \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

## MCP Tools (Function Catalog)

1. `list_agencies(limit=50)`
2. `search_dataflows(query, limit=10)`
3. `list_dataflows_grouped(query=None, prefixMap=None, limitPerTheme=50)`
4. `get_default_theme_prefix_map()`
5. `list_theme_prefixes(limit=50)`
6. `list_theme_prefix_conflicts(limit=100)`
7. `describe_flow(flowRef)`
8. `list_dimensions(flowRef)`
9. `list_codes(flowRef, dimension, query=None, limit=50)`
10. `find_indicator_candidates(flowRef, query, limit=10)`
11. `get_flow_structure(flowRef)`
12. `build_key(flowRef, selections)`
13. `query_data(flowRef, key=None, startPeriod=None, endPeriod=None, format='sdmx-json', labels=None, maxObs=50000, filters=None, lastNObservations=None)`

## Troubleshooting

1. `406 Not Acceptable`
- Use `/mcp` and include `Accept: application/json` for JSON-RPC POST tests.

2. `404 Not Found`
- Check path and transport. Default HTTP path is `/mcp`.

3. Empty discovery results
- Review scope filters (`SDMX_DATAFLOW_*`, `SDMX_AGENCY_ALLOWLIST`, `SDMX_EXCLUDE_DRAFT`).

4. Data query 404 with SDMX message `No data for data query against the dataflow`
- Query syntax is valid but the selected dimensional slice has no observations.
