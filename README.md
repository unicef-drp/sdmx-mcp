# UNICEF SDMX MCP

This repository hosts a [FastMCP](https://github.com/modelcontextprotocol/servers/tree/main/python/fastmcp) server that exposes UNICEF’s SDMX data service. The server can be run locally with `uv`, containerized with Docker, or deployed to Fly.io.

## 1. Test locally

```
git clone https://github.com/<you>/sdmx_mcp.git
cd sdmx_mcp
rm -rf .venv .uv-cache __pycache__
UV_CACHE_DIR="$PWD/.uv-cache" uv sync
UV_CACHE_DIR="$PWD/.uv-cache" uv run fastmcp run server.py --transport stdio
```

Notes:
- `UV_CACHE_DIR` keeps uv’s cache inside the repo, avoiding macOS permission prompts.
- The STDIO transport is what MCP-aware clients expect.

Manual HTTP test:
```
UV_CACHE_DIR="$PWD/.uv-cache" uv run fastmcp run server.py --transport http --host 127.0.0.1 --port 8000 --path /mcp
curl -X POST http://127.0.0.1:8000/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

## 2. Docker workflow

1. Build the image:
   ```
   docker build -t sdmx-mcp:latest .
   ```
2. Run locally:
   ```
   docker run --rm -p 8000:8000 sdmx-mcp:latest
   ```
3. Test:
   ```
   curl -X POST http://localhost:8000/mcp \
     -H "Content-Type: application/json" \
     -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
   ```

## 3. Deploy to Fly.io

```
fly auth login
fly launch --no-deploy  # already configured via fly.toml
fly deploy --build-arg UV_CACHE_DIR=/app/.uv-cache
```

Runtime expectations:
- App name: `sdmx-mcp` (see `fly.toml`).
- Port 8000 exposed via Fly’s `http_service`.
- FastMCP command: `fastmcp run server.py --transport http --host 0.0.0.0 --port 8000 --path /`.

Sanity check:
```
curl -X POST https://sdmx-mcp.fly.dev/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

## 4. Test in the FastMCP Inspector

Start the Inspector UI (local):
```
UV_CACHE_DIR="$PWD/.uv-cache" uv run fastmcp dev server.py --ui-port 3333 --server-port 8000
```

In the Inspector, set "Server URL" to:
- Local: `http://127.0.0.1:8000/mcp`
- Fly: `https://sdmx-mcp.fly.dev/mcp`

## 5. MCP client configuration example

`~/.config/mcp/servers/unicef-sdmx.json`:

```json
{
  "command": "uv",
  "args": [
    "run",
    "fastmcp",
    "run",
    "server.py",
    "--transport",
    "stdio"
  ],
  "cwd": "/Users/<you>/git/sdmx_mcp",
  "env": {
    "UV_CACHE_DIR": "/Users/<you>/git/sdmx_mcp/.uv-cache"
  }
}
```

Restart your MCP-compatible client (Claude Desktop, Cursor, etc.) and enable the `unicef-sdmx` server.
