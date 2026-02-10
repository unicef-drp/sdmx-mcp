FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FASTMCP_JSON_RESPONSE=true \
    FASTMCP_STATELESS_HTTP=true

WORKDIR /app

COPY pyproject.toml uv.lock* ./
RUN pip install --upgrade pip

COPY . .
RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["fastmcp", "run", "server.py", "--transport", "streamable-http", "--host", "0.0.0.0", "--port", "8000", "--path", "/mcp"]
