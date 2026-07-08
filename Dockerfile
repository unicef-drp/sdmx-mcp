FROM python:3.14.6-slim

ARG GIT_SHA=local
ARG APP_VERSION=

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FASTMCP_JSON_RESPONSE=true \
    FASTMCP_STATELESS_HTTP=true \
    SDMX_BUILD_ID=${GIT_SHA} \
    SDMX_APP_VERSION=${APP_VERSION} \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

RUN groupadd --system --gid 10001 app \
    && useradd  --system --uid 10001 --gid app --home-dir /app --shell /usr/sbin/nologin app

COPY pyproject.toml uv.lock* ./
RUN pip install --upgrade pip uv

COPY . .
RUN uv sync --frozen --no-dev --no-cache --python python3 \
    && chown -R app:app /app

USER app

EXPOSE 8000

CMD ["fastmcp", "run", "server.py", "--transport", "streamable-http", "--host", "0.0.0.0", "--port", "8000", "--path", "/mcp"]
