FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml uv.lock* ./
RUN pip install --upgrade pip

COPY . .
RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["fastmcp", "run", "server.py", "--transport", "http", "--host", "0.0.0.0", "--port", "8000", "--path", "/mcp"]
