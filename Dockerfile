FROM python:3.12-slim

WORKDIR /app

# System deps for DuckDB spatial
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir .

EXPOSE 8082
CMD ["python", "-m", "spatial_lakehouse_mcp.server"]
