FROM python:3.12-slim

WORKDIR /app

# System deps for DuckDB spatial
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgeos-dev \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir .

COPY src/ src/

EXPOSE 8082
CMD ["python", "-m", "spatial_lakehouse_mcp.server"]
