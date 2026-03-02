# spatial-lakehouse-mcp

Iceberg-native geospatial MCP server powered by DuckDB.

Provides 18 tools for catalog discovery, spatial queries, analysis, and data management over an Apache Iceberg lakehouse. An LLM agent connects via the Model Context Protocol (MCP) and can explore schemas, run spatial SQL, perform point-in-polygon aggregation, export GeoJSON, and more — all through a single DuckDB connection.

## Architecture

```
MCP Client (LLM)
    ↕  (Streamable HTTP, port 8082)
spatial-lakehouse-mcp
    ↕
DuckDB (in-process, :memory:)
    ├── iceberg extension  → LakeKeeper REST Catalog (port 8181)
    ├── httpfs extension   → Garage S3 (port 3900)
    └── spatial extension  → ST_* geospatial functions
```

**Key design decision:** DuckDB v1.4+ natively supports `ATTACH` to Iceberg REST Catalogs. Once attached, the catalog appears as a regular DuckDB database — `SHOW ALL TABLES`, `DESCRIBE`, `iceberg_snapshots()`, time travel, and full SQL (including spatial functions) all work through a single connection. No pyiceberg dependency.

### Infrastructure Stack

| Component | Technology | Default Port |
|-----------|-----------|-------------|
| Object Storage | [Garage](https://garagehq.deuxfleurs.fr/) (S3-compatible) | 3900 |
| Iceberg Catalog | [LakeKeeper](https://github.com/lakekeeper/lakekeeper) (REST) | 8181 |
| Catalog Metadata | PostgreSQL | 5432 |
| MCP Server | This project (FastMCP + DuckDB) | 8082 |

## Tools (18 total)

### Catalog Discovery
| Tool | Description |
|------|-------------|
| `list_namespaces` | List schemas in the Iceberg catalog |
| `list_tables` | List tables, optionally filtered by namespace |
| `describe_table` | Column names, types, geometry detection |
| `table_snapshots` | Snapshot history for time-travel queries |
| `search_tables` | Search tables by name, column, or geometry presence |

### Spatial Queries
| Tool | Description |
|------|-------------|
| `query` | Read-only SQL with spatial functions |
| `spatial_filter` | Structured spatial predicates (intersects, within, bbox, within_distance) |
| `nearest_features` | K-nearest-neighbor search |
| `get_bbox` | Bounding box / spatial extent |
| `time_travel_query` | Query at a specific Iceberg snapshot or timestamp |
| `multi_table_query` | Cross-table analytics with safety rails |

### Spatial Analysis
| Tool | Description |
|------|-------------|
| `spatial_join` | Join two tables on spatial predicates (intersects, contains, dwithin, etc.) |
| `aggregate_within` | Point-in-polygon aggregation (count, sum, avg, min, max, stddev) |
| `buffer_analysis` | Buffer zones with optional dissolve (union) |

### Data Management
| Tool | Description |
|------|-------------|
| `sample_data` | Preview rows from a table |
| `table_stats` | Row counts, column stats, geometry summary |
| `export_geojson` | Export as GeoJSON FeatureCollection |

### System
| Tool | Description |
|------|-------------|
| `health_check` | Connection, extension, and catalog status |

## Project Structure

```
spatial-lakehouse-mcp/
├── pyproject.toml
├── Dockerfile
├── .env.example
├── validate_stack.py              # Pre-flight infrastructure validation
├── src/
│   └── spatial_lakehouse_mcp/
│       ├── __init__.py
│       ├── server.py              # FastMCP server + 18 tool definitions
│       ├── config.py              # Pydantic settings (SLM_ env prefix)
│       ├── engine.py              # DuckDB connection + catalog attachment
│       └── validators.py          # SQL safety + input validation
└── tests/
    ├── conftest.py                # Shared fixtures (local DuckDB test data)
    └── test_tools.py              # 21 tests across all phases
```

## Quick Start (Local Development)

### Prerequisites

- Python 3.11+
- A running lakehouse stack (LakeKeeper + Garage + PostgreSQL)

### 1. Install

```bash
git clone https://github.com/aoneil42/Spatial-Lakehouse-MCP.git
cd Spatial-Lakehouse-MCP

python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### 2. Configure

```bash
cp .env.example .env
```

Edit `.env` with your infrastructure credentials:

```bash
# Required — Garage S3 credentials
GARAGE_KEY_ID=your_garage_key
GARAGE_SECRET_KEY=your_garage_secret
SLM_S3_ENDPOINT=localhost:3900

# Required — LakeKeeper catalog
SLM_CATALOG_URI=http://localhost:8181
SLM_CATALOG_WAREHOUSE=lakehouse

# Auth (leave empty for LakeKeeper allowall dev mode)
SLM_CATALOG_TOKEN=
```

### 3. Validate Stack Connectivity

Before running the server, verify the MCP server can reach your infrastructure:

```bash
python validate_stack.py
```

Expected output:
```
DuckDB Python version: 1.4.4

  [1/3 Garage S3 + httpfs] PASS
      5 files found, first file has 4096 rows

  [2/3 LakeKeeper ATTACH] PASS
      ATTACH OK, 7 table(s) found, lakehouse.colorado.lines has 30000 rows

  [3/3 Extension coexistence] PASS
      All 3 extensions loaded, ST_Point → POINT (-104.99 39.74)

Results: 3 passed, 0 failed
```

### 4. Run Tests

```bash
pytest -v
```

Tests run against local in-memory DuckDB tables (no infrastructure required).

### 5. Start the Server

```bash
python -m spatial_lakehouse_mcp.server
```

The server starts on `http://0.0.0.0:8082` using Streamable HTTP transport.

## Docker Deployment

```bash
docker build -t spatial-lakehouse-mcp .
docker run --env-file .env -p 8082:8082 spatial-lakehouse-mcp
```

Or add to an existing docker-compose stack:

```yaml
services:
  mcp-server:
    build: .
    ports:
      - "8082:8082"
    environment:
      GARAGE_KEY_ID: "${GARAGE_KEY_ID}"
      GARAGE_SECRET_KEY: "${GARAGE_SECRET_KEY}"
      SLM_S3_ENDPOINT: garage:3900
      SLM_CATALOG_URI: http://lakekeeper:8181
      SLM_CATALOG_WAREHOUSE: lakehouse
    depends_on:
      lakekeeper:
        condition: service_healthy
```

## Configuration Reference

All environment variables use the `SLM_` prefix (Spatial Lakehouse MCP), except Garage credentials which use `GARAGE_` for compatibility.

| Variable | Default | Description |
|----------|---------|-------------|
| `SLM_CATALOG_URI` | `http://localhost:8181` | LakeKeeper REST endpoint |
| `SLM_CATALOG_WAREHOUSE` | `warehouse` | Warehouse name in LakeKeeper |
| `SLM_CATALOG_ALIAS` | `lakehouse` | DuckDB alias for the attached catalog |
| `SLM_CATALOG_TOKEN` | (empty) | Bearer token for LakeKeeper auth |
| `SLM_CATALOG_CLIENT_ID` | (empty) | OAuth2 client ID |
| `SLM_CATALOG_CLIENT_SECRET` | (empty) | OAuth2 client secret |
| `SLM_CATALOG_OAUTH2_SCOPE` | (empty) | OAuth2 scope |
| `SLM_CATALOG_OAUTH2_SERVER_URI` | (empty) | OAuth2 token endpoint |
| `GARAGE_KEY_ID` | (empty) | Garage S3 access key ID |
| `GARAGE_SECRET_KEY` | (empty) | Garage S3 secret access key |
| `SLM_S3_ENDPOINT` | `localhost:3900` | S3 endpoint (host:port, no scheme) |
| `SLM_S3_REGION` | `garage` | S3 region |
| `SLM_S3_USE_SSL` | `false` | Use HTTPS for S3 |
| `SLM_S3_URL_STYLE` | `path` | S3 URL style (path or vhost) |
| `SLM_MAX_RESULT_ROWS` | `100` | Max rows returned per query |
| `SLM_QUERY_TIMEOUT_SECONDS` | `30` | Query timeout |
| `SLM_SERVER_PORT` | `8082` | MCP server port |

## Important Notes

### ACCESS_DELEGATION_MODE

When running the MCP server **outside Docker** (on the host), the `ATTACH` statement uses `ACCESS_DELEGATION_MODE 'none'`. This bypasses LakeKeeper's remote signing, which otherwise returns S3 URLs containing Docker-internal hostnames (e.g., `garage:3900` instead of `localhost:3900`). The server's local S3 secret provides the correct host-accessible endpoint.

### SQL Safety

All user-supplied queries are validated as read-only:
- Only `SELECT` and `WITH` (CTE) queries are allowed
- Dangerous keywords (`DROP`, `DELETE`, `INSERT`, `ALTER`, etc.) are rejected
- Multi-statement queries (`;` in the middle) are blocked
- Table/column identifiers are validated against injection
