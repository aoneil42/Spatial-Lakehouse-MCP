"""MCP tool function tests — call actual tool functions against local DuckDB.

Uses set_connection() to inject a test DuckDB connection and patches
the CATALOG constant so table references resolve against the test schema.
"""

import json
import pytest

import spatial_lakehouse_mcp.server as server_module
from spatial_lakehouse_mcp.engine import set_connection, reset_connection


@pytest.fixture(autouse=True)
def inject_test_connection(duckdb_conn):
    """Inject the test DuckDB connection and patch CATALOG for all tests."""
    original_catalog = server_module.CATALOG
    server_module.CATALOG = "memory"
    set_connection(duckdb_conn)
    yield
    server_module.CATALOG = original_catalog
    # Don't reset — the session-scoped duckdb_conn is shared across tests


# ── Catalog Tools ────────────────────────────────────────────────


def test_describe_table_tool():
    result = server_module.describe_table("test.buildings")
    data = json.loads(result)
    assert data["row_count"] >= 1
    col_names = [r["column_name"] for r in data["rows"]]
    assert "id" in col_names
    assert "geometry" in col_names
    # Check geometry detection
    geom_rows = [r for r in data["rows"] if r.get("is_geometry")]
    assert len(geom_rows) >= 1


def test_search_tables_tool():
    result = server_module.search_tables(pattern="buildings")
    data = json.loads(result)
    assert data["row_count"] >= 1
    assert data["rows"][0]["has_geometry"] is True


# ── Query Tools ──────────────────────────────────────────────────


def test_sample_data_tool():
    result = server_module.sample_data("test.buildings", n=3)
    data = json.loads(result)
    assert data["row_count"] == 3


def test_sample_data_excludes_geometry():
    result = server_module.sample_data("test.buildings", n=3, include_geometry=False)
    data = json.loads(result)
    assert data["row_count"] == 3
    for row in data["rows"]:
        assert "geometry" not in row


def test_get_bbox_tool():
    result = server_module.get_bbox("test.buildings")
    data = json.loads(result)
    assert data["min_lon"] <= data["max_lon"]
    assert data["min_lat"] <= data["max_lat"]
    assert data["min_lon"] < 0  # Western hemisphere


def test_table_stats_tool():
    result = server_module.table_stats("test.buildings")
    data = json.loads(result)
    assert data["row_count"] == 5
    assert data["column_count"] >= 4


def test_export_geojson_tool():
    result = server_module.export_geojson("test.buildings", limit=3)
    data = json.loads(result)
    assert data["type"] == "FeatureCollection"
    assert len(data["features"]) == 3
    assert data["features"][0]["geometry"]["type"] == "Point"


def test_health_check_tool():
    result = server_module.health_check()
    data = json.loads(result)
    assert data["duckdb"] is True


# ── WHERE Clause Validation ──────────────────────────────────────


def test_where_clause_validation():
    from spatial_lakehouse_mcp.validators import validate_where_clause

    # Valid clauses
    assert validate_where_clause("type = 'residential'") == "type = 'residential'"
    assert validate_where_clause("") == ""
    assert validate_where_clause("  ") == ""

    # Rejected clauses
    with pytest.raises(ValueError, match="semicolons"):
        validate_where_clause("1=1; DROP TABLE foo")

    with pytest.raises(ValueError, match="disallowed keyword"):
        validate_where_clause("1=1 UNION SELECT * FROM secrets")

    with pytest.raises(ValueError, match="disallowed keyword"):
        validate_where_clause("1=1 COPY TO '/tmp/dump'")


def test_spatial_filter_rejects_injection():
    result = server_module.spatial_filter(
        table="test.buildings",
        operation="within_distance",
        lon=-104.990, lat=39.740,
        distance_meters=2000,
        where="1=1 UNION SELECT * FROM secrets",
    )
    data = json.loads(result)
    assert data["error"] is True
    assert "disallowed keyword" in data["message"]
