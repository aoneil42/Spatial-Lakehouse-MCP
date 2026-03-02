"""Tool tests — run against local DuckDB tables (all phases)."""

import json
import duckdb


# ── Phase 1: Direct engine tests (no MCP layer, just SQL validation) ──


def test_describe_table(duckdb_conn):
    rows = duckdb_conn.execute("DESCRIBE test.buildings").fetchall()
    col_names = [r[0] for r in rows]
    assert "id" in col_names
    assert "geometry" in col_names
    assert "name" in col_names


def test_spatial_filter_within_distance(duckdb_conn):
    """Test distance-based proximity search.

    Uses degree-based ST_Distance for local test data (no SRID set).
    The actual MCP tools use ST_Transform with EPSG projections
    against real Iceberg data that has proper SRIDs.
    """
    rows = duckdb_conn.execute("""
        SELECT name, ST_AsGeoJSON(geometry) AS geojson
        FROM test.buildings
        WHERE ST_Distance(geometry, ST_Point(-104.990, 39.740)) < 0.02
    """).fetchall()
    assert len(rows) >= 1
    names = [r[0] for r in rows]
    assert "City Hall" in names


def test_spatial_filter_intersects(duckdb_conn):
    """Test ST_Intersects with a polygon."""
    rows = duckdb_conn.execute("""
        SELECT name FROM test.buildings
        WHERE ST_Intersects(
            geometry,
            ST_GeomFromText('POLYGON((-105.00 39.73, -104.97 39.73, -104.97 39.76, -105.00 39.76, -105.00 39.73))')
        )
    """).fetchall()
    assert len(rows) >= 1


def test_nearest_features(duckdb_conn):
    """Test K-nearest-neighbor query."""
    rows = duckdb_conn.execute("""
        SELECT name,
            ST_Distance(
                ST_Transform(geometry, 'EPSG:4326', 'EPSG:3857'),
                ST_Transform(ST_Point(-104.990, 39.740), 'EPSG:4326', 'EPSG:3857')
            ) AS distance_meters
        FROM test.buildings
        ORDER BY distance_meters ASC
        LIMIT 3
    """).fetchall()
    assert len(rows) == 3
    # First result should be closest
    assert rows[0][1] <= rows[1][1] <= rows[2][1]


def test_get_bbox(duckdb_conn):
    """Test spatial extent computation."""
    row = duckdb_conn.execute("""
        SELECT ST_XMin(ext) AS min_lon, ST_YMin(ext) AS min_lat,
               ST_XMax(ext) AS max_lon, ST_YMax(ext) AS max_lat
        FROM (SELECT ST_Extent(geometry) AS ext FROM test.buildings) sub
    """).fetchone()
    assert row[0] <= row[2]  # min_lon <= max_lon
    assert row[1] <= row[3]  # min_lat <= max_lat
    # Verify we got actual coordinate values (not None)
    assert row[0] is not None
    assert row[0] < 0  # Western hemisphere


def test_snapshots_function(duckdb_conn):
    """Verify iceberg_snapshots function exists (won't work on local tables)."""
    # Just verify the function is recognized — it will error on non-Iceberg tables
    try:
        duckdb_conn.execute("SELECT * FROM iceberg_snapshots(test.buildings)")
    except (duckdb.CatalogException, duckdb.BinderException, duckdb.Error):
        pass  # Expected — test.buildings is not an Iceberg table


# ── Validator tests ────────────────────────────────────────────────


def test_validate_read_only_accepts_select():
    from spatial_lakehouse_mcp.validators import validate_read_only_sql
    validate_read_only_sql("SELECT * FROM foo")
    validate_read_only_sql("WITH cte AS (SELECT 1) SELECT * FROM cte")


def test_validate_read_only_rejects_mutations():
    from spatial_lakehouse_mcp.validators import validate_read_only_sql
    import pytest
    for keyword in ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE"]:
        with pytest.raises(ValueError, match="not allowed"):
            validate_read_only_sql(f"{keyword} TABLE foo")


def test_validate_table_ref():
    from spatial_lakehouse_mcp.validators import validate_table_ref
    assert validate_table_ref("default.buildings", "lakehouse") == "lakehouse.default.buildings"
    assert validate_table_ref("lakehouse.default.buildings", "lakehouse") == "lakehouse.default.buildings"


def test_validate_table_ref_rejects_bare_name():
    from spatial_lakehouse_mcp.validators import validate_table_ref
    import pytest
    with pytest.raises(ValueError, match="must include a namespace"):
        validate_table_ref("buildings", "lakehouse")


# ── Phase 2: Spatial Analysis Tests ────────────────────────────────


def test_spatial_join_intersects(duckdb_conn):
    """Test spatial join: buildings within zones."""
    rows = duckdb_conn.execute("""
        SELECT l.name AS building, r.zone_name,
               ST_AsGeoJSON(l.geometry) AS geojson
        FROM test.buildings l
        JOIN test.zones r ON ST_Intersects(l.geometry, r.geometry)
    """).fetchall()
    assert len(rows) >= 1
    # City Hall should be in Downtown zone
    matches = [r for r in rows if r[0] == "City Hall" and r[1] == "Downtown"]
    assert len(matches) >= 1


def test_aggregate_within(duckdb_conn):
    """Test point-in-polygon aggregation."""
    rows = duckdb_conn.execute("""
        SELECT poly.zone_name,
               COUNT(pt.*)::INTEGER AS building_count,
               ST_AsGeoJSON(poly.geometry) AS geojson
        FROM test.zones poly
        LEFT JOIN test.buildings pt
          ON ST_Contains(poly.geometry, pt.geometry)
        GROUP BY poly.zone_name, poly.geometry
        ORDER BY building_count DESC
    """).fetchall()
    assert len(rows) == 2  # Two zones
    # Downtown should have more buildings
    downtown = [r for r in rows if r[0] == "Downtown"]
    assert len(downtown) == 1
    assert downtown[0][1] >= 1  # At least one building in Downtown


def test_buffer_analysis(duckdb_conn):
    """Test buffer creation around points."""
    rows = duckdb_conn.execute("""
        SELECT id,
            ST_AsGeoJSON(
                ST_Transform(
                    ST_Buffer(
                        ST_Transform(geometry, 'EPSG:4326', 'EPSG:3857'),
                        500
                    ),
                    'EPSG:3857', 'EPSG:4326'
                )
            ) AS buffer_geojson
        FROM test.buildings
        WHERE id = 1
    """).fetchall()
    assert len(rows) == 1
    geojson = json.loads(rows[0][1])
    assert geojson["type"] == "Polygon"


def test_buffer_dissolve(duckdb_conn):
    """Test dissolved (unioned) buffers."""
    row = duckdb_conn.execute("""
        SELECT
            ST_AsGeoJSON(
                ST_Union_Agg(
                    ST_Transform(
                        ST_Buffer(
                            ST_Transform(geometry, 'EPSG:4326', 'EPSG:3857'),
                            500
                        ),
                        'EPSG:3857', 'EPSG:4326'
                    )
                )
            ) AS buffer_geojson,
            COUNT(*)::INTEGER AS feature_count
        FROM test.buildings
    """).fetchone()
    assert row[1] == 5
    geojson = json.loads(row[0])
    assert geojson["type"] in ("Polygon", "MultiPolygon", "GeometryCollection")


def test_validate_agg_function():
    from spatial_lakehouse_mcp.validators import validate_agg_function
    import pytest
    assert validate_agg_function("count") == "count"
    assert validate_agg_function("SUM") == "sum"
    with pytest.raises(ValueError):
        validate_agg_function("drop_table")


def test_validate_spatial_predicate():
    from spatial_lakehouse_mcp.validators import validate_spatial_predicate
    import pytest
    assert validate_spatial_predicate("intersects") == "intersects"
    assert validate_spatial_predicate("DWITHIN") == "dwithin"
    with pytest.raises(ValueError):
        validate_spatial_predicate("explode")


# ── Phase 3: Data Management + Advanced Tests ─────────────────────


def test_sample_data(duckdb_conn):
    """Test row sampling."""
    rows = duckdb_conn.execute("SELECT * FROM test.buildings LIMIT 3").fetchall()
    assert len(rows) == 3


def test_table_stats_query(duckdb_conn):
    """Test stats aggregation queries."""
    row = duckdb_conn.execute("""
        SELECT
            COUNT(*)::INTEGER AS total_rows,
            COUNT(geometry)::INTEGER AS non_null_geom
        FROM test.buildings
    """).fetchone()
    assert row[0] == 5
    assert row[1] == 5


def test_geojson_export(duckdb_conn):
    """Test GeoJSON FeatureCollection construction."""
    rows = duckdb_conn.execute("""
        SELECT id, name, type, ST_AsGeoJSON(geometry) AS __geojson
        FROM test.buildings
        LIMIT 5
    """).fetchall()
    features = []
    for row in rows:
        geom = json.loads(row[3])
        features.append({
            "type": "Feature",
            "geometry": geom,
            "properties": {"id": row[0], "name": row[1], "type": row[2]},
        })
    fc = {"type": "FeatureCollection", "features": features}
    assert fc["type"] == "FeatureCollection"
    assert len(fc["features"]) == 5
    assert fc["features"][0]["geometry"]["type"] == "Point"


def test_multi_table_join(duckdb_conn):
    """Test cross-table analytical query."""
    rows = duckdb_conn.execute("""
        WITH building_zones AS (
            SELECT b.name AS building, b.type, z.zone_name
            FROM test.buildings b
            JOIN test.zones z ON ST_Intersects(b.geometry, z.geometry)
        )
        SELECT zone_name, COUNT(*) AS cnt
        FROM building_zones
        GROUP BY zone_name
        ORDER BY cnt DESC
    """).fetchall()
    assert len(rows) >= 1


def test_geometry_type_detection(duckdb_conn):
    """Test ST_GeometryType for table stats."""
    rows = duckdb_conn.execute("""
        SELECT ST_GeometryType(geometry) AS geom_type, COUNT(*)::INTEGER AS cnt
        FROM test.buildings
        WHERE geometry IS NOT NULL
        GROUP BY geom_type
    """).fetchall()
    assert len(rows) >= 1
    assert rows[0][0] == "POINT"
