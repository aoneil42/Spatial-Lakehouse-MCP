"""Shared test fixtures.

Tests run against local DuckDB tables — no LakeKeeper or Garage needed.
The fixtures create sample spatial tables that mimic what the catalog
would provide.
"""

import pytest
import duckdb
import json


@pytest.fixture(scope="session")
def duckdb_conn():
    """Session-scoped DuckDB connection with spatial extension and test data."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL spatial; LOAD spatial;")

    # Create test tables in a 'test' schema to simulate namespace.table pattern
    conn.execute("CREATE SCHEMA test;")

    conn.execute("""
        CREATE TABLE test.buildings (
            id INTEGER,
            name VARCHAR,
            type VARCHAR,
            height_m DOUBLE,
            geometry GEOMETRY
        );
    """)

    conn.execute("""
        INSERT INTO test.buildings VALUES
        (1, 'City Hall', 'government', 45.0,
         ST_GeomFromText('POINT(-104.9903 39.7392)')),
        (2, 'Library', 'public', 12.0,
         ST_GeomFromText('POINT(-104.9847 39.7502)')),
        (3, 'Stadium', 'sports', 30.0,
         ST_GeomFromText('POINT(-104.9716 39.7437)')),
        (4, 'Hospital', 'medical', 25.0,
         ST_GeomFromText('POINT(-104.9825 39.7312)')),
        (5, 'School', 'education', 15.0,
         ST_GeomFromText('POINT(-104.9950 39.7455)'));
    """)

    conn.execute("""
        CREATE TABLE test.zones (
            id INTEGER,
            zone_name VARCHAR,
            zone_type VARCHAR,
            geometry GEOMETRY
        );
    """)

    conn.execute("""
        INSERT INTO test.zones VALUES
        (1, 'Downtown', 'commercial',
         ST_GeomFromText('POLYGON((-105.00 39.73, -104.97 39.73, -104.97 39.76, -105.00 39.76, -105.00 39.73))')),
        (2, 'Suburbs', 'residential',
         ST_GeomFromText('POLYGON((-105.05 39.70, -105.00 39.70, -105.00 39.73, -105.05 39.73, -105.05 39.70))'));
    """)

    yield conn
    conn.close()


@pytest.fixture
def parse_result():
    """Helper to parse JSON tool results."""
    def _parse(result_str: str) -> dict:
        return json.loads(result_str)
    return _parse
