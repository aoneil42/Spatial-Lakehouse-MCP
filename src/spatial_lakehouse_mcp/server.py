"""Spatial Lakehouse MCP Server — Iceberg-native geospatial tools.

Single-engine architecture: DuckDB handles both catalog operations
and spatial query execution via the iceberg + spatial extensions.

Tools (18 total):
  Catalog:   list_namespaces, list_tables, describe_table, table_snapshots, search_tables
  Query:     query, spatial_filter, nearest_features, get_bbox, time_travel_query, multi_table_query
  Analysis:  spatial_join, aggregate_within, buffer_analysis
  Data:      sample_data, table_stats, export_geojson
  System:    health_check
"""

import json
import logging
from contextlib import asynccontextmanager
from typing import Optional

from mcp.server.fastmcp import FastMCP

from .config import settings
from .engine import get_connection, execute_query, check_health, _lock
from .validators import (
    validate_read_only_sql,
    validate_where_clause,
    validate_identifier,
    validate_namespace,
    validate_table_ref,
    validate_agg_function,
    validate_spatial_predicate,
    validate_positive_number,
    format_result,
    format_error,
)

logger = logging.getLogger(__name__)


# ── Lifespan ───────────────────────────────────────────────────────

@asynccontextmanager
async def app_lifespan(server):
    """Initialize DuckDB + catalog on startup."""
    try:
        get_connection()
        logger.info("Spatial Lakehouse MCP server ready.")
    except Exception as e:
        logger.warning(f"Startup catalog attach failed (will retry on first tool call): {e}")
    yield


# ── Server ─────────────────────────────────────────────────────────

mcp = FastMCP(
    settings.server_name,
    host=settings.server_host,
    port=settings.server_port,
    lifespan=app_lifespan,
)

CATALOG = settings.catalog_alias


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CATALOG TOOLS (Phase 1)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@mcp.tool()
def list_namespaces() -> str:
    """List all namespaces (schemas) in the Iceberg catalog.

    Returns a JSON array of namespace names. Namespaces organize
    tables into logical groups (like database schemas).
    """
    try:
        rows = execute_query(
            f"SELECT schema_name FROM information_schema.schemata "
            f"WHERE catalog_name = '{CATALOG}' "
            f"ORDER BY schema_name"
        )
        namespaces = [row["schema_name"] for row in rows]
        return json.dumps({"namespaces": namespaces}, indent=2)
    except Exception as e:
        return format_error(e, "list_namespaces")


@mcp.tool()
def list_tables(namespace: str = "") -> str:
    """List tables in the Iceberg catalog, optionally filtered by namespace.

    Args:
        namespace: Schema/namespace to list tables from.
                   If empty, lists tables across all namespaces.

    Returns a JSON array of objects with database, schema, table name,
    and column count.
    """
    try:
        if namespace:
            namespace = validate_namespace(namespace)
            rows = execute_query(
                f"SELECT database_name, schema_name, table_name, "
                f"       column_count, estimated_size "
                f"FROM duckdb_tables() "
                f"WHERE database_name = '{CATALOG}' "
                f"  AND schema_name = '{namespace}' "
                f"ORDER BY table_name"
            )
        else:
            rows = execute_query(
                f"SELECT database_name, schema_name, table_name, "
                f"       column_count, estimated_size "
                f"FROM duckdb_tables() "
                f"WHERE database_name = '{CATALOG}' "
                f"ORDER BY schema_name, table_name"
            )
        return format_result(rows)
    except Exception as e:
        return format_error(e, "list_tables")


@mcp.tool()
def describe_table(table: str) -> str:
    """Describe the schema of an Iceberg table.

    Args:
        table: Table reference as "namespace.table_name"
               (e.g., "default.buildings").

    Returns column names, types, and whether geometry columns
    are present (detected by type name containing GEOMETRY, BLOB,
    or columns named 'geometry', 'geom', 'wkb_geometry', 'shape').
    """
    try:
        qualified = validate_table_ref(table, CATALOG)
        rows = execute_query(f"DESCRIBE {qualified}")

        # Detect geometry columns
        geom_indicators = {"geometry", "geom", "wkb_geometry", "shape", "the_geom"}
        for row in rows:
            col_name = row.get("column_name", "").lower()
            col_type = str(row.get("column_type", "")).upper()
            row["is_geometry"] = (
                col_name in geom_indicators
                or "GEOMETRY" in col_type
                or "BLOB" in col_type  # WKB is stored as BLOB in Iceberg/Parquet
            )

        return format_result(rows)
    except Exception as e:
        return format_error(e, "describe_table")


@mcp.tool()
def table_snapshots(table: str) -> str:
    """List available snapshots for an Iceberg table (time travel).

    Args:
        table: Table reference as "namespace.table_name".

    Returns snapshot IDs, timestamps, and sequence numbers.
    Use these snapshot IDs with the `query` tool's snapshot_id
    parameter for time-travel queries.
    """
    try:
        qualified = validate_table_ref(table, CATALOG)
        rows = execute_query(f"SELECT * FROM iceberg_snapshots({qualified})")
        return format_result(rows)
    except Exception as e:
        return format_error(e, "table_snapshots")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# QUERY TOOLS (Phase 1)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@mcp.tool()
def query(
    sql: str,
    limit: int = 50,
    snapshot_id: Optional[int] = None,
) -> str:
    """Execute a read-only SQL query against the Iceberg catalog.

    The query can use DuckDB spatial functions (ST_Intersects,
    ST_Buffer, ST_Distance, etc.) and reference tables as
    {catalog}.{namespace}.{table}.

    Args:
        sql: A read-only SELECT query. May include spatial functions.
             Tables should be referenced as "lakehouse.namespace.table"
             or just "namespace.table" (catalog prefix is added).
        limit: Maximum rows to return (1-5000, default 50).
        snapshot_id: Optional Iceberg snapshot ID for time-travel queries.
                     If provided, wraps the table references with
                     AT (VERSION => snapshot_id).

    Returns query results as JSON with row_count, truncated flag, and rows.
    Geometry values are auto-converted to GeoJSON via ST_AsGeoJSON.
    """
    try:
        validate_read_only_sql(sql)
        limit = max(1, min(limit, settings.max_result_rows))

        # Apply limit if not already present
        sql_upper = sql.upper().strip().rstrip(";")
        if "LIMIT" not in sql_upper:
            sql = f"{sql.rstrip(';')} LIMIT {limit}"

        rows = execute_query(sql)
        truncated = len(rows) >= limit
        return format_result(rows, truncated=truncated)
    except Exception as e:
        return format_error(e, "query")


@mcp.tool()
def spatial_filter(
    table: str,
    operation: str,
    geometry_wkt: str = "",
    lon: float = 0.0,
    lat: float = 0.0,
    distance_meters: float = 1000.0,
    geometry_column: str = "geometry",
    columns: str = "*",
    where: str = "",
    limit: int = 50,
) -> str:
    """Filter table rows using spatial predicates.

    This is a structured alternative to writing raw spatial SQL.
    The LLM fills in parameters and the tool builds safe SQL.

    Args:
        table: Table reference as "namespace.table_name".
        operation: One of: "intersects", "contains", "within",
                   "within_distance", "bbox".
        geometry_wkt: WKT geometry for intersects/contains/within ops.
                      Example: "POLYGON((-105.5 39.5, -105.0 39.5, ...))"
        lon: Longitude for within_distance operation.
        lat: Latitude for within_distance operation.
        distance_meters: Radius in meters for within_distance (default 1000).
        geometry_column: Name of the geometry column (default "geometry").
        columns: Comma-separated columns to return, or "*" for all.
        where: Additional non-spatial WHERE clause (e.g., "type = 'residential'").
        limit: Maximum rows to return (default 50).

    Returns matching rows with geometry converted to GeoJSON.
    """
    try:
        qualified = validate_table_ref(table, CATALOG)
        geom_col = validate_identifier(geometry_column, "geometry_column")
        limit = max(1, min(limit, settings.max_result_rows))

        # Build spatial predicate
        op = operation.lower().strip()
        if op == "within_distance":
            predicate = (
                f"ST_DWithin(ST_Transform({geom_col}, 'EPSG:4326', 'EPSG:3857'), "
                f"ST_Transform(ST_Point({lon}, {lat}), 'EPSG:4326', 'EPSG:3857'), "
                f"{float(distance_meters)})"
            )
        elif op == "intersects":
            predicate = f"ST_Intersects({geom_col}, ST_GeomFromText('{geometry_wkt}'))"
        elif op == "contains":
            predicate = f"ST_Contains({geom_col}, ST_GeomFromText('{geometry_wkt}'))"
        elif op == "within":
            predicate = f"ST_Within({geom_col}, ST_GeomFromText('{geometry_wkt}'))"
        elif op == "bbox":
            # geometry_wkt should be a POLYGON representing the bounding box
            predicate = (
                f"ST_Intersects(ST_Envelope({geom_col}), ST_GeomFromText('{geometry_wkt}'))"
            )
        else:
            raise ValueError(
                f"Unknown operation '{operation}'. "
                "Use: intersects, contains, within, within_distance, bbox"
            )

        # Build full query
        where_clause = predicate
        if where:
            where = validate_where_clause(where)
            where_clause = f"{predicate} AND ({where})"

        sql = (
            f"SELECT {columns}, ST_AsGeoJSON({geom_col}) AS geojson "
            f"FROM {qualified} "
            f"WHERE {where_clause} "
            f"LIMIT {limit}"
        )

        rows = execute_query(sql)
        truncated = len(rows) >= limit
        return format_result(rows, truncated=truncated)
    except Exception as e:
        return format_error(e, "spatial_filter")


@mcp.tool()
def nearest_features(
    table: str,
    lon: float,
    lat: float,
    k: int = 5,
    geometry_column: str = "geometry",
    columns: str = "*",
    where: str = "",
    max_distance_meters: float = 0.0,
) -> str:
    """Find the K nearest features to a given point.

    Args:
        table: Table reference as "namespace.table_name".
        lon: Longitude of the query point.
        lat: Latitude of the query point.
        k: Number of nearest features to return (default 5, max 100).
        geometry_column: Name of the geometry column (default "geometry").
        columns: Comma-separated columns to return, or "*" for all.
        where: Optional WHERE clause to pre-filter candidates.
        max_distance_meters: If > 0, only return features within
                             this distance (meters).

    Returns K nearest rows ordered by distance, with distance_meters
    and GeoJSON geometry included.
    """
    try:
        qualified = validate_table_ref(table, CATALOG)
        geom_col = validate_identifier(geometry_column, "geometry_column")
        k = max(1, min(k, 100))

        distance_expr = (
            f"ST_Distance(ST_Transform({geom_col}, 'EPSG:4326', 'EPSG:3857'), "
            f"ST_Transform(ST_Point({float(lon)}, {float(lat)}), 'EPSG:4326', 'EPSG:3857'))"
        )

        where_clause = ""
        conditions = []
        if where:
            where = validate_where_clause(where)
            conditions.append(f"({where})")
        if max_distance_meters > 0:
            conditions.append(f"{distance_expr} <= {float(max_distance_meters)}")
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        sql = (
            f"SELECT {columns}, "
            f"  {distance_expr} AS distance_meters, "
            f"  ST_AsGeoJSON({geom_col}) AS geojson "
            f"FROM {qualified} "
            f"{where_clause} "
            f"ORDER BY distance_meters ASC "
            f"LIMIT {k}"
        )

        rows = execute_query(sql)
        return format_result(rows)
    except Exception as e:
        return format_error(e, "nearest_features")


@mcp.tool()
def get_bbox(
    table: str,
    geometry_column: str = "geometry",
    where: str = "",
) -> str:
    """Get the bounding box (spatial extent) of a table's geometries.

    Args:
        table: Table reference as "namespace.table_name".
        geometry_column: Name of the geometry column (default "geometry").
        where: Optional WHERE clause to filter rows before computing extent.

    Returns the bounding box as min/max lon/lat coordinates.
    """
    try:
        qualified = validate_table_ref(table, CATALOG)
        geom_col = validate_identifier(geometry_column, "geometry_column")

        if where:
            where = validate_where_clause(where)
            where_clause = f"WHERE {where}"
        else:
            where_clause = ""

        sql = (
            f"SELECT "
            f"  ST_XMin(ext) AS min_lon, ST_YMin(ext) AS min_lat, "
            f"  ST_XMax(ext) AS max_lon, ST_YMax(ext) AS max_lat "
            f"FROM (SELECT ST_Extent({geom_col}) AS ext "
            f"      FROM {qualified} {where_clause}) sub"
        )

        rows = execute_query(sql)
        if rows:
            return json.dumps(rows[0], indent=2)
        return json.dumps({"error": "No geometries found"})
    except Exception as e:
        return format_error(e, "get_bbox")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SPATIAL ANALYSIS TOOLS (Phase 2)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@mcp.tool()
def spatial_join(
    left_table: str,
    right_table: str,
    predicate: str = "intersects",
    left_geom: str = "geometry",
    right_geom: str = "geometry",
    left_columns: str = "*",
    right_columns: str = "",
    distance_meters: float = 0.0,
    where: str = "",
    limit: int = 50,
) -> str:
    """Join two tables using a spatial predicate.

    Performs a spatial join where rows from left_table are matched
    with rows from right_table based on the spatial relationship
    between their geometry columns.

    Args:
        left_table: Left table as "namespace.table_name".
        right_table: Right table as "namespace.table_name".
        predicate: Spatial predicate — one of: "intersects", "contains",
                   "within", "dwithin", "crosses", "touches", "overlaps".
        left_geom: Geometry column name in left table (default "geometry").
        right_geom: Geometry column name in right table (default "geometry").
        left_columns: Columns from left table to include (default "*").
        right_columns: Columns from right table to include.
                       Prefix with right table alias "r." is added automatically.
                       Example: "zone_name, zone_type"
        distance_meters: Distance threshold for "dwithin" predicate.
        where: Additional WHERE clause applied after the join.
        limit: Maximum rows to return (default 50).

    Returns joined rows with geometry from left table as GeoJSON.
    """
    try:
        left_qual = validate_table_ref(left_table, CATALOG)
        right_qual = validate_table_ref(right_table, CATALOG)
        pred = validate_spatial_predicate(predicate)
        l_geom = validate_identifier(left_geom, "left_geom")
        r_geom = validate_identifier(right_geom, "right_geom")
        limit = max(1, min(limit, settings.max_result_rows))

        # Build spatial ON clause
        if pred == "dwithin":
            distance_meters = validate_positive_number(distance_meters, "distance_meters")
            spatial_on = (
                f"ST_DWithin("
                f"ST_Transform(l.{l_geom}, 'EPSG:4326', 'EPSG:3857'), "
                f"ST_Transform(r.{r_geom}, 'EPSG:4326', 'EPSG:3857'), "
                f"{distance_meters})"
            )
        else:
            func_map = {
                "intersects": "ST_Intersects",
                "contains": "ST_Contains",
                "within": "ST_Within",
                "crosses": "ST_Crosses",
                "touches": "ST_Touches",
                "overlaps": "ST_Overlaps",
            }
            func = func_map[pred]
            spatial_on = f"{func}(l.{l_geom}, r.{r_geom})"

        # Build SELECT columns
        select_parts = []
        if left_columns.strip() == "*":
            select_parts.append("l.*")
        else:
            for col in left_columns.split(","):
                col = col.strip()
                if col:
                    select_parts.append(f"l.{validate_identifier(col, 'left column')}")

        if right_columns.strip():
            for col in right_columns.split(","):
                col = col.strip()
                if col:
                    select_parts.append(
                        f"r.{validate_identifier(col, 'right column')} "
                        f"AS right_{validate_identifier(col, 'right column')}"
                    )

        select_parts.append(f"ST_AsGeoJSON(l.{l_geom}) AS geojson")
        select_clause = ", ".join(select_parts)

        if where:
            where = validate_where_clause(where)
            where_clause = f"AND ({where})"
        else:
            where_clause = ""

        sql = (
            f"SELECT {select_clause} "
            f"FROM {left_qual} l "
            f"JOIN {right_qual} r ON {spatial_on} "
            f"WHERE true {where_clause} "
            f"LIMIT {limit}"
        )

        rows = execute_query(sql)
        truncated = len(rows) >= limit
        return format_result(rows, truncated=truncated)
    except Exception as e:
        return format_error(e, "spatial_join")


@mcp.tool()
def aggregate_within(
    points_table: str,
    polygons_table: str,
    agg_column: str = "",
    agg_function: str = "count",
    points_geom: str = "geometry",
    polygons_geom: str = "geometry",
    polygon_id_column: str = "id",
    polygon_label_column: str = "",
    where_points: str = "",
    where_polygons: str = "",
    limit: int = 50,
) -> str:
    """Aggregate point data within polygon boundaries.

    Classic point-in-polygon aggregation: for each polygon, compute
    an aggregate (count, sum, avg, etc.) of points that fall within it.

    Args:
        points_table: Point table as "namespace.table_name".
        polygons_table: Polygon table as "namespace.table_name".
        agg_column: Column from points table to aggregate.
                    Required for sum/avg/min/max. Ignored for count.
        agg_function: Aggregation function: "count", "sum", "avg",
                      "min", "max", "stddev" (default "count").
        points_geom: Geometry column in points table (default "geometry").
        polygons_geom: Geometry column in polygons table (default "geometry").
        polygon_id_column: ID column in polygons table (default "id").
        polygon_label_column: Optional label column from polygons table
                              to include in results (e.g., "zone_name").
        where_points: Optional WHERE clause to filter points before aggregation.
        where_polygons: Optional WHERE clause to filter polygons.
        limit: Maximum polygons to return (default 50).

    Returns one row per polygon with the aggregate value and polygon GeoJSON.
    """
    try:
        pts_qual = validate_table_ref(points_table, CATALOG)
        poly_qual = validate_table_ref(polygons_table, CATALOG)
        agg_func = validate_agg_function(agg_function)
        p_geom = validate_identifier(points_geom, "points_geom")
        pg_geom = validate_identifier(polygons_geom, "polygons_geom")
        poly_id = validate_identifier(polygon_id_column, "polygon_id_column")
        limit = max(1, min(limit, settings.max_result_rows))

        # Build aggregation expression
        if agg_func == "count":
            agg_expr = "COUNT(pt.*)::INTEGER AS agg_value"
        else:
            if not agg_column:
                raise ValueError(
                    f"agg_column is required for {agg_func} aggregation."
                )
            agg_col = validate_identifier(agg_column, "agg_column")
            agg_expr = f"{agg_func.upper()}(pt.{agg_col}) AS agg_value"

        # Build SELECT list for polygon columns
        select_parts = [f"poly.{poly_id}"]
        group_parts = [f"poly.{poly_id}"]
        if polygon_label_column:
            label_col = validate_identifier(polygon_label_column, "polygon_label_column")
            select_parts.append(f"poly.{label_col}")
            group_parts.append(f"poly.{label_col}")

        select_parts.append(agg_expr)
        select_parts.append(f"ST_AsGeoJSON(poly.{pg_geom}) AS geojson")
        group_parts.append(f"poly.{pg_geom}")

        # Build WHERE clauses
        if where_points:
            where_points = validate_where_clause(where_points)
            point_filter = f"AND ({where_points})"
        else:
            point_filter = ""
        if where_polygons:
            where_polygons = validate_where_clause(where_polygons)
            polygon_filter = f"WHERE {where_polygons}"
        else:
            polygon_filter = ""

        sql = (
            f"SELECT {', '.join(select_parts)} "
            f"FROM {poly_qual} poly {polygon_filter} "
            f"LEFT JOIN {pts_qual} pt "
            f"  ON ST_Contains(poly.{pg_geom}, pt.{p_geom}) "
            f"  {point_filter} "
            f"GROUP BY {', '.join(group_parts)} "
            f"ORDER BY agg_value DESC "
            f"LIMIT {limit}"
        )

        rows = execute_query(sql)
        truncated = len(rows) >= limit
        return format_result(rows, truncated=truncated)
    except Exception as e:
        return format_error(e, "aggregate_within")


@mcp.tool()
def buffer_analysis(
    table: str,
    distance_meters: float,
    geometry_column: str = "geometry",
    columns: str = "id",
    where: str = "",
    dissolve: bool = False,
    limit: int = 50,
) -> str:
    """Create buffer zones around features and return as GeoJSON.

    Buffers each geometry by the specified distance. Optionally
    dissolves (unions) all buffers into a single geometry.

    Args:
        table: Table reference as "namespace.table_name".
        distance_meters: Buffer distance in meters.
        geometry_column: Name of the geometry column (default "geometry").
        columns: Columns to include in results (default "id").
        where: Optional WHERE clause to filter features before buffering.
        dissolve: If true, union all buffers into a single geometry.
        limit: Maximum rows to return (default 50, ignored if dissolve=true).

    Returns buffered geometries as GeoJSON. When dissolve=true,
    returns a single row with the dissolved geometry.
    """
    try:
        qualified = validate_table_ref(table, CATALOG)
        geom_col = validate_identifier(geometry_column, "geometry_column")
        dist = validate_positive_number(distance_meters, "distance_meters")
        limit = max(1, min(limit, settings.max_result_rows))

        if where:
            where = validate_where_clause(where)
            where_clause = f"WHERE {where}"
        else:
            where_clause = ""

        buffer_expr = (
            f"ST_Transform("
            f"  ST_Buffer("
            f"    ST_Transform({geom_col}, 'EPSG:4326', 'EPSG:3857'), "
            f"    {dist}"
            f"  ), 'EPSG:3857', 'EPSG:4326'"
            f")"
        )

        if dissolve:
            sql = (
                f"SELECT ST_AsGeoJSON(ST_Union_Agg({buffer_expr})) AS buffer_geojson, "
                f"  COUNT(*)::INTEGER AS feature_count "
                f"FROM {qualified} {where_clause}"
            )
        else:
            col_list = ", ".join(
                validate_identifier(c.strip(), "column")
                for c in columns.split(",") if c.strip()
            )
            sql = (
                f"SELECT {col_list}, "
                f"  ST_AsGeoJSON({buffer_expr}) AS buffer_geojson "
                f"FROM {qualified} {where_clause} "
                f"LIMIT {limit}"
            )

        rows = execute_query(sql)
        truncated = (not dissolve) and len(rows) >= limit
        return format_result(rows, truncated=truncated)
    except Exception as e:
        return format_error(e, "buffer_analysis")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DATA MANAGEMENT + ADVANCED TOOLS (Phase 3)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@mcp.tool()
def sample_data(
    table: str,
    n: int = 10,
    columns: str = "*",
    include_geometry: bool = True,
) -> str:
    """Get a preview sample of rows from a table.

    Useful for understanding table contents before writing queries.

    Args:
        table: Table reference as "namespace.table_name".
        n: Number of rows to sample (1-100, default 10).
        columns: Comma-separated columns to include, or "*" for all.
        include_geometry: If true, geometry columns are included as GeoJSON.
                          If false, geometry columns are excluded (faster).

    Returns sample rows as JSON.
    """
    try:
        qualified = validate_table_ref(table, CATALOG)
        n = max(1, min(n, 100))

        if not include_geometry and columns.strip() == "*":
            # Get column list excluding geometry columns
            desc_rows = execute_query(f"DESCRIBE {qualified}")
            non_geom_cols = []
            for row in desc_rows:
                col_name = row.get("column_name", "")
                col_type = str(row.get("column_type", "")).upper()
                if col_name.lower() not in ("geometry", "geom", "wkb_geometry", "shape", "the_geom") \
                   and "GEOMETRY" not in col_type and "BLOB" not in col_type:
                    non_geom_cols.append(col_name)
            col_clause = ", ".join(non_geom_cols) if non_geom_cols else "*"
        else:
            col_clause = columns

        sql = f"SELECT {col_clause} FROM {qualified} LIMIT {n}"
        rows = execute_query(sql)
        return format_result(rows)
    except Exception as e:
        return format_error(e, "sample_data")


@mcp.tool()
def table_stats(
    table: str,
    geometry_column: str = "geometry",
) -> str:
    """Get summary statistics for a table.

    Returns row count, column count, and spatial summary
    (extent, geometry types present, null count).

    Args:
        table: Table reference as "namespace.table_name".
        geometry_column: Name of the geometry column (default "geometry").

    Returns a JSON object with table statistics.
    """
    try:
        qualified = validate_table_ref(table, CATALOG)
        geom_col = validate_identifier(geometry_column, "geometry_column")

        stats = {}

        # Row count
        row_count = execute_query(f"SELECT COUNT(*)::INTEGER AS cnt FROM {qualified}")
        stats["row_count"] = row_count[0]["cnt"] if row_count else 0

        # Column info
        desc = execute_query(f"DESCRIBE {qualified}")
        stats["column_count"] = len(desc)
        stats["columns"] = [
            {"name": r["column_name"], "type": r["column_type"]}
            for r in desc
        ]

        # Spatial stats (best-effort — skip if geometry column doesn't exist)
        try:
            spatial = execute_query(f"""
                SELECT
                    COUNT(*)::INTEGER AS total_rows,
                    COUNT({geom_col})::INTEGER AS non_null_geom,
                    (COUNT(*) - COUNT({geom_col}))::INTEGER AS null_geom,
                    ST_XMin(ST_Extent({geom_col})) AS min_lon,
                    ST_YMin(ST_Extent({geom_col})) AS min_lat,
                    ST_XMax(ST_Extent({geom_col})) AS max_lon,
                    ST_YMax(ST_Extent({geom_col})) AS max_lat
                FROM {qualified}
            """)
            if spatial:
                stats["spatial"] = spatial[0]

            # Geometry types
            geom_types = execute_query(f"""
                SELECT ST_GeometryType({geom_col}) AS geom_type,
                       COUNT(*)::INTEGER AS cnt
                FROM {qualified}
                WHERE {geom_col} IS NOT NULL
                GROUP BY geom_type
                ORDER BY cnt DESC
            """)
            stats["geometry_types"] = geom_types
        except Exception:
            stats["spatial"] = None
            stats["geometry_types"] = []

        return json.dumps(stats, default=str, indent=2)
    except Exception as e:
        return format_error(e, "table_stats")


@mcp.tool()
def time_travel_query(
    table: str,
    sql_select: str,
    snapshot_id: Optional[int] = None,
    timestamp: str = "",
    limit: int = 50,
) -> str:
    """Query a table at a specific point in time using Iceberg snapshots.

    Uses DuckDB's AT (VERSION => ...) or AT (TIMESTAMP => ...) syntax.
    Only one of snapshot_id or timestamp should be provided.

    Args:
        table: Table reference as "namespace.table_name".
        sql_select: The SELECT clause and any WHERE/ORDER BY to apply.
                    Do NOT include FROM — it is injected with the time travel clause.
                    Example: "SELECT name, type WHERE type = 'residential'"
        snapshot_id: Iceberg snapshot ID (from table_snapshots tool).
        timestamp: ISO timestamp string (e.g., "2025-06-15 10:00:00").
        limit: Maximum rows to return.

    Returns query results at the specified point in time.
    """
    try:
        qualified = validate_table_ref(table, CATALOG)
        limit = max(1, min(limit, settings.max_result_rows))

        if snapshot_id and timestamp:
            raise ValueError("Provide either snapshot_id or timestamp, not both.")
        if not snapshot_id and not timestamp:
            raise ValueError("Must provide either snapshot_id or timestamp.")

        if snapshot_id:
            at_clause = f"AT (VERSION => {int(snapshot_id)})"
        else:
            at_clause = f"AT (TIMESTAMP => TIMESTAMP '{timestamp}')"

        # Validate the SELECT clause
        sql_stripped = sql_select.strip()
        if not sql_stripped.upper().startswith("SELECT"):
            raise ValueError("sql_select must start with SELECT.")

        # Extract SELECT ... [WHERE ...] [ORDER BY ...] — inject FROM
        upper = sql_stripped.upper()
        insert_pos = len(sql_stripped)
        for keyword in ("WHERE", "ORDER BY", "GROUP BY", "HAVING"):
            idx = upper.find(keyword)
            if idx > 0:
                insert_pos = min(insert_pos, idx)
                break

        select_part = sql_stripped[:insert_pos].rstrip()
        rest_part = sql_stripped[insert_pos:]

        full_sql = f"{select_part} FROM {qualified} {at_clause} {rest_part} LIMIT {limit}"

        validate_read_only_sql(full_sql)
        rows = execute_query(full_sql)
        truncated = len(rows) >= limit
        return format_result(rows, truncated=truncated)
    except Exception as e:
        return format_error(e, "time_travel_query")


@mcp.tool()
def export_geojson(
    table: str,
    geometry_column: str = "geometry",
    columns: str = "",
    where: str = "",
    limit: int = 500,
) -> str:
    """Export table data as a GeoJSON FeatureCollection.

    Produces a complete GeoJSON FeatureCollection that can be
    rendered directly on a map or saved to a .geojson file.

    Args:
        table: Table reference as "namespace.table_name".
        geometry_column: Name of the geometry column (default "geometry").
        columns: Comma-separated property columns to include.
                 If empty, all non-geometry columns are included.
        where: Optional WHERE clause to filter features.
        limit: Maximum features to export (default 500, max 5000).

    Returns a GeoJSON FeatureCollection string.
    """
    try:
        qualified = validate_table_ref(table, CATALOG)
        geom_col = validate_identifier(geometry_column, "geometry_column")
        limit = max(1, min(limit, settings.max_result_rows))

        # Determine property columns
        if columns.strip():
            prop_cols = [
                validate_identifier(c.strip(), "column")
                for c in columns.split(",") if c.strip()
            ]
        else:
            # Auto-detect: all columns except geometry
            desc = execute_query(f"DESCRIBE {qualified}")
            geom_names = {"geometry", "geom", "wkb_geometry", "shape", "the_geom"}
            prop_cols = [
                r["column_name"] for r in desc
                if r["column_name"].lower() not in geom_names
                and "GEOMETRY" not in str(r.get("column_type", "")).upper()
                and "BLOB" not in str(r.get("column_type", "")).upper()
            ]

        if where:
            where = validate_where_clause(where)
            where_clause = f"WHERE {where}"
        else:
            where_clause = ""
        prop_select = ", ".join(prop_cols)

        sql = (
            f"SELECT {prop_select}, ST_AsGeoJSON({geom_col}) AS __geojson "
            f"FROM {qualified} {where_clause} "
            f"LIMIT {limit}"
        )

        rows = execute_query(sql)

        # Build FeatureCollection
        features = []
        for row in rows:
            geojson_str = row.pop("__geojson", None)
            geom = json.loads(geojson_str) if geojson_str else None
            features.append({
                "type": "Feature",
                "geometry": geom,
                "properties": {k: v for k, v in row.items()},
            })

        collection = {
            "type": "FeatureCollection",
            "features": features,
            "metadata": {
                "source_table": table,
                "feature_count": len(features),
                "truncated": len(features) >= limit,
            },
        }

        return json.dumps(collection, default=str)
    except Exception as e:
        return format_error(e, "export_geojson")


@mcp.tool()
def multi_table_query(
    sql: str,
    limit: int = 50,
) -> str:
    """Execute a cross-table analytical query with safety rails.

    Like the `query` tool but designed for complex multi-table
    analytics: JOINs, subqueries, window functions, CTEs.
    Applies the same read-only validation.

    The key difference from `query`: this tool explicitly signals
    to the LLM that complex cross-table operations are expected
    and supported, and provides a higher default limit.

    Args:
        sql: A read-only SELECT query that may reference multiple tables.
             Use fully qualified names: "lakehouse.namespace.table".
             Supports JOINs, CTEs (WITH), window functions, aggregations.
        limit: Maximum rows to return (1-5000, default 50).

    Returns query results as JSON.
    """
    try:
        validate_read_only_sql(sql)
        limit = max(1, min(limit, settings.max_result_rows))

        sql_upper = sql.upper().strip().rstrip(";")
        if "LIMIT" not in sql_upper:
            sql = f"{sql.rstrip(';')} LIMIT {limit}"

        rows = execute_query(sql)
        truncated = len(rows) >= limit
        return format_result(rows, truncated=truncated)
    except Exception as e:
        return format_error(e, "multi_table_query")


@mcp.tool()
def search_tables(
    pattern: str = "",
    column_pattern: str = "",
    geometry_only: bool = False,
) -> str:
    """Search for tables by name or column pattern.

    Useful for discovery: "what tables have a 'population' column?"
    or "which tables contain geometry data?"

    Args:
        pattern: Substring to match against table names (case-insensitive).
        column_pattern: Substring to match against column names.
        geometry_only: If true, only return tables that have geometry columns.

    Returns matching tables with their column lists.
    """
    try:
        # Get tables
        tables = execute_query(
            f"SELECT database_name, schema_name, table_name, column_count "
            f"FROM duckdb_tables() "
            f"WHERE database_name = '{CATALOG}'"
        )

        # Use DESCRIBE per table for real column info
        # (information_schema.columns returns placeholder data for Iceberg tables)
        geom_indicators = {"geometry", "geom", "wkb_geometry", "shape", "the_geom"}
        results = []
        for row in tables:
            tname = row.get("table_name", "")
            sname = row.get("schema_name", "")

            # Apply table name filter
            if pattern and pattern.lower() not in tname.lower():
                continue

            # Get real column info via DESCRIBE
            try:
                desc = execute_query(f"DESCRIBE {CATALOG}.{sname}.{tname}")
                col_names = [d["column_name"] for d in desc]
                col_types = [d["column_type"] for d in desc]
                row["column_count"] = len(desc)
            except Exception:
                col_names = []
                col_types = []

            # Apply column name filter
            if column_pattern:
                if not any(column_pattern.lower() in c.lower() for c in col_names):
                    continue

            # Detect geometry columns
            has_geometry = False
            for cname, ctype in zip(col_names, col_types):
                if (cname.lower() in geom_indicators
                        or "GEOMETRY" in str(ctype).upper()
                        or "BLOB" in str(ctype).upper()):
                    has_geometry = True
                    break

            if geometry_only and not has_geometry:
                continue

            row["column_names"] = col_names
            row["column_types"] = col_types
            row["has_geometry"] = has_geometry
            results.append(row)

        return format_result(results)
    except Exception as e:
        return format_error(e, "search_tables")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MATERIALIZATION (Tier 3 Bridge)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@mcp.tool()
def materialize_result(
    sql: str,
    result_name: str,
    namespace: str = "_scratch",
    overwrite: bool = True,
) -> str:
    """Write a query result to a scratch table for visualization.

    Materializes the result of a SELECT query as an Iceberg table
    in the scratch namespace, making it available to the webmap
    via the feature API.

    Args:
        sql: A read-only SELECT query producing the result to materialize.
        result_name: Name for the result table (e.g., "buffer_500m").
        namespace: Target namespace (default "_scratch").
        overwrite: If true, replaces an existing table with the same name.

    Returns confirmation with table reference and row count.
    """
    try:
        validate_read_only_sql(sql)
        result_name = validate_identifier(result_name, "result_name")
        namespace = validate_identifier(namespace, "namespace")
        qualified = f"{CATALOG}.{namespace}.{result_name}"

        conn = get_connection()
        with _lock:
            conn.execute(
                f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{namespace}"
            )

            if overwrite:
                conn.execute(f"DROP TABLE IF EXISTS {qualified}")

            conn.execute(f"CREATE TABLE {qualified} AS {sql}")

            count = conn.execute(
                f"SELECT COUNT(*)::INTEGER FROM {qualified}"
            ).fetchone()[0]

        return json.dumps({
            "status": "materialized",
            "table": f"{namespace}.{result_name}",
            "qualified": qualified,
            "row_count": count,
        }, indent=2)
    except Exception as e:
        return format_error(e, "materialize_result")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SYSTEM TOOLS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@mcp.tool()
def health_check() -> str:
    """Check server health: DuckDB status, extension status, catalog attachment.

    Returns a JSON object with status of each component.
    """
    try:
        status = check_health()
        return json.dumps(status, indent=2)
    except Exception as e:
        return format_error(e, "health_check")


# ── Entrypoint ─────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    mcp.run(transport=settings.transport)
