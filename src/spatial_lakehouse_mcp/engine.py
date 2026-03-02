"""DuckDB engine: single connection handling catalog attachment + spatial queries."""

import logging
import threading
import duckdb
from typing import Optional

from .config import settings

logger = logging.getLogger(__name__)

# Module-level connection — lazily initialized
_conn: Optional[duckdb.DuckDBPyConnection] = None
_lock = threading.Lock()


def _build_s3_secret_sql() -> str:
    """Build CREATE SECRET statement for Garage S3 access."""
    return f"""
    CREATE OR REPLACE SECRET garage_s3 (
        TYPE s3,
        KEY_ID '{settings.s3_access_key_id}',
        SECRET '{settings.s3_secret_access_key}',
        ENDPOINT '{settings.s3_endpoint}',
        REGION '{settings.s3_region}',
        URL_STYLE '{settings.s3_url_style}',
        USE_SSL {str(settings.s3_use_ssl).lower()}
    );
    """


def _build_iceberg_secret_sql() -> str:
    """Build CREATE SECRET statement for LakeKeeper auth.

    Supports three modes:
      1. OAuth2 (client_id + client_secret + oauth2_server_uri)
      2. Bearer token (catalog_token)
      3. No auth (dummy token for local dev without auth)
    """
    if settings.catalog_client_id and settings.catalog_oauth2_server_uri:
        # OAuth2 flow
        parts = [
            "CREATE OR REPLACE SECRET iceberg_catalog_secret (",
            "    TYPE iceberg,",
            f"    CLIENT_ID '{settings.catalog_client_id}',",
            f"    CLIENT_SECRET '{settings.catalog_client_secret}',",
        ]
        if settings.catalog_oauth2_scope:
            parts.append(f"    OAUTH2_SCOPE '{settings.catalog_oauth2_scope}',")
        parts.append(f"    OAUTH2_SERVER_URI '{settings.catalog_oauth2_server_uri}'")
        parts.append(");")
        return "\n".join(parts)
    elif settings.catalog_token:
        # Bearer token
        return f"""
        CREATE OR REPLACE SECRET iceberg_catalog_secret (
            TYPE iceberg,
            TOKEN '{settings.catalog_token}'
        );
        """
    else:
        # No auth — use dummy token (works with LakeKeeper in dev mode)
        return """
        CREATE OR REPLACE SECRET iceberg_catalog_secret (
            TYPE iceberg,
            TOKEN 'dummy_token'
        );
        """


def _build_attach_sql() -> str:
    """Build ATTACH statement for LakeKeeper Iceberg catalog.

    ACCESS_DELEGATION_MODE is configurable:
      - 'none': host-side (bypasses LakeKeeper signing, uses local S3 secret)
      - 'vended-credentials': Docker (uses LakeKeeper's S3 credentials)
    """
    attach_parts = [
        f"ATTACH '{settings.catalog_warehouse}' AS {settings.catalog_alias} (",
        f"    TYPE iceberg,",
        f"    ENDPOINT '{settings.catalog_uri}/catalog',",
        f"    SECRET iceberg_catalog_secret",
    ]
    if settings.access_delegation_mode:
        attach_parts.append(
            f"    , ACCESS_DELEGATION_MODE '{settings.access_delegation_mode}'"
        )
    attach_parts.append(");")
    return "\n".join(attach_parts)


def get_connection() -> duckdb.DuckDBPyConnection:
    """Get or create the DuckDB connection with catalog attached.

    Uses lazy initialization with double-checked locking — first call
    installs extensions, creates secrets, and attaches the catalog.
    Subsequent calls return the existing connection.
    """
    global _conn
    if _conn is not None:
        return _conn

    with _lock:
        # Double-check after acquiring lock
        if _conn is not None:
            return _conn

        logger.info("Initializing DuckDB connection...")
        conn = duckdb.connect(database=":memory:")

        # Install and load extensions
        for ext in ("iceberg", "httpfs", "spatial"):
            conn.execute(f"INSTALL {ext};")
            conn.execute(f"LOAD {ext};")
            logger.info(f"Loaded extension: {ext}")

        # Create S3 secret for Garage
        if settings.s3_access_key_id:
            conn.execute(_build_s3_secret_sql())
            logger.info(f"Created S3 secret for endpoint {settings.s3_endpoint}")

        # Create Iceberg secret for LakeKeeper
        conn.execute(_build_iceberg_secret_sql())
        logger.info("Created Iceberg catalog secret")

        # Attach the catalog
        try:
            conn.execute(_build_attach_sql())
            logger.info(
                f"Attached catalog '{settings.catalog_warehouse}' "
                f"as '{settings.catalog_alias}' from {settings.catalog_uri}"
            )
        except Exception as e:
            logger.error(f"Failed to attach catalog: {e}")
            # Store connection even if attach fails — health_check will report status
            _conn = conn
            raise

        # Enforce query timeout
        conn.execute(
            f"SET statement_timeout = '{settings.query_timeout_seconds}s'"
        )

        _conn = conn
        return _conn


def execute_query(sql: str, params: list | None = None) -> list[dict]:
    """Execute a SQL query and return results as list of dicts.

    Handles DuckDB result conversion including geometry columns.
    """
    conn = get_connection()
    with _lock:
        try:
            if params:
                result = conn.execute(sql, params)
            else:
                result = conn.execute(sql)

            if result.description is None:
                return []

            columns = [desc[0] for desc in result.description]
            rows = result.fetchmany(settings.max_result_rows)
            return [dict(zip(columns, row)) for row in rows]
        except Exception as e:
            logger.error(f"Query execution failed: {e}\nSQL: {sql}")
            raise


def execute_scalar(sql: str) -> any:
    """Execute a query and return a single scalar value."""
    conn = get_connection()
    with _lock:
        return conn.execute(sql).fetchone()[0]


def check_health() -> dict:
    """Check connection health and catalog status."""
    status = {"duckdb": False, "extensions": {}, "catalog_attached": False}
    try:
        conn = get_connection()
        # Basic connection test
        conn.execute("SELECT 1")
        status["duckdb"] = True

        # Check extensions
        exts = conn.execute(
            "SELECT extension_name, loaded, installed "
            "FROM duckdb_extensions() "
            "WHERE extension_name IN ('iceberg', 'httpfs', 'spatial')"
        ).fetchall()
        for name, loaded, installed in exts:
            status["extensions"][name] = {
                "installed": installed,
                "loaded": loaded,
            }

        # Check catalog attachment
        dbs = conn.execute(
            "SELECT database_name FROM duckdb_databases()"
        ).fetchall()
        db_names = [row[0] for row in dbs]
        status["catalog_attached"] = settings.catalog_alias in db_names
        status["catalog_alias"] = settings.catalog_alias

    except Exception as e:
        status["error"] = str(e)

    return status


def set_connection(conn: duckdb.DuckDBPyConnection):
    """Inject a connection (for testing)."""
    global _conn
    _conn = conn


def reset_connection():
    """Reset the connection (for testing or reconnection)."""
    global _conn
    if _conn is not None:
        try:
            _conn.close()
        except Exception:
            pass
        _conn = None
