#!/usr/bin/env python3
"""Pre-implementation validation for spatial-lakehouse MCP server.

Validates three prerequisites against the running infrastructure stack:
  1. Garage S3 connectivity from host-side DuckDB (httpfs)
  2. LakeKeeper ATTACH from host-side DuckDB (iceberg, /catalog suffix, ACCESS_DELEGATION_MODE)
  3. Extension coexistence (httpfs + iceberg + spatial in one connection)

Requirements:
  - duckdb>=1.4.0 Python package installed
  - Lakehouse stack running (garage on :3900, lakekeeper on :8181)

Usage:
  python validate_stack.py
"""

import os
import sys
import traceback

import duckdb

# ── Configuration (matches running stack at ../lakehouse/.env) ─────
GARAGE_KEY_ID = os.environ.get("GARAGE_KEY_ID", "GK8e7e0e5439eda8588e0a2693")
GARAGE_SECRET = os.environ.get(
    "GARAGE_SECRET_KEY",
    "3dfca5c6f09403e7b954f94a91f59a61c1c3c9230ac5d50e8bba58c81c3a1533",
)
GARAGE_ENDPOINT = os.environ.get("SLM_S3_ENDPOINT", "localhost:3900")
LAKEKEEPER_ENDPOINT = os.environ.get(
    "SLM_CATALOG_URI", "http://localhost:8181"
) + "/catalog"
LAKEKEEPER_TOKEN = os.environ.get("SLM_CATALOG_TOKEN", "dummy")
S3_BUCKET = os.environ.get("SLM_CATALOG_WAREHOUSE", "lakehouse")

pass_count = 0
fail_count = 0


def run_check(label: str, fn):
    """Run a validation check and print PASS/FAIL."""
    global pass_count, fail_count
    try:
        detail = fn()
        print(f"  [{label}] PASS")
        print(f"      {detail}")
        pass_count += 1
    except Exception as e:
        print(f"  [{label}] FAIL")
        print(f"      {type(e).__name__}: {e}")
        traceback.print_exc(limit=2)
        fail_count += 1
    print()


def _create_s3_secret(conn: duckdb.DuckDBPyConnection):
    """Create the Garage S3 secret on a connection."""
    conn.execute(f"""
        CREATE SECRET garage_s3 (
            TYPE S3,
            KEY_ID '{GARAGE_KEY_ID}',
            SECRET '{GARAGE_SECRET}',
            REGION 'garage',
            ENDPOINT '{GARAGE_ENDPOINT}',
            URL_STYLE 'path',
            USE_SSL false
        )
    """)


# ── Check 1: Garage S3 + httpfs ──────────────────────────────────

def check_s3_connectivity() -> str:
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    _create_s3_secret(conn)

    files = conn.execute(
        f"SELECT * FROM glob('s3://{S3_BUCKET}/**') LIMIT 5"
    ).fetchall()

    if files:
        path = files[0][0]
        count = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{path}')"
        ).fetchone()[0]
        conn.close()
        return f"{len(files)} files found, first file has {count} rows"

    conn.close()
    return "Bucket accessible (empty — fresh catalog)"


# ── Check 2: LakeKeeper ATTACH ───────────────────────────────────

def check_lakekeeper_attach() -> str:
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL iceberg; LOAD iceberg;")

    # S3 secret MUST exist before ATTACH (used with ACCESS_DELEGATION_MODE='none')
    _create_s3_secret(conn)

    conn.execute(
        f"CREATE SECRET lk (TYPE ICEBERG, TOKEN '{LAKEKEEPER_TOKEN}')"
    )

    conn.execute(f"""
        ATTACH '{S3_BUCKET}' AS lakehouse (
            TYPE ICEBERG,
            ENDPOINT '{LAKEKEEPER_ENDPOINT}',
            SECRET lk,
            ACCESS_DELEGATION_MODE 'none'
        )
    """)

    tables = conn.execute("SHOW ALL TABLES").fetchall()

    if tables:
        # columns: database, schema, name, column_names, column_types, temporary
        db, schema, name = tables[0][0], tables[0][1], tables[0][2]
        count = conn.execute(
            f"SELECT COUNT(*) FROM {db}.{schema}.{name}"
        ).fetchone()[0]
        conn.close()
        return (
            f"ATTACH OK, {len(tables)} table(s) found, "
            f"{db}.{schema}.{name} has {count} rows"
        )

    conn.close()
    return "ATTACH OK (catalog is empty)"


# ── Check 3: Extension coexistence ───────────────────────────────

def check_extension_coexistence() -> str:
    conn = duckdb.connect()

    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL spatial; LOAD spatial;")

    exts = conn.execute("""
        SELECT extension_name, installed, loaded
        FROM duckdb_extensions()
        WHERE extension_name IN ('httpfs', 'iceberg', 'spatial')
    """).fetchall()

    loaded_map = {name: (inst, ld) for name, inst, ld in exts}
    for name in ("httpfs", "iceberg", "spatial"):
        if name not in loaded_map:
            raise RuntimeError(f"Extension '{name}' not found in duckdb_extensions()")
        if not loaded_map[name][1]:
            raise RuntimeError(f"Extension '{name}' installed but not loaded")

    wkt = conn.execute(
        "SELECT ST_AsText(ST_Point(-104.99, 39.74))"
    ).fetchone()[0]

    if "POINT" not in wkt:
        raise RuntimeError(f"ST_Point returned unexpected result: {wkt}")

    conn.close()
    return f"All 3 extensions loaded, ST_Point → {wkt}"


# ── Main ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    ver = duckdb.__version__
    print(f"DuckDB Python version: {ver}")
    if tuple(int(x) for x in ver.split(".")[:2]) < (1, 4):
        print(f"  WARNING: duckdb {ver} < 1.4.0 — iceberg ATTACH may not work")
    print()

    run_check("1/3 Garage S3 + httpfs", check_s3_connectivity)
    run_check("2/3 LakeKeeper ATTACH", check_lakekeeper_attach)
    run_check("3/3 Extension coexistence", check_extension_coexistence)

    print(f"Results: {pass_count} passed, {fail_count} failed")
    sys.exit(0 if fail_count == 0 else 1)
