"""Input validation and SQL safety checks."""

import re
import json
from typing import Any

from .config import settings


# ── SQL Safety ──────────────────────────────────────────────────────

# Keywords that must NOT appear as the first word of a user-supplied query
DANGEROUS_FIRST_KEYWORDS = {
    "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE",
    "TRUNCATE", "GRANT", "REVOKE", "COPY", "EXECUTE", "CALL",
    "ATTACH", "DETACH", "INSTALL", "LOAD", "PRAGMA", "SET",
}

def validate_read_only_sql(sql: str) -> None:
    """Validate that a SQL string is a read-only SELECT query.

    Raises ValueError with a descriptive message if validation fails.
    """
    if not sql or not sql.strip():
        raise ValueError("Query cannot be empty.")

    if len(sql) > settings.max_query_length:
        raise ValueError(
            f"Query exceeds maximum length of {settings.max_query_length} characters."
        )

    stripped = sql.strip().rstrip(";")

    # Check first keyword
    first_word = stripped.split()[0].upper() if stripped.split() else ""
    if first_word in DANGEROUS_FIRST_KEYWORDS:
        raise ValueError(
            f"Query rejected: {first_word} operations are not allowed. "
            "Only SELECT queries and table functions are permitted."
        )

    # Must start with SELECT or WITH (CTEs)
    if first_word not in ("SELECT", "WITH"):
        raise ValueError(
            f"Query must start with SELECT or WITH. Got: {first_word}"
        )

    # Reject multi-statement queries (semicolons in the middle)
    if ";" in stripped:
        raise ValueError("Multi-statement queries are not allowed.")


# ── WHERE Clause Safety ────────────────────────────────────────────

# Patterns that should never appear in a WHERE clause
_WHERE_BANNED = re.compile(
    r'\b(UNION|INTO|COPY|ATTACH|DETACH|INSTALL|LOAD|PRAGMA|SET)\b',
    re.IGNORECASE,
)


def validate_where_clause(where: str) -> str:
    """Validate a user-supplied WHERE clause fragment.

    Rejects multi-statement injection and dangerous keywords.
    Returns the validated clause or raises ValueError.
    """
    if not where or not where.strip():
        return ""
    where = where.strip()

    if ";" in where:
        raise ValueError("WHERE clause cannot contain semicolons.")

    if _WHERE_BANNED.search(where):
        raise ValueError(
            "WHERE clause contains a disallowed keyword. "
            "Only filtering expressions are permitted."
        )

    return where


# ── Identifier Safety ──────────────────────────────────────────────

_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_QUALIFIED_IDENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_.]*$")


def validate_identifier(name: str, label: str = "identifier") -> str:
    """Validate a SQL identifier (table name, column name, etc.).

    Returns the validated name. Raises ValueError if invalid.
    """
    if not name or not name.strip():
        raise ValueError(f"{label} cannot be empty.")
    name = name.strip()
    if len(name) > 128:
        raise ValueError(f"{label} exceeds maximum length of 128 characters.")
    if not _QUALIFIED_IDENT_RE.match(name):
        raise ValueError(
            f"Invalid {label}: '{name}'. Must contain only letters, digits, "
            "underscores, and dots."
        )
    return name


def validate_namespace(namespace: str) -> str:
    """Validate a namespace/schema name."""
    return validate_identifier(namespace, "namespace")


def validate_table_ref(table_ref: str, catalog_alias: str) -> str:
    """Validate and qualify a table reference.

    Accepts:
      - "table_name" → raises error (requires namespace)
      - "namespace.table" → "{catalog_alias}.namespace.table"
      - "catalog.namespace.table" → passed through as-is

    Returns the fully qualified table reference.
    """
    parts = table_ref.strip().split(".")
    for part in parts:
        if not _IDENT_RE.match(part):
            raise ValueError(
                f"Invalid table reference component: '{part}'. "
                "Must contain only letters, digits, and underscores."
            )

    if len(parts) == 1:
        raise ValueError(
            f"Table reference '{table_ref}' must include a namespace. "
            f"Example: my_namespace.{parts[0]}"
        )
    elif len(parts) == 2:
        return f"{catalog_alias}.{parts[0]}.{parts[1]}"
    elif len(parts) == 3:
        return f"{parts[0]}.{parts[1]}.{parts[2]}"
    else:
        raise ValueError(f"Invalid table reference: '{table_ref}'. Too many parts.")


# ── Result Formatting ──────────────────────────────────────────────

def format_result(rows: list[dict], truncated: bool = False) -> str:
    """Format query results as a JSON string for LLM consumption.

    Returns a JSON object with:
      - row_count: number of rows returned
      - truncated: whether results were truncated
      - rows: the actual data
    """
    result = {
        "row_count": len(rows),
        "truncated": truncated,
        "rows": rows,
    }
    return json.dumps(result, default=str, indent=2)


def format_error(error: Exception, context: str = "") -> str:
    """Format an error as a JSON string for LLM consumption."""
    result = {
        "error": True,
        "error_type": type(error).__name__,
        "message": str(error),
    }
    if context:
        result["context"] = context
    return json.dumps(result, indent=2)


# ── Phase 2 Validators ────────────────────────────────────────────

def validate_agg_function(func: str) -> str:
    """Validate an aggregation function name."""
    allowed = {"count", "sum", "avg", "min", "max", "stddev"}
    if func.lower().strip() not in allowed:
        raise ValueError(
            f"Invalid aggregation function '{func}'. "
            f"Allowed: {', '.join(sorted(allowed))}"
        )
    return func.lower().strip()


def validate_spatial_predicate(predicate: str) -> str:
    """Validate a spatial predicate name."""
    allowed = {"intersects", "contains", "within", "dwithin", "crosses", "touches", "overlaps"}
    if predicate.lower().strip() not in allowed:
        raise ValueError(
            f"Invalid spatial predicate '{predicate}'. "
            f"Allowed: {', '.join(sorted(allowed))}"
        )
    return predicate.lower().strip()


def validate_positive_number(value: float, label: str = "value") -> float:
    """Validate that a number is positive."""
    if value <= 0:
        raise ValueError(f"{label} must be positive. Got: {value}")
    return float(value)
