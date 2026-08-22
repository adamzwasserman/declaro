"""
Connection-level latency instrumentation for declaro_persistum.

Records timing data for a query execution.
Zero overhead when disabled — no proxy wrapping, no timing, no logging.

Usage:
"""

import logging
import re
from datetime import UTC, datetime
from typing import TypedDict

from declaro_persistum.types import Nothing, resolved


class LatencyRecord(TypedDict):
    """One timing record per execute() call."""

    ts: str           # ISO 8601 timestamp
    tier: str         # Caller-supplied label (e.g. "central", "project")
    op: str           # Classified from SQL: select|insert|update|delete|create|alter|other
    duration_ms: float
    success: bool
    sql: str          # First 120 chars of SQL
    error: str        # First 200 chars of exception string, or ""


_SQL_OP_MAP: dict[str, str] = {
    "select": "select",
    "insert": "insert",
    "update": "update",
    "delete": "delete",
    "create": "create",
    "alter": "alter",
    "drop": "alter",
    "pragma": "other",
    "with": "select",
    # SQLite's upsert. It was absent, so `REPLACE INTO` classified as "other"
    # and `is_write_op` said False about a statement that rewrites rows.
    "replace": "insert",
}

_WRITE_OPS = {"insert", "update", "delete"}


def classify_sql(sql: str) -> str:
    """Classify SQL statement into an op type via dict lookup on first keyword.

    A CTE IS CLASSIFIED BY THE STATEMENT IT LEADS TO, not by the `WITH`. A
    data-modifying CTE is ordinary SQL -- `WITH x AS (...) DELETE FROM t ...`
    on PostgreSQL, and the same for UPDATE and INSERT -- and reading the first
    word called all of them `select`. Measured 2026-08-17:

        REPLACE INTO t VALUES (1)          -> 'other'   write=False
        WITH x AS (...) DELETE FROM t ...  -> 'select'  write=False

    Both are writes. A consumer using `is_write_op` to decide what goes
    through the write queue would have sent them down the read path.

    An unrecognised statement is still "other" rather than a guess, and that
    fallback is right here in a way it is not elsewhere in this package: SQL
    has more verbs than this table will ever hold, so an unknown one is not a
    caller error the way an unknown dialect is.
    """
    stripped = sql.strip()
    if not stripped:
        return "other"

    first = stripped.split(None, 1)[0].lower()
    if first == "with":
        return _SQL_OP_MAP.get(_after_the_cte(stripped), "select")
    return _SQL_OP_MAP.get(first, "other")


def _after_the_cte(sql: str) -> str:
    """The keyword a `WITH ...` statement actually performs.

    Scans past balanced parentheses so a SELECT inside the CTE body is not
    mistaken for the statement's own verb.
    """
    depth = 0
    token = ""
    for char in sql[len("with"):]:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0:
            if char.isalpha():
                token += char
                continue
            if token:
                lowered = token.lower()
                if lowered in _SQL_OP_MAP and lowered != "with":
                    return lowered
                token = ""
    return token.lower()


def is_write_op(op: str) -> bool:
    """Return True if the op type is a write (insert/update/delete)."""
    return op in _WRITE_OPS


# RETURNING is whole-word, case-insensitive, and not inside a string literal
# or identifier. The regex below uses ``\b`` word boundaries so column names
# like ``returning_user`` do not match. RETURNING only appears at top level
# in declaro-generated SQL — we do not emit it inside CTEs or subqueries.
_RETURNING_RE = re.compile(r"\bRETURNING\b", re.IGNORECASE)


def has_returning_clause(sql: str) -> bool:
    """Return True if the SQL has a RETURNING clause.

    Used by the executor to decide whether a write op should be routed
    through the fetch path (RETURNING present, rows expected) or the
    count path (no RETURNING, just rowcount). Pure function.
    """
    return _RETURNING_RE.search(sql) is not None


def build_record(
    *,
    tier: str,
    op: str,
    duration_ms: float,
    success: bool,
    sql: str,
    error: str | Nothing,
) -> LatencyRecord:
    """Build a LatencyRecord from execution metadata."""
    # THE VISIBLE RESOLVE. A default would hide this inside the
    # signature, where no reader sees it happen and no test reaches it.
    error = resolved(error, '')
    return {
        "ts": datetime.now(UTC).isoformat(),
        "tier": tier,
        "op": op,
        "duration_ms": round(duration_ms, 3),
        "success": success,
        "sql": sql[:120],
        "error": error[:200],
    }


def format_jsonl(record: LatencyRecord) -> str:
    """Format a LatencyRecord as a JSONL line (no trailing newline added by caller)."""
    import json

    return json.dumps(record)








def emit_record(logger: logging.Logger, record: LatencyRecord) -> None:
    """Write a LatencyRecord to the latency logger."""
    logger.info(format_jsonl(record))


