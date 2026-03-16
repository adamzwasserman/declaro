"""
Connection-level latency instrumentation for declaro_persistum.

Records timing data for every execute() call through the pool.
Zero overhead when disabled — no proxy wrapping, no timing, no logging.

Usage:
    pool = await ConnectionPool.turso(
        url, auth_token=token,
        instrumentation=True,
        tier_label="project",
        latency_sink="jsonl",
        latency_path="./data/db_latency.jsonl",
    )
"""

import logging
import time
from datetime import UTC, datetime
from typing import Any, TypedDict


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
}

_WRITE_OPS = {"insert", "update", "delete"}


def classify_sql(sql: str) -> str:
    """Classify SQL statement into an op type via dict lookup on first keyword."""
    first = sql.strip().split(None, 1)[0].lower() if sql.strip() else ""
    return _SQL_OP_MAP.get(first, "other")


def is_write_op(op: str) -> bool:
    """Return True if the op type is a write (insert/update/delete)."""
    return op in _WRITE_OPS


def build_record(
    *,
    tier: str,
    op: str,
    duration_ms: float,
    success: bool,
    sql: str,
    error: str = "",
) -> LatencyRecord:
    """Build a LatencyRecord from execution metadata."""
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


def get_latency_logger() -> logging.Logger:
    """
    Get the dedicated latency logger.

    Uses a separate logger with propagate=False so latency records
    don't pollute application logs.
    """
    logger = logging.getLogger("declaro_persistum.latency")
    logger.propagate = False
    return logger


def setup_jsonl_sink(logger: logging.Logger, path: str) -> None:
    """
    Attach a JSONL file handler to the latency logger.

    Lazy: file and directory are created on first write, not at setup time.
    """

    class _LazyJSONLHandler(logging.FileHandler):
        """FileHandler that creates the file/dir on first emit."""

        def __init__(self, path: str) -> None:
            self._path = path
            self._initialised = False
            # Don't call super().__init__ yet — defer file creation
            logging.Handler.__init__(self)

        def _ensure_file(self) -> None:
            if not self._initialised:
                import os

                os.makedirs(os.path.dirname(os.path.abspath(self._path)), exist_ok=True)
                super(_LazyJSONLHandler, self).__init__(self._path, mode="a", encoding="utf-8")
                self._initialised = True

        def emit(self, record: logging.LogRecord) -> None:
            self._ensure_file()
            super().emit(record)

    handler = _LazyJSONLHandler(path)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def setup_callable_sink(logger: logging.Logger, fn: Any) -> None:
    """
    Attach a callable sink to the latency logger.

    The callable receives a LatencyRecord dict on each execute().
    Useful for Prometheus, StatsD, or custom sinks.
    """
    import json

    class _CallableHandler(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            try:
                data = json.loads(record.getMessage())
                fn(data)
            except Exception:
                pass

    handler = _CallableHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def emit_record(logger: logging.Logger, record: LatencyRecord) -> None:
    """Write a LatencyRecord to the latency logger."""
    logger.info(format_jsonl(record))


def record_execution(
    pool: Any,
    sql: str,
    duration_ms: float,
    success: bool,
    error: str = "",
) -> None:
    """
    Record one execute() call if the pool has instrumentation enabled.

    Called from execute_with_pool() after every execute. No-op if pool
    has no _latency_logger attribute (instrumentation disabled).
    """
    logger = getattr(pool, "_latency_logger", None)
    if logger is None:
        return
    tier = getattr(pool, "_tier", "")
    op = classify_sql(sql)
    record = build_record(
        tier=tier,
        op=op,
        duration_ms=duration_ms,
        success=success,
        sql=sql,
        error=error,
    )
    emit_record(logger, record)
