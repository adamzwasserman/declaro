"""
Slow query recording and retrieval.

Records queries that exceed a configurable threshold for analysis.
"""

from dataclasses import dataclass
from typing import Any, Protocol


class QueryRecorder(Protocol):
    """Protocol for recording slow queries."""

    def record(
        self,
        fingerprint: str,
        sql: str,
        params: dict[str, Any],
        elapsed_ms: float,
    ) -> None:
        """Record a slow query."""
        ...


@dataclass
class SlowQueryConfig:
    """Configuration for slow query recording."""

    enabled: bool = True
    threshold_ms: float = 500.0
    retention_hours: int = 168  # 7 days

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SlowQueryConfig":
        """Create config from dictionary."""
        return cls(
            enabled=data.get("enabled", True),
            threshold_ms=data.get("threshold_ms", 500.0),
            retention_hours=data.get("retention_hours", 168),
        )


class SlowQueryObserver:
    """Observer that records queries above a time threshold."""

    def __init__(self, threshold_ms: float, recorder: QueryRecorder):
        self._threshold_ms = threshold_ms
        self._recorder = recorder

    def on_query_executed(
        self,
        sql: str,
        params: dict[str, Any],
        elapsed_ms: float,
        fingerprint: str,
    ) -> None:
        """Record query if above threshold."""
        if elapsed_ms >= self._threshold_ms:
            self._recorder.record(fingerprint, sql, params, elapsed_ms)


def setup_slow_query_table_sql(dialect: str) -> str:
    """
    Generate SQL to create the slow query table.

    Args:
        dialect: Database dialect ("postgresql" or "sqlite").

    Returns:
        CREATE TABLE SQL statement.
    """
    if dialect == "postgresql":
        return """
CREATE TABLE IF NOT EXISTS declaro_slow_queries (
    id SERIAL PRIMARY KEY,
    fingerprint TEXT NOT NULL,
    sql_text TEXT NOT NULL,
    elapsed_ms REAL NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_declaro_slow_queries_fingerprint ON declaro_slow_queries(fingerprint);
CREATE INDEX IF NOT EXISTS idx_declaro_slow_queries_recorded_at ON declaro_slow_queries(recorded_at);
"""
    else:
        # SQLite
        return """
CREATE TABLE IF NOT EXISTS declaro_slow_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fingerprint TEXT NOT NULL,
    sql_text TEXT NOT NULL,
    elapsed_ms REAL NOT NULL,
    recorded_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_declaro_slow_queries_fingerprint ON declaro_slow_queries(fingerprint);
CREATE INDEX IF NOT EXISTS idx_declaro_slow_queries_recorded_at ON declaro_slow_queries(recorded_at);
"""


def record_slow_query_sql() -> str:
    """
    Generate INSERT SQL for recording a slow query.

    Returns:
        INSERT SQL statement with named parameters.
    """
    return """
INSERT INTO declaro_slow_queries (fingerprint, sql_text, elapsed_ms)
VALUES (:fingerprint, :sql_text, :elapsed_ms)
"""


def get_slow_queries_sql(
    since_hours: int | None = None,
    limit: int | None = None,
) -> str:
    """
    Generate SELECT SQL for retrieving slow queries.

    Args:
        since_hours: Optional filter for queries in last N hours.
        limit: Optional limit on number of results.

    Returns:
        SELECT SQL statement.
    """
    sql = "SELECT id, fingerprint, sql_text, elapsed_ms, recorded_at FROM declaro_slow_queries"

    if since_hours is not None:
        sql += f" WHERE recorded_at >= NOW() - INTERVAL '{since_hours} hours'"

    sql += " ORDER BY recorded_at DESC"

    if limit is not None:
        sql += f" LIMIT {limit}"

    return sql


def get_slow_queries_by_fingerprint_sql() -> str:
    """
    Generate SELECT SQL for queries with a specific fingerprint.

    Returns:
        SELECT SQL statement with fingerprint parameter.
    """
    return """
SELECT id, fingerprint, sql_text, elapsed_ms, recorded_at
FROM declaro_slow_queries
WHERE fingerprint = :fingerprint
ORDER BY recorded_at DESC
"""


def cleanup_slow_queries_sql(retention_hours: int = 168) -> str:
    """
    Generate DELETE SQL for cleaning up old queries.

    Args:
        retention_hours: Delete queries older than this many hours.

    Returns:
        DELETE SQL statement.
    """
    return f"""
DELETE FROM declaro_slow_queries
WHERE recorded_at < NOW() - INTERVAL '{retention_hours} hours'
"""


def get_slow_query_stats_sql() -> str:
    """
    Generate SQL for slow query statistics.

    Returns summary of slow queries grouped by fingerprint.

    Returns:
        SELECT SQL statement.
    """
    return """
SELECT
    fingerprint,
    COUNT(*) as occurrence_count,
    AVG(elapsed_ms) as avg_elapsed_ms,
    MAX(elapsed_ms) as max_elapsed_ms,
    MIN(elapsed_ms) as min_elapsed_ms,
    SUM(elapsed_ms) as total_elapsed_ms
FROM declaro_slow_queries
GROUP BY fingerprint
ORDER BY occurrence_count DESC
"""


def create_slow_query_observer(
    config: SlowQueryConfig,
    recorder: QueryRecorder | None,
) -> SlowQueryObserver | None:
    """
    Create a slow query observer from config.

    Args:
        config: Slow query configuration.
        recorder: Query recorder implementation.

    Returns:
        SlowQueryObserver if enabled, None otherwise.
    """
    if not config.enabled or recorder is None:
        return None

    return SlowQueryObserver(
        threshold_ms=config.threshold_ms,
        recorder=recorder,
    )
