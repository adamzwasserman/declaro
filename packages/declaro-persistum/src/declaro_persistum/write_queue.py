"""
Write queue for deferred persistence of slow writes.

When a write operation exceeds the configured threshold (default 50ms),
the entry is placed in the queue and the caller returns immediately with
the data. The supervisor drains the queue in the background.

Design:
- One queue per pool
- Keyed by "{table}:{pk_value}"
- Atomic persistence: write to tmp, then rename
- Supervisor with semaphore(3) concurrency, exponential backoff
- CRITICAL log after 6 hours of continuous failure
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from datetime import UTC, datetime
from typing import Any, TypedDict

logger = logging.getLogger(__name__)

_CRITICAL_THRESHOLD_SECONDS = 3600  # 1 hour


class PendingEntry(TypedDict):
    """One queued write operation."""

    entry_id: str          # UUID
    table: str             # Table name
    pk_column: str         # Primary key column name
    pk_value: Any          # Primary key value
    op: str                # "insert" | "update" | "delete"
    data: dict[str, Any]   # Row data as returned to the caller
    sql: str               # SQL to re-execute
    params: Any            # Bound parameters
    dialect: str           # "postgresql" | "sqlite" | "turso"
    queued_at: str         # ISO 8601
    attempt_count: int     # How many drain attempts so far
    last_error: str        # Last exception message, or ""


class WriteQueue:
    """
    In-memory write queue with optional disk persistence.

    Usage::

        queue = WriteQueue(pool, persistence_path="./db/write_queue.jsonl")
        queue.start_supervisor()
        ...
        await pool.close()  # calls stop_supervisor() which flushes
    """

    def __init__(
        self,
        pool: Any,
        persistence_path: str | None = None,
        threshold_ms: float = 50.0,
        max_concurrent_drains: int = 3,
    ) -> None:
        self._pool = pool
        self._persistence_path = persistence_path
        self._threshold_ms = threshold_ms
        self._max_concurrent_drains = max_concurrent_drains
        # Key: "{table}:{pk_value}"
        self._queue: dict[str, PendingEntry] = {}
        self._supervisor_task: asyncio.Task[None] | None = None
        self._semaphore = asyncio.Semaphore(max_concurrent_drains)
        # Track when each key first started failing continuously
        self._first_failure_time: dict[str, float] = {}
        # Debounced sync task
        self._pending_sync_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------
    # Public queue API
    # ------------------------------------------------------------------

    def enqueue(
        self,
        table: str,
        pk_column: str,
        pk_value: Any,
        op: str,
        data: dict[str, Any],
        sql: str,
        params: Any,
        dialect: str,
    ) -> str:
        """Add an entry to the queue. Returns entry_id."""
        key = f"{table}:{pk_value}"
        entry_id = str(uuid.uuid4())
        entry: PendingEntry = {
            "entry_id": entry_id,
            "table": table,
            "pk_column": pk_column,
            "pk_value": pk_value,
            "op": op,
            "data": data,
            "sql": sql,
            "params": params,
            "dialect": dialect,
            "queued_at": datetime.now(UTC).isoformat(),
            "attempt_count": 0,
            "last_error": "",
        }
        self._queue[key] = entry
        self._persist_to_disk()
        return entry_id

    def remove_entry(self, table: str, pk_value: Any) -> None:
        """Remove entry from queue on successful write."""
        key = f"{table}:{pk_value}"
        self._queue.pop(key, None)
        self._first_failure_time.pop(key, None)
        self._persist_to_disk()

    def is_pending(self, table: str, pk_value: Any) -> bool:
        """Return True if there is a pending write for this row."""
        return f"{table}:{pk_value}" in self._queue

    def get_pending_for_table(self, table: str) -> list[PendingEntry]:
        """Return all pending entries for a given table."""
        prefix = f"{table}:"
        return [entry for key, entry in self._queue.items() if key.startswith(prefix)]

    # ------------------------------------------------------------------
    # Disk persistence
    # ------------------------------------------------------------------

    def _persist_to_disk(self) -> None:
        """Atomically persist queue to disk via tmp+rename."""
        if self._persistence_path is None:
            return
        try:
            dir_path = os.path.dirname(os.path.abspath(self._persistence_path))
            os.makedirs(dir_path, exist_ok=True)
            tmp_path = self._persistence_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                for entry in self._queue.values():
                    f.write(json.dumps(entry) + "\n")
            os.replace(tmp_path, self._persistence_path)
        except Exception:
            logger.exception("Failed to persist write queue to disk")

    def load_from_disk(self) -> None:
        """Load previously persisted queue from disk."""
        if self._persistence_path is None:
            return
        if not os.path.exists(self._persistence_path):
            return
        try:
            with open(self._persistence_path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    entry: PendingEntry = json.loads(line)
                    key = f"{entry['table']}:{entry['pk_value']}"
                    self._queue[key] = entry
        except Exception:
            logger.exception("Failed to load write queue from disk")

    # ------------------------------------------------------------------
    # Supervisor
    # ------------------------------------------------------------------

    def start_supervisor(self) -> None:
        """Start the background supervisor coroutine."""
        if self._supervisor_task is None or self._supervisor_task.done():
            self._supervisor_task = asyncio.create_task(self._supervisor_loop())

    async def stop_supervisor(self) -> None:
        """Cancel supervisor and flush remaining entries."""
        if self._supervisor_task is not None and not self._supervisor_task.done():
            self._supervisor_task.cancel()
            try:
                await self._supervisor_task
            except asyncio.CancelledError:
                pass
        await self._flush()

    async def _supervisor_loop(self) -> None:
        """Spawn persistent retry tasks for each pending queue entry."""
        active_tasks: dict[str, asyncio.Task[bool]] = {}
        while True:
            # Spawn retry tasks for new entries without an active task
            for key, entry in list(self._queue.items()):
                if key not in active_tasks or active_tasks[key].done():
                    active_tasks[key] = asyncio.create_task(self._write_retry(key, entry))
            # Clean up done tasks for entries no longer in queue
            for key in list(active_tasks.keys()):
                if key not in self._queue and active_tasks[key].done():
                    del active_tasks[key]
            await asyncio.sleep(0.1)

    async def _write_retry(self, key: str, entry: PendingEntry) -> bool:
        """Retry a pending write forever until it succeeds or the key is removed."""
        from declaro_persistum.instrumentation import record_execution
        from declaro_persistum.query.builder import Query
        from declaro_persistum.query.executor import _execute_update, _prepare_query

        backoff = 0.1
        while key in self._queue:
            async with self._semaphore:
                t0 = time.monotonic()
                try:
                    acquire = getattr(self._pool, "acquire_write", self._pool.acquire)
                    q: Query = {"sql": entry["sql"], "params": entry["params"], "dialect": entry["dialect"]}
                    async with acquire() as conn:
                        prepared_sql, prepared_params = _prepare_query(q, conn)
                        await _execute_update(conn, prepared_sql, prepared_params)
                    duration_ms = (time.monotonic() - t0) * 1000
                    record_execution(self._pool, entry["sql"], duration_ms, success=True)
                    self.remove_entry(entry["table"], entry["pk_value"])
                    self._schedule_sync()
                    return True
                except Exception as exc:
                    error_str = str(exc)
                    duration_ms = (time.monotonic() - t0) * 1000
                    record_execution(self._pool, entry["sql"], duration_ms, success=False, error=error_str)
                    if key in self._queue:
                        self._queue[key]["attempt_count"] += 1
                        self._queue[key]["last_error"] = error_str[:200]
                        self._persist_to_disk()
                    self._check_critical_threshold(key, entry, exc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 60.0)
        return True

    def _check_critical_threshold(self, key: str, entry: PendingEntry, exc: Exception) -> None:
        """Log CRITICAL if a key has been failing for longer than the threshold."""
        now = time.monotonic()
        if key not in self._first_failure_time:
            self._first_failure_time[key] = now
        elif now - self._first_failure_time[key] >= _CRITICAL_THRESHOLD_SECONDS:
            logger.critical(
                "WRITE_QUEUE_EXHAUSTED: entry %s for %s:%s has been failing for >1 hour. "
                "Last error: %s",
                entry["entry_id"],
                entry["table"],
                entry["pk_value"],
                str(exc),
            )

    def _schedule_sync(self) -> None:
        """Schedule a debounced sync after a successful write (max one per 500ms)."""
        if self._pending_sync_task is not None and not self._pending_sync_task.done():
            return  # debounce: one sync per 500ms window
        self._pending_sync_task = asyncio.create_task(self._debounced_sync())

    async def _debounced_sync(self) -> None:
        """Wait 500ms then sync the pool if it supports sync."""
        await asyncio.sleep(0.5)
        if hasattr(self._pool, "sync"):
            await self._pool.sync()

    async def _flush(self) -> None:
        """Final drain attempt for all remaining entries on shutdown."""
        if not self._queue:
            return
        logger.info("WriteQueue flushing %d pending entries on shutdown", len(self._queue))
        from declaro_persistum.instrumentation import record_execution
        from declaro_persistum.query.builder import Query
        from declaro_persistum.query.executor import _execute_update, _prepare_query

        for key in list(self._queue.keys()):
            if key not in self._queue:
                continue
            entry = self._queue[key]
            t0 = time.monotonic()
            try:
                acquire = getattr(self._pool, "acquire_write", self._pool.acquire)
                q: Query = {"sql": entry["sql"], "params": entry["params"], "dialect": entry["dialect"]}
                async with acquire() as conn:
                    prepared_sql, prepared_params = _prepare_query(q, conn)
                    await _execute_update(conn, prepared_sql, prepared_params)
                duration_ms = (time.monotonic() - t0) * 1000
                record_execution(self._pool, entry["sql"], duration_ms, success=True)
                self.remove_entry(entry["table"], entry["pk_value"])
            except Exception:
                pass
        self._persist_to_disk()


# ------------------------------------------------------------------
# Pure merge functions (Phase 5)
# ------------------------------------------------------------------


def merge_pending_into_results(
    results: list[dict[str, Any]],
    pending: list[PendingEntry],
    pk_column: str,
) -> list[dict[str, Any]]:
    """
    Merge pending write queue entries into SELECT results.

    - Entries whose PK is already in results replace the DB row (latest wins).
    - Entries not in results are appended (they haven't been written yet).
    - Delete ops are excluded from results.
    """
    existing_pks: dict[Any, int] = {row[pk_column]: i for i, row in enumerate(results)}
    merged = list(results)

    for entry in pending:
        pk_val = entry["pk_value"]
        if entry["op"] == "delete":
            if pk_val in existing_pks:
                del merged[existing_pks[pk_val]]
                # Rebuild index after deletion
                existing_pks = {row[pk_column]: i for i, row in enumerate(merged)}
        elif pk_val in existing_pks:
            merged[existing_pks[pk_val]] = entry["data"]
        else:
            merged.append(entry["data"])

    return merged


def merge_pending_into_join_results(
    results: list[dict[str, Any]],
    pending_by_table: dict[str, list[PendingEntry]],
    schema: dict[str, Any],
    join_tables: list[str],
    base_table: str,
) -> list[dict[str, Any]]:
    """
    FK-aware merge for JOIN results.

    For each joined table, find the FK relationship so we can match
    pending entries to the correct joined rows.
    """
    merged = list(results)

    for table in join_tables:
        pending = pending_by_table.get(table, [])
        if not pending:
            continue
        fk_col, ref_col = _find_fk_relationship(schema, base_table, table)
        if fk_col is None:
            continue
        for entry in pending:
            pk_val = entry["pk_value"]
            matched = False
            for i, row in enumerate(merged):
                if row.get(ref_col) == pk_val:
                    merged[i] = {**row, **entry["data"]}
                    matched = True
                    break
            if not matched and entry["op"] != "delete":
                merged.append(entry["data"])

    return merged


def _find_fk_relationship(
    schema: dict[str, Any],
    from_table: str,
    to_table: str,
) -> tuple[str | None, str | None]:
    """
    Find FK column in from_table that references to_table.

    Returns (fk_column, referenced_column) or (None, None) if not found.
    """
    table_def = schema.get(from_table, {})
    columns = table_def.get("columns", {})
    for col_name, col_def in columns.items():
        ref = col_def.get("references", "")
        if ref and ref.startswith(f"{to_table}."):
            ref_col = ref.split(".", 1)[1]
            return col_name, ref_col
    return None, None


def _extract_join_tables(sql: str) -> list[str]:
    """
    Parse SQL for JOIN clauses and return list of joined table names.

    Works with any JOIN type (INNER, LEFT, RIGHT, FULL, CROSS).
    """
    return re.findall(r"\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql, re.IGNORECASE)


def _extract_order_by(sql: str) -> list[tuple[str, str]]:
    """
    Extract ORDER BY columns from SQL.

    Returns list of (column_name, direction) pairs where direction is "ASC" or "DESC".
    Returns empty list if no ORDER BY clause.
    """
    match = re.search(
        r"\bORDER\s+BY\s+(.+?)(?:\s+LIMIT\b|\s+OFFSET\b|\s*;|\s*$)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return []
    order_clause = match.group(1).strip()
    result: list[tuple[str, str]] = []
    for part in order_clause.split(","):
        part = part.strip()
        if not part:
            continue
        tokens = part.split()
        col = tokens[0].strip("\"'`[]")
        direction = tokens[1].upper() if len(tokens) > 1 and tokens[1].upper() in ("ASC", "DESC") else "ASC"
        result.append((col, direction))
    return result


def _resort(
    results: list[dict[str, Any]],
    order_by: list[tuple[str, str]],
) -> list[dict[str, Any]]:
    """
    Re-sort results according to ORDER BY column/direction pairs.

    Applied in reverse so the first ORDER BY column has final precedence.
    Missing keys sort before present keys.
    """
    if not order_by:
        return results
    # Apply in reverse for stable multi-column sort
    for col, direction in reversed(order_by):
        reverse = direction == "DESC"
        results = sorted(
            results,
            key=lambda r, c=col: (r.get(c) is None, r.get(c)),
            reverse=reverse,
        )
    return results
