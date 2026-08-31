# Implementation Plan: Instrumented Pool + Write Queue

**Created**: 2026-03-11
**Status**: COMPLETE (2026-03-11)

## Design Decisions (settled)

1. No connection on the surface — pool bound at `table("users", schema, pool=pool)`
2. No raw SQL bypass — everything through query builder
3. No mocks in tests — real databases only
4. Queue is invisible to caller — no merge callbacks
5. Per-write 50ms threshold — `asyncio.wait_for(asyncio.shield(task))` race
6. One queue per pool — keyed by `"{table}:{pk_value}"`
7. Supervisor — one coroutine per pool, concurrency limit 3, 6hr CRITICAL log
8. Transparent read merging — including JOINs via FK knowledge, re-sort after merge
9. Race acquires its own connection — background task keeps running independently

## Phase 1: Pool Binding ✅ COMPLETE

Remove connection from caller surface. Pool bound at table creation.

### Files to modify

- `query/table.py` — `table()` takes `pool` param, `TableProxy` stores `_pool`, passes to all builders
- `query/select.py` — `_pool` in slots, all fluent methods propagate, `execute()` acquires internally
- `query/insert.py` — same pattern
- `query/update.py` — same pattern
- `query/delete.py` — same pattern
- `query/django_style.py` — `QuerySet` gets `_pool`, all chainable methods propagate, terminal methods acquire internally
- `query/prisma_style.py` — same pattern
- All test files — `.execute(conn)` becomes `.execute()`, remove `async with pool.acquire()` from callers

### Key change pattern

```python
# Before
async def execute(self, connection: Any) -> list[dict[str, Any]]:
    ...

# After
async def execute(self) -> list[dict[str, Any]]:
    from declaro_persistum.query.executor import execute_with_pool
    return await execute_with_pool(self._pool, self.to_query, mode="all")
```

## Phase 2: Instrumentation ✅ COMPLETE

Time every execute in executor. Trigger queue when write >50ms.

### Files to create

- `instrumentation.py`:
  - `LatencyRecord` TypedDict: ts, tier, op, duration_ms, success, sql (120 chars), error (200 chars)
  - `classify_sql(sql) -> str` — dict-lookup on first keyword
  - `is_write_op(op) -> bool`
  - `build_record(...)  -> LatencyRecord`
  - `format_jsonl(record) -> str`
  - `get_latency_logger() -> Logger` — `declaro_persistum.latency`, propagate=False
  - `setup_jsonl_sink(logger, path)` — lazy file/dir creation
  - `setup_callable_sink(logger, fn)`

### Files to modify

- `query/executor.py` — add `execute_with_pool()`:
  - Acquires connection from pool
  - Detects dialect
  - Times execution via `time.monotonic()`
  - Records latency if `pool._instrumented`
  - For writes: races against 50ms threshold if `pool._write_queue` exists (Phase 3 hook)

- `pool.py` — `BasePool` gains `_tier`, `_instrumented`, `_write_queue` fields. Factory methods gain `instrumentation`, `tier_label`, `latency_sink`, `latency_path` params.

## Phase 3: Write Queue ✅ COMPLETE

### Files to create

- `write_queue.py`:
  - `PendingEntry` TypedDict: entry_id, table, pk_column, pk_value, op, data, sql, params, dialect, queued_at, attempt_count, last_error
  - `WriteQueue` class:
    - `__init__(pool, persistence_path, threshold_ms=50.0, max_concurrent_drains=3)`
    - `enqueue(table, pk_column, pk_value, op, data, sql, params, dialect) -> str`
    - `remove_entry(table, pk_value)`
    - `is_pending(table, pk_value) -> bool`
    - `get_pending_for_table(table) -> list[PendingEntry]`
    - `_persist_to_disk()` — atomic tmp+rename
    - `load_from_disk()`

### 50ms race in executor.py

```python
async def _race_write(pool, query, queue, table_name, pk_column, pk_value, op, data, mode):
    async def _do_write():
        async with pool.acquire() as conn:
            # execute the write with its own connection
            ...

    write_task = asyncio.create_task(_do_write())
    try:
        result = await asyncio.wait_for(asyncio.shield(write_task), timeout=0.05)
        return result  # fast path, queue never touched
    except asyncio.TimeoutError:
        await queue.enqueue(table_name, pk_column, pk_value, op, data, sql, params, dialect)

        async def _on_complete():
            try:
                await write_task
                await queue.remove_entry(table_name, pk_value)
            except Exception as e:
                # update entry error, supervisor will retry
                ...

        asyncio.create_task(_on_complete())
        return data  # return immediately
```

## Phase 4: Supervisor ✅ COMPLETE

In `write_queue.py`, add to `WriteQueue`:

- `start_supervisor()` — `asyncio.create_task(self._supervisor_loop())`
- `stop_supervisor()` — cancel task, call `_flush()`
- `_supervisor_loop()` — drains queue with semaphore(3), exponential backoff on failure, CRITICAL log after 6 hours continuous failure with prefix `WRITE_QUEUE_EXHAUSTED`
- `_drain_one(key, entry)` — acquire connection, re-execute SQL, remove on success
- `_flush()` — shutdown hook, attempt final persist for all entries

Pool integration:
- Factory methods gain `write_queue_path`, `write_queue_threshold_ms`, `write_queue_concurrency` params
- `pool.close()` calls `queue.stop_supervisor()`

## Phase 5: Read Merging ✅ COMPLETE

Pure functions in `write_queue.py`:

- `merge_pending_into_results(results, pending, pk_column) -> list[dict]` — dedup on PK, append pending not in DB results
- `merge_pending_into_join_results(results, pending_by_table, schema, join_tables, base_table) -> list[dict]` — use FK relationships for cross-table merge
- `_find_fk_relationship(schema, from_table, to_table) -> (fk_col, ref_col)`

After merge, re-sort using the query's ORDER BY columns.

Modify `execute_with_pool` to call merge functions on SELECT results when queue has pending entries.

## Phase 6: Tests ✅ COMPLETE

Real databases only. File: `tests/unit/test_instrumented_pool.py` and `tests/unit/test_write_queue.py`

1. `test_pool_binding_table_requires_pool` — TypeError without pool
2. `test_execute_acquires_from_pool` — insert + select, no conn param
3. `test_instrumentation_records_latency` — callable sink receives LatencyRecord
4. `test_instrumentation_disabled_zero_overhead` — no sink fired
5. `test_classify_sql` — pure function unit tests
6. `test_write_queue_enqueue_and_dequeue` — is_pending true/false
7. `test_write_queue_disk_persistence` — new instance loads from same file
8. `test_write_queue_race_fast_write` — SQLite sub-1ms, queue stays empty
9. `test_write_queue_race_slow_write` — threshold=0.001ms forces queue
10. `test_read_merge_pending_entries` — pending entry in SELECT results
11. `test_read_merge_dedup` — no duplicate when entry also in DB
12. `test_supervisor_drains_queue` — entries drained, rows in DB
13. `test_supervisor_backoff_on_failure` — attempt_count incremented
14. `test_supervisor_critical_log_after_6_hours` — mock time.monotonic
15. `test_pool_close_flushes_queue` — flush attempted on close
16. `test_merge_with_joins` — FK-aware merge across joined tables
17. `test_merge_preserves_order_by` — re-sort after merge

## Phase 7: Exports ✅ COMPLETE

- `__init__.py` — export `WriteQueue`, `PendingEntry`, `LatencyRecord`, `WriteQueueError`
- `exceptions.py` — add `WriteQueueError(PoolError)`
