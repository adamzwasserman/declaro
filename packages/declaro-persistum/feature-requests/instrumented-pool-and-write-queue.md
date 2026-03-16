# Feature Request: Instrumented Connections + Optimistic Write Queue

**Requested by**: multicardz application layer
**Priority**: High — currently implemented as app-level shims that should live in persistum
**Status**: IMPLEMENTED (2026-03-11)
**Context**: multicardz uses Turso Cloud (libsql) with latencies of 750-1100ms per write from local dev. Production latency TBD but needs ongoing measurement. App currently papers over this with an optimistic write pattern built outside persistum.

---

## 1. Connection-Level Latency Instrumentation

### Problem

Every `conn.execute()` call needs timing data for ongoing latency monitoring. Currently we wrap persistum's pools with `_InstrumentedPool` / `_InstrumentedConnection` proxy classes at the app layer. This is fragile — it intercepts `acquire()`, wraps the returned connection, and delegates everything else via `__getattr__`. It should be a first-class persistum feature.

### Proposed API

```python
from declaro_persistum import ConnectionPool

# Enable instrumentation at pool creation
pool = await ConnectionPool.libsql(url, auth_token=token, instrumentation=True)

# Or configure globally
ConnectionPool.configure_instrumentation(
    enabled=True,
    sink="jsonl",              # "jsonl" | "syslog" | callable
    path="./data/db_latency.jsonl",  # for jsonl sink
    tier_label="project",      # tags every record from this pool
)
```

### What to record per execute()

```json
{
  "ts": "2026-03-10T14:22:01-0500",
  "tier": "project",
  "op": "insert",
  "duration_ms": 842.31,
  "success": true,
  "sql": "INSERT INTO cards (card_id, name, ...",
  "error": ""
}
```

Fields:
- **ts**: ISO 8601 timestamp
- **tier**: Caller-supplied label (e.g. "central", "settings", "project") set at pool creation
- **op**: Classified from SQL — `select`, `insert`, `update`, `delete`, `create`, `alter`, `other`
- **duration_ms**: Wall-clock time for the execute call (not including fetchone/fetchall)
- **success**: bool
- **sql**: First 120 chars of the SQL statement (for diagnosis without leaking full queries)
- **error**: First 200 chars of exception string on failure

### Requirements

- **Zero overhead when disabled**: No timing, no logging, no proxy wrapping. The pool returns raw connections.
- **Covers all backends**: SQLite (aiosqlite), Turso (pyturso), LibSQL (libsql_experimental), PostgreSQL (asyncpg). The instrumentation wraps at the connection protocol level, not per-backend.
- **Covers `execute()` and `executemany()`**: Both timed individually.
- **Pluggable sink**: Start with JSONL file and syslog. Accept a callable `(record: dict) -> None` for custom sinks (Prometheus, StatsD, etc).
- **Separate logger**: Use a dedicated `logging.Logger` (e.g. `declaro_persistum.latency`) with `propagate=False` so it doesn't pollute application logs.
- **Lazy file handler**: Don't create the log file or directory at import time. Create on first write.
- **Thread/async safe**: The JSONL handler must not block the event loop. For syslog, use `SysLogHandler` from stdlib.

### Current App-Level Implementation (to be replaced)

Lives in `apps/shared/services/pool_factory.py`:
- `_InstrumentedPool` — wraps `pool.acquire()` to yield `_InstrumentedConnection`
- `_InstrumentedConnection` — proxies `execute()` / `executemany()` with `time.monotonic()` timing, delegates everything else via `__getattr__`
- `_record_db_latency()` — writes JSONL via a dedicated `logging.Logger`
- `_classify_sql()` — extracts op type from SQL prefix
- Pools are wrapped at creation: `pool = _InstrumentedPool(raw_pool, "central")`

This works but is the wrong layer. Persistum owns the connection lifecycle.

---

## 2. Optimistic Write Queue (Eventual Consistency)

### Problem

When Turso Cloud writes take 750-1100ms, synchronous insert-then-read patterns make the UI unusable. multicardz currently implements an app-level optimistic write queue:

1. Caller submits a write (e.g. create card)
2. Write goes into an in-memory queue + persisted to disk (JSONL/JSON)
3. Caller gets an immediate response (no DB wait)
4. Background `asyncio.create_task()` persists to DB with infinite retry + exponential backoff
5. Read queries merge DB results + queue contents so pending writes appear in results
6. On successful DB write, entry is removed from queue
7. On shutdown, a flush hook attempts final sync persist for all queued items
8. On startup, queue is loaded from disk and retries resume

This pattern is general enough to belong in persistum.

### Proposed API

```python
from declaro_persistum import ConnectionPool, WriteQueue

# Create a write queue backed by a pool
queue = WriteQueue(
    pool=pool,
    persistence_path="./data/pending_writes.jsonl",  # durable queue on disk
    max_backoff=60,           # cap retry backoff at 60s
    flush_on_shutdown=True,   # attempt sync persist on graceful shutdown
)

# Optimistic write — returns immediately, persists in background
entry_id = await queue.submit(
    sql="INSERT INTO cards (card_id, name, workspace_id, user_id) VALUES (?, ?, ?, ?)",
    params=(card_id, name, workspace_id, user_id),
    # Metadata for merging into reads
    merge_key="card_id",
    merge_value=card_id,
    merge_data={"card_id": card_id, "name": name, "workspace_id": workspace_id},
    # Optional callback on successful persist
    on_success=lambda: invalidate_cache(workspace_id),
)

# Check if a specific key is still pending
queue.is_pending("card_id", some_card_id)  # -> bool

# Get all pending entries matching a filter
pending = queue.get_pending(workspace_id=workspace_id, user_id=user_id)

# Merged read — fetches from DB, appends pending entries not yet in results
rows = await queue.merged_query(
    pool=pool,
    sql="SELECT card_id, name FROM cards WHERE workspace_id = ?",
    params=(workspace_id,),
    merge_key="card_id",     # dedup key between DB rows and pending
    pending_filter={"workspace_id": workspace_id},
)

# Lifecycle hooks (call from app startup/shutdown)
await queue.load()           # load pending from disk
await queue.resume()         # spawn retry tasks for loaded entries
await queue.flush()          # shutdown: attempt final sync persist
```

### Queue Internals

- **In-memory dict**: `entry_id -> {sql, params, merge_data, metadata, attempt_count, queued_at}`
- **Disk persistence**: Atomic write (tmp + rename) to JSONL or JSON file. Updated on every add/remove.
- **Retry**: `asyncio.create_task()` per entry. Exponential backoff capped at `max_backoff`. Infinite retries (no max). Each attempt is instrumented (if instrumentation enabled).
- **Dedup on merge**: `merged_query` fetches DB rows, collects their `merge_key` values into a set, then appends pending entries whose `merge_key` value is not in that set.
- **Cleanup**: On successful DB write, entry removed from memory + disk. On startup, load from disk and resume.
- **Flush**: On shutdown, iterate pending entries and attempt one synchronous persist each. Failures remain on disk for next startup.

### Requirements

- **Single-worker safe**: Uses in-memory dict + disk file. No cross-worker coordination needed (multicardz runs single-worker).
- **Backend-agnostic**: Works with any persistum backend (SQLite, Turso, LibSQL, PostgreSQL).
- **Integrates with instrumentation**: Each background retry attempt should appear in the latency log with an `attempt` field.
- **No data loss on crash**: Disk persistence ensures pending writes survive `kill -9`. Atomic file writes (tmp + rename) prevent corruption.
- **Configurable merge**: The caller defines how pending data maps to DB row shape. Persistum doesn't need to understand the schema — it just dedup on the merge key.

### Current App-Level Implementation (to be replaced)

Lives in `apps/shared/services/pending_writes.py`:
- `_pending_cards: dict[str, dict]` — in-memory store
- `_save_to_disk()` / `load_pending()` — JSON file persistence with atomic write
- `add_pending_card()` / `remove_pending_card()` / `get_pending_cards()` / `is_pending()`
- `persist_card_with_retry()` — infinite retry with exponential backoff, runs as `asyncio.create_task()`
- `resume_pending_writes()` — startup hook, spawns tasks for loaded entries
- `flush_pending()` — shutdown hook, attempts final persist

The merge logic lives in the render path (`cards_api.py:compute_card_sets`):
```python
pending = get_pending_cards(user_id, workspace_id)
if pending:
    db_card_ids = {c.id for c in filtered_cards}
    for pc in pending:
        if pc["card_id"] not in db_card_ids:
            # Create card-like object from pending data
            filtered_cards.append(make_card_from_pending(pc))
```

This is card-specific. The persistum version should be generic.

---

## 3. Combined: Instrumented Pool with Write Queue

The two features compose naturally:

```python
pool = await ConnectionPool.libsql(
    url, auth_token=token,
    instrumentation=True,
    tier_label="project",
    latency_sink="jsonl",
    latency_path="./data/db_latency.jsonl",
)

queue = WriteQueue(pool=pool, persistence_path="./data/pending.jsonl")
await queue.load()
await queue.resume()

# App uses queue.submit() for writes, queue.merged_query() for reads
# Every DB call (including retries) appears in latency log
# On shutdown: await queue.flush(); await pool.close()
```

---

## Migration Path

1. Implement instrumentation first (simpler, no state management)
2. Implement WriteQueue second
3. multicardz removes `_InstrumentedPool`, `_InstrumentedConnection` from pool_factory.py
4. multicardz removes `pending_writes.py` entirely
5. multicardz replaces card-specific merge logic with `queue.merged_query()`

---

## Non-Goals

- **Multi-worker coordination**: Not needed. multicardz is single-worker. If needed later, swap disk file for Redis/DB-backed queue.
- **Conflict resolution**: Queue assumes append-only writes (INSERT). UPDATE/DELETE conflict resolution is out of scope.
- **Schema awareness**: Persistum doesn't interpret the merge_data shape. Caller is responsible for mapping pending data to row format.
- **Ordered delivery**: Writes may complete out of order. Caller should not depend on insertion order for correctness.
