# declaro-observe Architecture

> **Event Sourcing for the Functional Python Stack**

Observability without Big State. Every operation is an event. State is derived, never stored.

---

## Philosophy

Traditional observability tools are Big State offenders:
- Global logger singletons with hidden configuration
- Metric registries that accumulate state
- Trace contexts threaded through your entire codebase
- "Just import logging" — and inherit someone else's state

**declaro-observe** takes a different approach:

1. **Events are data** — Immutable TypedDicts, not method calls on global objects
2. **State is derived** — Materialized views over events, not counters you increment
3. **Configuration is declarative** — TOML defines what to observe, not scattered decorators
4. **Middleware is a function** — No class inheritance, no framework coupling

---

## Core Concepts

### Events

An event is an immutable record of something that happened:

```python
Event = TypedDict("Event", {
    "id": str,           # UUID
    "ts": str,           # ISO timestamp
    "type": str,         # request, query, error, custom
    "source": str,       # function/endpoint name
    "payload": dict,     # event-specific data
    "correlation_id": str | None,  # trace correlation
    "duration_ms": int | None,     # for timed events
})
```

Events are **write-once, read-many**. You never update an event. You emit new events.

### Event Store

Events are persisted via `declaro-persistum`:

```toml
# schema/events.toml
[events]
table = "events"

[events.fields]
id = { type = "uuid", primary_key = true }
ts = { type = "timestamp", default = "now" }
type = { type = "str", index = true }
source = { type = "str", index = true }
payload = { type = "json" }
correlation_id = { type = "uuid", nullable = true, index = true }
duration_ms = { type = "int", nullable = true }
```

### Projections (Materialized Views)

Instead of incrementing counters, you define projections over events:

```toml
# schema/projections.toml

[request_counts]
query = """
    SELECT
        date(ts) as day,
        source as endpoint,
        COUNT(*) as count
    FROM events
    WHERE type = 'request'
    GROUP BY 1, 2
"""
materialized = true
refresh = "trigger"
trigger_sources = ["events"]

[error_rates]
query = """
    SELECT
        source as endpoint,
        COUNT(*) FILTER (WHERE payload->>'status' >= '400') as errors,
        COUNT(*) as total
    FROM events
    WHERE type = 'request'
    GROUP BY 1
"""
materialized = true
refresh = "hybrid"
trigger_sources = ["events"]

[slow_queries]
query = """
    SELECT
        source,
        payload->>'query' as query,
        duration_ms,
        ts
    FROM events
    WHERE type = 'query' AND duration_ms > 1000
    ORDER BY duration_ms DESC
    LIMIT 100
"""
materialized = true
refresh = "manual"
```

**This is observability as data, not as side effects.**

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        FastAPI Application                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   Request ──→ observe_middleware ──→ Handler ──→ Response       │
│                      │                   │                       │
│                      ▼                   ▼                       │
│                   emit()              emit()                     │
│                      │                   │                       │
│                      └─────────┬─────────┘                       │
│                                ▼                                 │
│                         Event Buffer                             │
│                                │                                 │
└────────────────────────────────┼────────────────────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────┐
                    │    declaro-persistum   │
                    │                        │
                    │   ┌──────────────┐     │
                    │   │ events table │     │
                    │   └──────────────┘     │
                    │          │             │
                    │          ▼             │
                    │   ┌──────────────┐     │
                    │   │ projections  │     │
                    │   │ (mat views)  │     │
                    │   └──────────────┘     │
                    └────────────────────────┘
```

---

## API Design

### Configuration (TOML)

```toml
# schema/observe.toml

[observe]
# Where to store events
events_table = "events"

# Retention policy
retention = "30d"

# Batch settings (for performance)
buffer_size = 100
flush_interval_ms = 1000

[observe.capture]
# What to automatically capture
requests = true          # HTTP requests/responses
queries = true           # declaro-persistum queries
errors = true            # Unhandled exceptions
custom = ["audit.*"]     # Custom event patterns

[observe.context]
# What to include in every event
include_headers = ["x-request-id", "x-correlation-id"]
include_user = true      # From request.state.user if present
include_timing = true    # Duration for all events

[observe.export]
# Optional: export to external systems
format = "otlp"          # OpenTelemetry Protocol
endpoint = "http://collector:4318"
```

### Creating the Middleware

```python
from declaro_observe import create_observer

# Pure function: config in, middleware out
observe = create_observer("schema/observe.toml")

# Or with explicit config
observe = create_observer({
    "events_table": "events",
    "capture": {"requests": True, "errors": True},
})
```

### FastAPI Integration

```python
from fastapi import FastAPI
from declaro_observe import create_observer

app = FastAPI()
observe = create_observer("schema/observe.toml")

# Add as middleware
app.middleware("http")(observe)

# Or use the integration helper
from declaro_observe.fastapi import init_observe
init_observe(app, "schema/observe.toml")
```

### Emitting Custom Events

```python
from declaro_observe import emit

# Within a request context (correlation_id automatic)
await emit("audit.login", {"user_id": user_id, "ip": ip})

# Explicit event
await emit("batch.complete", {
    "job_id": job_id,
    "records": count,
}, correlation_id=job_correlation_id)
```

### Querying Events

Events are just data in persistum:

```python
from declaro_persistum import query

# Recent errors
errors = await query.select("events").where(
    type="error",
    ts__gte=datetime.now() - timedelta(hours=1)
).all()

# Use projections for aggregates
from declaro_persistum import query

daily_counts = await query.select("request_counts").where(
    day__gte=date.today() - timedelta(days=7)
).all()
```

---

## Module Structure

```
src/declaro_observe/
├── __init__.py          # Public API: create_observer, emit
├── types.py             # Event, ObserveConfig, EmitContext
├── config.py            # load_config(), validate_config()
├── events.py            # emit(), buffer management
├── middleware.py        # create_middleware() for ASGI
├── context.py           # Correlation ID, request context
├── exporters/
│   ├── __init__.py
│   ├── otlp.py          # OpenTelemetry export
│   └── stdout.py        # Development logging
└── integrations/
    ├── __init__.py
    ├── fastapi.py       # FastAPI helpers
    └── persistum.py     # Query observation hooks
```

---

## Key Functions

### Core

| Function | Signature | Purpose |
|----------|-----------|---------|
| `create_observer` | `(config: str \| dict) -> Middleware` | Create configured middleware |
| `emit` | `(type: str, payload: dict, **ctx) -> None` | Emit a custom event |
| `get_correlation_id` | `() -> str \| None` | Get current correlation ID |

### Configuration

| Function | Signature | Purpose |
|----------|-----------|---------|
| `load_config` | `(path: str) -> ObserveConfig` | Load and validate TOML config |
| `validate_config` | `(config: dict) -> list[Error]` | Validate config structure |

### Middleware

| Function | Signature | Purpose |
|----------|-----------|---------|
| `create_middleware` | `(config: ObserveConfig, store: EventStore) -> Middleware` | Create ASGI middleware |
| `wrap_endpoint` | `(fn: Callable, config: ObserveConfig) -> Callable` | Wrap individual endpoint |

### Events

| Function | Signature | Purpose |
|----------|-----------|---------|
| `create_event` | `(type: str, source: str, payload: dict) -> Event` | Create event dict |
| `create_buffer` | `(config: ObserveConfig) -> EventBuffer` | Create buffered writer |
| `flush_buffer` | `(buffer: EventBuffer, store: EventStore) -> int` | Flush pending events |

---

## Event Types

### Built-in Events

| Type | Source | Payload |
|------|--------|---------|
| `request` | endpoint path | `{method, path, status, duration_ms}` |
| `query` | query source | `{query, params, rows, duration_ms}` |
| `error` | exception location | `{type, message, traceback}` |

### Custom Events

Emit any event type matching your `observe.capture.custom` patterns:

```python
# If custom = ["audit.*", "batch.*"]
await emit("audit.login", {...})      # ✓ captured
await emit("batch.start", {...})      # ✓ captured
await emit("debug.trace", {...})      # ✗ not captured
```

---

## Correlation & Tracing

Every event within a request shares a `correlation_id`:

```
Request: GET /users/123
correlation_id: 550e8400-e29b-41d4-a716-446655440000

Events:
├── request.start  {method: GET, path: /users/123}
├── query          {query: SELECT * FROM users WHERE id = $1}
├── query          {query: SELECT * FROM orders WHERE user_id = $1}
└── request.end    {status: 200, duration_ms: 45}
```

Correlation flows through:
- HTTP headers (`x-correlation-id`)
- Background tasks (explicit passing)
- Cross-service calls (header propagation)

---

## Why Event Sourcing?

### Traditional Metrics (Big State)

```python
# Global counter — where does it live? Who resets it?
request_count.inc()
error_count.inc()
latency_histogram.observe(duration)

# Questions you can't answer:
# - What was the error rate at 3:42 PM yesterday?
# - Which user triggered most errors?
# - What was the sequence of events before the crash?
```

### Event Sourcing (Just Data)

```python
# Emit immutable facts
await emit("request", {
    "method": "GET",
    "path": "/users",
    "status": 500,
    "duration_ms": 1234,
    "user_id": "abc",
})

# Answer any question by querying events:
# - Error rate at 3:42 PM? Query events with status >= 400
# - Which user? GROUP BY user_id
# - Sequence before crash? ORDER BY ts WHERE correlation_id = X
```

**Events are facts. Metrics are derived. Facts don't lie.**

---

## Integration with Declaro Stack

```
┌─────────────────────────────────────────────────────────────┐
│                      TOML Schema                             │
│   (models, events, projections — single source of truth)    │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
     ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
     │  persistum  │  │   observe   │  │   ximinez   │
     │   (data)    │  │  (events)   │  │   (types)   │
     └─────────────┘  └─────────────┘  └─────────────┘
              │               │               │
              └───────────────┼───────────────┘
                              ▼
                    ┌─────────────────┐
                    │   declaro-api   │
                    │    (FastAPI)    │
                    └─────────────────┘
```

**observe** uses **persistum** for storage and **ximinez** validates event schemas at pre-commit.

---

## Performance Considerations

1. **Buffered writes** — Events batch before flushing to reduce I/O
2. **Async emit** — `emit()` is non-blocking, buffer flushes in background
3. **Projection refresh** — Use `trigger` for real-time, `manual` for heavy aggregates
4. **Retention policies** — Auto-cleanup old events to bound storage

---

## Future: OpenTelemetry Export

For integration with existing observability infrastructure:

```toml
[observe.export]
format = "otlp"
endpoint = "http://otel-collector:4318"
headers = { "Authorization" = "Bearer ${OTEL_TOKEN}" }
```

Events translate to OTLP:
- `request` events → Spans
- `error` events → Span events with error status
- `query` events → Child spans
- Custom events → Span events

This provides escape hatch to traditional tools while keeping the functional core.
