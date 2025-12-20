# declaro-observe

> Event Sourcing Observability for the Functional Python Stack

Part of the [Declaro](https://github.com/adamzwasserman/declaro) functional Python stack.

## Philosophy

Traditional observability tools are Big State offenders — global loggers, metric registries, trace contexts. **declaro-observe** takes a different approach:

- **Events are data** — Immutable TypedDicts, not method calls on global objects
- **State is derived** — Materialized views over events, not counters you increment
- **Configuration is declarative** — TOML defines what to observe
- **Middleware is a function** — No class inheritance, no framework coupling

## Quick Start

```python
from fastapi import FastAPI
from declaro_observe import create_observer

app = FastAPI()

# Pure function: config in, middleware out
observe = create_observer("schema/observe.toml")
app.middleware("http")(observe)
```

Configure in TOML:

```toml
# schema/observe.toml
[observe]
events_table = "events"

[observe.capture]
requests = true
queries = true
errors = true
```

Emit custom events:

```python
from declaro_observe import emit

await emit("audit.login", {"user_id": user_id, "ip": ip})
```

## Why Event Sourcing?

**Traditional metrics:**
```python
request_count.inc()  # Where does this live? Who resets it?
```

**Event sourcing:**
```python
await emit("request", {"path": "/users", "status": 200})
# Query any metric from events. Facts don't lie.
```

## Documentation

See [Architecture](docs/architecture/observe-architecture.md) for full details.

## License

MIT
