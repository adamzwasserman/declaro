# declaro-ximinez

**Strict type enforcement for Python.**

Part of the [Declaro](https://github.com/adamzwasserman/declaro) functional Python stack.

## Overview

Ximinez is a preprocessing static type enforcer for Python that provides strict type safety:

1. **Mandatory explicit local typing** — every variable must be typed
2. **Declaro integration** — validates model usage against TOML schemas
3. **Seamless IDE compatibility** — works with mypy, Pyright, Pylance

## Two Typing Styles

**Inline (default):**

```python
def process(x: int, y: str) -> float:
    count: int = 0
    result: float = 0.0
    return result
```

**Block declaration:**

```python
def process(x: int, y: str) -> float:
    types:
        count: int = 0
        result: float

    # ... code ...
    return result
```

## Declaro Integration

Ximinez validates that your Python code uses models correctly according to their TOML schema definitions.

**TOML Schema:**

```toml
# schema/user.toml
[user]
table = "users"

[user.fields]
id = { type = "uuid" }
email = { type = "str" }
name = { type = "str", nullable = true }
```

**Python with violation:**

```python
username = user["username"]  # Error: no such field
```

## CLI Flags

| Flag | Behaviour |
|------|-----------|
| (default) | Standard output |
| `--quiet` | Minimal output |
| `--machine` | CI-friendly plain format |
| `--full` | Full output |
| `--declaro-schema=PATH` | TOML schema directory |

## Configuration

```toml
# pyproject.toml
[tool.ximinez]
enabled = true
paths = ["src/", "tests/"]

[tool.ximinez.declaro]
enabled = true
schema_paths = ["schema/"]
```

## Pre-Commit Hook

```yaml
repos:
  - repo: https://github.com/adamzwasserman/declaro
    rev: v0.1.0
    hooks:
      - id: ximinez
```

## Part of the Declaro Stack

| Package | Purpose |
|---------|---------|
| `declaro-persistum` | Schema-first database toolkit |
| **`declaro-ximinez`** | Type enforcement |
| `declaro-observe` | Event sourcing observability |
| `declaro-api` | FastAPI integration |

---

*Declaro ergo fit.*
