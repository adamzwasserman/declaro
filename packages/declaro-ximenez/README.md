# declaro-ximenez

**Type enforcement with memorable errors.**

> Nobody expects the Ximénez Inquisition!

## Overview

Ximénez is a preprocessing static type enforcer for Python that delivers uncompromising type safety with a distinctive Monty Python Spanish Inquisition flair.

Its chief weapons are:

1. **Mandatory explicit local typing** — every variable must be typed
2. **Ruthless, unforgettable error messages** — Spanish Inquisition themed
3. **Declaro integration** — validates model usage against TOML schemas
4. **Seamless IDE compatibility** — works with mypy, Pyright, Pylance

## Two Typing Styles

**Inline (default):**

```python
def confess(x: int, y: str) -> float:
    sins: int = 0
    penance: float = 0.0
    return penance
```

**Block declaration:**

```python
def confess(x: int, y: str) -> float:
    types:
        sins: int = 0
        penance: float
        fear: bool = True

    # ... code ...
    return penance
```

## Declaro Integration

Ximénez validates that your Python code uses models correctly according to their TOML schema definitions.

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
username = user["username"]  # No such field!
```

**Error output:**

```text
NOBODY expects a model violation!

Our chief violation is:
- app.py:7:16: 'User' has no field 'username' (did you mean 'name'?)
```

## CLI Flags

| Flag | Behaviour |
|------|-----------|
| (default) | Full dramatic comedy mode |
| `--quiet` | Minimal output |
| `--comfy-chair` | Lenient mode (warnings only) |
| `--rack` | Strict mode |
| `--machine` | CI-friendly plain format |
| `--declaro-schema=PATH` | TOML schema directory |

## Configuration

```toml
# pyproject.toml
[tool.ximenez]
enabled = true
paths = ["src/", "tests/"]

[tool.ximenez.declaro]
enabled = true
schema_paths = ["schema/"]
```

## Pre-Commit Hook

```yaml
repos:
  - repo: https://github.com/adamzwasserman/declaro
    rev: v0.1.0
    hooks:
      - id: ximenez
```

## Error Messages

```text
NOBODY expects a type violation!

Our chief violation is:
- src/app.py:42:5: expected 'int', got 'str'
- src/app.py:47:9: local variable 'count' used without type declaration

...TWO! Our TWO chief violations are fear and surprise!
```

**No violations:**

```text
Dismissed! The accused is free to go.
```

## Part of the Declaro Stack

| Package | Purpose |
|---------|---------|
| `declaro-persistum` | Schema-first database toolkit |
| **`declaro-ximenez`** | Type enforcement with memorable errors |
| `declaro-api` | FastAPI integration |
| `declaro-http` | Functional HTTP client |

---

*Declaro ergo fit.*
