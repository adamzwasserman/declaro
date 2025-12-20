# Ximénez Architectural Design Document

**Project Name:** Ximénez
**Version:** 0.1.0 (Design Specification)
**Date:** December 18, 2025
**Author:** Adam Zachary Wasserman

---

## 1. Overview

Ximénez is a preprocessing static type enforcer for Python that delivers uncompromising type safety with a distinctive Monty Python Spanish Inquisition flair.

**Nobody expects the Ximénez Inquisition!**

Its chief weapon is mandatory explicit local typing, delivered in two ergonomic forms:

- Inline annotations on first use
- Upfront `types:` declaration blocks

Its TWO chief weapons are mandatory explicit local typing and ruthless, unforgettable error messages… amongst its weaponry are such diverse elements as mandatory local typing, ruthless error messages, seamless IDE compatibility, Pythonic design, and an almost fanatical devotion to making type violations impossible to ignore…

The tool functions as a source-to-source transpiler and strict type checker, primarily intended for pre-commit hooks and CI pipelines. It provides stronger static guarantees than existing gradual type checkers while remaining fully compatible with mypy, Pyright/Pylance, and the broader Python ecosystem.

---

## 2. Core Principles

- **Nobody expects untyped locals** – Every local variable must be explicitly typed.
- **Pythonic to the core** – Significant whitespace, minimal syntax extensions, no terminator keywords.
- **Complementary integration** – Works alongside existing tools, preserves real-time IDE experience.
- **Declaro integration** – Validates model usage against TOML schema definitions.
- **Evolutionary pathway** – Architecture designed to support future stages toward advanced functional programming concepts.
- **Unexpected humour** – All error output follows the Spanish Inquisition sketch pattern (suppressible via flags).

---

## 3. High-Level Architecture

```text
Source Files (.py) ──► Ximénez Preprocessor ──► Transpiled .py (standard typed Python)
                              │
                              ▼
                    Strict Type Checker
                              │
                              ├──► Variable/Function Type Validation
                              │
                              ├──► Declaro Model Validation (NEW)
                              │         │
                              │         ▼
                              │    TOML Schema Loader
                              │         │
                              │         ▼
                              │    Model Usage Checker
                              │
                              ▼
                Ximénez Error Message Engine
                              │
                              ▼
                  Success → "Dismissed! The accused is free to go."
                  Failure → Spanish Inquisition
```

---

## 4. Stage 1 Feature Set (MVP)

### 4.1 Supported Typing Styles (Mutually Exclusive per Function)

**A. Inline Style (default, lowest friction)**

```python
def confess(x: int, y: str) -> float:
    sins: int = 0
    penance: float = 0.0
    fear: bool = True

    # ...
    return penance
```

**B. Declaration Block Style**

```python
def confess(x: int, y: str) -> float:
    types:
        sins: int = 0
        penance: float
        fear: bool = True

    # ...
    return penance
```

### 4.2 Enforced Rules (Stage 1)

- Function parameters and return type annotation (`-> ReturnType`) are mandatory.
- Every local variable must be explicitly typed exactly once:
  - Inline: on first assignment
  - Block: inside the `types:` block
- If a `types:` block is present:
  - It must be the first statement in the function body (docstrings permitted before).
  - Only one block per function.
  - No inline type annotations allowed in the body.
- Full type compatibility checking using standard Python typing semantics.
- No implicit `Any` for local variables.
- Use-before-declaration and redeclaration forbidden.

---

## 5. Declaro Integration (NEW)

### 5.1 Model Validation at Pre-Commit

Ximénez integrates with `declaro-persistum` to validate that Python code uses models correctly according to their TOML schema definitions.

**What it checks:**

| Check | Description |
|-------|-------------|
| Field existence | `user["username"]` fails if `User` has no `username` field |
| Field types | `user["age"] = "thirty"` fails if `age` is declared as `int` |
| Relationship validity | `user["orders"]` fails if no relationship declared |
| Required fields | Missing required fields on insert/update |

### 5.2 Configuration

```toml
[tool.ximenez]
enabled = true
stage = 1
paths = ["src/", "tests/"]

# Declaro integration
[tool.ximenez.declaro]
enabled = true
schema_paths = ["schema/"]  # Where to find TOML model definitions
strict_models = true         # Require all model access to be validated
```

### 5.3 Model Validation Examples

**TOML Schema:**

```toml
# schema/user.toml
[user]
table = "users"

[user.fields]
id = { type = "uuid" }
email = { type = "str", validate = ["email"] }
name = { type = "str", nullable = true }

[user.relationships]
orders = { type = "has_many", target = "order", foreign_key = "user_id" }
```

**Python Code with Violations:**

```python
def get_user_info(user: User) -> str:
    types:
        username: str
        order_count: int

    username = user["username"]  # VIOLATION: no 'username' field
    order_count = len(user["purchases"])  # VIOLATION: no 'purchases' relationship
    return f"{username}: {order_count}"
```

**Error Output:**

```text
NOBODY expects a model violation!

Our chief violation is:
- app.py:7:16: 'User' has no field 'username' (did you mean 'name'?)
- app.py:8:26: 'User' has no relationship 'purchases' (did you mean 'orders'?)

...TWO! Our TWO chief violations are fear and surprise!
```

### 5.4 Query Builder Validation

Ximénez also validates `declaro-persistum` query builder usage:

```python
# VIOLATION: 'users' table has no 'username' column
users = await query.select("users").where(username="bob").all()

# CORRECT: using 'name' field
users = await query.select("users").where(name="bob").all()
```

**Error Output:**

```text
NOBODY expects a query violation!

Our chief violation is:
- queries.py:12:45: 'users' table has no column 'username' (did you mean 'name'?)

The Inquisition has examined your queries and found them... wanting.
```

### 5.5 Pre-Commit Hook

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/adamzwasserman/declaro
    rev: v0.1.0
    hooks:
      - id: ximenez
        args: [--declaro-schema=schema/]
```

---

## 6. Component Design

### 6.1 Parser / AST Transformer

- Built on `libcst` for formatting-preserving source transformation.
- Recognises `types:` as a custom indented statement block.

### 6.2 Symbol Table & Type Checker

- Per-function symbol table tracking name, declared type, location, initialization, and usage.
- Multi-pass analysis:
  1. Declaration collection
  2. Usage verification
  3. Assignment and operation type compatibility
  4. Coverage (no undeclared locals)

### 6.3 Declaro Schema Loader (NEW)

- Loads TOML model definitions from configured paths
- Builds in-memory model registry with:
  - Field names and types
  - Relationships and targets
  - Validation rules
- Caches parsed schemas for performance

### 6.4 Model Usage Checker (NEW)

- Tracks TypedDict/model variable usage
- Validates field access against schema
- Validates relationship traversal
- Suggests corrections for typos (Levenshtein distance)

### 6.5 Transpiler

- Inline style → preserved unchanged.
- `types:` block style → expanded to equivalent inline annotations on first assignment (optional clean mode preserves as comments).

### 6.6 Ximénez Error Message Engine

Fully implements the Spanish Inquisition-themed error reporting specification (see Section 7).

### 6.7 CLI & Modes

| Flag | Behaviour |
|------|-----------|
| (default) | Full dramatic Ximénez comedy mode |
| `--quiet` | Minimal output – count and locations only |
| `--comfy-chair` | Lenient mode – violations as warnings, never fails |
| `--rack` | Strict mode – promotes warnings to errors |
| `--machine` | CI-friendly plain format |
| `--declaro-schema=PATH` | Path to TOML schema directory |
| `--no-declaro` | Disable Declaro model validation |

### 6.8 Exit Codes

| Code | Meaning |
|------|---------|
| 0 | No violations → "Dismissed! The accused is free to go." |
| 1 | Violations found |
| 2 | Parse/configuration error → "Cardinal Biggles! Fetch... THE DOCUMENTATION!" |

---

## 7. Error Message Specification

### 7.1 Core Principle

All error reporting follows Cardinal Ximénez's enumeration pattern: **always off by one**. Confidently announce a count, then list one more than announced, realize the mistake, restart with correction. This is the core joke.

### 7.2 Message Structure

**One Violation (announces chief, lists one — baseline):**

```text
NOBODY expects a type violation!

Our chief violation is:
- {violation 1}
```

**Two Violations (announces ONE, lists TWO):**

```text
NOBODY expects a type violation!

Our chief violation is:
- {violation 1}
- {violation 2}

...TWO! Our TWO chief violations are fear and surprise!
```

**Three Violations (announces TWO, lists THREE):**

```text
NOBODY expects a type violation!

Our TWO chief violations are:
- {violation 1}
- {violation 2}
- {violation 3}

...THREE! Our THREE chief violations are fear, surprise, and ruthless efficiency!
```

**Four Violations (announces THREE, lists FOUR — classic restart):**

```text
NOBODY expects a type violation!

Our THREE chief violations are:
- {violation 1}
- {violation 2}
- {violation 3}
- {violation 4}

...FOUR! Amongst our violations...
I'll come in again.

NOBODY expects a type violation!

Our FOUR chief violations are:
- {all four violations}
...and a fanatical devotion to the Pope.
```

**Five or More Violations (escalating chaos):**

```text
NOBODY expects a type violation!

Our FOUR chief violations are:
- {violation 1}
- {violation 2}
- {violation 3}
- {violation 4}
- {violation 5}

...FIVE! Our FIVE... no...

Amongst our violations are such diverse elements as:
- {first five violations}
...I'll come in again.

Cardinal Biggles, read the charges.

{remaining violations listed plainly}

...and an almost fanatical devotion to the Pope.
```

### 7.3 Model Violation Messages (NEW)

```text
NOBODY expects a model violation!

Our chief violation is:
- {file}:{line}:{col}: '{Model}' has no field '{field}' (did you mean '{suggestion}'?)

The Inquisition has examined your models and found them... heretical.
```

### 7.4 Violation Format

```text
{file}:{line}:{col}: {message}
```

Common messages:

- `expected {expected_type}, got {actual_type}`
- `local variable {name} used without type declaration`
- `inline annotation not allowed when 'types:' block is present`
- `missing return type annotation`
- `'{Model}' has no field '{field}'`
- `'{Model}' has no relationship '{rel}'`
- `'{table}' has no column '{col}'`

### 7.5 Special Cases

**No Violations:**

```text
Dismissed! The accused is free to go.
```

**Parse / Configuration Error:**

```text
Cardinal Biggles! Fetch... THE DOCUMENTATION!

{file}:{line}: {error details}
```

**Schema Not Found:**

```text
Cardinal Fang! The sacred texts are missing!

Could not load schema from: {path}
```

### 7.6 Machine-Readable Output (`--machine`)

```text
{file}:{line}:{col}: error: {message} [XIxxx]
```

Error codes prefixed with `XI`. Model violations use `XIM` prefix.

---

## 8. Configuration

**pyproject.toml:**

```toml
[tool.ximenez]
enabled = true
stage = 1
allow_block_style = true
paths = ["src/", "tests/"]

[tool.ximenez.declaro]
enabled = true
schema_paths = ["schema/"]
strict_models = true
```

**File-level directive:**

```python
# ximenez: enable, style=block
# ximenez: declaro-model=User
```

---

## 9. Integration Strategy

- Pre-commit hook included.
- Transpiled output fully compatible with Pylance/Pyright/mypy.
- Designed for parallel execution with existing checkers.
- Declaro schema validation runs as additional pass.
- Planned migration helper (`ximenez migrate`).

---

## 10. Implementation Roadmap

| Phase | Goals | Estimated Effort |
|-------|-------|------------------|
| 1 | CLI, inline style parsing, symbol table | 1–2 weeks |
| 2 | `types:` block support | 1 week |
| 3 | Full type compatibility checking | 2–3 weeks |
| 4 | Transpiler & complete Ximénez error engine | 2 weeks |
| 5 | **Declaro schema loader & model validation** | 2 weeks |
| 6 | **Query builder validation** | 1 week |
| 7 | Hooks, config, docs, release 0.1 | 2 weeks |

**Estimated MVP:** 3–4 months part-time.

---

## 11. Testing Strategy

- Unit tests for parser, checker, and error engine.
- Integration tests verifying sketch-accurate dramatic output.
- Round-trip tests ensuring IDE compatibility after transpilation.
- **Model validation tests** against sample TOML schemas.
- **Query validation tests** for declaro-persistum integration.

---

## 12. Conclusion

Ximénez provides the strongest practical static typing enforcement available for Python today, closing the largest remaining gradual-typing escape hatch while making violations hilariously memorable.

With Declaro integration, it extends this enforcement to model usage—ensuring that your code not only has correct types, but uses your data models correctly according to their declared schemas.

Our chief weapons are type safety, explicit local annotations, dual ergonomic styles, model validation, and unexpected humour… our four chief weapons are type safety, explicit annotations, dual styles, model validation, unexpected humour, and seamless ecosystem integration… amongst our weaponry…

*Declaro ergo fit.*

Dismissed!
