# Ximinez Architectural Design Document

**Project Name:** Ximinez
**Version:** 0.1.0 (Design Specification)
**Date:** December 2025
**Author:** Adam Zachary Wasserman

---

## 1. Overview

Ximinez is a preprocessing static type enforcer for Python that provides strict type safety with mandatory explicit local typing.

The tool functions as a source-to-source transpiler and strict type checker, primarily intended for pre-commit hooks and CI pipelines. It provides stronger static guarantees than existing gradual type checkers while remaining fully compatible with mypy, Pyright/Pylance, and the broader Python ecosystem.

---

## 2. Core Principles

- **No untyped locals** – Every local variable must be explicitly typed.
- **Pythonic to the core** – Significant whitespace, minimal syntax extensions, no terminator keywords.
- **Complementary integration** – Works alongside existing tools, preserves real-time IDE experience.
- **Declaro integration** – Validates model usage against TOML schema definitions.
- **Evolutionary pathway** – Architecture designed to support future stages toward advanced functional programming concepts.

---

## 3. High-Level Architecture

```text
Source Files (.py) ──► Ximinez Preprocessor ──► Transpiled .py (standard typed Python)
                              │
                              ▼
                    Strict Type Checker
                              │
                              ├──► Variable/Function Type Validation
                              │
                              ├──► Declaro Model Validation
                              │         │
                              │         ▼
                              │    TOML Schema Loader
                              │         │
                              │         ▼
                              │    Model Usage Checker
                              │
                              ▼
                    Error Message Engine
                              │
                              ▼
                      Exit Code (0 or 1)
```

---

## 4. Stage 1 Feature Set (MVP)

### 4.1 Supported Typing Styles (Mutually Exclusive per Function)

**A. Inline Style (default, lowest friction)**

```python
def process(x: int, y: str) -> float:
    count: int = 0
    result: float = 0.0
    active: bool = True

    # ...
    return result
```

**B. Declaration Block Style**

```python
def process(x: int, y: str) -> float:
    types:
        count: int = 0
        result: float
        active: bool = True

    # ...
    return result
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

## 5. Declaro Integration

### 5.1 Model Validation at Pre-Commit

Ximinez integrates with `declaro-persistum` to validate that Python code uses models correctly according to their TOML schema definitions.

**What it checks:**

| Check | Description |
|-------|-------------|
| Field existence | `user["username"]` fails if `User` has no `username` field |
| Field types | `user["age"] = "thirty"` fails if `age` is declared as `int` |
| Relationship validity | `user["orders"]` fails if no relationship declared |
| Required fields | Missing required fields on insert/update |

### 5.2 Configuration

```toml
[tool.ximinez]
enabled = true
stage = 1
paths = ["src/", "tests/"]

# Declaro integration
[tool.ximinez.declaro]
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

### 5.4 Query Builder Validation

Ximinez also validates `declaro-persistum` query builder usage:

```python
# VIOLATION: 'users' table has no 'username' column
users = await query.select("users").where(username="bob").all()

# CORRECT: using 'name' field
users = await query.select("users").where(name="bob").all()
```

### 5.5 Pre-Commit Hook

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/adamzwasserman/declaro
    rev: v0.1.0
    hooks:
      - id: ximinez
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

### 6.3 Declaro Schema Loader

- Loads TOML model definitions from configured paths
- Builds in-memory model registry with:
  - Field names and types
  - Relationships and targets
  - Validation rules
- Caches parsed schemas for performance

### 6.4 Model Usage Checker

- Tracks TypedDict/model variable usage
- Validates field access against schema
- Validates relationship traversal
- Suggests corrections for typos (Levenshtein distance)

### 6.5 Transpiler

- Inline style → preserved unchanged.
- `types:` block style → expanded to equivalent inline annotations on first assignment (optional clean mode preserves as comments).

### 6.6 Error Message Engine

Formats violations for output. Multiple output modes supported.

### 6.7 CLI & Modes

| Flag | Behaviour |
|------|-----------|
| (default) | Standard output |
| `--quiet` | Minimal output – count and locations only |
| `--machine` | CI-friendly plain format |
| `--full` | Full output |
| `--declaro-schema=PATH` | Path to TOML schema directory |
| `--no-declaro` | Disable Declaro model validation |

### 6.8 Exit Codes

| Code | Meaning |
|------|---------|
| 0 | No violations |
| 1 | Violations found |
| 2 | Parse/configuration error |

---

## 7. Error Message Format

### 7.1 Violation Format

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

### 7.2 Machine-Readable Output (`--machine`)

```text
{file}:{line}:{col}: error: {message} [XIxxx]
```

Error codes prefixed with `XI`. Model violations use `XIM` prefix.

---

## 8. Configuration

**pyproject.toml:**

```toml
[tool.ximinez]
enabled = true
stage = 1
allow_block_style = true
paths = ["src/", "tests/"]

[tool.ximinez.declaro]
enabled = true
schema_paths = ["schema/"]
strict_models = true
```

**File-level directive:**

```python
# ximinez: enable, style=block
# ximinez: declaro-model=User
```

---

## 9. Integration Strategy

- Pre-commit hook included.
- Transpiled output fully compatible with Pylance/Pyright/mypy.
- Designed for parallel execution with existing checkers.
- Declaro schema validation runs as additional pass.
- Planned migration helper (`ximinez migrate`).

---

## 10. Implementation Roadmap

| Phase | Goals |
|-------|-------|
| 1 | CLI, inline style parsing, symbol table |
| 2 | `types:` block support |
| 3 | Full type compatibility checking |
| 4 | Transpiler & error engine |
| 5 | Declaro schema loader & model validation |
| 6 | Query builder validation |
| 7 | Hooks, config, docs, release 0.1 |

---

## 11. Testing Strategy

- Unit tests for parser, checker, and error engine.
- Integration tests verifying output correctness.
- Round-trip tests ensuring IDE compatibility after transpilation.
- Model validation tests against sample TOML schemas.
- Query validation tests for declaro-persistum integration.

---

## 12. Conclusion

Ximinez provides strict static typing enforcement for Python, closing the largest remaining gradual-typing escape hatch.

With Declaro integration, it extends this enforcement to model usage—ensuring that your code not only has correct types, but uses your data models correctly according to their declared schemas.

---

*Declaro ergo fit.*
