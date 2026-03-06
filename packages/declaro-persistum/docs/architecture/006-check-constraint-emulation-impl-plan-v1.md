# CHECK Constraint Emulation Implementation Plan

**Document Version**: 1.1
**Date**: 2026-02-01
**Updated**: 2026-03-06
**Status**: Superseded
**Architecture Doc**: 005-check-constraint-emulation-v1.md

---

**IMPLEMENTATION STATUS**: SUPERSEDED
**LAST VERIFIED**: 2026-03-06
**IMPLEMENTATION EVIDENCE**: Turso Database (Rust) now supports CHECK constraints natively. The Turso applier emits CHECK clauses in SQL directly. Python-side emulation is no longer needed.

---

> **UPDATE (2026-03-06)**: This implementation plan is **superseded**. Turso Database (Rust) now
> supports CHECK constraints natively. Instead of building Python-side emulation (Phases 5-6),
> the Turso applier was updated to emit CHECK clauses in SQL — the same approach used by
> SQLite and PostgreSQL. Phases 1-4 (parser, validator, registry) were partially implemented
> in `check_compat.py` and remain available for any future backend lacking CHECK support.
> Phase 5 (Turso applier modification) was implemented in reverse — CHECK is now **included**
> in SQL rather than stripped.
>
> This document is preserved for historical context.

## Overview

~~This implementation plan follows an 8-step process to build the CHECK constraint emulation layer for Turso Database (Rust/pyturso).~~ **Superseded** — Turso now supports CHECK natively; emulation is unnecessary.

**Total Estimated Effort**: ~~3-4 days~~ N/A (superseded)
**Risk Level**: ~~Medium (core infrastructure change)~~ N/A

---

## Phase 1: Core Types and Exceptions

**Objective**: Define data types and exceptions for CHECK constraint handling

**Duration**: 2-4 hours

### Tasks

#### 1.1 Create check_compat.py Module Structure

**File**: `src/declaro_persistum/abstractions/check_compat.py`

```python
"""
CHECK constraint compatibility layer for Turso Database (Rust).

Turso Database (Rust/pyturso) cannot parse CHECK constraints in CREATE TABLE SQL.
This module provides:
1. CHECK expression parsing into AST
2. Python validator generation from AST
3. Validator registry for runtime enforcement
4. Monitoring counters for usage tracking

Pattern follows pragma_compat.py:
- Try native first (PostgreSQL, SQLite)
- Fall back to Python validation (Turso)
- Same behavior, different enforcement mechanism
"""

from __future__ import annotations

import logging
import re
from collections.abc import Callable, Mapping
from typing import Any, Literal, TypedDict

logger = logging.getLogger(__name__)


# =============================================================================
# Type Definitions
# =============================================================================


class CheckAST(TypedDict, total=False):
    """
    Abstract syntax tree node for CHECK expression.

    Attributes:
        op: Operation type
        left: Left operand (column name or nested AST)
        right: Right operand (value or nested AST)
        operator: Comparison operator for 'compare' op
        values: List of values for 'in' op
        operand: Single operand for unary ops
    """
    op: Literal[
        "compare",
        "between",
        "in",
        "and",
        "or",
        "not",
        "is_null",
        "is_not_null",
    ]
    left: str | dict[str, Any]
    right: Any | dict[str, Any]
    operator: str
    values: list[Any]
    operand: str | dict[str, Any]


# Validator function signature: (row_dict) -> (is_valid, error_message_or_none)
ValidatorFn = Callable[[Mapping[str, Any]], tuple[bool, str | None]]


class ValidationResult(TypedDict):
    """Result of row validation."""
    valid: bool
    errors: list[str]
    table: str
    operation: str


# =============================================================================
# Exceptions
# =============================================================================


class CheckParseError(Exception):
    """Raised when a CHECK expression cannot be parsed."""

    def __init__(self, expression: str, reason: str):
        self.expression = expression
        self.reason = reason
        super().__init__(f"Cannot parse CHECK: {expression!r} - {reason}")


class CheckValidationError(Exception):
    """Raised when CHECK constraint validation fails."""

    def __init__(
        self,
        table: str,
        column: str,
        expression: str,
        row: Mapping[str, Any],
        message: str,
    ):
        self.table = table
        self.column = column
        self.expression = expression
        self.row = dict(row)
        self.message = message
        super().__init__(message)
```

### Validation

```bash
uv run python -c "from declaro_persistum.abstractions.check_compat import CheckAST, CheckParseError, CheckValidationError; print('Types OK')"
```

---

## Phase 2: CHECK Expression Parser

**Objective**: Implement parser for SQL CHECK expressions

**Duration**: 4-6 hours

**Depends on**: Phase 1

### Tasks

#### 2.1 Implement Tokenizer

```python
# Token types for lexer
_TOKEN_PATTERNS = [
    ("WHITESPACE", r"\s+"),
    ("LPAREN", r"\("),
    ("RPAREN", r"\)"),
    ("COMMA", r","),
    ("STRING", r"'[^']*'|\"[^\"]*\""),
    ("NUMBER", r"-?\d+(?:\.\d+)?"),
    ("OP_CMP", r"<=|>=|<>|!=|=|<|>"),
    ("KEYWORD", r"\b(?:AND|OR|NOT|IN|BETWEEN|IS|NULL)\b", re.IGNORECASE),
    ("IDENT", r"[a-zA-Z_][a-zA-Z0-9_]*"),
]


def _tokenize(expression: str) -> list[tuple[str, str]]:
    """
    Tokenize a CHECK expression.

    Args:
        expression: SQL CHECK expression

    Returns:
        List of (token_type, token_value) tuples
    """
    ...
```

#### 2.2 Implement Recursive Descent Parser

```python
def parse_check_expression(
    expression: str,
    column_name: str,
) -> CheckAST:
    """
    Parse a SQL CHECK expression into an AST.

    Supported expressions:
    - Comparison: col <= value, col >= other_col
    - Range: col BETWEEN a AND b
    - Set membership: col IN ('a', 'b', 'c')
    - Null checks: col IS NULL, col IS NOT NULL
    - Boolean logic: expr AND expr, expr OR expr, NOT expr

    Args:
        expression: SQL CHECK expression string
        column_name: Column this constraint is attached to

    Returns:
        CheckAST representing the parsed expression

    Raises:
        CheckParseError: If expression cannot be parsed

    Examples:
        >>> parse_check_expression("start <= end", "end")
        {'op': 'compare', 'left': 'start', 'operator': '<=', 'right': 'end'}

        >>> parse_check_expression("status IN ('active', 'inactive')", "status")
        {'op': 'in', 'left': 'status', 'values': ['active', 'inactive']}
    """
    tokens = _tokenize(expression)
    tokens = [(t, v) for t, v in tokens if t != "WHITESPACE"]

    if not tokens:
        raise CheckParseError(expression, "Empty expression")

    ast, remaining = _parse_or_expr(tokens, expression)

    if remaining:
        raise CheckParseError(
            expression,
            f"Unexpected tokens after expression: {remaining}"
        )

    return ast


def _parse_or_expr(
    tokens: list[tuple[str, str]],
    original: str,
) -> tuple[CheckAST, list[tuple[str, str]]]:
    """Parse OR expressions (lowest precedence)."""
    ...


def _parse_and_expr(
    tokens: list[tuple[str, str]],
    original: str,
) -> tuple[CheckAST, list[tuple[str, str]]]:
    """Parse AND expressions."""
    ...


def _parse_not_expr(
    tokens: list[tuple[str, str]],
    original: str,
) -> tuple[CheckAST, list[tuple[str, str]]]:
    """Parse NOT expressions."""
    ...


def _parse_primary(
    tokens: list[tuple[str, str]],
    original: str,
) -> tuple[CheckAST, list[tuple[str, str]]]:
    """Parse primary expressions (comparisons, IN, BETWEEN, IS NULL)."""
    ...
```

### Unit Tests

```python
# tests/unit/test_check_compat_parser.py

import pytest
from declaro_persistum.abstractions.check_compat import (
    parse_check_expression,
    CheckParseError,
)


class TestParseComparison:
    def test_simple_comparison(self):
        ast = parse_check_expression("start <= end", "end")
        assert ast["op"] == "compare"
        assert ast["left"] == "start"
        assert ast["operator"] == "<="
        assert ast["right"] == "end"

    def test_value_comparison(self):
        ast = parse_check_expression("age >= 18", "age")
        assert ast["op"] == "compare"
        assert ast["left"] == "age"
        assert ast["operator"] == ">="
        assert ast["right"] == 18


class TestParseIn:
    def test_string_values(self):
        ast = parse_check_expression("status IN ('a', 'b', 'c')", "status")
        assert ast["op"] == "in"
        assert ast["left"] == "status"
        assert ast["values"] == ["a", "b", "c"]


class TestParseBetween:
    def test_numeric_between(self):
        ast = parse_check_expression("price BETWEEN 0 AND 1000", "price")
        assert ast["op"] == "between"
        assert ast["left"] == "price"
        assert ast["low"] == 0
        assert ast["high"] == 1000


class TestParseCompound:
    def test_and(self):
        ast = parse_check_expression("a > 0 AND b > 0", "a")
        assert ast["op"] == "and"

    def test_or(self):
        ast = parse_check_expression("a > 0 OR b > 0", "a")
        assert ast["op"] == "or"

    def test_not(self):
        ast = parse_check_expression("NOT (x = y)", "x")
        assert ast["op"] == "not"


class TestParseErrors:
    def test_empty_expression(self):
        with pytest.raises(CheckParseError):
            parse_check_expression("", "col")

    def test_invalid_syntax(self):
        with pytest.raises(CheckParseError):
            parse_check_expression("start <= <= end", "end")
```

### Validation

```bash
uv run pytest tests/unit/test_check_compat_parser.py -xvs
```

---

## Phase 3: Validator Generator

**Objective**: Generate Python validator functions from AST

**Duration**: 3-4 hours

**Depends on**: Phase 2

### Tasks

#### 3.1 Implement Validator Generation

```python
def generate_validator(
    ast: CheckAST,
    table_name: str,
    column_name: str,
    expression: str,
) -> ValidatorFn:
    """
    Generate a Python validator function from a CHECK AST.

    The validator takes a row dict and returns (is_valid, error_message).
    Error message is None if valid, descriptive string if invalid.

    Args:
        ast: Parsed CHECK expression AST
        table_name: Table name for error messages
        column_name: Column name for error messages
        expression: Original expression for error messages

    Returns:
        Validator function (row_dict) -> (bool, str | None)
    """
    op = ast.get("op")

    if op == "compare":
        return _gen_compare_validator(ast, table_name, column_name, expression)
    elif op == "in":
        return _gen_in_validator(ast, table_name, column_name, expression)
    elif op == "between":
        return _gen_between_validator(ast, table_name, column_name, expression)
    elif op == "is_null":
        return _gen_is_null_validator(ast, table_name, column_name, expression)
    elif op == "is_not_null":
        return _gen_is_not_null_validator(ast, table_name, column_name, expression)
    elif op == "and":
        return _gen_and_validator(ast, table_name, column_name, expression)
    elif op == "or":
        return _gen_or_validator(ast, table_name, column_name, expression)
    elif op == "not":
        return _gen_not_validator(ast, table_name, column_name, expression)
    else:
        # Unknown op - return always-valid validator with warning
        logger.warning(
            f"Unknown CHECK op '{op}' for {table_name}.{column_name}, skipping validation"
        )
        return lambda row: (True, None)


def _gen_compare_validator(
    ast: CheckAST,
    table_name: str,
    column_name: str,
    expression: str,
) -> ValidatorFn:
    """Generate validator for comparison expressions."""
    left = ast["left"]
    right = ast["right"]
    operator = ast["operator"]

    # Map operators to Python comparisons
    ops = {
        "=": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
        "<>": lambda a, b: a != b,
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
    }

    compare_fn = ops.get(operator)
    if not compare_fn:
        return lambda row: (True, None)  # Unknown operator, skip

    def validator(row: Mapping[str, Any]) -> tuple[bool, str | None]:
        # Get values
        left_val = row.get(left) if isinstance(left, str) else left
        right_val = row.get(right) if isinstance(right, str) else right

        # NULL handling: NULL comparisons always false (SQL semantics)
        if left_val is None or right_val is None:
            return (True, None)  # NULL passes CHECK (standard SQL)

        try:
            if compare_fn(left_val, right_val):
                return (True, None)
            else:
                return (
                    False,
                    f"CHECK constraint failed: {table_name}.{column_name} "
                    f"requires {expression}"
                )
        except TypeError:
            # Incompatible types
            return (True, None)  # Skip validation on type error

    return validator


# Similar implementations for _gen_in_validator, _gen_between_validator, etc.
```

#### 3.2 Implement Compound Validators

```python
def compile_row_validator(
    validators: frozenset[tuple[str, ValidatorFn]],
) -> ValidatorFn:
    """
    Compile multiple column validators into a single row validator.

    Runs all validators and collects errors.

    Args:
        validators: Frozenset of (column_name, validator_fn) tuples

    Returns:
        Combined validator function
    """
    def combined_validator(row: Mapping[str, Any]) -> tuple[bool, str | None]:
        errors: list[str] = []

        for col_name, validator in validators:
            is_valid, error = validator(row)
            if not is_valid and error:
                errors.append(error)

        if errors:
            return (False, "; ".join(errors))
        return (True, None)

    return combined_validator
```

### Unit Tests

```python
# tests/unit/test_check_compat_validator.py

import pytest
from declaro_persistum.abstractions.check_compat import (
    parse_check_expression,
    generate_validator,
)


class TestCompareValidator:
    def test_column_comparison_valid(self):
        ast = parse_check_expression("start <= end", "end")
        validator = generate_validator(ast, "events", "end", "start <= end")

        is_valid, error = validator({"start": 5, "end": 10})
        assert is_valid
        assert error is None

    def test_column_comparison_invalid(self):
        ast = parse_check_expression("start <= end", "end")
        validator = generate_validator(ast, "events", "end", "start <= end")

        is_valid, error = validator({"start": 10, "end": 5})
        assert not is_valid
        assert "CHECK constraint failed" in error

    def test_null_handling(self):
        ast = parse_check_expression("start <= end", "end")
        validator = generate_validator(ast, "events", "end", "start <= end")

        # NULL should pass (SQL semantics)
        is_valid, error = validator({"start": None, "end": 10})
        assert is_valid


class TestInValidator:
    def test_valid_value(self):
        ast = parse_check_expression("status IN ('a', 'b')", "status")
        validator = generate_validator(ast, "orders", "status", "status IN ('a', 'b')")

        is_valid, error = validator({"status": "a"})
        assert is_valid

    def test_invalid_value(self):
        ast = parse_check_expression("status IN ('a', 'b')", "status")
        validator = generate_validator(ast, "orders", "status", "status IN ('a', 'b')")

        is_valid, error = validator({"status": "c"})
        assert not is_valid
```

### Validation

```bash
uv run pytest tests/unit/test_check_compat_validator.py -xvs
```

---

## Phase 4: Validator Registry

**Objective**: Implement module-level registry with monitoring

**Duration**: 2-3 hours

**Depends on**: Phase 3

### Tasks

#### 4.1 Implement Registry

```python
# =============================================================================
# Validator Registry (Module-Level State)
# =============================================================================

# Registry: (table, column) -> (expression, validator_fn)
_validator_registry: dict[tuple[str, str], tuple[str, ValidatorFn]] = {}

# Monitoring counters
_validation_counters = {
    "checks_registered": 0,
    "validations_run": 0,
    "validations_passed": 0,
    "validations_failed": 0,
}

_affected_tables: set[str] = set()


def register_check_constraint(
    table: str,
    column: str,
    expression: str,
) -> None:
    """
    Register a CHECK constraint for Python-side validation.

    Parses the expression and stores the generated validator.

    Args:
        table: Table name
        column: Column name
        expression: SQL CHECK expression
    """
    try:
        ast = parse_check_expression(expression, column)
        validator = generate_validator(ast, table, column, expression)

        _validator_registry[(table, column)] = (expression, validator)
        _validation_counters["checks_registered"] += 1
        _affected_tables.add(table)

        logger.info(
            f"Registered CHECK emulation: {table}.{column} -> {expression}"
        )
    except CheckParseError as e:
        logger.warning(
            f"Cannot parse CHECK for {table}.{column}: {e.reason}. "
            f"Constraint will not be enforced in Python."
        )


def get_table_validators(
    table: str,
) -> frozenset[tuple[str, str, ValidatorFn]]:
    """
    Get all validators for a table.

    Returns:
        Frozenset of (column_name, expression, validator_fn) tuples
    """
    result: list[tuple[str, str, ValidatorFn]] = []

    for (tbl, col), (expr, validator) in _validator_registry.items():
        if tbl == table:
            result.append((col, expr, validator))

    return frozenset(result)


def validate_row(
    table: str,
    row: Mapping[str, Any],
    *,
    operation: str = "INSERT",
) -> tuple[bool, list[str]]:
    """
    Validate a row against all registered CHECK constraints for a table.

    Args:
        table: Table name
        row: Row data as dict
        operation: Operation type for error messages ("INSERT" or "UPDATE")

    Returns:
        Tuple of (is_valid, list_of_error_messages)
    """
    _validation_counters["validations_run"] += 1

    validators = get_table_validators(table)
    if not validators:
        _validation_counters["validations_passed"] += 1
        return (True, [])

    errors: list[str] = []

    for col_name, expression, validator in validators:
        is_valid, error = validator(row)
        if not is_valid and error:
            errors.append(error)

    if errors:
        _validation_counters["validations_failed"] += 1
        return (False, errors)

    _validation_counters["validations_passed"] += 1
    return (True, [])


def clear_registry() -> None:
    """Clear all registered validators (for testing)."""
    global _validator_registry, _validation_counters, _affected_tables
    _validator_registry = {}
    _validation_counters = {k: 0 for k in _validation_counters}
    _affected_tables = set()


def get_validation_stats() -> dict[str, Any]:
    """Get validation statistics for monitoring."""
    return {
        **_validation_counters,
        "affected_tables": list(_affected_tables),
        "total_constraints": len(_validator_registry),
    }


def get_affected_tables() -> set[str]:
    """Get set of tables with registered CHECK emulation."""
    return _affected_tables.copy()
```

### Unit Tests

```python
# tests/unit/test_check_compat_registry.py

import pytest
from declaro_persistum.abstractions.check_compat import (
    register_check_constraint,
    validate_row,
    clear_registry,
    get_validation_stats,
)


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear registry before each test."""
    clear_registry()
    yield
    clear_registry()


class TestRegistry:
    def test_register_and_validate(self):
        register_check_constraint("events", "end", "start <= end")

        is_valid, errors = validate_row("events", {"start": 5, "end": 10})
        assert is_valid
        assert not errors

    def test_validation_failure(self):
        register_check_constraint("events", "end", "start <= end")

        is_valid, errors = validate_row("events", {"start": 10, "end": 5})
        assert not is_valid
        assert len(errors) == 1

    def test_no_constraints_passes(self):
        is_valid, errors = validate_row("unknown_table", {"a": 1})
        assert is_valid
        assert not errors

    def test_stats_tracking(self):
        register_check_constraint("t", "c", "c > 0")
        validate_row("t", {"c": 5})
        validate_row("t", {"c": -1})

        stats = get_validation_stats()
        assert stats["checks_registered"] == 1
        assert stats["validations_run"] == 2
        assert stats["validations_passed"] == 1
        assert stats["validations_failed"] == 1
```

### Validation

```bash
uv run pytest tests/unit/test_check_compat_registry.py -xvs
```

---

## Phase 5: Turso Applier Modification

**Objective**: Modify Turso applier to skip CHECK SQL and register validators

**Duration**: 2-3 hours

**Depends on**: Phase 4

### Tasks

#### 5.1 Modify TursoApplier._column_definition

```python
# In applier/turso.py

def _column_definition(self, name: str, col: Column, table: str | None = None) -> str:
    """Generate column definition for CREATE TABLE."""
    sql_type = self._map_type(col.get("type", "text"))
    parts = [f'"{name}"', sql_type]

    if col.get("primary_key"):
        parts.append("PRIMARY KEY")

    if col.get("nullable") is False:
        parts.append("NOT NULL")

    if col.get("unique"):
        parts.append("UNIQUE")

    if "default" in col:
        parts.append(f"DEFAULT {col['default']}")

    # CHECK constraint handling - SKIP for Turso, register for Python validation
    # Note: Do NOT add CHECK clause to SQL - Turso parser rejects it
    # Registration happens in _create_table_sql where we have table context

    if "references" in col:
        ref = col["references"]
        ref_table, ref_col = ref.split(".")
        fk_sql = f'REFERENCES "{ref_table}"("{ref_col}")'

        if col.get("on_delete"):
            fk_sql += f" ON DELETE {col['on_delete'].upper().replace('_', ' ')}"
        if col.get("on_update"):
            fk_sql += f" ON UPDATE {col['on_update'].upper().replace('_', ' ')}"

        parts.append(fk_sql)

    return " ".join(parts)
```

#### 5.2 Add CHECK Registration in _create_table_sql

```python
def _create_table_sql(self, table: str, details: dict[str, Any]) -> str:
    """Generate CREATE TABLE statement."""
    columns = details.get("columns", {})
    primary_key = details.get("primary_key", [])

    # Register CHECK constraints for Python-side validation
    self._register_table_checks(table, columns)

    col_defs: list[str] = []

    for col_name, col_def in columns.items():
        col_sql = self._column_definition(col_name, col_def, table=table)
        col_defs.append(col_sql)

    if primary_key and len(primary_key) > 1:
        pk_cols = ", ".join(f'"{c}"' for c in primary_key)
        col_defs.append(f"PRIMARY KEY ({pk_cols})")

    columns_sql = ",\n    ".join(col_defs)
    return f'CREATE TABLE "{table}" (\n    {columns_sql}\n)'


def _register_table_checks(
    self,
    table: str,
    columns: dict[str, Column],
) -> None:
    """Register CHECK constraints for Python-side validation."""
    from declaro_persistum.abstractions.check_compat import register_check_constraint

    for col_name, col_def in columns.items():
        if "check" in col_def:
            register_check_constraint(table, col_name, col_def["check"])
```

### Integration Test

```python
# tests/integration/test_turso_check_emulation.py

import pytest


@pytest.mark.turso
async def test_create_table_no_check_syntax(turso_connection):
    """Verify CHECK constraint doesn't appear in SQL."""
    from declaro_persistum.applier.turso import TursoApplier
    from declaro_persistum.abstractions.check_compat import clear_registry

    clear_registry()

    applier = TursoApplier()

    operations = [
        {
            "op": "create_table",
            "table": "events",
            "details": {
                "columns": {
                    "id": {"type": "integer", "primary_key": True},
                    "start": {"type": "integer"},
                    "end": {
                        "type": "integer",
                        "check": "start <= end",
                    },
                },
            },
        }
    ]

    sql_statements = applier.generate_sql(operations, [0])

    # SQL should NOT contain CHECK
    assert "CHECK" not in sql_statements[0]
    assert "start <= end" not in sql_statements[0]
```

### Validation

```bash
uv run pytest tests/integration/test_turso_check_emulation.py -xvs -m turso
```

---

## Phase 6: Query Layer Integration

**Objective**: Add validation hooks to InsertQuery and UpdateQuery for Turso

**Duration**: 2-3 hours

**Depends on**: Phase 5

### Tasks

#### 6.1 Modify InsertQuery.execute

```python
# In query/insert.py

async def execute(self, connection: Any) -> list[dict[str, Any]]:
    """Execute insert and return results (if RETURNING specified)."""
    from declaro_persistum.query.executor import execute

    dialect = _detect_dialect(connection)

    # For Turso, validate CHECK constraints before execution
    if dialect == "turso":
        from declaro_persistum.abstractions.check_compat import (
            validate_row,
            CheckValidationError,
        )

        is_valid, errors = validate_row(self._table, self._values, operation="INSERT")
        if not is_valid:
            raise CheckValidationError(
                table=self._table,
                column="(multiple)",
                expression="(see errors)",
                row=self._values,
                message=f"INSERT validation failed: {'; '.join(errors)}",
            )

    return await execute(self.to_query(dialect), connection)
```

#### 6.2 Modify UpdateQuery.execute

```python
# In query/update.py

async def execute(self, connection: Any) -> list[dict[str, Any]]:
    """Execute update and return results (if RETURNING specified)."""
    from declaro_persistum.query.executor import execute

    dialect = _detect_dialect(connection)

    # For Turso, validate CHECK constraints before execution
    if dialect == "turso":
        from declaro_persistum.abstractions.check_compat import (
            validate_row,
            CheckValidationError,
        )

        # Note: For UPDATE, we only validate the columns being updated
        # This is a partial validation - full row validation would require
        # fetching current row first
        is_valid, errors = validate_row(self._table, self._values, operation="UPDATE")
        if not is_valid:
            raise CheckValidationError(
                table=self._table,
                column="(multiple)",
                expression="(see errors)",
                row=self._values,
                message=f"UPDATE validation failed: {'; '.join(errors)}",
            )

    return await execute(self.to_query(dialect), connection)
```

### Integration Test

```python
# tests/integration/test_query_check_validation.py

import pytest
from declaro_persistum.abstractions.check_compat import (
    register_check_constraint,
    clear_registry,
    CheckValidationError,
)


@pytest.fixture(autouse=True)
def clean_registry():
    clear_registry()
    yield
    clear_registry()


@pytest.mark.turso
async def test_insert_validation_passes(turso_connection, sample_schema):
    """INSERT with valid data succeeds."""
    from declaro_persistum.query.table import Table

    register_check_constraint("events", "end", "start <= end")

    events = Table("events", sample_schema)
    result = await events.insert(start=5, end=10).execute(turso_connection)

    assert result is not None


@pytest.mark.turso
async def test_insert_validation_fails(turso_connection, sample_schema):
    """INSERT with invalid data raises CheckValidationError."""
    from declaro_persistum.query.table import Table

    register_check_constraint("events", "end", "start <= end")

    events = Table("events", sample_schema)

    with pytest.raises(CheckValidationError) as exc_info:
        await events.insert(start=10, end=5).execute(turso_connection)

    assert "CHECK constraint failed" in str(exc_info.value)
```

### Validation

```bash
uv run pytest tests/integration/test_query_check_validation.py -xvs -m turso
```

---

## Phase 7: Schema Processing Hook

**Objective**: Add function to process CHECK constraints from schema at initialization

**Duration**: 1-2 hours

**Depends on**: Phase 4

### Tasks

#### 7.1 Implement process_schema_checks

```python
# In abstractions/check_compat.py

def process_schema_checks(
    schema: dict[str, Any],
    dialect: str,
) -> int:
    """
    Process all CHECK constraints in a schema.

    For Turso dialect, registers Python validators.
    For other dialects, no-op (native CHECK used).

    Args:
        schema: Schema dict with table definitions
        dialect: Target database dialect

    Returns:
        Number of CHECK constraints registered
    """
    if dialect != "turso":
        logger.debug(
            f"Skipping CHECK emulation for dialect '{dialect}' (native support)"
        )
        return 0

    count = 0

    for table_name, table_def in schema.items():
        # Skip internal tables (enums, etc.)
        if table_name.startswith("_dp_"):
            continue

        columns = table_def.get("columns", {})
        for col_name, col_def in columns.items():
            if "check" in col_def:
                register_check_constraint(
                    table_name,
                    col_name,
                    col_def["check"],
                )
                count += 1

    logger.info(f"Registered {count} CHECK constraints for Python emulation")
    return count
```

### Usage Example

```python
# Application initialization
from declaro_persistum.pydantic_loader import load_schema_from_models
from declaro_persistum.abstractions.check_compat import process_schema_checks

schema = load_schema_from_models("models/")
process_schema_checks(schema, dialect="turso")
```

---

## Phase 8: Documentation and Export

**Objective**: Update __init__.py exports and documentation

**Duration**: 1-2 hours

**Depends on**: All previous phases

### Tasks

#### 8.1 Update abstractions/__init__.py

```python
# Add to abstractions/__init__.py

from .check_compat import (
    CheckAST,
    CheckParseError,
    CheckValidationError,
    ValidatorFn,
    ValidationResult,
    parse_check_expression,
    generate_validator,
    register_check_constraint,
    validate_row,
    process_schema_checks,
    clear_registry,
    get_validation_stats,
    get_affected_tables,
)

__all__ = [
    # ... existing exports ...

    # CHECK Constraint Emulation (Turso)
    "CheckAST",
    "CheckParseError",
    "CheckValidationError",
    "ValidatorFn",
    "ValidationResult",
    "parse_check_expression",
    "generate_validator",
    "register_check_constraint",
    "validate_row",
    "process_schema_checks",
    "clear_registry",
    "get_validation_stats",
    "get_affected_tables",
]
```

#### 8.2 Update Usage Documentation

Add section to `docs/usage.md` explaining CHECK constraint behavior across backends.

### Final Validation

```bash
# Run all tests
uv run pytest tests/ -xvs

# Type checking
uv run mypy src/declaro_persistum/abstractions/check_compat.py

# Lint
uv run ruff check src/declaro_persistum/abstractions/check_compat.py
```

---

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Complex CHECK expressions not parseable | Medium | Low | Log warning, skip validation |
| Performance overhead on INSERT/UPDATE | Low | Medium | Lazy evaluation, caching |
| Breaking change to Turso applier | Low | High | Comprehensive tests |
| Type errors in validator generation | Medium | Medium | Extensive unit tests |

---

## Rollback Procedure

If issues are discovered after deployment:

1. **Revert Turso applier changes**: Restore CHECK in SQL (will break Turso but other backends OK)
2. **Remove registry calls**: Comment out `register_check_constraint` calls
3. **Skip validation**: Set `DECLARO_SKIP_CHECK_EMULATION=1` env var (implement as escape hatch)

---

## Success Metrics

- [ ] All unit tests pass (95%+ coverage)
- [ ] All integration tests pass
- [ ] Turso table creation succeeds without CHECK syntax errors
- [ ] Invalid data rejected with clear error messages
- [ ] No performance regression (< 1ms overhead per validation)
- [ ] Documentation complete

---

## bd Issue Commands

```bash
# Create epic
EPIC_ID=$(bd create "Implement CHECK Constraint Emulation for Turso" -t epic -p 1 \
  -d "Turso Database cannot parse CHECK constraints. Implement Python-side validation." \
  --json | jq -r '.id')

# Phase 1: Types
bd create "Define CHECK emulation types and exceptions" -t task -p 1 \
  -d "Create CheckAST, CheckParseError, CheckValidationError in check_compat.py" \
  --deps parent-child:$EPIC_ID --json

# Phase 2: Parser
bd create "Implement CHECK expression parser" -t task -p 1 \
  -d "Tokenizer and recursive descent parser for SQL CHECK expressions" \
  --deps parent-child:$EPIC_ID --json

# Phase 3: Validator
bd create "Implement validator generator" -t task -p 1 \
  -d "Generate Python validators from CHECK AST" \
  --deps parent-child:$EPIC_ID --json

# Phase 4: Registry
bd create "Implement validator registry" -t task -p 1 \
  -d "Module-level registry with monitoring counters" \
  --deps parent-child:$EPIC_ID --json

# Phase 5: Applier
bd create "Modify Turso applier for CHECK emulation" -t task -p 1 \
  -d "Skip CHECK SQL, register validators in _create_table_sql" \
  --deps parent-child:$EPIC_ID --json

# Phase 6: Query
bd create "Add validation hooks to InsertQuery/UpdateQuery" -t task -p 2 \
  -d "Validate before execution for Turso dialect" \
  --deps parent-child:$EPIC_ID --json

# Phase 7: Schema
bd create "Add process_schema_checks function" -t task -p 2 \
  -d "Batch registration of CHECK constraints from schema" \
  --deps parent-child:$EPIC_ID --json

# Phase 8: Docs
bd create "Update exports and documentation" -t task -p 3 \
  -d "Update __init__.py, usage.md" \
  --deps parent-child:$EPIC_ID --json
```
