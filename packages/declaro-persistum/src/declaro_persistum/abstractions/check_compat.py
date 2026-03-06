"""
CHECK constraint compatibility layer.

Turso Database now supports CHECK constraints natively (as of COMPAT.md update).
The TursoApplier emits CHECK clauses in SQL directly.

This module is retained for:
1. Older Turso versions that may lack CHECK support
2. Python-side pre-validation (application layer enforcement)
3. Testing and introspection of CHECK expressions
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
        low: Lower bound for 'between' op
        high: Upper bound for 'between' op
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
    low: Any
    high: Any


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


# =============================================================================
# Validator Registry (Module-Level State)
#
# NOTE: This is I/O-boundary monitoring state, not business logic.
# The registry stores validators for Python-side CHECK emulation
# (currently unused since all backends support CHECK natively).
# The counters track usage for debugging/observability.
# Use clear_registry() between tests.
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


# =============================================================================
# Tokenizer
# =============================================================================

_TOKEN_PATTERNS = [
    ("WHITESPACE", r"\s+"),
    ("LPAREN", r"\("),
    ("RPAREN", r"\)"),
    ("COMMA", r","),
    ("STRING", r"'[^']*'|\"[^\"]*\""),
    ("NUMBER", r"-?\d+(?:\.\d+)?"),
    ("OP_CMP", r"<=|>=|<>|!=|=|<|>"),
    ("KEYWORD", r"\b(?:AND|OR|NOT|IN|BETWEEN|IS|NULL)\b"),
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
    tokens: list[tuple[str, str]] = []
    pos = 0

    while pos < len(expression):
        matched = False

        for token_type, pattern in _TOKEN_PATTERNS:
            regex = re.compile(pattern, re.IGNORECASE)
            match = regex.match(expression, pos)

            if match:
                value = match.group(0)
                if token_type != "WHITESPACE":
                    tokens.append((token_type, value))
                pos = match.end()
                matched = True
                break

        if not matched:
            raise CheckParseError(
                expression, f"Unexpected character at position {pos}: {expression[pos]}"
            )

    return tokens


# =============================================================================
# Parser
# =============================================================================


def parse_check_expression(
    expression: str,
    column_name: str,  # noqa: ARG001 - kept for API consistency
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

    if not tokens:
        raise CheckParseError(expression, "Empty expression")

    ast, remaining = _parse_or_expr(tokens, expression)

    if remaining:
        raise CheckParseError(
            expression, f"Unexpected tokens after expression: {remaining}"
        )

    return ast


def _parse_or_expr(
    tokens: list[tuple[str, str]],
    original: str,
) -> tuple[CheckAST, list[tuple[str, str]]]:
    """Parse OR expressions (lowest precedence)."""
    left, remaining = _parse_and_expr(tokens, original)

    while remaining and _peek_keyword(remaining, "OR"):
        remaining = remaining[1:]  # consume OR
        right, remaining = _parse_and_expr(remaining, original)
        left = CheckAST(op="or", left=left, right=right)  # type: ignore[typeddict-item]

    return left, remaining


def _parse_and_expr(
    tokens: list[tuple[str, str]],
    original: str,
) -> tuple[CheckAST, list[tuple[str, str]]]:
    """Parse AND expressions."""
    left, remaining = _parse_not_expr(tokens, original)

    while remaining and _peek_keyword(remaining, "AND"):
        remaining = remaining[1:]  # consume AND
        right, remaining = _parse_not_expr(remaining, original)
        left = CheckAST(op="and", left=left, right=right)  # type: ignore[typeddict-item]

    return left, remaining


def _parse_not_expr(
    tokens: list[tuple[str, str]],
    original: str,
) -> tuple[CheckAST, list[tuple[str, str]]]:
    """Parse NOT expressions."""
    if tokens and _peek_keyword(tokens, "NOT"):
        tokens = tokens[1:]  # consume NOT
        operand, remaining = _parse_primary(tokens, original)
        return CheckAST(op="not", operand=operand), remaining  # type: ignore[typeddict-item]

    return _parse_primary(tokens, original)


def _parse_primary(
    tokens: list[tuple[str, str]],
    original: str,
) -> tuple[CheckAST, list[tuple[str, str]]]:
    """Parse primary expressions (comparisons, IN, BETWEEN, IS NULL, parentheses)."""
    if not tokens:
        raise CheckParseError(original, "Unexpected end of expression")

    # Handle parenthesized expressions
    if tokens[0][0] == "LPAREN":
        tokens = tokens[1:]  # consume (
        expr, remaining = _parse_or_expr(tokens, original)

        if not remaining or remaining[0][0] != "RPAREN":
            raise CheckParseError(original, "Missing closing parenthesis")

        return expr, remaining[1:]  # consume )

    # Parse left operand (identifier or value)
    left, remaining = _parse_value(tokens, original)

    if not remaining:
        raise CheckParseError(original, "Incomplete expression")

    # Check for IS NULL / IS NOT NULL
    if _peek_keyword(remaining, "IS"):
        remaining = remaining[1:]  # consume IS

        if _peek_keyword(remaining, "NOT"):
            remaining = remaining[1:]  # consume NOT
            if not _peek_keyword(remaining, "NULL"):
                raise CheckParseError(original, "Expected NULL after IS NOT")
            remaining = remaining[1:]  # consume NULL
            return CheckAST(op="is_not_null", operand=left), remaining
        elif _peek_keyword(remaining, "NULL"):
            remaining = remaining[1:]  # consume NULL
            return CheckAST(op="is_null", operand=left), remaining
        else:
            raise CheckParseError(original, "Expected NULL or NOT NULL after IS")

    # Check for BETWEEN
    if _peek_keyword(remaining, "BETWEEN"):
        remaining = remaining[1:]  # consume BETWEEN
        low, remaining = _parse_value(remaining, original)

        if not _peek_keyword(remaining, "AND"):
            raise CheckParseError(original, "Expected AND in BETWEEN expression")

        remaining = remaining[1:]  # consume AND
        high, remaining = _parse_value(remaining, original)

        return CheckAST(op="between", left=left, low=low, high=high), remaining

    # Check for IN
    if _peek_keyword(remaining, "IN"):
        remaining = remaining[1:]  # consume IN

        if not remaining or remaining[0][0] != "LPAREN":
            raise CheckParseError(original, "Expected ( after IN")

        remaining = remaining[1:]  # consume (

        values: list[Any] = []
        while remaining and remaining[0][0] != "RPAREN":
            value, remaining = _parse_value(remaining, original)
            values.append(value)

            if remaining and remaining[0][0] == "COMMA":
                remaining = remaining[1:]  # consume ,
            elif remaining and remaining[0][0] != "RPAREN":
                raise CheckParseError(original, "Expected , or ) in IN clause")

        if not remaining or remaining[0][0] != "RPAREN":
            raise CheckParseError(original, "Missing closing ) in IN clause")

        remaining = remaining[1:]  # consume )

        return CheckAST(op="in", left=left, values=values), remaining

    # Check for comparison operator
    if remaining[0][0] == "OP_CMP":
        operator = remaining[0][1]
        remaining = remaining[1:]  # consume operator

        right, remaining = _parse_value(remaining, original)

        return CheckAST(op="compare", left=left, operator=operator, right=right), remaining

    raise CheckParseError(original, f"Unexpected token: {remaining[0]}")


def _parse_value(
    tokens: list[tuple[str, str]],
    original: str,
) -> tuple[Any, list[tuple[str, str]]]:
    """
    Parse a value (identifier, string, number, or NULL).

    Returns the value. For column references (IDENT), returns a dict marker
    so validators can distinguish between column names and literal strings.
    """
    if not tokens:
        raise CheckParseError(original, "Expected value")

    token_type, token_value = tokens[0]

    if token_type == "IDENT":
        # Column reference - mark it as such
        return {"_column": token_value}, tokens[1:]
    elif token_type == "STRING":
        # String literal - remove quotes and return as-is
        value = token_value[1:-1]
        return value, tokens[1:]
    elif token_type == "NUMBER":
        # Numeric literal - convert to int or float
        if "." in token_value:
            return float(token_value), tokens[1:]
        else:
            return int(token_value), tokens[1:]
    elif token_type == "KEYWORD" and token_value.upper() == "NULL":
        return None, tokens[1:]
    else:
        raise CheckParseError(original, f"Expected value, got {token_type}")


def _peek_keyword(tokens: list[tuple[str, str]], keyword: str) -> bool:
    """Check if the next token is a specific keyword."""
    if not tokens:
        return False
    return tokens[0][0] == "KEYWORD" and tokens[0][1].upper() == keyword


# =============================================================================
# Validator Generator
# =============================================================================


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

    _VALIDATOR_GENERATORS = {
        "compare": _gen_compare_validator,
        "in": _gen_in_validator,
        "between": _gen_between_validator,
        "is_null": _gen_is_null_validator,
        "is_not_null": _gen_is_not_null_validator,
        "and": _gen_and_validator,
        "or": _gen_or_validator,
        "not": _gen_not_validator,
    }

    generator = _VALIDATOR_GENERATORS.get(op)
    if generator is None:
        logger.warning(
            f"Unknown CHECK op '{op}' for {table_name}.{column_name}, skipping validation"
        )
        return lambda _row: (True, None)
    return generator(ast, table_name, column_name, expression)


def _is_column_ref(value: Any) -> tuple[bool, str | Any]:
    """Check if value is a column reference marker, return (is_column, value)."""
    if isinstance(value, dict) and "_column" in value:
        return True, value["_column"]
    return False, value


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

    # Determine if left/right are column references
    left_is_column, left_value = _is_column_ref(left)
    right_is_column, right_value = _is_column_ref(right)

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
        return lambda _row: (True, None)  # Unknown operator, skip

    def validator(row: Mapping[str, Any]) -> tuple[bool, str | None]:
        # Get values - only look up in row if it's a column reference
        left_val = row.get(left_value) if left_is_column else left_value

        right_val = row.get(right_value) if right_is_column else right_value

        # NULL handling: NULL comparisons always pass (SQL semantics)
        # But only if the NULL came from the row, not from a literal
        if left_is_column and left_val is None:
            return (True, None)
        if right_is_column and right_val is None:
            return (True, None)

        try:
            if compare_fn(left_val, right_val):
                return (True, None)
            else:
                return (
                    False,
                    f"CHECK constraint failed: {table_name}.{column_name} "
                    f"requires {expression}",
                )
        except TypeError:
            # Incompatible types - skip validation
            return (True, None)

    return validator


def _gen_in_validator(
    ast: CheckAST,
    table_name: str,
    column_name: str,
    expression: str,
) -> ValidatorFn:
    """Generate validator for IN expressions."""
    left = ast["left"]
    values = ast["values"]
    left_is_column, left_value = _is_column_ref(left)

    def validator(row: Mapping[str, Any]) -> tuple[bool, str | None]:
        left_val = row.get(left_value) if left_is_column else left_value

        # NULL passes (only if from column)
        if left_is_column and left_val is None:
            return (True, None)

        if left_val in values:
            return (True, None)
        else:
            return (
                False,
                f"CHECK constraint failed: {table_name}.{column_name} "
                f"requires {expression}",
            )

    return validator


def _gen_between_validator(
    ast: CheckAST,
    table_name: str,
    column_name: str,
    expression: str,
) -> ValidatorFn:
    """Generate validator for BETWEEN expressions."""
    left = ast["left"]
    low = ast["low"]
    high = ast["high"]
    left_is_column, left_value = _is_column_ref(left)

    def validator(row: Mapping[str, Any]) -> tuple[bool, str | None]:
        left_val = row.get(left_value) if left_is_column else left_value

        # NULL passes (only if from column)
        if left_is_column and left_val is None:
            return (True, None)

        try:
            if low <= left_val <= high:
                return (True, None)
            else:
                return (
                    False,
                    f"CHECK constraint failed: {table_name}.{column_name} "
                    f"requires {expression}",
                )
        except TypeError:
            return (True, None)

    return validator


def _gen_is_null_validator(
    ast: CheckAST,
    table_name: str,
    column_name: str,
    expression: str,
) -> ValidatorFn:
    """Generate validator for IS NULL expressions."""
    operand = ast["operand"]
    operand_is_column, operand_value = _is_column_ref(operand)

    def validator(row: Mapping[str, Any]) -> tuple[bool, str | None]:
        val = row.get(operand_value) if operand_is_column else operand_value

        if val is None:
            return (True, None)
        else:
            return (
                False,
                f"CHECK constraint failed: {table_name}.{column_name} "
                f"requires {expression}",
            )

    return validator


def _gen_is_not_null_validator(
    ast: CheckAST,
    table_name: str,
    column_name: str,
    expression: str,
) -> ValidatorFn:
    """Generate validator for IS NOT NULL expressions."""
    operand = ast["operand"]
    operand_is_column, operand_value = _is_column_ref(operand)

    def validator(row: Mapping[str, Any]) -> tuple[bool, str | None]:
        val = row.get(operand_value) if operand_is_column else operand_value

        if val is not None:
            return (True, None)
        else:
            return (
                False,
                f"CHECK constraint failed: {table_name}.{column_name} "
                f"requires {expression}",
            )

    return validator


def _gen_and_validator(
    ast: CheckAST,
    table_name: str,
    column_name: str,
    expression: str,
) -> ValidatorFn:
    """Generate validator for AND expressions."""
    left_ast = ast["left"]
    right_ast = ast["right"]

    # Recursively generate validators for left and right
    left_validator = generate_validator(
        left_ast, table_name, column_name, expression  # type: ignore
    )
    right_validator = generate_validator(
        right_ast, table_name, column_name, expression  # type: ignore
    )

    def validator(row: Mapping[str, Any]) -> tuple[bool, str | None]:
        left_valid, left_error = left_validator(row)
        if not left_valid:
            return (False, left_error)

        right_valid, right_error = right_validator(row)
        if not right_valid:
            return (False, right_error)

        return (True, None)

    return validator


def _gen_or_validator(
    ast: CheckAST,
    table_name: str,
    column_name: str,
    expression: str,
) -> ValidatorFn:
    """Generate validator for OR expressions."""
    left_ast = ast["left"]
    right_ast = ast["right"]

    # Recursively generate validators for left and right
    left_validator = generate_validator(
        left_ast, table_name, column_name, expression  # type: ignore
    )
    right_validator = generate_validator(
        right_ast, table_name, column_name, expression  # type: ignore
    )

    def validator(row: Mapping[str, Any]) -> tuple[bool, str | None]:
        left_valid, _ = left_validator(row)
        if left_valid:
            return (True, None)

        right_valid, _ = right_validator(row)
        if right_valid:
            return (True, None)

        return (
            False,
            f"CHECK constraint failed: {table_name}.{column_name} "
            f"requires {expression}",
        )

    return validator


def _gen_not_validator(
    ast: CheckAST,
    table_name: str,
    column_name: str,
    expression: str,
) -> ValidatorFn:
    """Generate validator for NOT expressions."""
    operand_ast = ast["operand"]

    # Recursively generate validator for operand
    operand_validator = generate_validator(
        operand_ast, table_name, column_name, expression  # type: ignore
    )

    def validator(row: Mapping[str, Any]) -> tuple[bool, str | None]:
        operand_valid, _ = operand_validator(row)

        if not operand_valid:
            return (True, None)
        else:
            return (
                False,
                f"CHECK constraint failed: {table_name}.{column_name} "
                f"requires {expression}",
            )

    return validator


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

        for _col_name, validator in validators:
            is_valid, error = validator(row)
            if not is_valid and error:
                errors.append(error)

        if errors:
            return (False, "; ".join(errors))
        return (True, None)

    return combined_validator


# =============================================================================
# Registry Functions
# =============================================================================


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

        logger.info(f"Registered CHECK emulation: {table}.{column} -> {expression}")
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
    operation: str = "INSERT",  # noqa: ARG001 - reserved for future error messages
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

    for _col_name, _expression, validator in validators:
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
    _validation_counters = dict.fromkeys(_validation_counters, 0)
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


# =============================================================================
# Schema Processing
# =============================================================================


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
    if dialect in ("turso", "postgresql", "sqlite"):
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
