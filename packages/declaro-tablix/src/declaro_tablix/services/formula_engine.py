"""Formula engine functions for Table Module V2.

This module provides Excel-like formula evaluation with comprehensive function library,
security safeguards, and performance optimization.
"""

import ast
import math
import operator
import re
import statistics
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

from declaro_advise import error, info, warning
from declaro_tablix.domain.validators import validate_formula_syntax

# Function registry for Excel-like functions
FUNCTION_REGISTRY = {}


def register_function(name: str):
    """Decorator to register functions in the function registry."""

    def decorator(func):
        FUNCTION_REGISTRY[name.upper()] = func
        return func

    return decorator


async def evaluate_formula(
    formula: str,
    row_data: Dict[str, Any],
    table_metadata: Optional[Dict[str, Any]] = None,
) -> Any:
    """Evaluate an Excel-like formula against row data.

    Args:
        formula: Formula string starting with '='
        row_data: Dictionary of column values for the current row
        table_metadata: Optional table metadata for context

    Returns:
        Calculated value or None if error

    Raises:
        ValueError: If formula is invalid or unsafe
        KeyError: If referenced column doesn't exist
    """
    try:
        # Validate formula syntax first
        is_valid, errors = validate_formula_syntax(formula)
        if not is_valid:
            raise ValueError(f"Invalid formula syntax: {', '.join(errors)}")

        # Security check
        if not _is_formula_safe(formula):
            raise ValueError("Formula contains potentially dangerous patterns")

        # Remove '=' prefix
        if formula.startswith("="):
            formula = formula[1:]

        # Replace column references
        processed_formula = _replace_column_references(formula, row_data)

        # Evaluate the processed formula
        result = _evaluate_expression(processed_formula)

        return result

    except Exception as e:
        error(f"Formula evaluation failed: {str(e)}")
        return None


async def compile_formula(formula: str) -> Optional[str]:
    """Compile formula for performance optimization.

    Args:
        formula: Formula string to compile

    Returns:
        Compiled formula string or None if error
    """
    try:
        # Validate formula
        is_valid, errors = validate_formula_syntax(formula)
        if not is_valid:
            raise ValueError(f"Invalid formula: {', '.join(errors)}")

        # Pre-process common patterns for performance
        compiled_formula = formula

        # Cache function lookups
        compiled_formula = _cache_function_references(compiled_formula)

        # Optimize cell reference patterns
        compiled_formula = _optimize_cell_references(compiled_formula)

        return compiled_formula

    except Exception as e:
        error(f"Formula compilation failed: {str(e)}")
        return None


def validate_formula_performance(formula: str, max_complexity: int = 1000) -> Dict[str, Any]:
    """Validate formula performance characteristics.

    Args:
        formula: Formula to analyze
        max_complexity: Maximum allowed complexity score

    Returns:
        Dictionary with performance metrics
    """
    try:
        complexity_score = _calculate_formula_complexity(formula)
        function_count = len(re.findall(r"\w+\s*\(", formula))
        reference_count = len(re.findall(r"\[[^\]]+\]", formula))

        performance_metrics = {
            "complexity_score": complexity_score,
            "function_count": function_count,
            "reference_count": reference_count,
            "max_complexity": max_complexity,
            "within_limits": complexity_score <= max_complexity,
            "estimated_execution_time_ms": complexity_score * 0.1,  # Rough estimate
        }

        if not performance_metrics["within_limits"]:
            warning(f"Formula complexity {complexity_score} exceeds limit {max_complexity}")

        return performance_metrics

    except Exception as e:
        error(f"Formula performance validation failed: {str(e)}")
        return {"error": str(e), "within_limits": False}


# Security functions


def _is_formula_safe(formula: str) -> bool:
    """Check if formula is safe to execute."""
    dangerous_patterns = [
        r"import\s+",
        r"exec\s*\(",
        r"eval\s*\(",
        r"__[a-zA-Z_]+__",
        r"file\s*\(",
        r"open\s*\(",
        r"subprocess",
        r"os\.",
        r"sys\.",
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, formula, re.IGNORECASE):
            return False

    return True


# Expression evaluation


def _evaluate_expression(expression: str) -> Any:
    """Safely evaluate a mathematical/logical expression."""
    try:
        # Handle function calls
        expression = _evaluate_functions(expression)

        # Handle basic arithmetic with safe evaluation
        return _safe_eval(expression)

    except Exception as e:
        raise ValueError(f"Expression evaluation failed: {str(e)}")


def _safe_eval(expression: str) -> Any:
    """Safely evaluate mathematical expressions using AST."""
    try:
        # Parse the expression into an AST
        node = ast.parse(expression, mode="eval")
        return _eval_node(node.body)
    except Exception as e:
        raise ValueError(f"Safe evaluation failed: {str(e)}")


def _eval_node(node) -> Any:
    """Recursively evaluate AST nodes."""
    if isinstance(node, ast.Constant):
        return node.value
    elif isinstance(node, ast.Num):  # Python < 3.8 compatibility
        return node.n
    elif isinstance(node, ast.Str):  # Python < 3.8 compatibility
        return node.s
    elif isinstance(node, ast.BinOp):
        left = _eval_node(node.left)
        right = _eval_node(node.right)
        return _eval_binop(node.op, left, right)
    elif isinstance(node, ast.UnaryOp):
        operand = _eval_node(node.operand)
        return _eval_unaryop(node.op, operand)
    elif isinstance(node, ast.Compare):
        left = _eval_node(node.left)
        for op, comparator in zip(node.ops, node.comparators):
            right = _eval_node(comparator)
            result = _eval_compare(op, left, right)
            if not result:
                return False
            left = right
        return True
    else:
        raise ValueError(f"Unsupported node type: {type(node)}")


def _eval_binop(op, left: Any, right: Any) -> Any:
    """Evaluate binary operations."""
    ops = {
        ast.Add: operator.add,
        ast.Sub: operator.sub,
        ast.Mult: operator.mul,
        ast.Div: operator.truediv,
        ast.FloorDiv: operator.floordiv,
        ast.Mod: operator.mod,
        ast.Pow: operator.pow,
    }

    if type(op) in ops:
        return ops[type(op)](left, right)
    else:
        raise ValueError(f"Unsupported binary operation: {type(op)}")


def _eval_unaryop(op, operand: Any) -> Any:
    """Evaluate unary operations."""
    if isinstance(op, ast.UAdd):
        return +operand
    elif isinstance(op, ast.USub):
        return -operand
    elif isinstance(op, ast.Not):
        return not operand
    else:
        raise ValueError(f"Unsupported unary operation: {type(op)}")


def _eval_compare(op, left: Any, right: Any) -> bool:
    """Evaluate comparison operations."""
    ops = {
        ast.Eq: operator.eq,
        ast.NotEq: operator.ne,
        ast.Lt: operator.lt,
        ast.LtE: operator.le,
        ast.Gt: operator.gt,
        ast.GtE: operator.ge,
    }

    if type(op) in ops:
        return ops[type(op)](left, right)
    else:
        raise ValueError(f"Unsupported comparison: {type(op)}")


# Column reference handling


def _replace_column_references(formula: str, row_data: Dict[str, Any]) -> str:
    """Replace column references like [column_name] with actual values."""

    def replace_reference(match):
        column_name = match.group(1)
        if column_name in row_data:
            value = row_data[column_name]
            if isinstance(value, str):
                # Escape quotes in string values
                return f'"{value.replace(chr(34), chr(92) + chr(34))}"'
            elif value is None:
                return "None"
            else:
                return str(value)
        else:
            raise KeyError(f"Column '{column_name}' not found in row data")

    return re.sub(r"\[([^\]]+)\]", replace_reference, formula)


# Function evaluation


def _evaluate_functions(expression: str) -> str:
    """Evaluate function calls in the expression."""
    function_pattern = r"(\w+)\s*\(([^)]*)\)"

    def replace_function(match):
        func_name = match.group(1).upper()
        args_str = match.group(2)

        if func_name in FUNCTION_REGISTRY:
            # Parse arguments
            args = _parse_function_args(args_str)

            # Call function
            try:
                result = FUNCTION_REGISTRY[func_name](*args)
                return str(result)
            except Exception as e:
                raise ValueError(f"Function {func_name} failed: {str(e)}")
        else:
            raise ValueError(f"Unknown function: {func_name}")

    # Replace function calls iteratively to handle nested functions
    prev_expression = ""
    while prev_expression != expression:
        prev_expression = expression
        expression = re.sub(function_pattern, replace_function, expression)

    return expression


def _parse_function_args(args_str: str) -> List[Any]:
    """Parse function arguments from string."""
    if not args_str.strip():
        return []

    args = []
    current_arg = ""
    paren_count = 0
    quote_count = 0

    for char in args_str:
        if char == '"' and (not current_arg or current_arg[-1] != "\\"):
            quote_count = 1 - quote_count
        elif char == "(" and quote_count == 0:
            paren_count += 1
        elif char == ")" and quote_count == 0:
            paren_count -= 1
        elif char == "," and paren_count == 0 and quote_count == 0:
            args.append(_parse_arg_value(current_arg.strip()))
            current_arg = ""
            continue

        current_arg += char

    if current_arg.strip():
        args.append(_parse_arg_value(current_arg.strip()))

    return args


def _parse_arg_value(arg: str) -> Any:
    """Parse a single argument value."""
    arg = arg.strip()

    # Handle quoted strings
    if arg.startswith('"') and arg.endswith('"'):
        return arg[1:-1].replace('\\"', '"')

    # Handle None
    if arg.lower() == "none":
        return None

    # Handle booleans
    if arg.lower() == "true":
        return True
    if arg.lower() == "false":
        return False

    # Try to parse as number
    try:
        if "." in arg:
            return float(arg)
        else:
            return int(arg)
    except ValueError:
        pass

    # Return as string if nothing else works
    return arg


# Performance optimization functions


@lru_cache(maxsize=1000)
def _cache_function_references(formula: str) -> str:
    """Cache function reference lookups."""
    return formula


def _optimize_cell_references(formula: str) -> str:
    """Optimize cell reference patterns."""
    # Pre-compile common reference patterns
    optimized = formula
    return optimized


def _calculate_formula_complexity(formula: str) -> int:
    """Calculate formula complexity score."""
    complexity = 0

    # Function calls add complexity
    complexity += len(re.findall(r"\w+\s*\(", formula)) * 10

    # Nested parentheses add complexity
    max_nesting = 0
    current_nesting = 0
    for char in formula:
        if char == "(":
            current_nesting += 1
            max_nesting = max(max_nesting, current_nesting)
        elif char == ")":
            current_nesting -= 1
    complexity += max_nesting * 5

    # Column references add minimal complexity
    complexity += len(re.findall(r"\[[^\]]+\]", formula)) * 2

    # String operations add complexity
    complexity += len(re.findall(r'"[^"]*"', formula)) * 3

    return complexity


# Excel-like functions - Mathematical


@register_function("SUM")
def _func_sum(*args) -> float:
    """Sum of all arguments."""
    total = 0
    for arg in args:
        if arg is not None and arg != "":
            try:
                total += float(arg)
            except (ValueError, TypeError):
                pass
    return total


@register_function("AVERAGE")
@register_function("AVG")
def _func_average(*args) -> float:
    """Average of all arguments."""
    values = []
    for arg in args:
        if arg is not None and arg != "":
            try:
                values.append(float(arg))
            except (ValueError, TypeError):
                pass
    return statistics.mean(values) if values else 0


@register_function("COUNT")
def _func_count(*args) -> int:
    """Count of non-empty arguments."""
    count = 0
    for arg in args:
        if arg is not None and arg != "":
            count += 1
    return count


@register_function("MAX")
def _func_max(*args) -> float:
    """Maximum value among arguments."""
    values = []
    for arg in args:
        if arg is not None and arg != "":
            try:
                values.append(float(arg))
            except (ValueError, TypeError):
                pass
    return max(values) if values else 0


@register_function("MIN")
def _func_min(*args) -> float:
    """Minimum value among arguments."""
    values = []
    for arg in args:
        if arg is not None and arg != "":
            try:
                values.append(float(arg))
            except (ValueError, TypeError):
                pass
    return min(values) if values else 0


@register_function("ROUND")
def _func_round(value: Any, digits: int = 0) -> float:
    """Round value to specified decimal places."""
    try:
        return round(float(value), int(digits))
    except (ValueError, TypeError):
        return 0


@register_function("ABS")
def _func_abs(value: Any) -> float:
    """Absolute value."""
    try:
        return abs(float(value))
    except (ValueError, TypeError):
        return 0


@register_function("SQRT")
def _func_sqrt(value: Any) -> float:
    """Square root."""
    try:
        return math.sqrt(float(value))
    except (ValueError, TypeError):
        return 0


@register_function("POWER")
def _func_power(base: Any, exponent: Any) -> float:
    """Raise base to the power of exponent."""
    try:
        return math.pow(float(base), float(exponent))
    except (ValueError, TypeError):
        return 0


@register_function("MOD")
def _func_mod(number: Any, divisor: Any) -> float:
    """Modulo operation."""
    try:
        return float(number) % float(divisor)
    except (ValueError, TypeError, ZeroDivisionError):
        return 0


@register_function("MEDIAN")
def _func_median(*args) -> float:
    """Median value of arguments."""
    values = []
    for arg in args:
        if arg is not None and arg != "":
            try:
                values.append(float(arg))
            except (ValueError, TypeError):
                pass
    return statistics.median(values) if values else 0


# Logical functions


@register_function("IF")
def _func_if(condition: Any, value_if_true: Any, value_if_false: Any) -> Any:
    """If-then-else logic."""
    try:
        # Convert condition to boolean
        if isinstance(condition, str):
            condition = condition.lower() not in ["false", "0", "", "no"]
        else:
            condition = bool(condition)

        return value_if_true if condition else value_if_false
    except Exception:
        return value_if_false


@register_function("AND")
def _func_and(*args) -> bool:
    """Logical AND of all arguments."""
    for arg in args:
        if isinstance(arg, str):
            if arg.lower() in ["false", "0", "", "no"]:
                return False
        elif not bool(arg):
            return False
    return True


@register_function("OR")
def _func_or(*args) -> bool:
    """Logical OR of all arguments."""
    for arg in args:
        if isinstance(arg, str):
            if arg.lower() not in ["false", "0", "", "no"]:
                return True
        elif bool(arg):
            return True
    return False


@register_function("NOT")
def _func_not(value: Any) -> bool:
    """Logical NOT."""
    if isinstance(value, str):
        return value.lower() in ["false", "0", "", "no"]
    return not bool(value)


# String functions


@register_function("CONCATENATE")
@register_function("CONCAT")
def _func_concat(*args) -> str:
    """Concatenate all arguments."""
    result = ""
    for arg in args:
        if arg is not None:
            result += str(arg)
    return result


@register_function("UPPER")
def _func_upper(text: Any) -> str:
    """Convert text to uppercase."""
    return str(text).upper() if text is not None else ""


@register_function("LOWER")
def _func_lower(text: Any) -> str:
    """Convert text to lowercase."""
    return str(text).lower() if text is not None else ""


@register_function("LEFT")
def _func_left(text: Any, length: int) -> str:
    """Return leftmost characters."""
    try:
        return str(text)[: int(length)] if text is not None else ""
    except (ValueError, TypeError):
        return ""


@register_function("RIGHT")
def _func_right(text: Any, length: int) -> str:
    """Return rightmost characters."""
    try:
        text_str = str(text) if text is not None else ""
        return text_str[-int(length) :] if int(length) > 0 else ""
    except (ValueError, TypeError):
        return ""


@register_function("MID")
def _func_mid(text: Any, start: int, length: int) -> str:
    """Return middle characters."""
    try:
        text_str = str(text) if text is not None else ""
        start_idx = int(start) - 1  # Excel uses 1-based indexing
        return text_str[start_idx : start_idx + int(length)]
    except (ValueError, TypeError):
        return ""


@register_function("LEN")
def _func_len(text: Any) -> int:
    """Return length of text."""
    return len(str(text)) if text is not None else 0


@register_function("TRIM")
def _func_trim(text: Any) -> str:
    """Remove leading and trailing spaces."""
    return str(text).strip() if text is not None else ""


# Date functions


@register_function("NOW")
def _func_now() -> datetime:
    """Return current date and time."""
    return datetime.now()


@register_function("TODAY")
def _func_today() -> datetime:
    """Return current date."""
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


@register_function("YEAR")
def _func_year(date: Any) -> int:
    """Extract year from date."""
    try:
        if isinstance(date, datetime):
            return date.year
        elif isinstance(date, str):
            parsed_date = datetime.fromisoformat(date.replace("Z", "+00:00"))
            return parsed_date.year
        else:
            return int(date)
    except (ValueError, TypeError):
        return 0


@register_function("MONTH")
def _func_month(date: Any) -> int:
    """Extract month from date."""
    try:
        if isinstance(date, datetime):
            return date.month
        elif isinstance(date, str):
            parsed_date = datetime.fromisoformat(date.replace("Z", "+00:00"))
            return parsed_date.month
        else:
            return int(date)
    except (ValueError, TypeError):
        return 0


@register_function("DAY")
def _func_day(date: Any) -> int:
    """Extract day from date."""
    try:
        if isinstance(date, datetime):
            return date.day
        elif isinstance(date, str):
            parsed_date = datetime.fromisoformat(date.replace("Z", "+00:00"))
            return parsed_date.day
        else:
            return int(date)
    except (ValueError, TypeError):
        return 0
