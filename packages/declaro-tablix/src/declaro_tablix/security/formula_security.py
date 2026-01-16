"""
Formula security system for TableV2.

This module provides comprehensive security validation for user-created formulas,
including sandbox execution, infinite loop prevention, and dangerous pattern detection.
"""

import ast
import re
import resource
import signal
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from declaro_advise import error, info, success, warning


class SecurityLevel(Enum):
    """Security level for formula validation."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class SecurityViolation:
    """Security violation detected in formula."""

    violation_type: str
    description: str
    severity: SecurityLevel
    line_number: Optional[int] = None
    column_number: Optional[int] = None
    pattern: Optional[str] = None


@dataclass
class FormulaSecurityResult:
    """Result of formula security validation."""

    is_safe: bool
    security_level: SecurityLevel
    violations: List[SecurityViolation] = field(default_factory=list)
    sanitized_formula: Optional[str] = None
    execution_time_ms: float = 0.0
    memory_usage_mb: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class TimeoutException(Exception):
    """Exception raised when formula execution times out."""

    pass


class MemoryLimitException(Exception):
    """Exception raised when formula exceeds memory limit."""

    pass


class SecurityViolationException(Exception):
    """Exception raised when security violation is detected."""

    pass


# Global configuration
SECURITY_CONFIG = {
    "max_execution_time_seconds": 5,
    "max_memory_mb": 50,
    "max_iterations": 1000,
    "max_formula_length": 1000,
    "max_recursion_depth": 10,
    "allowed_builtins": {
        "abs",
        "round",
        "min",
        "max",
        "sum",
        "len",
        "str",
        "int",
        "float",
        "bool",
        "list",
        "dict",
        "tuple",
        "set",
        "range",
        "enumerate",
        "zip",
        "map",
        "filter",
        "sorted",
        "reversed",
        "any",
        "all",
    },
    "forbidden_patterns": [
        r"__.*__",  # Dunder methods
        r"import\s+",  # Import statements
        r"exec\s*\(",  # Exec function
        r"eval\s*\(",  # Eval function
        r"compile\s*\(",  # Compile function
        r"globals\s*\(",  # Globals function
        r"locals\s*\(",  # Locals function
        r"vars\s*\(",  # Vars function
        r"dir\s*\(",  # Dir function
        r"getattr\s*\(",  # Getattr function
        r"setattr\s*\(",  # Setattr function
        r"delattr\s*\(",  # Delattr function
        r"hasattr\s*\(",  # Hasattr function
        r"open\s*\(",  # File operations
        r"file\s*\(",  # File operations
        r"input\s*\(",  # Input function
        r"raw_input\s*\(",  # Raw input function
        r"exit\s*\(",  # Exit function
        r"quit\s*\(",  # Quit function
        r"subprocess",  # Subprocess module
        r"os\.",  # OS module
        r"sys\.",  # Sys module
        r"socket\.",  # Socket module
        r"urllib",  # Urllib module
        r"requests",  # Requests module
        r"http",  # HTTP module
        r"while\s+True:",  # Infinite while loops
        r"for\s+\w+\s+in\s+\w+:",  # Potentially infinite loops
    ],
    "dangerous_nodes": {
        ast.Import,
        ast.ImportFrom,
        ast.Global,
        ast.Nonlocal,
        ast.Delete,
        ast.ClassDef,
        ast.FunctionDef,
        ast.AsyncFunctionDef,
        ast.Lambda,
        ast.GeneratorExp,
        ast.ListComp,
        ast.SetComp,
        ast.DictComp,
    },
}


def validate_formula_security(
    formula: str, context: Optional[Dict[str, Any]] = None, security_level: SecurityLevel = SecurityLevel.MEDIUM
) -> FormulaSecurityResult:
    """
    Validate formula security and detect potential vulnerabilities.

    Args:
        formula: Formula expression to validate
        context: Optional execution context
        security_level: Security level for validation

    Returns:
        FormulaSecurityResult with validation results
    """
    start_time = time.time()
    violations = []

    try:
        # Basic validation
        if not formula or not isinstance(formula, str):
            violations.append(
                SecurityViolation(
                    violation_type="invalid_input",
                    description="Formula must be a non-empty string",
                    severity=SecurityLevel.CRITICAL,
                )
            )
            return FormulaSecurityResult(is_safe=False, security_level=SecurityLevel.CRITICAL, violations=violations)

        # Length validation
        if len(formula) > SECURITY_CONFIG["max_formula_length"]:
            violations.append(
                SecurityViolation(
                    violation_type="formula_too_long",
                    description=f"Formula exceeds maximum length of {SECURITY_CONFIG['max_formula_length']} characters",
                    severity=SecurityLevel.HIGH,
                )
            )

        # Pattern detection
        dangerous_patterns = detect_dangerous_patterns(formula)
        violations.extend(dangerous_patterns)

        # AST validation
        try:
            tree = ast.parse(formula, mode="eval")
            ast_violations = validate_ast_security(tree, security_level)
            violations.extend(ast_violations)
        except SyntaxError as e:
            violations.append(
                SecurityViolation(
                    violation_type="syntax_error",
                    description=f"Formula contains syntax error: {str(e)}",
                    severity=SecurityLevel.HIGH,
                    line_number=e.lineno,
                    column_number=e.offset,
                )
            )

        # Determine overall security level
        max_severity = SecurityLevel.LOW
        for violation in violations:
            if violation.severity.value == SecurityLevel.CRITICAL.value:
                max_severity = SecurityLevel.CRITICAL
                break
            elif violation.severity.value == SecurityLevel.HIGH.value:
                max_severity = SecurityLevel.HIGH
            elif violation.severity.value == SecurityLevel.MEDIUM.value and max_severity == SecurityLevel.LOW:
                max_severity = SecurityLevel.MEDIUM

        # Check if formula is safe
        is_safe = max_severity.value not in [SecurityLevel.CRITICAL.value, SecurityLevel.HIGH.value]

        # Sanitize formula if needed
        sanitized_formula = sanitize_formula(formula) if not is_safe else formula

        execution_time = (time.time() - start_time) * 1000

        return FormulaSecurityResult(
            is_safe=is_safe,
            security_level=max_severity,
            violations=violations,
            sanitized_formula=sanitized_formula,
            execution_time_ms=execution_time,
            metadata={
                "formula_length": len(formula),
                "validation_time_ms": execution_time,
                "security_level": security_level.value,
            },
        )

    except Exception as e:
        error(f"Security validation failed: {str(e)}")
        violations.append(
            SecurityViolation(
                violation_type="validation_error",
                description=f"Security validation failed: {str(e)}",
                severity=SecurityLevel.CRITICAL,
            )
        )

        return FormulaSecurityResult(
            is_safe=False,
            security_level=SecurityLevel.CRITICAL,
            violations=violations,
            execution_time_ms=(time.time() - start_time) * 1000,
        )


def detect_dangerous_patterns(formula: str) -> List[SecurityViolation]:
    """
    Detect dangerous patterns in formula using regex.

    Args:
        formula: Formula expression to check

    Returns:
        List of security violations found
    """
    violations = []

    for pattern in SECURITY_CONFIG["forbidden_patterns"]:
        matches = re.finditer(pattern, formula, re.IGNORECASE)
        for match in matches:
            violations.append(
                SecurityViolation(
                    violation_type="dangerous_pattern",
                    description=f"Dangerous pattern detected: {pattern}",
                    severity=SecurityLevel.HIGH,
                    pattern=pattern,
                )
            )

    return violations


def validate_ast_security(tree: ast.AST, security_level: SecurityLevel) -> List[SecurityViolation]:
    """
    Validate AST for security violations.

    Args:
        tree: AST to validate
        security_level: Security level for validation

    Returns:
        List of security violations found
    """
    violations = []

    class SecurityVisitor(ast.NodeVisitor):
        def __init__(self):
            self.violations = []
            self.recursion_depth = 0
            self.loop_count = 0

        def visit(self, node):
            # Check recursion depth
            self.recursion_depth += 1
            if self.recursion_depth > SECURITY_CONFIG["max_recursion_depth"]:
                self.violations.append(
                    SecurityViolation(
                        violation_type="recursion_depth_exceeded",
                        description=f"Recursion depth exceeds maximum of {SECURITY_CONFIG['max_recursion_depth']}",
                        severity=SecurityLevel.HIGH,
                        line_number=getattr(node, "lineno", None),
                        column_number=getattr(node, "col_offset", None),
                    )
                )
                return

            # Check for dangerous node types
            if type(node) in SECURITY_CONFIG["dangerous_nodes"]:
                self.violations.append(
                    SecurityViolation(
                        violation_type="dangerous_node",
                        description=f"Dangerous AST node type: {type(node).__name__}",
                        severity=SecurityLevel.HIGH,
                        line_number=getattr(node, "lineno", None),
                        column_number=getattr(node, "col_offset", None),
                    )
                )

            # Check for function calls
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    func_name = node.func.id
                    if func_name not in SECURITY_CONFIG["allowed_builtins"]:
                        self.violations.append(
                            SecurityViolation(
                                violation_type="forbidden_function",
                                description=f"Forbidden function call: {func_name}",
                                severity=SecurityLevel.HIGH,
                                line_number=getattr(node, "lineno", None),
                                column_number=getattr(node, "col_offset", None),
                            )
                        )

            # Check for loops
            if isinstance(node, (ast.For, ast.While)):
                self.loop_count += 1
                if self.loop_count > 5:  # Max 5 nested loops
                    self.violations.append(
                        SecurityViolation(
                            violation_type="too_many_loops",
                            description="Too many nested loops detected",
                            severity=SecurityLevel.MEDIUM,
                            line_number=getattr(node, "lineno", None),
                            column_number=getattr(node, "col_offset", None),
                        )
                    )

            # Check for attribute access
            if isinstance(node, ast.Attribute):
                if isinstance(node.value, ast.Name):
                    obj_name = node.value.id
                    attr_name = node.attr
                    if obj_name in ["os", "sys", "subprocess", "socket"]:
                        self.violations.append(
                            SecurityViolation(
                                violation_type="dangerous_attribute",
                                description=f"Dangerous attribute access: {obj_name}.{attr_name}",
                                severity=SecurityLevel.CRITICAL,
                                line_number=getattr(node, "lineno", None),
                                column_number=getattr(node, "col_offset", None),
                            )
                        )

            self.generic_visit(node)
            self.recursion_depth -= 1

    visitor = SecurityVisitor()
    visitor.visit(tree)

    return visitor.violations


def sanitize_formula(formula: str) -> str:
    """
    Sanitize formula by removing dangerous patterns.

    Args:
        formula: Formula to sanitize

    Returns:
        Sanitized formula
    """
    sanitized = formula

    # Remove dangerous patterns
    for pattern in SECURITY_CONFIG["forbidden_patterns"]:
        sanitized = re.sub(pattern, "", sanitized, flags=re.IGNORECASE)

    # Remove comments
    sanitized = re.sub(r"#.*$", "", sanitized, flags=re.MULTILINE)

    # Remove multiple spaces
    sanitized = re.sub(r"\s+", " ", sanitized)

    # Strip whitespace
    sanitized = sanitized.strip()

    return sanitized


@contextmanager
def timeout_context(seconds: int):
    """
    Context manager for timeout handling.

    Args:
        seconds: Timeout in seconds
    """
    # Use threading-based timeout instead of signal (more portable)
    import threading
    import time

    timeout_occurred = threading.Event()

    def timeout_handler():
        time.sleep(seconds)
        timeout_occurred.set()

    timeout_thread = threading.Thread(target=timeout_handler)
    timeout_thread.daemon = True
    timeout_thread.start()

    try:
        yield timeout_occurred
    finally:
        # Clean up
        pass


def execute_formula_safely(
    formula: str, context: Optional[Dict[str, Any]] = None, security_level: SecurityLevel = SecurityLevel.MEDIUM
) -> Tuple[Any, FormulaSecurityResult]:
    """
    Execute formula safely with security validation.

    Args:
        formula: Formula to execute
        context: Execution context
        security_level: Security level for validation

    Returns:
        Tuple of (result, security_result)
    """
    start_time = time.time()

    # Validate security first
    security_result = validate_formula_security(formula, context, security_level)

    if not security_result.is_safe:
        error(f"Formula security validation failed: {len(security_result.violations)} violations")
        return None, security_result

    try:
        # Set resource limits
        resource.setrlimit(
            resource.RLIMIT_AS,
            (SECURITY_CONFIG["max_memory_mb"] * 1024 * 1024, SECURITY_CONFIG["max_memory_mb"] * 1024 * 1024),
        )

        # Execute with timeout
        with timeout_context(SECURITY_CONFIG["max_execution_time_seconds"]) as timeout_event:
            # Create safe execution environment
            safe_builtins = {}
            for name in SECURITY_CONFIG["allowed_builtins"]:
                if hasattr(__builtins__, name):
                    safe_builtins[name] = getattr(__builtins__, name)

            safe_globals = {"__builtins__": safe_builtins}

            # Add context variables
            if context:
                safe_globals.update(context)

            # Check for timeout before execution
            if timeout_event.is_set():
                raise TimeoutException(
                    f"Formula execution timed out after {SECURITY_CONFIG['max_execution_time_seconds']} seconds"
                )

            try:
                # Compile and execute
                code = compile(formula, "<formula>", "eval")
                result = eval(code, safe_globals, {})

                # Check for timeout after execution
                if timeout_event.is_set():
                    raise TimeoutException(
                        f"Formula execution timed out after {SECURITY_CONFIG['max_execution_time_seconds']} seconds"
                    )

                # Update security result
                execution_time = (time.time() - start_time) * 1000
                security_result.execution_time_ms = execution_time
                security_result.metadata["execution_time_ms"] = execution_time

                success(f"Formula executed safely in {execution_time:.2f}ms")
                return result, security_result

            except Exception as e:
                if timeout_event.is_set():
                    raise TimeoutException(
                        f"Formula execution timed out after {SECURITY_CONFIG['max_execution_time_seconds']} seconds"
                    )
                raise e

    except TimeoutException:
        error("Formula execution timed out")
        security_result.violations.append(
            SecurityViolation(
                violation_type="execution_timeout", description="Formula execution timed out", severity=SecurityLevel.HIGH
            )
        )
        security_result.is_safe = False
        return None, security_result

    except MemoryLimitException:
        error("Formula exceeded memory limit")
        security_result.violations.append(
            SecurityViolation(
                violation_type="memory_limit_exceeded",
                description="Formula exceeded memory limit",
                severity=SecurityLevel.HIGH,
            )
        )
        security_result.is_safe = False
        return None, security_result

    except Exception as e:
        error(f"Formula execution failed: {str(e)}")
        security_result.violations.append(
            SecurityViolation(
                violation_type="execution_error",
                description=f"Formula execution failed: {str(e)}",
                severity=SecurityLevel.MEDIUM,
            )
        )
        security_result.is_safe = False
        return None, security_result


def check_formula_safety(formula: str, context: Optional[Dict[str, Any]] = None) -> bool:
    """
    Quick safety check for formula.

    Args:
        formula: Formula to check
        context: Optional context

    Returns:
        True if formula is safe, False otherwise
    """
    result = validate_formula_security(formula, context)
    return result.is_safe


def prevent_infinite_loops(formula: str) -> bool:
    """
    Check if formula contains potential infinite loops.

    Args:
        formula: Formula to check

    Returns:
        True if formula is safe from infinite loops, False otherwise
    """
    # Check for while True patterns
    if re.search(r"while\s+True:", formula, re.IGNORECASE):
        return False

    # Check for range with large values
    range_matches = re.finditer(r"range\s*\(\s*(\d+)\s*\)", formula)
    for match in range_matches:
        range_value = int(match.group(1))
        if range_value > SECURITY_CONFIG["max_iterations"]:
            return False

    # Check for nested loops
    loop_count = len(re.findall(r"for\s+\w+\s+in\s+", formula, re.IGNORECASE))
    if loop_count > 3:  # Max 3 nested loops
        return False

    return True


# Export all functions
__all__ = [
    "validate_formula_security",
    "detect_dangerous_patterns",
    "validate_ast_security",
    "sanitize_formula",
    "execute_formula_safely",
    "check_formula_safety",
    "prevent_infinite_loops",
    "FormulaSecurityResult",
    "SecurityViolation",
    "SecurityLevel",
    "TimeoutException",
    "MemoryLimitException",
    "SecurityViolationException",
]
