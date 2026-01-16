"""
TableV2 Security Module.

This module provides comprehensive security and validation for user-created content,
including formula security, input validation, audit logging, and rate limiting.
"""

from .audit_logging import (
    AuditLogEntry,
    AuditLogLevel,
    SecurityContext,
    get_audit_log,
    log_customization_change,
    log_formula_execution,
    log_security_event,
    log_user_action,
)
from .formula_security import (
    FormulaSecurityResult,
    SecurityLevel,
    SecurityViolation,
    check_formula_safety,
    detect_dangerous_patterns,
    execute_formula_safely,
    prevent_infinite_loops,
    sanitize_formula,
    validate_formula_security,
)
from .input_validation import (
    InputSecurityLevel,
    InputValidationResult,
    ValidationError,
    sanitize_column_name,
    sanitize_filter_value,
    validate_customization_input,
    validate_user_customizations,
    validate_user_input,
)
from .rate_limiting import (
    RateLimitConfig,
    RateLimitResult,
    RateLimitViolation,
    check_rate_limit,
    configure_rate_limits,
    get_rate_limit_status,
    increment_rate_limit,
    reset_rate_limit,
)
from .security_testing import (
    SecurityTestResult,
    SecurityTestSuite,
    run_security_test_suite,
    test_audit_logging,
    test_formula_injection,
    test_input_validation,
    test_rate_limiting,
)

# Security configuration constants
SECURITY_CONFIG = {
    "formula_timeout_seconds": 5,
    "max_formula_complexity": 100,
    "max_iterations": 1000,
    "max_memory_mb": 50,
    "rate_limit_per_minute": 60,
    "rate_limit_per_hour": 1000,
    "audit_log_retention_days": 90,
}

# Export all public functions and classes
__all__ = [
    # Formula security
    "validate_formula_security",
    "sanitize_formula",
    "execute_formula_safely",
    "check_formula_safety",
    "detect_dangerous_patterns",
    "prevent_infinite_loops",
    "FormulaSecurityResult",
    "SecurityViolation",
    "SecurityLevel",
    # Input validation
    "validate_user_input",
    "sanitize_column_name",
    "validate_customization_input",
    "sanitize_filter_value",
    "validate_user_customizations",
    "InputValidationResult",
    "ValidationError",
    "InputSecurityLevel",
    # Audit logging
    "log_formula_execution",
    "log_customization_change",
    "log_user_action",
    "log_security_event",
    "get_audit_log",
    "AuditLogEntry",
    "AuditLogLevel",
    "SecurityContext",
    # Rate limiting
    "check_rate_limit",
    "increment_rate_limit",
    "get_rate_limit_status",
    "reset_rate_limit",
    "configure_rate_limits",
    "RateLimitResult",
    "RateLimitConfig",
    "RateLimitViolation",
    # Security testing
    "test_formula_injection",
    "test_input_validation",
    "test_rate_limiting",
    "test_audit_logging",
    "run_security_test_suite",
    "SecurityTestResult",
    "SecurityTestSuite",
    # Configuration
    "SECURITY_CONFIG",
]
