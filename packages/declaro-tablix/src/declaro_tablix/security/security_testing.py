"""
Security testing framework for TableV2.

This module provides comprehensive security testing including formula injection,
input validation, rate limiting, and audit logging tests.
"""

import random
import string
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from declaro_advise import error, info, success, warning

from .audit_logging import AuditLogLevel, get_audit_log, log_security_event
from .formula_security import SecurityLevel, execute_formula_safely, validate_formula_security
from .input_validation import InputSecurityLevel, validate_user_customizations, validate_user_input
from .rate_limiting import RateLimitType, check_rate_limit, increment_rate_limit


class SecurityTestLevel(Enum):
    """Security test levels."""

    BASIC = "basic"
    COMPREHENSIVE = "comprehensive"
    PENETRATION = "penetration"
    STRESS = "stress"


@dataclass
class SecurityTestResult:
    """Result of a security test."""

    test_name: str
    test_type: str
    success: bool
    vulnerabilities_found: int
    blocked_attempts: int
    false_positives: int
    execution_time_ms: float
    details: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)


@dataclass
class SecurityTestSuite:
    """Collection of security test results."""

    suite_name: str
    test_level: SecurityTestLevel
    tests: List[SecurityTestResult]
    overall_success: bool
    total_vulnerabilities: int
    total_blocked_attempts: int
    total_execution_time_ms: float
    security_score: float
    recommendations: List[str] = field(default_factory=list)


# Security test payloads
MALICIOUS_FORMULAS = [
    # Code injection attempts
    "__import__('os').system('rm -rf /')",
    "exec('import os; os.system(\"whoami\")')",
    'eval(\'__import__("subprocess").call(["ls", "/"])\')',
    "compile('import sys; sys.exit()', '<string>', 'exec')",
    # Import attempts
    "import os; os.listdir('/')",
    "import subprocess; subprocess.call(['ls'])",
    "import sys; sys.modules",
    "from os import system; system('id')",
    # Dangerous function calls
    "globals()",
    "locals()",
    "vars()",
    "dir()",
    "getattr(object, '__class__')",
    "setattr(object, 'x', 1)",
    "delattr(object, 'x')",
    "hasattr(object, '__dict__')",
    # File operations
    "open('/etc/passwd', 'r').read()",
    "file('/etc/passwd').read()",
    "with open('/etc/passwd') as f: f.read()",
    # Network operations
    "urllib.request.urlopen('http://evil.com')",
    "requests.get('http://evil.com')",
    "socket.socket().connect(('evil.com', 80))",
    # Infinite loops
    "while True: pass",
    "for i in range(999999999): pass",
    "[i for i in range(999999999)]",
    # Memory exhaustion
    "'a' * 999999999",
    "list(range(999999999))",
    "{i: i for i in range(999999999)}",
    # Recursion bombs
    "def f(): return f(); f()",
    "lambda: (lambda: f())() if 'f' in locals() else None",
    # Advanced attacks
    "type('', (), {'__getattribute__': lambda *_: __import__('os').system('whoami')})()",
    "().__class__.__bases__[0].__subclasses__()[104].__init__.__globals__['sys'].exit()",
    "getattr(__builtins__, 'exec')('import os; os.system(\"id\")')",
]

MALICIOUS_INPUTS = [
    # SQL injection
    "'; DROP TABLE users; --",
    "1' OR '1'='1",
    "admin'--",
    "1' UNION SELECT * FROM users --",
    "'; EXEC xp_cmdshell('dir'); --",
    # XSS attacks
    "<script>alert('XSS')</script>",
    "<img src=x onerror=alert('XSS')>",
    "javascript:alert('XSS')",
    "<svg onload=alert('XSS')>",
    "';alert('XSS');//",
    # HTML injection
    "<iframe src='javascript:alert(1)'></iframe>",
    "<object data='javascript:alert(1)'></object>",
    "<embed src='javascript:alert(1)'>",
    "<link rel=stylesheet href='javascript:alert(1)'>",
    # Path traversal
    "../../../etc/passwd",
    "..\\..\\..\\windows\\system32\\config\\sam",
    "....//....//etc/passwd",
    "%2e%2e%2f%2e%2e%2f%2e%2e%2fetc%2fpasswd",
    # Command injection
    "; ls -la",
    "| whoami",
    "$(whoami)",
    "`whoami`",
    "&& dir",
    # Template injection
    "{{7*7}}",
    "${7*7}",
    "#{7*7}",
    "{{config.items()}}",
    "${@print(system('id'))}",
    # LDAP injection
    "*)(&(password=*))",
    "admin)(&(password=*))",
    "*))(|(password=*))",
    # XXE attacks
    "<!DOCTYPE foo [<!ENTITY xxe SYSTEM 'file:///etc/passwd'>]><foo>&xxe;</foo>",
    # Large inputs
    "A" * 10000,
    "1" * 10000,
    "x" * 100000,
    # Unicode attacks
    "\\u0000",
    "\\u0001",
    "\\u001f",
    "\\u007f",
    "\\u0080",
    "\\u00ff",
    # Null bytes
    "test\\x00.txt",
    "admin\\x00.php",
    # Format string attacks
    "%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s",
    "%08x" * 10,
    "%p" * 10,
]

STRESS_TEST_PATTERNS = [
    # High-frequency patterns
    "rapid_fire_requests",
    "concurrent_user_attack",
    "distributed_load",
    "resource_exhaustion",
    "memory_bomb",
    "cpu_bomb",
    "network_flood",
    "database_overload",
]


def test_formula_injection(
    test_level: SecurityTestLevel = SecurityTestLevel.BASIC, max_tests: int = 100
) -> SecurityTestResult:
    """
    Test formula injection vulnerabilities.

    Args:
        test_level: Security test level
        max_tests: Maximum number of tests to run

    Returns:
        SecurityTestResult with test results
    """
    start_time = time.time()

    vulnerabilities_found = 0
    blocked_attempts = 0
    false_positives = 0
    test_details = {
        "payloads_tested": [],
        "vulnerabilities": [],
        "blocked_payloads": [],
        "false_positives": [],
    }

    # Select test payloads based on level
    if test_level == SecurityTestLevel.BASIC:
        test_payloads = MALICIOUS_FORMULAS[:20]
    elif test_level == SecurityTestLevel.COMPREHENSIVE:
        test_payloads = MALICIOUS_FORMULAS[:50]
    else:  # PENETRATION or STRESS
        test_payloads = MALICIOUS_FORMULAS

    # Limit tests
    test_payloads = test_payloads[:max_tests]

    info(f"Testing {len(test_payloads)} formula injection payloads")

    for i, payload in enumerate(test_payloads):
        try:
            # Test formula security validation
            security_result = validate_formula_security(payload, security_level=SecurityLevel.HIGH)

            test_details["payloads_tested"].append(
                {
                    "payload": payload[:100] + "..." if len(payload) > 100 else payload,
                    "index": i,
                    "is_safe": security_result.is_safe,
                    "violations": len(security_result.violations),
                    "security_level": security_result.security_level.value,
                }
            )

            if security_result.is_safe:
                # Should have been blocked but wasn't
                vulnerabilities_found += 1
                test_details["vulnerabilities"].append(
                    {
                        "payload": payload,
                        "reason": "malicious_formula_not_blocked",
                        "security_level": security_result.security_level.value,
                    }
                )
                warning(f"Vulnerability found: Formula not blocked - {payload[:50]}...")
            else:
                # Correctly blocked
                blocked_attempts += 1
                test_details["blocked_payloads"].append(
                    {
                        "payload": payload[:100] + "..." if len(payload) > 100 else payload,
                        "violations": [v.violation_type for v in security_result.violations],
                    }
                )

            # Test formula execution
            try:
                result, exec_security_result = execute_formula_safely(payload, security_level=SecurityLevel.HIGH)

                if result is not None:
                    # Execution succeeded when it should have failed
                    vulnerabilities_found += 1
                    test_details["vulnerabilities"].append(
                        {
                            "payload": payload,
                            "reason": "malicious_formula_executed",
                            "result": str(result)[:100],
                        }
                    )
                    error(f"Critical vulnerability: Formula executed - {payload[:50]}...")

            except Exception:
                # Execution failed as expected
                pass

        except Exception as e:
            # Unexpected error
            test_details["vulnerabilities"].append(
                {
                    "payload": payload,
                    "reason": "unexpected_error",
                    "error": str(e),
                }
            )
            vulnerabilities_found += 1

    execution_time = (time.time() - start_time) * 1000

    # Generate recommendations
    recommendations = []
    if vulnerabilities_found > 0:
        recommendations.append("Strengthen formula validation patterns")
        recommendations.append("Implement additional AST-based security checks")
        recommendations.append("Add more dangerous function detection")

    if vulnerabilities_found > len(test_payloads) * 0.1:
        recommendations.append("CRITICAL: High vulnerability rate detected")
        recommendations.append("Implement emergency security measures")

    success_rate = (blocked_attempts / len(test_payloads)) * 100
    if success_rate < 90:
        recommendations.append("Improve security detection rate (currently {:.1f}%)".format(success_rate))

    return SecurityTestResult(
        test_name="formula_injection",
        test_type="security_validation",
        success=vulnerabilities_found == 0,
        vulnerabilities_found=vulnerabilities_found,
        blocked_attempts=blocked_attempts,
        false_positives=false_positives,
        execution_time_ms=execution_time,
        details=test_details,
        recommendations=recommendations,
    )


def test_input_validation(
    test_level: SecurityTestLevel = SecurityTestLevel.BASIC, max_tests: int = 100
) -> SecurityTestResult:
    """
    Test input validation vulnerabilities.

    Args:
        test_level: Security test level
        max_tests: Maximum number of tests to run

    Returns:
        SecurityTestResult with test results
    """
    start_time = time.time()

    vulnerabilities_found = 0
    blocked_attempts = 0
    false_positives = 0
    test_details = {
        "inputs_tested": [],
        "vulnerabilities": [],
        "blocked_inputs": [],
        "false_positives": [],
    }

    # Select test inputs based on level
    if test_level == SecurityTestLevel.BASIC:
        test_inputs = MALICIOUS_INPUTS[:30]
    elif test_level == SecurityTestLevel.COMPREHENSIVE:
        test_inputs = MALICIOUS_INPUTS[:70]
    else:  # PENETRATION or STRESS
        test_inputs = MALICIOUS_INPUTS

    # Limit tests
    test_inputs = test_inputs[:max_tests]

    info(f"Testing {len(test_inputs)} input validation payloads")

    for i, malicious_input in enumerate(test_inputs):
        try:
            # Test input validation
            validation_result = validate_user_input(
                malicious_input, f"test_field_{i}", security_level=InputSecurityLevel.STRICT
            )

            test_details["inputs_tested"].append(
                {
                    "input": malicious_input[:100] + "..." if len(malicious_input) > 100 else malicious_input,
                    "index": i,
                    "is_valid": validation_result.is_valid,
                    "errors": len(validation_result.errors),
                    "sanitized": validation_result.sanitized_value[:100] if validation_result.sanitized_value else None,
                }
            )

            if validation_result.is_valid:
                # Should have been blocked but wasn't
                vulnerabilities_found += 1
                test_details["vulnerabilities"].append(
                    {
                        "input": malicious_input,
                        "reason": "malicious_input_not_blocked",
                        "sanitized_value": validation_result.sanitized_value,
                    }
                )
                warning(f"Vulnerability found: Input not blocked - {malicious_input[:50]}...")
            else:
                # Correctly blocked
                blocked_attempts += 1
                test_details["blocked_inputs"].append(
                    {
                        "input": malicious_input[:100] + "..." if len(malicious_input) > 100 else malicious_input,
                        "errors": [e.error_type for e in validation_result.errors],
                    }
                )

            # Test customization validation
            if i % 10 == 0:  # Test every 10th input as customization
                customization_data = {
                    "column_name": malicious_input,
                    "filter_value": malicious_input,
                    "formula": malicious_input,
                }

                customization_result = validate_user_customizations(
                    customization_data, f"test_user_{i}", security_level=InputSecurityLevel.STRICT
                )

                if customization_result.is_valid:
                    vulnerabilities_found += 1
                    test_details["vulnerabilities"].append(
                        {
                            "input": malicious_input,
                            "reason": "malicious_customization_not_blocked",
                            "customization_data": customization_data,
                        }
                    )

        except Exception as e:
            # Unexpected error
            test_details["vulnerabilities"].append(
                {
                    "input": malicious_input,
                    "reason": "unexpected_error",
                    "error": str(e),
                }
            )
            vulnerabilities_found += 1

    execution_time = (time.time() - start_time) * 1000

    # Generate recommendations
    recommendations = []
    if vulnerabilities_found > 0:
        recommendations.append("Strengthen input validation patterns")
        recommendations.append("Implement additional sanitization rules")
        recommendations.append("Add more dangerous pattern detection")

    if vulnerabilities_found > len(test_inputs) * 0.1:
        recommendations.append("CRITICAL: High vulnerability rate detected")
        recommendations.append("Implement emergency input filtering")

    success_rate = (blocked_attempts / len(test_inputs)) * 100
    if success_rate < 95:
        recommendations.append("Improve input validation rate (currently {:.1f}%)".format(success_rate))

    return SecurityTestResult(
        test_name="input_validation",
        test_type="security_validation",
        success=vulnerabilities_found == 0,
        vulnerabilities_found=vulnerabilities_found,
        blocked_attempts=blocked_attempts,
        false_positives=false_positives,
        execution_time_ms=execution_time,
        details=test_details,
        recommendations=recommendations,
    )


def test_rate_limiting(
    test_level: SecurityTestLevel = SecurityTestLevel.BASIC, max_concurrent_users: int = 50
) -> SecurityTestResult:
    """
    Test rate limiting effectiveness.

    Args:
        test_level: Security test level
        max_concurrent_users: Maximum concurrent users to simulate

    Returns:
        SecurityTestResult with test results
    """
    start_time = time.time()

    vulnerabilities_found = 0
    blocked_attempts = 0
    false_positives = 0
    test_details = {
        "rate_limit_tests": [],
        "vulnerabilities": [],
        "blocked_requests": [],
        "concurrent_test_results": [],
    }

    # Determine test intensity based on level
    if test_level == SecurityTestLevel.BASIC:
        test_users = 10
        requests_per_user = 20
    elif test_level == SecurityTestLevel.COMPREHENSIVE:
        test_users = 25
        requests_per_user = 50
    else:  # PENETRATION or STRESS
        test_users = min(max_concurrent_users, 100)
        requests_per_user = 100

    info(f"Testing rate limiting with {test_users} users, {requests_per_user} requests each")

    # Test different rate limit types
    for limit_type in [RateLimitType.FORMULA_EXECUTION, RateLimitType.CUSTOMIZATION_CHANGE, RateLimitType.USER_ACTION]:
        user_id = f"test_user_{limit_type.value}"

        # Single user burst test
        allowed_requests = 0
        blocked_requests = 0

        for i in range(requests_per_user):
            rate_limit_result = increment_rate_limit(user_id, limit_type)

            if rate_limit_result.allowed:
                allowed_requests += 1
            else:
                blocked_requests += 1

        test_details["rate_limit_tests"].append(
            {
                "limit_type": limit_type.value,
                "user_id": user_id,
                "allowed_requests": allowed_requests,
                "blocked_requests": blocked_requests,
                "total_requests": requests_per_user,
            }
        )

        # Check if rate limiting is working
        if blocked_requests == 0 and requests_per_user > 30:
            # Should have been rate limited
            vulnerabilities_found += 1
            test_details["vulnerabilities"].append(
                {
                    "limit_type": limit_type.value,
                    "reason": "rate_limit_not_enforced",
                    "allowed_requests": allowed_requests,
                    "expected_blocks": "some",
                }
            )
            warning(f"Rate limiting not enforced for {limit_type.value}")
        else:
            blocked_attempts += blocked_requests

    # Concurrent user test
    def concurrent_user_test(user_index: int) -> Dict[str, Any]:
        """Test function for concurrent users."""
        user_id = f"concurrent_user_{user_index}"
        allowed = 0
        blocked = 0

        for _ in range(20):  # Each user makes 20 requests
            result = increment_rate_limit(user_id, RateLimitType.API_REQUEST)
            if result.allowed:
                allowed += 1
            else:
                blocked += 1

            time.sleep(0.01)  # Small delay between requests

        return {
            "user_id": user_id,
            "allowed": allowed,
            "blocked": blocked,
        }

    # Run concurrent test
    with ThreadPoolExecutor(max_workers=test_users) as executor:
        futures = [executor.submit(concurrent_user_test, i) for i in range(test_users)]
        concurrent_results = [future.result() for future in futures]

    test_details["concurrent_test_results"] = concurrent_results

    # Analyze concurrent results
    total_concurrent_allowed = sum(result["allowed"] for result in concurrent_results)
    total_concurrent_blocked = sum(result["blocked"] for result in concurrent_results)

    if total_concurrent_blocked == 0 and total_concurrent_allowed > test_users * 10:
        vulnerabilities_found += 1
        test_details["vulnerabilities"].append(
            {
                "reason": "concurrent_rate_limit_not_enforced",
                "total_allowed": total_concurrent_allowed,
                "total_blocked": total_concurrent_blocked,
            }
        )
    else:
        blocked_attempts += total_concurrent_blocked

    execution_time = (time.time() - start_time) * 1000

    # Generate recommendations
    recommendations = []
    if vulnerabilities_found > 0:
        recommendations.append("Strengthen rate limiting enforcement")
        recommendations.append("Implement stricter limits for suspicious activity")
        recommendations.append("Add distributed rate limiting support")

    if vulnerabilities_found > 0:
        recommendations.append("CRITICAL: Rate limiting bypassed")
        recommendations.append("Implement emergency rate limiting")

    return SecurityTestResult(
        test_name="rate_limiting",
        test_type="security_enforcement",
        success=vulnerabilities_found == 0,
        vulnerabilities_found=vulnerabilities_found,
        blocked_attempts=blocked_attempts,
        false_positives=false_positives,
        execution_time_ms=execution_time,
        details=test_details,
        recommendations=recommendations,
    )


def test_audit_logging(test_level: SecurityTestLevel = SecurityTestLevel.BASIC) -> SecurityTestResult:
    """
    Test audit logging completeness.

    Args:
        test_level: Security test level

    Returns:
        SecurityTestResult with test results
    """
    start_time = time.time()

    vulnerabilities_found = 0
    blocked_attempts = 0
    false_positives = 0
    test_details = {
        "audit_tests": [],
        "vulnerabilities": [],
        "logged_events": [],
        "missing_logs": [],
    }

    # Test different types of events
    test_events = [
        ("formula_execution", "test_formula", {"success": True}),
        ("customization_change", "test_customization", {"change_type": "column_alias"}),
        ("user_action", "test_action", {"action": "view_table"}),
        ("security_event", "test_security", {"severity": "high"}),
    ]

    if test_level in [SecurityTestLevel.COMPREHENSIVE, SecurityTestLevel.PENETRATION]:
        test_events.extend(
            [
                ("authentication", "test_auth", {"method": "password"}),
                ("authorization", "test_authz", {"resource": "table"}),
                ("data_access", "test_data", {"table": "users"}),
                ("configuration_change", "test_config", {"setting": "security"}),
            ]
        )

    info(f"Testing audit logging with {len(test_events)} event types")

    # Log test events
    for event_type, event_name, event_data in test_events:
        try:
            # Log the event
            log_entry = log_security_event(
                user_id="test_user",
                event_type=event_type,
                severity=AuditLogLevel.INFO,
                message=f"Test event: {event_name}",
                event_data=event_data,
            )

            test_details["logged_events"].append(
                {
                    "event_type": event_type,
                    "event_name": event_name,
                    "entry_id": log_entry.entry_id,
                    "timestamp": log_entry.timestamp.isoformat(),
                }
            )

        except Exception as e:
            vulnerabilities_found += 1
            test_details["vulnerabilities"].append(
                {
                    "event_type": event_type,
                    "reason": "logging_failed",
                    "error": str(e),
                }
            )

    # Verify logs were recorded
    time.sleep(0.1)  # Small delay to ensure logs are recorded

    audit_logs = get_audit_log(user_id="test_user", limit=len(test_events) * 2)

    logged_event_types = {log.event_type for log in audit_logs}
    expected_event_types = {event[0] for event in test_events}

    missing_event_types = expected_event_types - logged_event_types

    if missing_event_types:
        vulnerabilities_found += len(missing_event_types)
        test_details["missing_logs"] = list(missing_event_types)
        test_details["vulnerabilities"].append(
            {
                "reason": "incomplete_audit_logging",
                "missing_event_types": list(missing_event_types),
            }
        )

    # Test log retrieval with filters
    filtered_logs = get_audit_log(user_id="test_user", event_type="formula_execution", level=AuditLogLevel.INFO)

    if not filtered_logs:
        vulnerabilities_found += 1
        test_details["vulnerabilities"].append(
            {
                "reason": "log_filtering_failed",
                "expected_logs": "formula_execution logs",
            }
        )

    execution_time = (time.time() - start_time) * 1000

    # Generate recommendations
    recommendations = []
    if vulnerabilities_found > 0:
        recommendations.append("Fix audit logging gaps")
        recommendations.append("Implement comprehensive event logging")
        recommendations.append("Add log integrity verification")

    if missing_event_types:
        recommendations.append(f"Add logging for: {', '.join(missing_event_types)}")

    success_rate = len(logged_event_types) / len(expected_event_types) * 100
    if success_rate < 100:
        recommendations.append("Improve audit logging coverage ({:.1f}%)".format(success_rate))

    return SecurityTestResult(
        test_name="audit_logging",
        test_type="security_monitoring",
        success=vulnerabilities_found == 0,
        vulnerabilities_found=vulnerabilities_found,
        blocked_attempts=len(audit_logs),
        false_positives=false_positives,
        execution_time_ms=execution_time,
        details=test_details,
        recommendations=recommendations,
    )


def run_security_test_suite(
    test_level: SecurityTestLevel = SecurityTestLevel.COMPREHENSIVE, max_tests_per_category: int = 100
) -> SecurityTestSuite:
    """
    Run comprehensive security test suite.

    Args:
        test_level: Security test level
        max_tests_per_category: Maximum tests per category

    Returns:
        SecurityTestSuite with all test results
    """
    start_time = time.time()

    info(f"Starting security test suite at level: {test_level.value}")

    # Run all security tests
    tests = []

    # Formula injection tests
    formula_test = test_formula_injection(test_level, max_tests_per_category)
    tests.append(formula_test)

    # Input validation tests
    input_test = test_input_validation(test_level, max_tests_per_category)
    tests.append(input_test)

    # Rate limiting tests
    rate_limit_test = test_rate_limiting(test_level, max_tests_per_category)
    tests.append(rate_limit_test)

    # Audit logging tests
    audit_test = test_audit_logging(test_level)
    tests.append(audit_test)

    # Calculate overall results
    total_vulnerabilities = sum(test.vulnerabilities_found for test in tests)
    total_blocked_attempts = sum(test.blocked_attempts for test in tests)
    total_execution_time = (time.time() - start_time) * 1000

    # Calculate security score
    total_tests = sum(test.vulnerabilities_found + test.blocked_attempts for test in tests)

    if total_tests > 0:
        security_score = (total_blocked_attempts / total_tests) * 100
    else:
        security_score = 100.0

    # Overall success
    overall_success = total_vulnerabilities == 0

    # Collect all recommendations
    all_recommendations = []
    for test in tests:
        all_recommendations.extend(test.recommendations)

    # Add suite-level recommendations
    if security_score < 90:
        all_recommendations.append("CRITICAL: Security score below 90%")
        all_recommendations.append("Implement immediate security improvements")

    if total_vulnerabilities > 10:
        all_recommendations.append("URGENT: High number of vulnerabilities found")
        all_recommendations.append("Conduct security review and remediation")

    suite = SecurityTestSuite(
        suite_name=f"TableV2 Security Test Suite ({test_level.value})",
        test_level=test_level,
        tests=tests,
        overall_success=overall_success,
        total_vulnerabilities=total_vulnerabilities,
        total_blocked_attempts=total_blocked_attempts,
        total_execution_time_ms=total_execution_time,
        security_score=security_score,
        recommendations=list(set(all_recommendations)),
    )

    # Log results
    if overall_success:
        success(f"Security test suite passed! Score: {security_score:.1f}%")
    else:
        error(f"Security test suite failed! {total_vulnerabilities} vulnerabilities found")

    return suite


# Export all functions
__all__ = [
    "test_formula_injection",
    "test_input_validation",
    "test_rate_limiting",
    "test_audit_logging",
    "run_security_test_suite",
    "SecurityTestResult",
    "SecurityTestSuite",
    "SecurityTestLevel",
]
