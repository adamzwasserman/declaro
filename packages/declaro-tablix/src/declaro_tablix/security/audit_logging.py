"""
Audit logging system for TableV2 security.

This module provides comprehensive audit logging for all security-related
events including formula executions, customization changes, and user actions.
"""

import hashlib
import json
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from declaro_advise import error, info, success, warning


class AuditLogLevel(Enum):
    """Audit log levels."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    SECURITY = "security"


@dataclass
class SecurityContext:
    """Security context for audit logging."""

    user_id: str
    session_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    request_id: Optional[str] = None
    table_name: Optional[str] = None
    action: Optional[str] = None
    resource: Optional[str] = None
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now(timezone.utc)


@dataclass
class AuditLogEntry:
    """Single audit log entry."""

    entry_id: str
    timestamp: datetime
    level: AuditLogLevel
    event_type: str
    message: str
    security_context: SecurityContext
    event_data: Dict[str, Any] = field(default_factory=dict)
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    risk_score: float = 0.0
    tags: List[str] = field(default_factory=list)

    def __post_init__(self):
        if not self.entry_id:
            self.entry_id = self._generate_entry_id()

    def _generate_entry_id(self) -> str:
        """Generate unique entry ID."""
        data = f"{self.timestamp.isoformat()}{self.event_type}{self.security_context.user_id}"
        return hashlib.sha256(data.encode()).hexdigest()[:16]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "entry_id": self.entry_id,
            "timestamp": self.timestamp.isoformat(),
            "level": self.level.value,
            "event_type": self.event_type,
            "message": self.message,
            "security_context": asdict(self.security_context),
            "event_data": self.event_data,
            "performance_metrics": self.performance_metrics,
            "risk_score": self.risk_score,
            "tags": self.tags,
        }


# Global audit log storage
_audit_log_storage = deque(maxlen=10000)  # Keep last 10,000 entries
_audit_log_lock = threading.Lock()

# Audit log configuration
AUDIT_CONFIG = {
    "max_log_entries": 10000,
    "retention_days": 90,
    "enable_performance_metrics": True,
    "enable_risk_scoring": True,
    "high_risk_threshold": 0.7,
    "critical_risk_threshold": 0.9,
    "auto_alert_threshold": 0.8,
    "sensitive_fields": [
        "password",
        "token",
        "secret",
        "key",
        "credential",
        "ssn",
        "social_security",
        "credit_card",
        "bank_account",
    ],
    "event_types": {
        "formula_execution": {"base_risk": 0.3, "tags": ["formula", "execution"]},
        "customization_change": {"base_risk": 0.2, "tags": ["customization", "change"]},
        "user_action": {"base_risk": 0.1, "tags": ["user", "action"]},
        "security_violation": {"base_risk": 0.8, "tags": ["security", "violation"]},
        "authentication": {"base_risk": 0.4, "tags": ["auth", "login"]},
        "authorization": {"base_risk": 0.5, "tags": ["auth", "permission"]},
        "data_access": {"base_risk": 0.3, "tags": ["data", "access"]},
        "configuration_change": {"base_risk": 0.6, "tags": ["config", "change"]},
        "system_error": {"base_risk": 0.4, "tags": ["system", "error"]},
        "performance_issue": {"base_risk": 0.2, "tags": ["performance", "issue"]},
    },
}


def log_formula_execution(
    user_id: str,
    formula: str,
    execution_time_ms: float,
    success: bool,
    result: Any = None,
    error_message: Optional[str] = None,
    security_context: Optional[SecurityContext] = None,
) -> AuditLogEntry:
    """
    Log formula execution event.

    Args:
        user_id: User ID executing the formula
        formula: Formula being executed
        execution_time_ms: Execution time in milliseconds
        success: Whether execution was successful
        result: Execution result (if successful)
        error_message: Error message (if failed)
        security_context: Additional security context

    Returns:
        AuditLogEntry for the logged event
    """
    if security_context is None:
        security_context = SecurityContext(user_id=user_id, action="formula_execution")

    # Calculate risk score
    risk_score = _calculate_risk_score(
        "formula_execution",
        {
            "formula_length": len(formula),
            "execution_time_ms": execution_time_ms,
            "success": success,
            "has_error": bool(error_message),
        },
    )

    # Create log entry
    entry = AuditLogEntry(
        entry_id="",
        timestamp=datetime.now(timezone.utc),
        level=AuditLogLevel.INFO if success else AuditLogLevel.ERROR,
        event_type="formula_execution",
        message=f"Formula execution {'succeeded' if success else 'failed'} for user {user_id}",
        security_context=security_context,
        event_data={
            "formula": _sanitize_sensitive_data(formula),
            "formula_length": len(formula),
            "success": success,
            "result_type": type(result).__name__ if result is not None else None,
            "error_message": error_message,
        },
        performance_metrics={
            "execution_time_ms": execution_time_ms,
        },
        risk_score=risk_score,
        tags=["formula", "execution", "user_action"],
    )

    # Store log entry
    _store_log_entry(entry)

    # Auto-alert if high risk
    if risk_score >= AUDIT_CONFIG["auto_alert_threshold"]:
        warning(f"High-risk formula execution detected: {entry.entry_id}")

    return entry


def log_customization_change(
    user_id: str,
    table_name: str,
    customization_type: str,
    change_data: Dict[str, Any],
    security_context: Optional[SecurityContext] = None,
) -> AuditLogEntry:
    """
    Log customization change event.

    Args:
        user_id: User ID making the change
        table_name: Table being customized
        customization_type: Type of customization
        change_data: Details of the change
        security_context: Additional security context

    Returns:
        AuditLogEntry for the logged event
    """
    if security_context is None:
        security_context = SecurityContext(user_id=user_id, table_name=table_name, action="customization_change")

    # Calculate risk score
    risk_score = _calculate_risk_score(
        "customization_change",
        {
            "customization_type": customization_type,
            "change_data_size": len(json.dumps(change_data)),
            "table_name": table_name,
        },
    )

    # Create log entry
    entry = AuditLogEntry(
        entry_id="",
        timestamp=datetime.now(timezone.utc),
        level=AuditLogLevel.INFO,
        event_type="customization_change",
        message=f"Customization change ({customization_type}) for table {table_name} by user {user_id}",
        security_context=security_context,
        event_data={
            "table_name": table_name,
            "customization_type": customization_type,
            "change_data": _sanitize_sensitive_data(change_data),
            "change_data_size": len(json.dumps(change_data)),
        },
        risk_score=risk_score,
        tags=["customization", "change", "user_action"],
    )

    # Store log entry
    _store_log_entry(entry)

    return entry


def log_user_action(
    user_id: str,
    action: str,
    resource: str,
    details: Optional[Dict[str, Any]] = None,
    security_context: Optional[SecurityContext] = None,
) -> AuditLogEntry:
    """
    Log user action event.

    Args:
        user_id: User ID performing the action
        action: Action being performed
        resource: Resource being acted upon
        details: Additional details
        security_context: Additional security context

    Returns:
        AuditLogEntry for the logged event
    """
    if security_context is None:
        security_context = SecurityContext(user_id=user_id, action=action, resource=resource)

    # Calculate risk score
    risk_score = _calculate_risk_score(
        "user_action",
        {
            "action": action,
            "resource": resource,
            "has_details": bool(details),
        },
    )

    # Create log entry
    entry = AuditLogEntry(
        entry_id="",
        timestamp=datetime.now(timezone.utc),
        level=AuditLogLevel.INFO,
        event_type="user_action",
        message=f"User {user_id} performed action '{action}' on resource '{resource}'",
        security_context=security_context,
        event_data={
            "action": action,
            "resource": resource,
            "details": _sanitize_sensitive_data(details) if details else None,
        },
        risk_score=risk_score,
        tags=["user", "action"],
    )

    # Store log entry
    _store_log_entry(entry)

    return entry


def log_security_event(
    user_id: str,
    event_type: str,
    severity: AuditLogLevel,
    message: str,
    event_data: Optional[Dict[str, Any]] = None,
    security_context: Optional[SecurityContext] = None,
) -> AuditLogEntry:
    """
    Log security-related event.

    Args:
        user_id: User ID associated with the event
        event_type: Type of security event
        severity: Severity level
        message: Event message
        event_data: Additional event data
        security_context: Additional security context

    Returns:
        AuditLogEntry for the logged event
    """
    if security_context is None:
        security_context = SecurityContext(user_id=user_id, action="security_event")

    # Calculate risk score
    risk_score = _calculate_risk_score(
        "security_violation",
        {
            "severity": severity.value,
            "event_type": event_type,
            "has_event_data": bool(event_data),
        },
    )

    # Create log entry
    entry = AuditLogEntry(
        entry_id="",
        timestamp=datetime.now(timezone.utc),
        level=severity,
        event_type=event_type,
        message=message,
        security_context=security_context,
        event_data=_sanitize_sensitive_data(event_data) if event_data else {},
        risk_score=risk_score,
        tags=["security", "violation", "alert"],
    )

    # Store log entry
    _store_log_entry(entry)

    # Auto-alert for security events
    if severity in [AuditLogLevel.ERROR, AuditLogLevel.CRITICAL, AuditLogLevel.SECURITY]:
        error(f"Security event: {message} (Entry ID: {entry.entry_id})")

    return entry


def get_audit_log(
    user_id: Optional[str] = None,
    event_type: Optional[str] = None,
    level: Optional[AuditLogLevel] = None,
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None,
    limit: int = 100,
    include_sensitive: bool = False,
) -> List[AuditLogEntry]:
    """
    Retrieve audit log entries.

    Args:
        user_id: Filter by user ID
        event_type: Filter by event type
        level: Filter by log level
        start_time: Filter by start time
        end_time: Filter by end time
        limit: Maximum number of entries to return
        include_sensitive: Whether to include sensitive data

    Returns:
        List of matching audit log entries
    """
    with _audit_log_lock:
        filtered_entries = []

        for entry in _audit_log_storage:
            # Apply filters
            if user_id and entry.security_context.user_id != user_id:
                continue
            if event_type and entry.event_type != event_type:
                continue
            if level and entry.level != level:
                continue
            if start_time and entry.timestamp < start_time:
                continue
            if end_time and entry.timestamp > end_time:
                continue

            # Create copy for return
            entry_copy = AuditLogEntry(
                entry_id=entry.entry_id,
                timestamp=entry.timestamp,
                level=entry.level,
                event_type=entry.event_type,
                message=entry.message,
                security_context=entry.security_context,
                event_data=entry.event_data.copy() if include_sensitive else _sanitize_sensitive_data(entry.event_data),
                performance_metrics=entry.performance_metrics.copy(),
                risk_score=entry.risk_score,
                tags=entry.tags.copy(),
            )

            filtered_entries.append(entry_copy)

            if len(filtered_entries) >= limit:
                break

        return filtered_entries


def get_audit_statistics(user_id: Optional[str] = None, time_range_hours: int = 24) -> Dict[str, Any]:
    """
    Get audit log statistics.

    Args:
        user_id: Filter by user ID
        time_range_hours: Time range in hours

    Returns:
        Dictionary with audit statistics
    """
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=time_range_hours)

    with _audit_log_lock:
        relevant_entries = [
            entry
            for entry in _audit_log_storage
            if entry.timestamp >= cutoff_time and (not user_id or entry.security_context.user_id == user_id)
        ]

    # Calculate statistics
    stats = {
        "total_entries": len(relevant_entries),
        "time_range_hours": time_range_hours,
        "entries_by_level": {},
        "entries_by_type": {},
        "high_risk_entries": 0,
        "average_risk_score": 0.0,
        "most_active_users": {},
        "most_common_events": {},
        "performance_metrics": {
            "total_execution_time_ms": 0.0,
            "average_execution_time_ms": 0.0,
            "execution_count": 0,
        },
    }

    # Count by level
    for entry in relevant_entries:
        level = entry.level.value
        stats["entries_by_level"][level] = stats["entries_by_level"].get(level, 0) + 1

    # Count by type
    for entry in relevant_entries:
        event_type = entry.event_type
        stats["entries_by_type"][event_type] = stats["entries_by_type"].get(event_type, 0) + 1

    # High risk entries
    stats["high_risk_entries"] = len(
        [entry for entry in relevant_entries if entry.risk_score >= AUDIT_CONFIG["high_risk_threshold"]]
    )

    # Average risk score
    if relevant_entries:
        stats["average_risk_score"] = sum(entry.risk_score for entry in relevant_entries) / len(relevant_entries)

    # Most active users
    user_counts = {}
    for entry in relevant_entries:
        user_id = entry.security_context.user_id
        user_counts[user_id] = user_counts.get(user_id, 0) + 1

    stats["most_active_users"] = dict(sorted(user_counts.items(), key=lambda x: x[1], reverse=True)[:10])

    # Most common events
    event_counts = {}
    for entry in relevant_entries:
        event_type = entry.event_type
        event_counts[event_type] = event_counts.get(event_type, 0) + 1

    stats["most_common_events"] = dict(sorted(event_counts.items(), key=lambda x: x[1], reverse=True)[:10])

    # Performance metrics
    execution_times = [
        entry.performance_metrics.get("execution_time_ms", 0)
        for entry in relevant_entries
        if "execution_time_ms" in entry.performance_metrics
    ]

    if execution_times:
        stats["performance_metrics"]["total_execution_time_ms"] = sum(execution_times)
        stats["performance_metrics"]["average_execution_time_ms"] = sum(execution_times) / len(execution_times)
        stats["performance_metrics"]["execution_count"] = len(execution_times)

    return stats


def _store_log_entry(entry: AuditLogEntry) -> None:
    """
    Store log entry in the audit log storage.

    Args:
        entry: Log entry to store
    """
    with _audit_log_lock:
        _audit_log_storage.append(entry)

        # Log to standard logging system
        log_dict = entry.to_dict()
        info(f"Audit log entry: {json.dumps(log_dict, default=str)}")


def _calculate_risk_score(event_type: str, event_data: Dict[str, Any]) -> float:
    """
    Calculate risk score for an event.

    Args:
        event_type: Type of event
        event_data: Event data for scoring

    Returns:
        Risk score between 0.0 and 1.0
    """
    if not AUDIT_CONFIG["enable_risk_scoring"]:
        return 0.0

    # Base risk score
    base_risk = AUDIT_CONFIG["event_types"].get(event_type, {}).get("base_risk", 0.1)

    # Adjust based on event data
    risk_adjustments = 0.0

    # Formula-specific adjustments
    if event_type == "formula_execution":
        if not event_data.get("success", True):
            risk_adjustments += 0.2
        if event_data.get("execution_time_ms", 0) > 5000:  # > 5 seconds
            risk_adjustments += 0.1
        if event_data.get("formula_length", 0) > 500:  # Long formula
            risk_adjustments += 0.1

    # Security violation adjustments
    if event_type == "security_violation":
        severity = event_data.get("severity", "info")
        if severity == "critical":
            risk_adjustments += 0.3
        elif severity == "error":
            risk_adjustments += 0.2
        elif severity == "warning":
            risk_adjustments += 0.1

    # Data size adjustments
    data_size = event_data.get("change_data_size", 0)
    if data_size > 10000:  # Large data changes
        risk_adjustments += 0.1

    # Calculate final risk score
    risk_score = min(1.0, base_risk + risk_adjustments)

    return risk_score


def _sanitize_sensitive_data(data: Any) -> Any:
    """
    Sanitize sensitive data for logging.

    Args:
        data: Data to sanitize

    Returns:
        Sanitized data
    """
    if isinstance(data, dict):
        sanitized = {}
        for key, value in data.items():
            if any(sensitive in key.lower() for sensitive in AUDIT_CONFIG["sensitive_fields"]):
                sanitized[key] = "[REDACTED]"
            else:
                sanitized[key] = _sanitize_sensitive_data(value)
        return sanitized

    elif isinstance(data, list):
        return [_sanitize_sensitive_data(item) for item in data]

    elif isinstance(data, str):
        # Check for sensitive patterns
        for sensitive in AUDIT_CONFIG["sensitive_fields"]:
            if sensitive in data.lower():
                return "[REDACTED]"
        return data

    else:
        return data


def clear_audit_log() -> int:
    """
    Clear the audit log storage.

    Returns:
        Number of entries cleared
    """
    with _audit_log_lock:
        count = len(_audit_log_storage)
        _audit_log_storage.clear()
        return count


# Export all functions
__all__ = [
    "log_formula_execution",
    "log_customization_change",
    "log_user_action",
    "log_security_event",
    "get_audit_log",
    "get_audit_statistics",
    "clear_audit_log",
    "AuditLogEntry",
    "AuditLogLevel",
    "SecurityContext",
]
