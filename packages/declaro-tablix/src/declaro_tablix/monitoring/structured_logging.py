"""

Structured logging module for TableV2 with environment-based configuration.

This module provides comprehensive JSON-formatted logging with notification integration,
trace ID propagation, and performance-optimized logging for the table system.
"""

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from declaro_advise import error, info, success, warning

# Backend logging - falls back to standard logging when not in buckler
try:
    from backend.utilities.logging_config import get_logger
except ImportError:
    def get_logger(name: str) -> logging.Logger:
        return logging.getLogger(name)

# Global configuration
_LOGGING_CONFIG = {
    "enabled": True,
    "level": "INFO",
    "format_type": "json",
    "output": "console",
    "notification_integration": True,
    "trace_propagation": True,
    "performance_logging": True,
    "max_log_size": 1000000,  # 1MB
    "log_retention_hours": 24,
}

# Thread-local storage for context
_thread_local = threading.local()

# Global logger instance
_logger = None
_logger_lock = threading.Lock()


@dataclass
class LogLevel:
    """Log level enumeration with severity values."""

    DEBUG: str = "DEBUG"
    INFO: str = "INFO"
    WARNING: str = "WARNING"
    ERROR: str = "ERROR"
    CRITICAL: str = "CRITICAL"

    @classmethod
    def get_numeric_level(cls, level: str) -> int:
        """Get numeric level for comparison."""
        levels = {
            cls.DEBUG: 10,
            cls.INFO: 20,
            cls.WARNING: 30,
            cls.ERROR: 40,
            cls.CRITICAL: 50,
        }
        return levels.get(level.upper(), 20)


@dataclass
class LogEntry:
    """Structured log entry with comprehensive context."""

    timestamp: str
    level: str
    message: str
    trace_id: Optional[str] = None
    user_id: Optional[str] = None
    table_name: Optional[str] = None
    operation: Optional[str] = None
    duration_ms: Optional[float] = None
    context: Dict[str, Any] = field(default_factory=dict)
    thread_id: Optional[str] = None
    performance_metrics: Dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        """Convert log entry to JSON string."""
        return json.dumps(
            {
                "timestamp": self.timestamp,
                "level": self.level,
                "message": self.message,
                "trace_id": self.trace_id,
                "user_id": self.user_id,
                "table_name": self.table_name,
                "operation": self.operation,
                "duration_ms": self.duration_ms,
                "context": self.context,
                "thread_id": self.thread_id,
                "performance_metrics": self.performance_metrics,
            }
        )


class StructuredJSONFormatter(logging.Formatter):
    """Custom JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        try:
            # Get thread-local context
            context = getattr(_thread_local, "context", {})
            trace_id = getattr(_thread_local, "trace_id", None)

            # Create log entry
            log_entry = LogEntry(
                timestamp=datetime.utcnow().isoformat() + "Z",
                level=record.levelname,
                message=record.getMessage(),
                trace_id=trace_id,
                user_id=context.get("user_id"),
                table_name=context.get("table_name"),
                operation=context.get("operation"),
                duration_ms=context.get("duration_ms"),
                context=context,
                thread_id=str(threading.get_ident()),
                performance_metrics=context.get("performance_metrics", {}),
            )

            return log_entry.to_json()

        except Exception as e:
            # Fallback to simple format if JSON formatting fails
            return f"{datetime.utcnow().isoformat()}Z - {record.levelname} - {record.getMessage()}"


def configure_structured_logging(
    enabled: bool = None,
    level: str = None,
    format_type: str = None,
    output: str = None,
    notification_integration: bool = None,
    trace_propagation: bool = None,
    performance_logging: bool = None,
    max_log_size: int = None,
    log_retention_hours: int = None,
) -> None:
    """Configure structured logging with environment variables."""
    global _LOGGING_CONFIG

    # Update from environment variables
    _LOGGING_CONFIG.update(
        {
            "enabled": _get_env_bool("TABLEV2_LOGGING_ENABLED", enabled or _LOGGING_CONFIG["enabled"]),
            "level": _get_env_str("TABLEV2_LOGGING_LEVEL", level or _LOGGING_CONFIG["level"]),
            "format_type": _get_env_str("TABLEV2_LOGGING_FORMAT", format_type or _LOGGING_CONFIG["format_type"]),
            "output": _get_env_str("TABLEV2_LOGGING_OUTPUT", output or _LOGGING_CONFIG["output"]),
            "notification_integration": _get_env_bool(
                "TABLEV2_LOGGING_NOTIFY", notification_integration or _LOGGING_CONFIG["notification_integration"]
            ),
            "trace_propagation": _get_env_bool(
                "TABLEV2_LOGGING_TRACE", trace_propagation or _LOGGING_CONFIG["trace_propagation"]
            ),
            "performance_logging": _get_env_bool(
                "TABLEV2_LOGGING_PERFORMANCE", performance_logging or _LOGGING_CONFIG["performance_logging"]
            ),
            "max_log_size": _get_env_int("TABLEV2_LOGGING_MAX_SIZE", max_log_size or _LOGGING_CONFIG["max_log_size"]),
            "log_retention_hours": _get_env_int(
                "TABLEV2_LOGGING_RETENTION", log_retention_hours or _LOGGING_CONFIG["log_retention_hours"]
            ),
        }
    )

    # Initialize logger if enabled
    if _LOGGING_CONFIG["enabled"]:
        _initialize_logger()

    info(f"Structured logging configured: enabled={_LOGGING_CONFIG['enabled']}, level={_LOGGING_CONFIG['level']}")


def _initialize_logger() -> None:
    """Initialize the global logger with configuration."""
    global _logger

    with _logger_lock:
        if _logger is None:
            _logger = get_logger("tableV2.monitoring")
            _logger.setLevel(getattr(logging, _LOGGING_CONFIG["level"].upper()))

            # Remove existing handlers
            _logger.handlers.clear()

            # Add console handler
            if _LOGGING_CONFIG["output"] == "console":
                handler = logging.StreamHandler()
                if _LOGGING_CONFIG["format_type"] == "json":
                    handler.setFormatter(StructuredJSONFormatter())
                else:
                    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
                _logger.addHandler(handler)

            # Prevent propagation to root logger
            _logger.propagate = False


def set_logging_context(
    user_id: Optional[str] = None,
    table_name: Optional[str] = None,
    operation: Optional[str] = None,
    trace_id: Optional[str] = None,
    **kwargs,
) -> None:
    """Set thread-local logging context."""
    if not hasattr(_thread_local, "context"):
        _thread_local.context = {}

    if user_id is not None:
        _thread_local.context["user_id"] = user_id
    if table_name is not None:
        _thread_local.context["table_name"] = table_name
    if operation is not None:
        _thread_local.context["operation"] = operation
    if trace_id is not None:
        _thread_local.trace_id = trace_id

    # Add any additional context
    _thread_local.context.update(kwargs)


def clear_logging_context() -> None:
    """Clear thread-local logging context."""
    if hasattr(_thread_local, "context"):
        _thread_local.context.clear()
    if hasattr(_thread_local, "trace_id"):
        delattr(_thread_local, "trace_id")


def get_logging_context() -> Dict[str, Any]:
    """Get current thread-local logging context."""
    return getattr(_thread_local, "context", {}).copy()


def format_log_record_json(level: str, message: str, **kwargs) -> str:
    """Format a log record as JSON."""
    context = getattr(_thread_local, "context", {})
    trace_id = getattr(_thread_local, "trace_id", None)

    log_entry = LogEntry(
        timestamp=datetime.utcnow().isoformat() + "Z",
        level=level,
        message=message,
        trace_id=trace_id,
        user_id=context.get("user_id"),
        table_name=context.get("table_name"),
        operation=context.get("operation"),
        duration_ms=context.get("duration_ms"),
        context=dict(context, **kwargs),
        thread_id=str(threading.get_ident()),
        performance_metrics=context.get("performance_metrics", {}),
    )

    return log_entry.to_json()


def log_debug(message: str, **kwargs) -> None:
    """Log debug message with context."""
    _log_with_notification(LogLevel.DEBUG, message, **kwargs)


def log_info(message: str, **kwargs) -> None:
    """Log info message with context."""
    _log_with_notification(LogLevel.INFO, message, **kwargs)


def log_warning(message: str, **kwargs) -> None:
    """Log warning message with context and notification."""
    _log_with_notification(LogLevel.WARNING, message, **kwargs)


def log_error(message: str, **kwargs) -> None:
    """Log error message with context and notification."""
    _log_with_notification(LogLevel.ERROR, message, **kwargs)


def log_critical(message: str, **kwargs) -> None:
    """Log critical message with context and notification."""
    _log_with_notification(LogLevel.CRITICAL, message, **kwargs)


def log_performance(operation: str, duration_ms: float, **kwargs) -> None:
    """Log performance metrics."""
    if not _LOGGING_CONFIG["performance_logging"]:
        return

    # Update context with performance data
    if hasattr(_thread_local, "context"):
        _thread_local.context["duration_ms"] = duration_ms
        _thread_local.context["performance_metrics"] = dict(kwargs)

    log_info(f"Performance: {operation} completed in {duration_ms:.2f}ms", **kwargs)


def _log_with_notification(level: str, message: str, **kwargs) -> None:
    """Log message with optional notification integration."""
    if not _LOGGING_CONFIG["enabled"]:
        return

    # Check if we should log this level
    if LogLevel.get_numeric_level(level) < LogLevel.get_numeric_level(_LOGGING_CONFIG["level"]):
        return

    # Initialize logger if needed
    if _logger is None:
        _initialize_logger()

    # Update context with additional data
    if hasattr(_thread_local, "context"):
        _thread_local.context.update(kwargs)

    # Log the message
    try:
        if _logger:
            log_func = getattr(_logger, level.lower())
            log_func(message)
    except Exception as e:
        # Fallback logging
        print(f"Logging error: {e}")
        print(f"{level}: {message}")

    # Send notification if enabled and appropriate level
    if _LOGGING_CONFIG["notification_integration"]:
        if level == LogLevel.ERROR or level == LogLevel.CRITICAL:
            error(f"[{level}] {message}")
        elif level == LogLevel.WARNING:
            warning(f"[{level}] {message}")
        elif level == LogLevel.INFO:
            info(f"[{level}] {message}")


def get_logging_statistics() -> Dict[str, Any]:
    """Get logging system statistics."""
    return {
        "configuration": _LOGGING_CONFIG.copy(),
        "logger_initialized": _logger is not None,
        "thread_context": get_logging_context(),
        "trace_id": getattr(_thread_local, "trace_id", None),
    }


def structured_log_decorator(operation: str):
    """Decorator to add structured logging to functions."""

    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()

            # Set operation context
            set_logging_context(operation=operation)

            try:
                log_info(f"Starting operation: {operation}")
                result = func(*args, **kwargs)

                # Log performance
                duration_ms = (time.time() - start_time) * 1000
                log_performance(operation, duration_ms)

                log_info(f"Completed operation: {operation}")
                return result

            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                log_error(f"Operation failed: {operation} - {str(e)}", duration_ms=duration_ms)
                raise

            finally:
                clear_logging_context()

        return wrapper

    return decorator


def _get_env_bool(key: str, default: bool) -> bool:
    """Get boolean environment variable."""
    value = os.getenv(key)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes", "on")


def _get_env_str(key: str, default: str) -> str:
    """Get string environment variable."""
    return os.getenv(key, default)


def _get_env_int(key: str, default: int) -> int:
    """Get integer environment variable."""
    try:
        return int(os.getenv(key, str(default)))
    except ValueError:
        return default


# FastAPI dependency injection functions
def get_structured_logger_for_dependency():
    """FastAPI dependency for structured logging."""
    return {
        "log_debug": log_debug,
        "log_info": log_info,
        "log_warning": log_warning,
        "log_error": log_error,
        "log_critical": log_critical,
        "log_performance": log_performance,
        "set_context": set_logging_context,
        "clear_context": clear_logging_context,
        "get_context": get_logging_context,
        "get_statistics": get_logging_statistics,
    }


def get_logging_context_for_dependency():
    """FastAPI dependency for logging context management."""
    return {
        "set_context": set_logging_context,
        "clear_context": clear_logging_context,
        "get_context": get_logging_context,
    }


def get_performance_logger_for_dependency():
    """FastAPI dependency for performance logging."""
    return {
        "log_performance": log_performance,
        "decorator": structured_log_decorator,
    }


# Initialize with default configuration
configure_structured_logging()
