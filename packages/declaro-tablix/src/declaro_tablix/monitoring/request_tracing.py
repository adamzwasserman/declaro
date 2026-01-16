"""
Request tracing and debugging module for TableV2 monitoring system.

This module provides comprehensive request tracing capabilities with
trace ID propagation, span management, and debugging utilities.
"""

import asyncio
import json
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from declaro_advise import error, info, warning
from declaro_tablix.monitoring.structured_logging import clear_logging_context, set_logging_context

# Global tracing configuration
TRACING_CONFIG = {
    "enabled": True,
    "sampling_rate": 1.0,  # 100% sampling
    "max_traces_in_memory": 1000,
    "max_spans_per_trace": 100,
    "trace_retention_hours": 24,
    "debug_mode": False,
    "export_enabled": False,
    "export_endpoint": None,
}

# Thread-local storage for current trace context
_thread_local = threading.local()

# Global trace storage
_traces = {}
_traces_lock = threading.Lock()


@dataclass
class TraceSpan:
    """Represents a single span in a distributed trace."""

    span_id: str
    trace_id: str
    parent_span_id: Optional[str] = None
    operation_name: str = ""
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    duration_ms: Optional[float] = None
    tags: Dict[str, Any] = field(default_factory=dict)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    status: str = "active"  # active, completed, failed
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert span to dictionary format."""
        return {
            "span_id": self.span_id,
            "trace_id": self.trace_id,
            "parent_span_id": self.parent_span_id,
            "operation_name": self.operation_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_ms": self.duration_ms,
            "tags": self.tags,
            "logs": self.logs,
            "status": self.status,
            "error": self.error,
        }


@dataclass
class RequestTrace:
    """Represents a complete request trace with multiple spans."""

    trace_id: str
    user_id: Optional[str] = None
    table_name: Optional[str] = None
    operation_type: Optional[str] = None
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: Optional[datetime] = None
    duration_ms: Optional[float] = None
    spans: List[TraceSpan] = field(default_factory=list)
    root_span_id: Optional[str] = None
    status: str = "active"  # active, completed, failed
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert trace to dictionary format."""
        return {
            "trace_id": self.trace_id,
            "user_id": self.user_id,
            "table_name": self.table_name,
            "operation_type": self.operation_type,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_ms": self.duration_ms,
            "spans": [span.to_dict() for span in self.spans],
            "root_span_id": self.root_span_id,
            "status": self.status,
            "metadata": self.metadata,
        }


def configure_request_tracing(
    enabled: bool = None,
    sampling_rate: float = None,
    max_traces_in_memory: int = None,
    max_spans_per_trace: int = None,
    trace_retention_hours: int = None,
    debug_mode: bool = None,
    export_enabled: bool = None,
    export_endpoint: str = None,
) -> None:
    """
    Configure request tracing system.

    Args:
        enabled: Enable/disable tracing
        sampling_rate: Trace sampling rate (0.0 to 1.0)
        max_traces_in_memory: Maximum traces to keep in memory
        max_spans_per_trace: Maximum spans per trace
        trace_retention_hours: Hours to retain traces
        debug_mode: Enable debug mode
        export_enabled: Enable trace export
        export_endpoint: Export endpoint URL
    """
    global TRACING_CONFIG

    if enabled is not None:
        TRACING_CONFIG["enabled"] = enabled
    if sampling_rate is not None:
        TRACING_CONFIG["sampling_rate"] = max(0.0, min(1.0, sampling_rate))
    if max_traces_in_memory is not None:
        TRACING_CONFIG["max_traces_in_memory"] = max_traces_in_memory
    if max_spans_per_trace is not None:
        TRACING_CONFIG["max_spans_per_trace"] = max_spans_per_trace
    if trace_retention_hours is not None:
        TRACING_CONFIG["trace_retention_hours"] = trace_retention_hours
    if debug_mode is not None:
        TRACING_CONFIG["debug_mode"] = debug_mode
    if export_enabled is not None:
        TRACING_CONFIG["export_enabled"] = export_enabled
    if export_endpoint is not None:
        TRACING_CONFIG["export_endpoint"] = export_endpoint

    info(f"Request tracing configured: enabled={TRACING_CONFIG['enabled']}, sampling_rate={TRACING_CONFIG['sampling_rate']}")


def generate_trace_id() -> str:
    """Generate a unique trace ID."""
    return str(uuid.uuid4())


def generate_span_id() -> str:
    """Generate a unique span ID."""
    return str(uuid.uuid4())


def should_trace() -> bool:
    """Determine if current request should be traced based on sampling rate."""
    if not TRACING_CONFIG["enabled"]:
        return False

    import random

    return random.random() < TRACING_CONFIG["sampling_rate"]


def get_current_trace() -> Optional[RequestTrace]:
    """Get the current trace from thread-local storage."""
    return getattr(_thread_local, "current_trace", None)


def get_current_span() -> Optional[TraceSpan]:
    """Get the current span from thread-local storage."""
    return getattr(_thread_local, "current_span", None)


def set_current_trace(trace: RequestTrace) -> None:
    """Set the current trace in thread-local storage."""
    _thread_local.current_trace = trace

    # Update logging context with trace ID
    set_logging_context(trace_id=trace.trace_id, user_id=trace.user_id, table_name=trace.table_name)


def set_current_span(span: TraceSpan) -> None:
    """Set the current span in thread-local storage."""
    _thread_local.current_span = span


def start_request_trace(
    operation_name: str,
    user_id: Optional[str] = None,
    table_name: Optional[str] = None,
    operation_type: Optional[str] = None,
    trace_id: Optional[str] = None,
    **metadata,
) -> Optional[RequestTrace]:
    """
    Start a new request trace.

    Args:
        operation_name: Name of the operation being traced
        user_id: User ID for the request
        table_name: Table name if applicable
        operation_type: Type of operation
        trace_id: Existing trace ID to use
        **metadata: Additional metadata

    Returns:
        RequestTrace instance if tracing is enabled
    """
    if not should_trace():
        return None

    trace_id = trace_id or generate_trace_id()

    trace = RequestTrace(
        trace_id=trace_id,
        user_id=user_id,
        table_name=table_name,
        operation_type=operation_type,
        metadata=metadata,
    )

    # Create root span
    root_span = TraceSpan(
        span_id=generate_span_id(),
        trace_id=trace_id,
        operation_name=operation_name,
        tags={
            "user_id": user_id,
            "table_name": table_name,
            "operation_type": operation_type,
        },
    )

    trace.spans.append(root_span)
    trace.root_span_id = root_span.span_id

    # Store trace globally
    with _traces_lock:
        _traces[trace_id] = trace

        # Clean up old traces if needed
        if len(_traces) > TRACING_CONFIG["max_traces_in_memory"]:
            _cleanup_old_traces_locked()

    # Set as current trace
    set_current_trace(trace)
    set_current_span(root_span)

    if TRACING_CONFIG["debug_mode"]:
        info(f"Started request trace: {trace_id} for operation: {operation_name}")

    return trace


def finish_request_trace(status: str = "completed", error_message: str = None) -> Optional[RequestTrace]:
    """
    Finish the current request trace.

    Args:
        status: Final status of the trace
        error_message: Error message if trace failed

    Returns:
        Completed RequestTrace instance
    """
    trace = get_current_trace()
    if not trace:
        return None

    # Finish current span if active
    current_span = get_current_span()
    if current_span and current_span.status == "active":
        finish_span(status, error_message)

    # Finish trace
    trace.end_time = datetime.utcnow()
    trace.duration_ms = (trace.end_time - trace.start_time).total_seconds() * 1000
    trace.status = status

    if error_message:
        trace.metadata["error"] = error_message

    # Clear thread-local storage
    _thread_local.current_trace = None
    _thread_local.current_span = None
    clear_logging_context()

    if TRACING_CONFIG["debug_mode"]:
        info(f"Finished request trace: {trace.trace_id} with status: {status}, duration: {trace.duration_ms:.2f}ms")

    # Export trace if enabled
    if TRACING_CONFIG["export_enabled"]:
        _export_trace_async(trace)

    return trace


def start_span(operation_name: str, parent_span_id: Optional[str] = None, **tags) -> Optional[TraceSpan]:
    """
    Start a new span within the current trace.

    Args:
        operation_name: Name of the operation
        parent_span_id: Parent span ID (defaults to current span)
        **tags: Additional tags for the span

    Returns:
        TraceSpan instance if tracing is active
    """
    trace = get_current_trace()
    if not trace:
        return None

    current_span = get_current_span()
    parent_span_id = parent_span_id or (current_span.span_id if current_span else None)

    # Check span limit
    if len(trace.spans) >= TRACING_CONFIG["max_spans_per_trace"]:
        if TRACING_CONFIG["debug_mode"]:
            warning(f"Span limit reached for trace: {trace.trace_id}")
        return None

    span = TraceSpan(
        span_id=generate_span_id(),
        trace_id=trace.trace_id,
        parent_span_id=parent_span_id,
        operation_name=operation_name,
        tags=tags,
    )

    trace.spans.append(span)
    set_current_span(span)

    if TRACING_CONFIG["debug_mode"]:
        info(f"Started span: {span.span_id} for operation: {operation_name}")

    return span


def finish_span(status: str = "completed", error_message: str = None) -> Optional[TraceSpan]:
    """
    Finish the current span.

    Args:
        status: Final status of the span
        error_message: Error message if span failed

    Returns:
        Completed TraceSpan instance
    """
    span = get_current_span()
    if not span:
        return None

    span.end_time = datetime.utcnow()
    span.duration_ms = (span.end_time - span.start_time).total_seconds() * 1000
    span.status = status

    if error_message:
        span.error = error_message

    # Find parent span and set as current
    trace = get_current_trace()
    if trace and span.parent_span_id:
        parent_span = next((s for s in trace.spans if s.span_id == span.parent_span_id), None)
        if parent_span:
            set_current_span(parent_span)

    if TRACING_CONFIG["debug_mode"]:
        info(f"Finished span: {span.span_id} with status: {status}, duration: {span.duration_ms:.2f}ms")

    return span


def add_span_tag(key: str, value: Any) -> None:
    """
    Add a tag to the current span.

    Args:
        key: Tag key
        value: Tag value
    """
    span = get_current_span()
    if span:
        span.tags[key] = value


def add_span_log(message: str, level: str = "info", **fields) -> None:
    """
    Add a log entry to the current span.

    Args:
        message: Log message
        level: Log level
        **fields: Additional fields
    """
    span = get_current_span()
    if span:
        log_entry = {"timestamp": datetime.utcnow().isoformat(), "level": level, "message": message, **fields}
        span.logs.append(log_entry)


@contextmanager
def trace_span(operation_name: str, **tags):
    """
    Context manager for automatic span lifecycle management.

    Args:
        operation_name: Name of the operation
        **tags: Additional tags for the span

    Yields:
        TraceSpan instance
    """
    span = start_span(operation_name, **tags)
    try:
        yield span
        if span:
            finish_span("completed")
    except Exception as e:
        if span:
            finish_span("failed", str(e))
        raise


async def async_trace_span(operation_name: str, **tags):
    """
    Async context manager for automatic span lifecycle management.

    Args:
        operation_name: Name of the operation
        **tags: Additional tags for the span

    Yields:
        TraceSpan instance
    """
    span = start_span(operation_name, **tags)
    try:
        yield span
        if span:
            finish_span("completed")
    except Exception as e:
        if span:
            finish_span("failed", str(e))
        raise


def trace_function(operation_name: str = None, **tags):
    """
    Decorator to automatically trace function execution.

    Args:
        operation_name: Name of the operation (defaults to function name)
        **tags: Additional tags for the span

    Returns:
        Decorated function
    """

    def decorator(func):
        nonlocal operation_name
        operation_name = operation_name or f"{func.__module__}.{func.__name__}"

        def wrapper(*args, **kwargs):
            with trace_span(operation_name, **tags):
                return func(*args, **kwargs)

        async def async_wrapper(*args, **kwargs):
            async with async_trace_span(operation_name, **tags):
                return await func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return wrapper

    return decorator


def trace_table_operation(operation_type: str, table_name: str = None, **tags):
    """
    Decorator specifically for table operations.

    Args:
        operation_type: Type of table operation
        table_name: Name of the table
        **tags: Additional tags for the span

    Returns:
        Decorated function
    """

    def decorator(func):
        operation_name = f"table.{operation_type}"

        def wrapper(*args, **kwargs):
            # Try to extract table name from arguments if not provided
            actual_table_name = table_name
            if not actual_table_name and args:
                if hasattr(args[0], "table_name"):
                    actual_table_name = args[0].table_name

            span_tags = {"operation_type": operation_type, "table_name": actual_table_name, **tags}

            with trace_span(operation_name, **span_tags):
                return func(*args, **kwargs)

        async def async_wrapper(*args, **kwargs):
            # Try to extract table name from arguments if not provided
            actual_table_name = table_name
            if not actual_table_name and args:
                if hasattr(args[0], "table_name"):
                    actual_table_name = args[0].table_name

            span_tags = {"operation_type": operation_type, "table_name": actual_table_name, **tags}

            async with async_trace_span(operation_name, **span_tags):
                return await func(*args, **kwargs)

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return wrapper

    return decorator


def get_trace_by_id(trace_id: str) -> Optional[RequestTrace]:
    """
    Get a trace by its ID.

    Args:
        trace_id: Trace ID to look up

    Returns:
        RequestTrace instance if found
    """
    with _traces_lock:
        return _traces.get(trace_id)


def get_traces_by_user(user_id: str, limit: int = 50) -> List[RequestTrace]:
    """
    Get traces for a specific user.

    Args:
        user_id: User ID to filter by
        limit: Maximum number of traces to return

    Returns:
        List of RequestTrace instances
    """
    with _traces_lock:
        user_traces = [trace for trace in _traces.values() if trace.user_id == user_id]

        # Sort by start time (newest first)
        user_traces.sort(key=lambda t: t.start_time, reverse=True)

        return user_traces[:limit]


def get_traces_by_operation(operation_type: str, limit: int = 50) -> List[RequestTrace]:
    """
    Get traces for a specific operation type.

    Args:
        operation_type: Operation type to filter by
        limit: Maximum number of traces to return

    Returns:
        List of RequestTrace instances
    """
    with _traces_lock:
        operation_traces = [trace for trace in _traces.values() if trace.operation_type == operation_type]

        # Sort by start time (newest first)
        operation_traces.sort(key=lambda t: t.start_time, reverse=True)

        return operation_traces[:limit]


def get_tracing_statistics() -> Dict[str, Any]:
    """
    Get comprehensive tracing statistics.

    Returns:
        Tracing statistics
    """
    with _traces_lock:
        active_traces = [t for t in _traces.values() if t.status == "active"]
        completed_traces = [t for t in _traces.values() if t.status == "completed"]
        failed_traces = [t for t in _traces.values() if t.status == "failed"]

        total_spans = sum(len(trace.spans) for trace in _traces.values())

        # Calculate average duration for completed traces
        if completed_traces:
            avg_duration = sum(t.duration_ms for t in completed_traces if t.duration_ms) / len(completed_traces)
        else:
            avg_duration = 0

        return {
            "configuration": TRACING_CONFIG.copy(),
            "trace_counts": {
                "total_traces": len(_traces),
                "active_traces": len(active_traces),
                "completed_traces": len(completed_traces),
                "failed_traces": len(failed_traces),
            },
            "span_statistics": {
                "total_spans": total_spans,
                "average_spans_per_trace": total_spans / len(_traces) if _traces else 0,
            },
            "performance": {
                "average_duration_ms": round(avg_duration, 2),
                "success_rate": len(completed_traces) / len(_traces) if _traces else 0,
            },
            "memory_usage": {
                "traces_in_memory": len(_traces),
                "max_traces_allowed": TRACING_CONFIG["max_traces_in_memory"],
                "memory_usage_percent": (len(_traces) / TRACING_CONFIG["max_traces_in_memory"]) * 100,
            },
        }


def export_traces_json(limit: int = 100) -> str:
    """
    Export traces to JSON format.

    Args:
        limit: Maximum number of traces to export

    Returns:
        JSON string of traces
    """
    with _traces_lock:
        traces_list = list(_traces.values())

        # Sort by start time (newest first)
        traces_list.sort(key=lambda t: t.start_time, reverse=True)

        # Limit results
        traces_list = traces_list[:limit]

        # Convert to dictionary format
        traces_dict = [trace.to_dict() for trace in traces_list]

        return json.dumps(traces_dict, indent=2)


def cleanup_old_traces(hours: int = None) -> int:
    """
    Clean up traces older than specified hours.

    Args:
        hours: Hours to keep traces (defaults to config value)

    Returns:
        Number of traces cleaned up
    """
    hours = hours or TRACING_CONFIG["trace_retention_hours"]
    cutoff_time = datetime.utcnow() - timedelta(hours=hours)

    with _traces_lock:
        return _cleanup_old_traces_locked(cutoff_time)


def _cleanup_old_traces_locked(cutoff_time: datetime = None) -> int:
    """
    Clean up old traces (must be called with _traces_lock held).

    Args:
        cutoff_time: Cutoff time for cleanup

    Returns:
        Number of traces cleaned up
    """
    if cutoff_time is None:
        cutoff_time = datetime.utcnow() - timedelta(hours=TRACING_CONFIG["trace_retention_hours"])

    traces_to_remove = []
    for trace_id, trace in _traces.items():
        if trace.start_time < cutoff_time:
            traces_to_remove.append(trace_id)

    for trace_id in traces_to_remove:
        del _traces[trace_id]

    return len(traces_to_remove)


def reset_tracing_data() -> None:
    """Reset all tracing data."""
    with _traces_lock:
        _traces.clear()

    # Clear thread-local storage
    _thread_local.current_trace = None
    _thread_local.current_span = None

    info("Tracing data reset")


def debug_current_trace() -> Dict[str, Any]:
    """
    Get debug information about the current trace.

    Returns:
        Debug information
    """
    trace = get_current_trace()
    span = get_current_span()

    return {
        "current_trace": trace.to_dict() if trace else None,
        "current_span": span.to_dict() if span else None,
        "thread_id": threading.get_ident(),
        "tracing_enabled": TRACING_CONFIG["enabled"],
        "sampling_rate": TRACING_CONFIG["sampling_rate"],
    }


def debug_trace_tree(trace_id: str) -> Dict[str, Any]:
    """
    Get debug information about a trace's span tree.

    Args:
        trace_id: Trace ID to analyze

    Returns:
        Trace tree structure
    """
    trace = get_trace_by_id(trace_id)
    if not trace:
        return {"error": f"Trace {trace_id} not found"}

    # Build span tree
    span_tree = {}
    for span in trace.spans:
        span_tree[span.span_id] = {"span": span.to_dict(), "children": []}

    # Link children to parents
    for span in trace.spans:
        if span.parent_span_id and span.parent_span_id in span_tree:
            span_tree[span.parent_span_id]["children"].append(span_tree[span.span_id])

    # Find root spans
    root_spans = [
        span_tree[span.span_id]
        for span in trace.spans
        if span.parent_span_id is None or span.parent_span_id not in span_tree
    ]

    return {
        "trace_id": trace_id,
        "trace_info": trace.to_dict(),
        "span_tree": root_spans,
        "total_spans": len(trace.spans),
    }


def _export_trace_async(trace: RequestTrace) -> None:
    """
    Export trace asynchronously (placeholder for actual implementation).

    Args:
        trace: Trace to export
    """
    # This would implement actual trace export to external systems
    # For now, just log the export
    if TRACING_CONFIG["debug_mode"]:
        info(f"Exporting trace: {trace.trace_id} to {TRACING_CONFIG['export_endpoint']}")


# FastAPI dependency injection functions
def get_tracing_config_for_dependency() -> Dict[str, Any]:
    """FastAPI dependency for tracing configuration."""
    return TRACING_CONFIG.copy()


def get_current_trace_for_dependency() -> Optional[RequestTrace]:
    """FastAPI dependency for current trace."""
    return get_current_trace()


def get_tracing_stats_for_dependency() -> Dict[str, Any]:
    """FastAPI dependency for tracing statistics."""
    return get_tracing_statistics()


def get_trace_manager_for_dependency() -> Dict[str, callable]:
    """FastAPI dependency for trace management functions."""
    return {
        "start_request_trace": start_request_trace,
        "finish_request_trace": finish_request_trace,
        "start_span": start_span,
        "finish_span": finish_span,
        "add_span_tag": add_span_tag,
        "add_span_log": add_span_log,
        "get_trace_by_id": get_trace_by_id,
        "get_traces_by_user": get_traces_by_user,
        "get_traces_by_operation": get_traces_by_operation,
        "get_tracing_statistics": get_tracing_statistics,
        "cleanup_old_traces": cleanup_old_traces,
        "debug_current_trace": debug_current_trace,
        "debug_trace_tree": debug_trace_tree,
    }
