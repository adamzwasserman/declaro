"""Type definitions for declaro-observe."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Literal, TypedDict


# =============================================================================
# Core Event Types
# =============================================================================


class Event(TypedDict, total=False):
    """An immutable record of something that happened.

    Events are write-once, read-many. You never update an event.
    """

    id: str  # UUID
    ts: str  # ISO timestamp
    type: str  # request, query, error, custom
    source: str  # function/endpoint name
    payload: dict[str, Any]  # event-specific data
    correlation_id: str | None  # trace correlation
    duration_ms: int | None  # for timed events


class RequestEvent(TypedDict):
    """Payload for request events."""

    method: str
    path: str
    status: int
    duration_ms: int
    query_params: dict[str, str] | None
    user_id: str | None


class QueryEvent(TypedDict):
    """Payload for query events."""

    query: str
    params: dict[str, Any] | None
    rows: int
    duration_ms: int


class ErrorEvent(TypedDict):
    """Payload for error events."""

    type: str  # Exception class name
    message: str
    traceback: str | None
    context: dict[str, Any] | None


# =============================================================================
# Configuration Types
# =============================================================================


class CaptureConfig(TypedDict, total=False):
    """What to automatically capture."""

    requests: bool
    queries: bool
    errors: bool
    custom: list[str]  # Patterns like "audit.*", "batch.*"


class ContextConfig(TypedDict, total=False):
    """What context to include in events."""

    include_headers: list[str]
    include_user: bool
    include_timing: bool


class BufferConfig(TypedDict, total=False):
    """Event buffer settings."""

    size: int  # Max events before flush
    flush_interval_ms: int  # Max time before flush


class ExportConfig(TypedDict, total=False):
    """Export to external systems."""

    format: Literal["otlp", "stdout", "none"]
    endpoint: str | None
    headers: dict[str, str] | None


class ObserveConfig(TypedDict, total=False):
    """Full observe configuration."""

    events_table: str
    retention: str  # e.g., "30d"
    buffer: BufferConfig
    capture: CaptureConfig
    context: ContextConfig
    export: ExportConfig


# =============================================================================
# Function Types
# =============================================================================

# ASGI middleware type
Middleware = Callable[
    [Any, Callable[[], Awaitable[dict[str, Any]]], Callable[[dict[str, Any]], Awaitable[None]]],
    Awaitable[None],
]

# Event emitter function type
Emitter = Callable[[str, dict[str, Any]], Awaitable[None]]

# Event store interface
EventStore = Callable[[list[Event]], Awaitable[int]]


# =============================================================================
# Buffer Types
# =============================================================================


class EventBuffer(TypedDict):
    """In-memory event buffer."""

    events: list[Event]
    last_flush: float  # timestamp
    config: BufferConfig
