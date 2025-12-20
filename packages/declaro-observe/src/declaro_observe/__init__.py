"""declaro-observe: Event sourcing observability for the functional Python stack.

Events are data. State is derived. No Big State.

Example:
    from fastapi import FastAPI
    from declaro_observe import create_observer, emit

    app = FastAPI()
    observe = create_observer("schema/observe.toml")
    app.middleware("http")(observe)

    # In your handlers
    await emit("audit.login", {"user_id": user_id})
"""

from __future__ import annotations

__version__ = "0.1.0"

from .types import (
    Event,
    EventBuffer,
    ObserveConfig,
    CaptureConfig,
    ContextConfig,
    ExportConfig,
    RequestEvent,
    QueryEvent,
    ErrorEvent,
)

__all__ = [
    # Version
    "__version__",
    # Types
    "Event",
    "EventBuffer",
    "ObserveConfig",
    "CaptureConfig",
    "ContextConfig",
    "ExportConfig",
    "RequestEvent",
    "QueryEvent",
    "ErrorEvent",
    # Core functions (to be implemented)
    "create_observer",
    "emit",
    "get_correlation_id",
    "load_config",
]


# =============================================================================
# Core Functions (Stubs)
# =============================================================================


def create_observer(config: str | dict) -> ...:
    """Create an ASGI middleware from configuration.

    Args:
        config: Path to TOML config file or config dict.

    Returns:
        ASGI middleware function.

    Example:
        observe = create_observer("schema/observe.toml")
        app.middleware("http")(observe)
    """
    raise NotImplementedError("create_observer not yet implemented")


async def emit(
    event_type: str,
    payload: dict,
    *,
    correlation_id: str | None = None,
    source: str | None = None,
) -> None:
    """Emit a custom event.

    Args:
        event_type: Event type (e.g., "audit.login", "batch.complete").
        payload: Event-specific data.
        correlation_id: Optional correlation ID (uses current context if not provided).
        source: Optional source identifier (uses caller if not provided).

    Example:
        await emit("audit.login", {"user_id": user_id, "ip": ip})
    """
    raise NotImplementedError("emit not yet implemented")


def get_correlation_id() -> str | None:
    """Get the current correlation ID from context.

    Returns:
        Correlation ID if in a request context, None otherwise.
    """
    raise NotImplementedError("get_correlation_id not yet implemented")


def load_config(path: str) -> ObserveConfig:
    """Load and validate observe configuration from TOML.

    Args:
        path: Path to TOML config file.

    Returns:
        Validated ObserveConfig.

    Raises:
        FileNotFoundError: If config file not found.
        ValueError: If config is invalid.
    """
    raise NotImplementedError("load_config not yet implemented")
