"""declaro-advise: Notification and messaging system for declaro stack.

This module provides notification functions compatible with the buckler notification system.
When deployed back into buckler/idd, these will be replaced with the full implementation.

For standalone use, notifications are logged but not displayed.
"""

import logging
from enum import Enum
from typing import Any, Optional

__version__ = "0.1.0"

logger = logging.getLogger(__name__)


class Priority(str, Enum):
    """Notification priority levels."""
    INFORMATIONAL = "informational"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class MessageType(str, Enum):
    """Notification message types."""
    STATUS = "status"
    ALERT = "alert"
    CONFIRMATION = "confirmation"
    PROGRESS = "progress"


def success(message: str, duration: int = 3000, **kwargs: Any) -> None:
    """Send success notification with green checkmark.

    Args:
        message: The success message to display
        duration: Display duration in milliseconds (default: 3000)
        **kwargs: Additional notification options
    """
    logger.info(f"[SUCCESS] {message}")


def error(message: str, priority: Any = Priority.INFORMATIONAL, **kwargs: Any) -> None:
    """Send error notification with red X icon.

    Args:
        message: The error message to display
        priority: Notification priority level
        **kwargs: Additional notification options
    """
    logger.error(f"[ERROR] {message}")


def info(
    message: str,
    action_url: Optional[str] = None,
    action_label: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """Send info notification with blue info icon.

    Args:
        message: The info message to display
        action_url: Optional URL for action button
        action_label: Optional label for action button
        **kwargs: Additional notification options
    """
    logger.info(f"[INFO] {message}")


def warning(message: str, priority: Any = Priority.INFORMATIONAL, **kwargs: Any) -> None:
    """Send warning notification with yellow warning icon.

    Args:
        message: The warning message to display
        priority: Notification priority level
        **kwargs: Additional notification options
    """
    logger.warning(f"[WARNING] {message}")


# Request context storage (thread-local in production)
_request_context: dict[str, Any] = {}


def set_request_context(
    request: Any = None,
    user_id: Optional[str] = None,
    session_id: Optional[str] = None,
    **kwargs: Any,
) -> None:
    """Set the request context for notifications.

    In production (buckler), this connects notifications to the correct user session.
    In standalone mode, this just stores context for logging purposes.

    Args:
        request: FastAPI Request object (optional, extracts user info)
        user_id: User identifier (optional)
        session_id: Session identifier (optional)
        **kwargs: Additional context values
    """
    global _request_context

    if request is not None:
        # Try to extract user info from request
        try:
            if hasattr(request, "state"):
                if hasattr(request.state, "user_id"):
                    user_id = user_id or request.state.user_id
                if hasattr(request.state, "session_id"):
                    session_id = session_id or request.state.session_id
        except Exception:
            pass

    if user_id:
        _request_context["user_id"] = user_id
    if session_id:
        _request_context["session_id"] = session_id
    _request_context.update(kwargs)

    logger.debug(f"Request context set: user_id={user_id}, session_id={session_id}")


def reset_request_context() -> None:
    """Reset the request context.

    Clears all stored context information.
    """
    global _request_context
    _request_context = {}
    logger.debug("Request context reset")


def get_request_context() -> dict[str, Any]:
    """Get the current request context.

    Returns:
        Dictionary containing current context values
    """
    return _request_context.copy()


__all__ = [
    "__version__",
    "Priority",
    "MessageType",
    "success",
    "error",
    "info",
    "warning",
    "set_request_context",
    "reset_request_context",
    "get_request_context",
]
