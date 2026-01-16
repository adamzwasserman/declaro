"""
Rate limiting system for TableV2 security.

This module provides comprehensive rate limiting for formula execution,
customization changes, and user actions to prevent abuse and DoS attacks.
"""

import hashlib
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from declaro_advise import error, info, success, warning


class RateLimitType(Enum):
    """Types of rate limits."""

    FORMULA_EXECUTION = "formula_execution"
    CUSTOMIZATION_CHANGE = "customization_change"
    USER_ACTION = "user_action"
    API_REQUEST = "api_request"
    LOGIN_ATTEMPT = "login_attempt"
    DATA_EXPORT = "data_export"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""

    limit_type: RateLimitType
    requests_per_minute: int
    requests_per_hour: int
    requests_per_day: int
    burst_limit: int = 10
    window_size_seconds: int = 60
    enable_sliding_window: bool = True
    penalty_multiplier: float = 1.5
    cooldown_seconds: int = 300

    def __post_init__(self):
        if self.burst_limit > self.requests_per_minute:
            self.burst_limit = self.requests_per_minute


@dataclass
class RateLimitViolation:
    """Rate limit violation details."""

    user_id: str
    limit_type: RateLimitType
    current_count: int
    limit_exceeded: int
    window_start: float
    window_end: float
    penalty_until: Optional[float] = None
    violation_count: int = 1


@dataclass
class RateLimitResult:
    """Result of rate limit check."""

    allowed: bool
    current_count: int
    limit: int
    reset_time: float
    retry_after_seconds: Optional[float] = None
    violation: Optional[RateLimitViolation] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


# Global rate limit storage
_rate_limit_storage: Dict[str, Dict[str, deque]] = defaultdict(lambda: defaultdict(deque))
_rate_limit_violations: Dict[str, List[RateLimitViolation]] = defaultdict(list)
_rate_limit_lock = threading.Lock()

# Rate limit configurations
RATE_LIMIT_CONFIGS = {
    RateLimitType.FORMULA_EXECUTION: RateLimitConfig(
        limit_type=RateLimitType.FORMULA_EXECUTION,
        requests_per_minute=60,
        requests_per_hour=1000,
        requests_per_day=5000,
        burst_limit=20,
        window_size_seconds=60,
        cooldown_seconds=300,
    ),
    RateLimitType.CUSTOMIZATION_CHANGE: RateLimitConfig(
        limit_type=RateLimitType.CUSTOMIZATION_CHANGE,
        requests_per_minute=30,
        requests_per_hour=500,
        requests_per_day=2000,
        burst_limit=10,
        window_size_seconds=60,
        cooldown_seconds=180,
    ),
    RateLimitType.USER_ACTION: RateLimitConfig(
        limit_type=RateLimitType.USER_ACTION,
        requests_per_minute=120,
        requests_per_hour=2000,
        requests_per_day=10000,
        burst_limit=30,
        window_size_seconds=60,
        cooldown_seconds=120,
    ),
    RateLimitType.API_REQUEST: RateLimitConfig(
        limit_type=RateLimitType.API_REQUEST,
        requests_per_minute=100,
        requests_per_hour=3000,
        requests_per_day=20000,
        burst_limit=25,
        window_size_seconds=60,
        cooldown_seconds=60,
    ),
    RateLimitType.LOGIN_ATTEMPT: RateLimitConfig(
        limit_type=RateLimitType.LOGIN_ATTEMPT,
        requests_per_minute=5,
        requests_per_hour=20,
        requests_per_day=100,
        burst_limit=3,
        window_size_seconds=60,
        cooldown_seconds=900,  # 15 minutes
        penalty_multiplier=2.0,
    ),
    RateLimitType.DATA_EXPORT: RateLimitConfig(
        limit_type=RateLimitType.DATA_EXPORT,
        requests_per_minute=10,
        requests_per_hour=100,
        requests_per_day=500,
        burst_limit=5,
        window_size_seconds=60,
        cooldown_seconds=600,
    ),
}


def check_rate_limit(
    user_id: str, limit_type: RateLimitType, action: Optional[str] = None, resource: Optional[str] = None
) -> RateLimitResult:
    """
    Check if user is within rate limits.

    Args:
        user_id: User ID to check
        limit_type: Type of rate limit to check
        action: Optional action being performed
        resource: Optional resource being accessed

    Returns:
        RateLimitResult with check results
    """
    current_time = time.time()
    config = RATE_LIMIT_CONFIGS.get(limit_type)

    if not config:
        # No rate limit configured, allow by default
        return RateLimitResult(
            allowed=True,
            current_count=0,
            limit=float("inf"),
            reset_time=current_time + 3600,
            metadata={
                "user_id": user_id,
                "limit_type": limit_type.value,
                "action": action,
                "resource": resource,
            },
        )

    with _rate_limit_lock:
        # Check for active penalty
        penalty_until = _get_penalty_until(user_id, limit_type)
        if penalty_until and current_time < penalty_until:
            return RateLimitResult(
                allowed=False,
                current_count=0,
                limit=0,
                reset_time=penalty_until,
                retry_after_seconds=penalty_until - current_time,
                metadata={
                    "user_id": user_id,
                    "limit_type": limit_type.value,
                    "penalty_until": penalty_until,
                    "reason": "user_in_penalty_period",
                },
            )

        # Get user's request history
        user_key = f"{user_id}:{limit_type.value}"
        request_history = _rate_limit_storage[user_key]["requests"]

        # Clean old requests
        window_start = current_time - config.window_size_seconds
        while request_history and request_history[0] < window_start:
            request_history.popleft()

        # Check current count
        current_count = len(request_history)

        # Determine applicable limit
        if current_count >= config.burst_limit:
            # Check per-minute limit
            limit = config.requests_per_minute
            window_seconds = 60
        else:
            # Allow burst
            limit = config.burst_limit
            window_seconds = config.window_size_seconds

        # Check longer time windows
        hour_start = current_time - 3600
        hour_count = sum(1 for req_time in request_history if req_time >= hour_start)

        day_start = current_time - 86400
        day_count = sum(1 for req_time in request_history if req_time >= day_start)

        # Check all limits
        if current_count >= limit:
            # Rate limit exceeded
            violation = RateLimitViolation(
                user_id=user_id,
                limit_type=limit_type,
                current_count=current_count,
                limit_exceeded=limit,
                window_start=window_start,
                window_end=current_time,
                penalty_until=current_time + config.cooldown_seconds,
            )

            _record_violation(violation)

            warning(f"Rate limit exceeded for user {user_id}: {limit_type.value}")

            return RateLimitResult(
                allowed=False,
                current_count=current_count,
                limit=limit,
                reset_time=current_time + window_seconds,
                retry_after_seconds=window_seconds,
                violation=violation,
                metadata={
                    "user_id": user_id,
                    "limit_type": limit_type.value,
                    "window_seconds": window_seconds,
                    "reason": "rate_limit_exceeded",
                },
            )

        elif hour_count >= config.requests_per_hour:
            # Hourly limit exceeded
            violation = RateLimitViolation(
                user_id=user_id,
                limit_type=limit_type,
                current_count=hour_count,
                limit_exceeded=config.requests_per_hour,
                window_start=hour_start,
                window_end=current_time,
                penalty_until=current_time + config.cooldown_seconds,
            )

            _record_violation(violation)

            warning(f"Hourly rate limit exceeded for user {user_id}: {limit_type.value}")

            return RateLimitResult(
                allowed=False,
                current_count=hour_count,
                limit=config.requests_per_hour,
                reset_time=current_time + 3600,
                retry_after_seconds=3600,
                violation=violation,
                metadata={
                    "user_id": user_id,
                    "limit_type": limit_type.value,
                    "window_type": "hourly",
                    "reason": "hourly_limit_exceeded",
                },
            )

        elif day_count >= config.requests_per_day:
            # Daily limit exceeded
            violation = RateLimitViolation(
                user_id=user_id,
                limit_type=limit_type,
                current_count=day_count,
                limit_exceeded=config.requests_per_day,
                window_start=day_start,
                window_end=current_time,
                penalty_until=current_time + config.cooldown_seconds * 2,
            )

            _record_violation(violation)

            warning(f"Daily rate limit exceeded for user {user_id}: {limit_type.value}")

            return RateLimitResult(
                allowed=False,
                current_count=day_count,
                limit=config.requests_per_day,
                reset_time=current_time + 86400,
                retry_after_seconds=86400,
                violation=violation,
                metadata={
                    "user_id": user_id,
                    "limit_type": limit_type.value,
                    "window_type": "daily",
                    "reason": "daily_limit_exceeded",
                },
            )

        # Within limits, allow request
        return RateLimitResult(
            allowed=True,
            current_count=current_count,
            limit=limit,
            reset_time=current_time + window_seconds,
            metadata={
                "user_id": user_id,
                "limit_type": limit_type.value,
                "window_seconds": window_seconds,
                "hour_count": hour_count,
                "day_count": day_count,
            },
        )


def increment_rate_limit(
    user_id: str, limit_type: RateLimitType, action: Optional[str] = None, resource: Optional[str] = None
) -> RateLimitResult:
    """
    Increment rate limit counter for user.

    Args:
        user_id: User ID
        limit_type: Type of rate limit
        action: Optional action being performed
        resource: Optional resource being accessed

    Returns:
        RateLimitResult after incrementing
    """
    current_time = time.time()

    with _rate_limit_lock:
        # Get user's request history
        user_key = f"{user_id}:{limit_type.value}"
        request_history = _rate_limit_storage[user_key]["requests"]

        # Add current request
        request_history.append(current_time)

        # Clean old requests
        config = RATE_LIMIT_CONFIGS.get(limit_type)
        if config:
            window_start = current_time - config.window_size_seconds
            while request_history and request_history[0] < window_start:
                request_history.popleft()

    # Check limits after increment
    return check_rate_limit(user_id, limit_type, action, resource)


def get_rate_limit_status(user_id: str, limit_type: Optional[RateLimitType] = None) -> Dict[str, RateLimitResult]:
    """
    Get rate limit status for user.

    Args:
        user_id: User ID
        limit_type: Optional specific limit type

    Returns:
        Dictionary of rate limit results
    """
    results = {}

    limit_types = [limit_type] if limit_type else list(RateLimitType)

    for ltype in limit_types:
        result = check_rate_limit(user_id, ltype)
        results[ltype.value] = result

    return results


def reset_rate_limit(user_id: str, limit_type: Optional[RateLimitType] = None) -> int:
    """
    Reset rate limit counters for user.

    Args:
        user_id: User ID
        limit_type: Optional specific limit type to reset

    Returns:
        Number of counters reset
    """
    with _rate_limit_lock:
        reset_count = 0

        if limit_type:
            # Reset specific limit type
            user_key = f"{user_id}:{limit_type.value}"
            if user_key in _rate_limit_storage:
                _rate_limit_storage[user_key]["requests"].clear()
                reset_count = 1
        else:
            # Reset all limit types for user
            for ltype in RateLimitType:
                user_key = f"{user_id}:{ltype.value}"
                if user_key in _rate_limit_storage:
                    _rate_limit_storage[user_key]["requests"].clear()
                    reset_count += 1

        # Clear violations
        if user_id in _rate_limit_violations:
            if limit_type:
                _rate_limit_violations[user_id] = [v for v in _rate_limit_violations[user_id] if v.limit_type != limit_type]
            else:
                _rate_limit_violations[user_id].clear()

        info(f"Reset {reset_count} rate limit counters for user {user_id}")
        return reset_count


def configure_rate_limits(limit_type: RateLimitType, config: RateLimitConfig) -> None:
    """
    Configure rate limits for a specific type.

    Args:
        limit_type: Type of rate limit
        config: Rate limit configuration
    """
    RATE_LIMIT_CONFIGS[limit_type] = config
    success(f"Updated rate limit configuration for {limit_type.value}")


def get_rate_limit_statistics(
    user_id: Optional[str] = None, limit_type: Optional[RateLimitType] = None, time_range_hours: int = 24
) -> Dict[str, Any]:
    """
    Get rate limit statistics.

    Args:
        user_id: Optional user ID filter
        limit_type: Optional limit type filter
        time_range_hours: Time range in hours

    Returns:
        Dictionary with rate limit statistics
    """
    current_time = time.time()
    cutoff_time = current_time - (time_range_hours * 3600)

    stats = {
        "time_range_hours": time_range_hours,
        "total_requests": 0,
        "total_violations": 0,
        "requests_by_type": {},
        "violations_by_type": {},
        "most_active_users": {},
        "top_violators": {},
        "average_requests_per_hour": 0,
        "peak_requests_per_minute": 0,
    }

    with _rate_limit_lock:
        # Count requests
        request_counts = defaultdict(int)
        user_requests = defaultdict(int)

        for user_key, data in _rate_limit_storage.items():
            user_id_part, limit_type_part = user_key.split(":", 1)

            if user_id and user_id_part != user_id:
                continue
            if limit_type and limit_type_part != limit_type.value:
                continue

            # Count requests in time range
            recent_requests = [req_time for req_time in data["requests"] if req_time >= cutoff_time]

            count = len(recent_requests)
            stats["total_requests"] += count
            request_counts[limit_type_part] += count
            user_requests[user_id_part] += count

        # Count violations
        violation_counts = defaultdict(int)
        user_violations = defaultdict(int)

        for user_id_part, violations in _rate_limit_violations.items():
            if user_id and user_id_part != user_id:
                continue

            recent_violations = [
                v for v in violations if v.window_end >= cutoff_time and (not limit_type or v.limit_type == limit_type)
            ]

            for violation in recent_violations:
                stats["total_violations"] += 1
                violation_counts[violation.limit_type.value] += 1
                user_violations[user_id_part] += 1

        # Calculate statistics
        stats["requests_by_type"] = dict(request_counts)
        stats["violations_by_type"] = dict(violation_counts)
        stats["most_active_users"] = dict(sorted(user_requests.items(), key=lambda x: x[1], reverse=True)[:10])
        stats["top_violators"] = dict(sorted(user_violations.items(), key=lambda x: x[1], reverse=True)[:10])

        if stats["total_requests"] > 0:
            stats["average_requests_per_hour"] = stats["total_requests"] / time_range_hours

    return stats


def _get_penalty_until(user_id: str, limit_type: RateLimitType) -> Optional[float]:
    """
    Get penalty end time for user and limit type.

    Args:
        user_id: User ID
        limit_type: Rate limit type

    Returns:
        Penalty end time or None if no penalty
    """
    if user_id not in _rate_limit_violations:
        return None

    violations = _rate_limit_violations[user_id]
    for violation in violations:
        if violation.limit_type == limit_type and violation.penalty_until:
            return violation.penalty_until

    return None


def _record_violation(violation: RateLimitViolation) -> None:
    """
    Record a rate limit violation.

    Args:
        violation: Violation to record
    """
    _rate_limit_violations[violation.user_id].append(violation)

    # Keep only recent violations (last 100 per user)
    if len(_rate_limit_violations[violation.user_id]) > 100:
        _rate_limit_violations[violation.user_id] = _rate_limit_violations[violation.user_id][-100:]


def clear_rate_limit_storage() -> Tuple[int, int]:
    """
    Clear rate limit storage.

    Returns:
        Tuple of (requests_cleared, violations_cleared)
    """
    with _rate_limit_lock:
        requests_cleared = sum(len(data["requests"]) for data in _rate_limit_storage.values())
        violations_cleared = sum(len(violations) for violations in _rate_limit_violations.values())

        _rate_limit_storage.clear()
        _rate_limit_violations.clear()

        return requests_cleared, violations_cleared


# Export all functions
__all__ = [
    "check_rate_limit",
    "increment_rate_limit",
    "get_rate_limit_status",
    "reset_rate_limit",
    "configure_rate_limits",
    "get_rate_limit_statistics",
    "clear_rate_limit_storage",
    "RateLimitResult",
    "RateLimitConfig",
    "RateLimitViolation",
    "RateLimitType",
]
