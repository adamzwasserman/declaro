"""
Query timing instrumentation.

Provides low-overhead timing measurement and query fingerprinting.
"""

import re
import time
from collections.abc import Awaitable, Callable, Iterator
from contextlib import contextmanager
from typing import Any, Protocol


class QueryObserver(Protocol):
    """Protocol for query execution observers."""

    def on_query_executed(
        self,
        sql: str,
        params: dict[str, Any],
        elapsed_ms: float,
        fingerprint: str,
    ) -> None:
        """Called after a query is executed."""
        ...


class Timer:
    """Simple timer for measuring elapsed time."""

    def __init__(self) -> None:
        self.start_time: float = 0.0
        self.end_time: float = 0.0

    @property
    def elapsed_ms(self) -> float:
        """Get elapsed time in milliseconds."""
        return (self.end_time - self.start_time) * 1000


@contextmanager
def measure_time() -> Iterator[Timer]:
    """
    Context manager for measuring elapsed time.

    Yields:
        Timer object with elapsed_ms property.

    Example:
        with measure_time() as timer:
            do_something()
        print(f"Took {timer.elapsed_ms}ms")
    """
    timer = Timer()
    timer.start_time = time.perf_counter()
    try:
        yield timer
    finally:
        timer.end_time = time.perf_counter()


# Regex patterns for fingerprinting
_NUMBER_PATTERN = re.compile(r"\b\d+\b")
_STRING_PATTERN = re.compile(r"'[^']*'")
_WHITESPACE_PATTERN = re.compile(r"\s+")
_IN_CLAUSE_PATTERN = re.compile(r"\bIN\s*\([^)]+\)", re.IGNORECASE)


def fingerprint_query(sql: str) -> str:
    """
    Generate a normalized fingerprint from a SQL query.

    Replaces literal values with placeholders to group similar queries.

    Args:
        sql: Raw SQL query string.

    Returns:
        Normalized fingerprint string.
    """
    # Normalize whitespace
    result = _WHITESPACE_PATTERN.sub(" ", sql.strip())

    # Replace string literals with ?
    result = _STRING_PATTERN.sub("?", result)

    # Replace IN clauses with normalized form
    result = _IN_CLAUSE_PATTERN.sub("IN (?)", result)

    # Replace numbers with ?
    result = _NUMBER_PATTERN.sub("?", result)

    return result


async def execute_with_timing(
    execute_fn: Callable[[str, dict[str, Any]], Awaitable[Any]],
    sql: str,
    params: dict[str, Any],
    observer: QueryObserver | None = None,
) -> tuple[Any, float]:
    """
    Execute a query with timing measurement.

    Args:
        execute_fn: Async function that executes the query.
        sql: SQL query string.
        params: Query parameters.
        observer: Optional observer to notify.

    Returns:
        Tuple of (result, elapsed_ms).
    """
    with measure_time() as timer:
        result = await execute_fn(sql, params)

    if observer is not None:
        fingerprint = fingerprint_query(sql)
        observer.on_query_executed(sql, params, timer.elapsed_ms, fingerprint)

    return result, timer.elapsed_ms


class CompositeObserver:
    """Observer that delegates to multiple observers."""

    def __init__(self, observers: list[QueryObserver]):
        self._observers = observers

    def on_query_executed(
        self,
        sql: str,
        params: dict[str, Any],
        elapsed_ms: float,
        fingerprint: str,
    ) -> None:
        """Notify all registered observers."""
        for observer in self._observers:
            observer.on_query_executed(sql, params, elapsed_ms, fingerprint)
