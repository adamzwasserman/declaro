"""The pool interface every backend implements.

Lifted out of pool.py, which was 2689 lines and a Slop Audit L1.17
god-file (declaro-tvx). It lives alone so that modules needing only the
interface — mirror.py, and anything a consumer writes — can import it
without pulling in the Turso driver, the cloud HTTP client, and the
replication machinery.

Structural subtyping, no ABC: a pool is anything with acquire(), close()
and closed. Connection pools are the Honest Code exemption for stateful
objects — file handles, sockets and cursors are state by nature.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any

logger = logging.getLogger(__name__)


class BasePool:
    """
    Protocol-style base for connection pools.

    Subclasses implement acquire(), close(), and closed.
    No ABC/abstractmethod — structural subtyping via duck typing.
    Connection pools are inherently stateful (Honest Code exemption
    for file handles, network connections, database cursors).

    Instrumentation fields (set by configure_instrumentation()):
        _tier: str — label for every latency record from this pool
        _latency_logger: logging.Logger | None — None means disabled (zero overhead)
    """

    _tier: str = ""
    _latency_logger: Any = None  # logging.Logger when instrumentation enabled

    def acquire(self) -> AbstractAsyncContextManager[Any]:
        """Acquire a connection from the pool."""
        raise NotImplementedError

    async def close(self) -> None:
        """Close the pool and all connections."""
        raise NotImplementedError

    @property
    def closed(self) -> bool:
        """Whether the pool has been closed."""
        raise NotImplementedError

    def configure_instrumentation(
        self,
        *,
        tier_label: str = "",
        sink: str | None = None,
        path: str | None = None,
        callable_sink: Any = None,
    ) -> None:
        """
        Enable latency instrumentation on this pool.

        Args:
            tier_label: Tag every record with this label (e.g. "central", "project")
            sink: "jsonl" to write JSONL to path, or None for callable_sink only
            path: File path for JSONL sink (required when sink="jsonl")
            callable_sink: Callable(record: dict) -> None for custom sinks
        """
        from declaro_persistum.instrumentation import (
            get_latency_logger,
            setup_callable_sink,
            setup_jsonl_sink,
        )

        self._tier = tier_label
        logger = get_latency_logger()

        if sink == "jsonl" and path:
            setup_jsonl_sink(logger, path)
        if callable_sink is not None:
            setup_callable_sink(logger, callable_sink)

        self._latency_logger = logger

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[Any]:
        """No-op transaction passthrough until real transaction support ships.

        Yields the pool itself.  Writes already go through acquire_write()
        (auto-commit per statement for Turso) so a passthrough is
        semantically correct for the current single-writer architecture.
        Subclasses can override for real BEGIN/COMMIT semantics.
        """
        yield self
