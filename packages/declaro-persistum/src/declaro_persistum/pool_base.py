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


