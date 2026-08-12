"""The blocking API surface: pools without an event loop.

Lifted out of pool.py, which was 2689 lines and a Slop Audit L1.17
god-file (declaro-tvx).

This surface still exists and is still exported. A note in the project
memory once claimed the sync code had been removed and the library was
async-only; that was false, and repeating it would tell a consumer a
public class does not exist. What WAS removed is SyncLibSQLPool,
SyncLibSQLConnection, apply_migrations_sync and TursoApplier.apply_sync.

The async path is the one that is maintained and exercised. These are
here for callers with no event loop.
"""

from __future__ import annotations

import logging
from typing import Any

from declaro_persistum.exceptions import PoolClosedError

logger = logging.getLogger(__name__)










