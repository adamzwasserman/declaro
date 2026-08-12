"""Dual-write mirroring, for verifying a replication cutover.

Lifted out of pool.py, which was 2689 lines and a Slop Audit L1.17
god-file (declaro-tvx). This is not connection pooling: it wraps TWO
pools and runs every operation against both, so a migration can be
verified against live traffic before the old database is retired.

Writes go to both in parallel. Reads fetch from both, compare, return the
PRIMARY's answer, and log any disagreement. The mirror never changes what
a caller sees — it only reports.

`fail_open` decides what a mirror failure means: True keeps serving from
the primary and logs, False lets the error out. During a cutover the
first is almost always what you want, because the mirror is the database
you do not trust yet.

Used by cutover.py. Re-exported from declaro_persistum for consumers.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from declaro_persistum.exceptions import PoolClosedError

logger = logging.getLogger(__name__)






