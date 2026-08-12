"""Provisioning databases on the Turso platform.

Lifted out of pool.py, which was 2689 lines and a Slop Audit L1.17
god-file (declaro-tvx). This is an HTTP client for the Turso platform
API, not a connection pool: it creates and deletes databases, mints
auth tokens, and lists what exists.

Turso cloud is one database per tenant, so a multi-tenant application
provisions rather than migrates. This holds a pool per tenant and hands
it out by database name.

It reaches the network on every call, which the pools deliberately do
not. Keeping it in its own module keeps that distinction visible.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from declaro_persistum.exceptions import PoolConnectionError

logger = logging.getLogger(__name__)


