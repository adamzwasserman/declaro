"""Concurrent writers against a real Turso engine, not fakes.

The unit tests for concurrent writes use fake connection objects. They prove
the pool hands out one connection per writer and returns them, but they
cannot prove the engine accepts concurrent writers on one database, nor that
every writer's rows actually land.

These use a real local Turso database. Each writer gets a disjoint id range,
so a dropped frame names the writer whose connection lost it rather than
showing up as a bare count shortfall.

What this does NOT cover, and what remains unverified: the same shape
against a *sync* replica with a cloud remote and a push connection running,
on free-threaded CPython with the GIL off. That is the configuration 0.1.17
creates in production. It needs cloud credentials and a patched
free-threaded wheel, so it is run downstream rather than here.
"""

import asyncio

import pytest

from declaro_persistum.pool import ConnectionPool

WRITERS = 5
ROWS_PER_WRITER = 40
BASE = 10_000


def _expected_ids() -> set[int]:
    return {
        writer * BASE + row
        for writer in range(1, WRITERS + 1)
        for row in range(ROWS_PER_WRITER)
    }




