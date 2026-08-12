"""
Regression tests: executor dispatches write ops on whether SQL has RETURNING.

Bug-class fixed in 0.1.6:
    The executor previously routed every write op on a pool with
    ``acquire_write`` through ``_execute_update`` (cursor rowcount path),
    regardless of whether the SQL had a ``RETURNING`` clause. On Turso /
    MVCC pools this meant prisma ``update_many`` returned an int and
    then called ``len(int)`` -> TypeError, and ``update_one`` / ``create``
    / ``delete`` silently returned ints instead of the documented
    ``dict | None``.

    Fix: the executor now checks ``has_returning_clause(sql)`` for write
    ops on ``acquire_write`` pools. RETURNING -> fetch path (rows). No
    RETURNING -> count path (int).

These tests assert the pure dispatch function directly. The executor's
use of it is by construction — no mocks of the executor needed.
"""

from declaro_persistum.instrumentation import has_returning_clause






