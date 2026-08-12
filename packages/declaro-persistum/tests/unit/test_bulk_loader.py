"""Bulk loaders: the SQL each backend issues, and where they differ.

`bulk_loader.py` sat at 26% with 18 branches — part of the Slop Audit
L1.19 gap (declaro-xu0).

The two implementations are not interchangeable and the tests say exactly
where they diverge, because that divergence is the reason the module
exists:

- stable ordering is `ctid` on PostgreSQL and `rowid` on SQLite/Turso
- writes use asyncpg's COPY protocol vs `executemany`
- FK checks go off via `session_replication_role` vs `PRAGMA foreign_keys`
- a deleted-row count is parsed out of asyncpg's "DELETE N" string, but
  counted before the delete on the generic path

`_normalize_pg_value` is the seam that makes a PostgreSQL to Turso
transfer work at all: a UUID object has no SQLite type, so it must become
a string on the way out.
"""

import uuid

import pytest

from declaro_persistum.bulk_loader import (
    GenericBulkLoader,
    PostgreSQLBulkLoader,
    _normalize_pg_value,
    create_bulk_loader,
)
















