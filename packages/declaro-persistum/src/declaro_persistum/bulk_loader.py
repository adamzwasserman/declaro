"""Moving rows between databases in bulk. Functions in a table, not classes.

    loader = BULK_LOADERS[dialect]
    await loader["load_rows"](conn, "users", columns, rows)

The lookup IS the dispatch (Rule 1). There is no factory: `create_bulk_loader`
was one, it wrapped a two-branch if/elif around a choice a dict already makes,
and it is gone.

WHAT WAS HERE BEFORE, AND WHY THIS IS A REPAIR RATHER THAN A REWRITE.
`PostgreSQLBulkLoader` and `GenericBulkLoader` were stateless classes: no
`__init__`, no fields, and `self` used once in the whole file. 7a5c9ac deleted
them and kept the factory returning them, so every dialect raised NameError from
that commit until now, and no test noticed because no test ran them.

The SQL below is the SQL those classes ran, recovered with `git show
7a5c9ac^:...` rather than reconstructed from the docstrings. The difference is
load-bearing: `ORDER BY ctid` and `ORDER BY rowid` are what make a paged read
stable, and inventing a plausible ordering would silently duplicate and drop
rows across page boundaries.

TWO IMPLEMENTATIONS, THREE DIALECTS. SQLite and Turso both speak DB-API and
share one entry, so a fix lands in one place. PostgreSQL gets its own because
`copy_records_to_table` is 10-100x faster than INSERT and because its FK switch
is a session setting rather than a PRAGMA.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol, TypedDict

from declaro_persistum.boundary import boundary, helper
from declaro_persistum.types import (
    DbapiConnection,
    FetchingConnection,
    translated_by,
)

logger = logging.getLogger(__name__)

__all__ = ["BULK_LOADERS", "TRANSFER_STATE_TABLE", "BulkLoader"]


class ReadRows(Protocol):
    async def __call__(
        self,
        conn: DbapiConnection | FetchingConnection,
        table: str,
        columns: list[str],
        *,
        offset: int,
        limit: int | None,
    ) -> list[tuple[Any, ...]]:
        """Read rows in a STABLE order, so paging cannot repeat or skip one."""
        raise NotImplementedError("CODE NOT WRITTEN")


class LoadRows(Protocol):
    async def __call__(
        self,
        conn: DbapiConnection | FetchingConnection,
        table: str,
        columns: list[str],
        rows: Sequence[tuple[Any, ...]],
    ) -> int:
        """Insert rows. Returns how many. `columns` matches tuple position."""
        raise NotImplementedError("CODE NOT WRITTEN")


class CountRows(Protocol):
    async def __call__(self, conn: DbapiConnection | FetchingConnection, table: str) -> int:
        raise NotImplementedError("CODE NOT WRITTEN")


class DeleteRows(Protocol):
    async def __call__(self, conn: DbapiConnection | FetchingConnection, table: str) -> int:
        """Empty the table. Returns how many rows went."""
        raise NotImplementedError("CODE NOT WRITTEN")


class SetFkChecks(Protocol):
    async def __call__(self, conn: DbapiConnection | FetchingConnection) -> None:
        """Turn FK enforcement off or on for this connection."""
        raise NotImplementedError("CODE NOT WRITTEN")


class Commit(Protocol):
    # POSITIONAL-ONLY, and the only Protocol here that is. A Protocol's
    # `__call__` matches on parameter NAMES as well as types, so an
    # implementation that ignores its argument cannot use the `_conn` spelling
    # this codebase uses for exactly that: mypy reads the rename as an
    # incompatible signature. The PostgreSQL commit is a genuine no-op,
    # because asyncpg owns its transaction, so it is the one implementation
    # that has nothing to read. Every caller passes positionally.
    # HOW MANY COMMITS IT SENT: one, or none. Returning nothing made the
    # PostgreSQL implementation a function with an empty body, which reads to
    # a scanner as code nobody wrote. The count is true for both.
    async def __call__(self, conn: DbapiConnection | FetchingConnection, /) -> int:
        """Make the work so far durable.

        asyncpg owns its transaction and has nothing to do here; the DB-API
        drivers need the call. `hasattr(conn, "commit")` answered this at two
        sites, which is the same dispatch-on-type as the three progress
        functions and is right for the same reason and asserted nowhere.
        """
        raise NotImplementedError("CODE NOT WRITTEN")


class EnsureProgress(Protocol):
    async def __call__(self, conn: DbapiConnection | FetchingConnection) -> None:
        """Create the transfer progress table if it is not there."""
        raise NotImplementedError("CODE NOT WRITTEN")


class ReadProgress(Protocol):
    async def __call__(
        self, conn: DbapiConnection | FetchingConnection, table: str
    ) -> dict[str, Any] | None:
        """One table's transfer status, or None if it has none."""
        raise NotImplementedError("CODE NOT WRITTEN")


class WriteProgress(Protocol):
    @helper
    async def __call__(
        self,
        conn: DbapiConnection | FetchingConnection,
        table: str,
        status: str,
        rows_transferred: int,
        total_rows: int,
        error: str | None,
        now: str,
    ) -> None:
        """Record where a table's transfer got to.

        `now` is passed rather than read, so the caller decides the clock and
        both families stamp the same instant.
        """
        raise NotImplementedError("CODE NOT WRITTEN")


# THE PROGRESS TABLE IS PART OF THE TRANSFER, so reading and writing it is
# part of what an engine has to know how to do. `transfer.py` used to answer
# `hasattr(conn, "fetch")` at three sites to decide between asyncpg and the
# DB-API drivers, which is a dispatch on type in a package built on dispatch
# tables. The dialect was already known and the loader was already passed in
# and ignored.
#
# THREE THINGS DIFFER AND NONE OF THEM IS COSMETIC. asyncpg takes `$1`
# positionally, returns rows from `fetchrow`, and owns its transaction; the
# DB-API drivers take `?` as a tuple, return a cursor from `execute`, and need
# an explicit commit. The row also comes back as a mapping from one and a
# plain tuple from the other.
TRANSFER_STATE_TABLE = "_dp_transfer_state"

_CREATE_PROGRESS_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS "{TRANSFER_STATE_TABLE}" (
    table_name TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'pending',
    rows_transferred INTEGER NOT NULL DEFAULT 0,
    total_rows INTEGER NOT NULL DEFAULT 0,
    started_at TEXT,
    completed_at TEXT,
    error TEXT
)
"""

_PROGRESS_COLUMNS = (
    "table_name",
    "status",
    "rows_transferred",
    "total_rows",
    "started_at",
    "completed_at",
    "error",
)


class BulkLoader(TypedDict):
    """Six operations, as data.

    A TypedDict rather than a Protocol-plus-class: nothing here holds state
    between calls, so there was never an object to be. The connection is a
    parameter, which is what makes each of these a plain function.
    """

    read_rows: ReadRows
    load_rows: LoadRows
    count_rows: CountRows
    delete_rows: DeleteRows
    disable_fk_checks: SetFkChecks
    enable_fk_checks: SetFkChecks
    commit: Commit
    ensure_progress: EnsureProgress
    read_progress: ReadProgress
    write_progress: WriteProgress


# ------------------------------------------------------------- PostgreSQL


# THE ONE TRANSLATION A CROSS-ENGINE TRANSFER MAKES, written as the table it
# is. Every other type is carried across untouched.
_PORTABLE: dict[type, Callable[[Any], Any]] = {uuid.UUID: str}


@helper
def _portable(value: Any) -> Any:
    """UUIDs become strings so a SQLite or Turso target accepts them.

    asyncpg hands back `uuid.UUID` objects, and the DB-API targets reject them.
    This is the one place a cross-engine transfer has to translate a value.
    """
    return translated_by(_PORTABLE, value)


@helper
def _paged(sql: str, offset: int, limit: int | None) -> str:
    """LIMIT and OFFSET, each written only when the caller asked for one.

    A comprehension over a one-item list is how a clause that may or may not
    be there gets built: no limit means no LIMIT, and offset 0 means no
    OFFSET, which is the whole table from its start either way.
    """
    clauses = [f" LIMIT {limit}" for kept in [limit] if kept is not None] + [
        f" OFFSET {offset}" for kept in [offset] if kept
    ]
    return sql + "".join(clauses)


@boundary
async def postgres_read_rows(
    conn: FetchingConnection,
    table: str,
    columns: list[str],
    *,
    offset: int,
    limit: int | None,
) -> list[tuple[Any, ...]]:
    """ORDER BY ctid is the stable order: physical row position."""
    col_list = ", ".join(f'"{c}"' for c in columns)
    sql = _paged(f'SELECT {col_list} FROM "{table}" ORDER BY ctid', offset, limit)
    rows = await conn.fetch(sql)
    return [tuple(_portable(v) for v in row.values()) for row in rows]


# WHETHER THERE ARE ROWS TO LOAD, AS A TABLE. An empty batch is an answer —
# nothing moved — rather than a path around the statement, so both entries
# return a count and the shared one says zero for either engine.
_LoadsRows = Callable[[Any, str, list[str], Sequence[tuple[Any, ...]]], Awaitable[int]]


@helper
async def _no_rows_loaded(
    # BOTH TABLES HOLD THIS ONE FUNCTION, and the two engines hand it
    # different connections. `Any` covered that and said nothing; naming both
    # says which two, and `test_the_bulk_loader_says_what_it_takes` refuses
    # `Any` on a connection for exactly this reason.
    _conn: FetchingConnection | DbapiConnection,
    _table: str,
    _columns: list[str],
    _rows: Sequence[tuple[Any, ...]],
) -> int:
    """An empty batch issues no statement, so no row moved."""
    return 0


@helper
async def _copied(
    conn: FetchingConnection,
    table: str,
    columns: list[str],
    rows: Sequence[tuple[Any, ...]],
) -> int:
    await conn.copy_records_to_table(table, columns=columns, records=rows)
    return len(rows)


_WHEN_POSTGRES_HAS_ROWS: dict[bool, _LoadsRows] = {
    True: _copied,
    False: _no_rows_loaded,
}


@helper
async def postgres_load_rows(
    conn: FetchingConnection,
    table: str,
    columns: list[str],
    rows: Sequence[tuple[Any, ...]],
) -> int:
    """COPY, not INSERT. That is the whole reason this dialect is separate."""
    return await _WHEN_POSTGRES_HAS_ROWS[bool(rows)](conn, table, columns, rows)


@boundary
async def postgres_count_rows(conn: FetchingConnection, table: str) -> int:
    return int(await conn.fetchval(f'SELECT COUNT(*) FROM "{table}"'))


@boundary
async def postgres_delete_rows(conn: FetchingConnection, table: str) -> int:
    # asyncpg returns the tag "DELETE N".
    result = await conn.execute(f'DELETE FROM "{table}"')
    return int(result.split()[-1]) if result else 0


@helper
async def postgres_commit(_conn: FetchingConnection) -> int:
    """No commit sent: asyncpg's transaction context commits on exit."""
    return 0


@boundary
async def postgres_ensure_progress(conn: FetchingConnection) -> None:
    await conn.execute(_CREATE_PROGRESS_TABLE_SQL)


@boundary
async def postgres_read_progress(conn: FetchingConnection, table: str) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        f'SELECT * FROM "{TRANSFER_STATE_TABLE}" WHERE table_name = $1',
        table,
    )
    return dict(row) if row else None


@boundary
async def postgres_write_progress(
    conn: FetchingConnection,
    table: str,
    status: str,
    rows_transferred: int,
    total_rows: int,
    error: str | None,
    now: str,
) -> None:
    completed = ", completed_at = $5" if status in ("completed", "failed") else ""
    await conn.execute(
        f'INSERT INTO "{TRANSFER_STATE_TABLE}" '
        f"(table_name, status, rows_transferred, total_rows, started_at, error) "
        f"VALUES ($1, $2, $3, $4, $5, $6) "
        f"ON CONFLICT (table_name) DO UPDATE SET "
        f"status = $2, rows_transferred = $3, total_rows = $4, error = $6" + completed,
        table,
        status,
        rows_transferred,
        total_rows,
        now,
        error,
    )


@boundary
async def postgres_disable_fk_checks(conn: FetchingConnection) -> None:
    await conn.execute("SET session_replication_role = 'replica'")


@boundary
async def postgres_enable_fk_checks(conn: FetchingConnection) -> None:
    await conn.execute("SET session_replication_role = 'origin'")


# ------------------------------------------------- SQLite and Turso (DB-API)


@boundary
async def dbapi_read_rows(
    conn: DbapiConnection,
    table: str,
    columns: list[str],
    *,
    offset: int,
    limit: int | None,
) -> list[tuple[Any, ...]]:
    """ORDER BY rowid is the stable order here."""
    col_list = ", ".join(f'"{c}"' for c in columns)
    sql = _paged(f'SELECT {col_list} FROM "{table}" ORDER BY rowid', offset, limit)
    cursor = await conn.execute(sql, ())
    return [tuple(row) for row in await cursor.fetchall()]


@boundary
async def _inserted(
    conn: DbapiConnection,
    table: str,
    columns: list[str],
    rows: Sequence[tuple[Any, ...]],
) -> int:
    placeholders = ", ".join("?" for _ in columns)
    col_list = ", ".join(f'"{c}"' for c in columns)
    sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})'
    await conn.executemany(sql, list(rows))
    return len(rows)


_WHEN_DBAPI_HAS_ROWS: dict[bool, _LoadsRows] = {
    True: _inserted,
    False: _no_rows_loaded,
}


@helper
async def dbapi_load_rows(
    conn: DbapiConnection,
    table: str,
    columns: list[str],
    rows: Sequence[tuple[Any, ...]],
) -> int:
    return await _WHEN_DBAPI_HAS_ROWS[bool(rows)](conn, table, columns, rows)


@boundary
async def dbapi_count_rows(conn: DbapiConnection, table: str) -> int:
    cursor = await conn.execute(f'SELECT COUNT(*) FROM "{table}"', ())
    row = await cursor.fetchone()
    return int(row[0]) if row else 0


@boundary
async def dbapi_delete_rows(conn: DbapiConnection, table: str) -> int:
    # Counted first: DELETE without a WHERE reports no row count here.
    count = await dbapi_count_rows(conn, table)
    await conn.execute(f'DELETE FROM "{table}"', ())
    return count


@boundary
async def dbapi_commit(conn: DbapiConnection) -> int:
    """One commit sent."""
    await conn.commit()
    return 1


@boundary
async def dbapi_ensure_progress(conn: DbapiConnection) -> None:
    await conn.execute(_CREATE_PROGRESS_TABLE_SQL, ())
    await conn.commit()


@boundary
async def dbapi_read_progress(conn: DbapiConnection, table: str) -> dict[str, Any] | None:
    """A cursor and a tuple, where asyncpg gives a row that is already a map."""
    cursor = await conn.execute(
        f'SELECT {", ".join(_PROGRESS_COLUMNS)} FROM "{TRANSFER_STATE_TABLE}" WHERE table_name = ?',
        (table,),
    )
    row = await cursor.fetchone()
    return dict(zip(_PROGRESS_COLUMNS, row, strict=True)) if row else None


@boundary
async def dbapi_write_progress(
    conn: DbapiConnection,
    table: str,
    status: str,
    rows_transferred: int,
    total_rows: int,
    error: str | None,
    now: str,
) -> None:
    completed = ", completed_at = excluded.started_at" if status in ("completed", "failed") else ""
    await conn.execute(
        f'INSERT INTO "{TRANSFER_STATE_TABLE}" '
        f"(table_name, status, rows_transferred, total_rows, started_at, error) "
        f"VALUES (?, ?, ?, ?, ?, ?) "
        f"ON CONFLICT (table_name) DO UPDATE SET "
        f"status = excluded.status, "
        f"rows_transferred = excluded.rows_transferred, "
        f"total_rows = excluded.total_rows, error = excluded.error" + completed,
        (table, status, rows_transferred, total_rows, now, error),
    )
    await conn.commit()


@boundary
async def dbapi_disable_fk_checks(conn: DbapiConnection) -> None:
    await conn.execute("PRAGMA foreign_keys = OFF", ())


@boundary
async def dbapi_enable_fk_checks(conn: DbapiConnection) -> None:
    await conn.execute("PRAGMA foreign_keys = ON", ())


POSTGRESQL: BulkLoader = {
    "read_rows": postgres_read_rows,
    "load_rows": postgres_load_rows,
    "count_rows": postgres_count_rows,
    "delete_rows": postgres_delete_rows,
    "disable_fk_checks": postgres_disable_fk_checks,
    "enable_fk_checks": postgres_enable_fk_checks,
    "commit": postgres_commit,
    "ensure_progress": postgres_ensure_progress,
    "read_progress": postgres_read_progress,
    "write_progress": postgres_write_progress,
}

DBAPI: BulkLoader = {
    "read_rows": dbapi_read_rows,
    "load_rows": dbapi_load_rows,
    "count_rows": dbapi_count_rows,
    "delete_rows": dbapi_delete_rows,
    "disable_fk_checks": dbapi_disable_fk_checks,
    "enable_fk_checks": dbapi_enable_fk_checks,
    "commit": dbapi_commit,
    "ensure_progress": dbapi_ensure_progress,
    "read_progress": dbapi_read_progress,
    "write_progress": dbapi_write_progress,
}

# SQLite and Turso share one entry rather than holding two copies that can drift.
BULK_LOADERS: dict[str, BulkLoader] = {
    "postgresql": POSTGRESQL,
    "sqlite": DBAPI,
    "turso": DBAPI,
}
