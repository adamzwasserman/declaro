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
from collections.abc import Sequence
from typing import Any, Protocol, TypedDict

logger = logging.getLogger(__name__)

__all__ = ["BULK_LOADERS", "BulkLoader"]


class ReadRows(Protocol):
    async def __call__(
        self,
        conn: Any,
        table: str,
        columns: list[str],
        *,
        offset: int = 0,
        limit: int | None = None,
    ) -> list[tuple[Any, ...]]:
        """Read rows in a STABLE order, so paging cannot repeat or skip one."""
        ...


class LoadRows(Protocol):
    async def __call__(
        self,
        conn: Any,
        table: str,
        columns: list[str],
        rows: Sequence[tuple[Any, ...]],
    ) -> int:
        """Insert rows. Returns how many. `columns` matches tuple position."""
        ...


class CountRows(Protocol):
    async def __call__(self, conn: Any, table: str) -> int:
        ...


class DeleteRows(Protocol):
    async def __call__(self, conn: Any, table: str) -> int:
        """Empty the table. Returns how many rows went."""
        ...


class SetFkChecks(Protocol):
    async def __call__(self, conn: Any) -> None:
        """Turn FK enforcement off or on for this connection."""
        ...


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


# ------------------------------------------------------------- PostgreSQL


def _portable(value: Any) -> Any:
    """UUIDs become strings so a SQLite or Turso target accepts them.

    asyncpg hands back `uuid.UUID` objects, and the DB-API targets reject them.
    This is the one place a cross-engine transfer has to translate a value.
    """
    return str(value) if isinstance(value, uuid.UUID) else value


async def postgres_read_rows(
    conn: Any,
    table: str,
    columns: list[str],
    *,
    offset: int = 0,
    limit: int | None = None,
) -> list[tuple[Any, ...]]:
    """ORDER BY ctid is the stable order: physical row position."""
    col_list = ", ".join(f'"{c}"' for c in columns)
    sql = f'SELECT {col_list} FROM "{table}" ORDER BY ctid'
    if limit is not None:
        sql += f" LIMIT {limit}"
    if offset:
        sql += f" OFFSET {offset}"
    rows = await conn.fetch(sql)
    return [tuple(_portable(v) for v in row.values()) for row in rows]


async def postgres_load_rows(
    conn: Any,
    table: str,
    columns: list[str],
    rows: Sequence[tuple[Any, ...]],
) -> int:
    """COPY, not INSERT. That is the whole reason this dialect is separate."""
    if not rows:
        return 0
    await conn.copy_records_to_table(table, columns=columns, records=rows)
    return len(rows)


async def postgres_count_rows(conn: Any, table: str) -> int:
    return int(await conn.fetchval(f'SELECT COUNT(*) FROM "{table}"'))


async def postgres_delete_rows(conn: Any, table: str) -> int:
    # asyncpg returns the tag "DELETE N".
    result = await conn.execute(f'DELETE FROM "{table}"')
    return int(result.split()[-1]) if result else 0


async def postgres_disable_fk_checks(conn: Any) -> None:
    await conn.execute("SET session_replication_role = 'replica'")


async def postgres_enable_fk_checks(conn: Any) -> None:
    await conn.execute("SET session_replication_role = 'origin'")


# ------------------------------------------------- SQLite and Turso (DB-API)


async def dbapi_read_rows(
    conn: Any,
    table: str,
    columns: list[str],
    *,
    offset: int = 0,
    limit: int | None = None,
) -> list[tuple[Any, ...]]:
    """ORDER BY rowid is the stable order here."""
    col_list = ", ".join(f'"{c}"' for c in columns)
    sql = f'SELECT {col_list} FROM "{table}" ORDER BY rowid'
    if limit is not None:
        sql += f" LIMIT {limit}"
    if offset:
        sql += f" OFFSET {offset}"
    cursor = await conn.execute(sql, ())
    return [tuple(row) for row in await cursor.fetchall()]


async def dbapi_load_rows(
    conn: Any,
    table: str,
    columns: list[str],
    rows: Sequence[tuple[Any, ...]],
) -> int:
    if not rows:
        return 0
    placeholders = ", ".join("?" for _ in columns)
    col_list = ", ".join(f'"{c}"' for c in columns)
    sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})'
    await conn.executemany(sql, list(rows))
    return len(rows)


async def dbapi_count_rows(conn: Any, table: str) -> int:
    cursor = await conn.execute(f'SELECT COUNT(*) FROM "{table}"', ())
    row = await cursor.fetchone()
    return int(row[0]) if row else 0


async def dbapi_delete_rows(conn: Any, table: str) -> int:
    # Counted first: DELETE without a WHERE reports no row count here.
    count = await dbapi_count_rows(conn, table)
    await conn.execute(f'DELETE FROM "{table}"', ())
    return count


async def dbapi_disable_fk_checks(conn: Any) -> None:
    await conn.execute("PRAGMA foreign_keys = OFF", ())


async def dbapi_enable_fk_checks(conn: Any) -> None:
    await conn.execute("PRAGMA foreign_keys = ON", ())


POSTGRESQL: BulkLoader = {
    "read_rows": postgres_read_rows,
    "load_rows": postgres_load_rows,
    "count_rows": postgres_count_rows,
    "delete_rows": postgres_delete_rows,
    "disable_fk_checks": postgres_disable_fk_checks,
    "enable_fk_checks": postgres_enable_fk_checks,
}

DBAPI: BulkLoader = {
    "read_rows": dbapi_read_rows,
    "load_rows": dbapi_load_rows,
    "count_rows": dbapi_count_rows,
    "delete_rows": dbapi_delete_rows,
    "disable_fk_checks": dbapi_disable_fk_checks,
    "enable_fk_checks": dbapi_enable_fk_checks,
}

# SQLite and Turso share one entry rather than holding two copies that can drift.
BULK_LOADERS: dict[str, BulkLoader] = {
    "postgresql": POSTGRESQL,
    "sqlite": DBAPI,
    "turso": DBAPI,
}
