"""Read a Turso database's shape back out of it. Functions, not a class.

`TursoInspector` was stateless, like the others. Turso aims at SQLite
compatibility but is a separate Rust engine, so every PRAGMA goes through
`abstractions/pragma_compat.py`, which emulates the ones the engine does not
implement natively. That indirection is the whole reason this is not simply
the SQLite inspector pointed at a different connection.

Logic that does not depend on the PRAGMA layer is shared with SQLite through
`inspector.shared`.

THIS FILE USED TO SAY "Turso (libSQL)". Turso Database is a Rust rewrite, not
libSQL and not a libSQL fork, and the difference is the reason pragma_compat
exists: SQLite compatibility is the aim, not a guarantee.
"""

import re
from typing import Any

from declaro_persistum.abstractions.materialized_views import (
    MATVIEW_METADATA_TABLE,
)
from declaro_persistum.abstractions.pragma_compat import (
    _maybe_await,
    pragma_foreign_key_list,
    pragma_index_info,
    pragma_index_list,
    pragma_table_info,
)
from declaro_persistum.exceptions import ConnectionError as DeclaroConnectionError
from declaro_persistum.inspector.shared import (
    apply_unique_columns,
    assemble_table,
    checks_from_ddl,
    columns_from_pragma_rows,
    fk_list_from_pragma_rows,
    indexes_from_rows,
    unique_cols_from_index_rows,
    views_from_rows,
)
from declaro_persistum.types import Column, Index, Schema, Table, View


def get_dialect() -> str:
    """Return dialect identifier."""
    return "turso"


async def introspect(
    connection: Any,
    *,
    _schema_name: str = "main",
    include_views: bool = False,
) -> Schema | tuple[Schema, dict[str, View]]:
    """
    Introspect Turso database schema.

    Uses pragma_compat for PRAGMA calls that may need emulation.
    """
    try:
        cursor = await _maybe_await(connection.execute(
            """
            SELECT name, sql FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
              AND name NOT LIKE '_litestream_%'
              AND name NOT LIKE '_declaro_%'
              AND name NOT LIKE '_dp_materialized_views'
              AND name NOT LIKE '__turso_%'
              AND name NOT LIKE 'turso_%'
            ORDER BY name
            """
        ))
        tables = await _maybe_await(cursor.fetchall())

        # A materialized view is a real TABLE on this engine too, and must
        # not be reported as one. See the same guard in inspector/sqlite.py.
        views_ = await introspect_views(connection)
        backing = {n for n, v in views_.items() if v.get("materialized")}

        schema: Schema = {}

        for row in tables:
            stored_name = row[0]
            if stored_name in backing:
                continue
            table_name = declared_name(stored_name, row[1])
            schema[table_name] = await _introspect_table(connection, stored_name)

        if include_views:
            return schema, views_

        return schema

    except Exception as e:
        error_msg = str(e).lower()
        if "network" in error_msg or "connection" in error_msg or "http" in error_msg:
            raise DeclaroConnectionError(
                f"Failed to connect to Turso database: {e}",
                dialect="turso",
            ) from e
        raise


def declared_name(stored: str, ddl: str | None) -> str:
    """Give back the name as it was written, which Turso does not store.

    TURSO FOLDS `sqlite_master.name` AND KEEPS THE ORIGINAL IN `sql`.
    Measured 2026-08-18 after `CREATE TABLE "UserProfile" (...)`:

        name: 'userprofile'
        sql : 'CREATE TABLE "UserProfile" (id INTEGER PRIMARY KEY, ...)'

    So a table declared with a capital letter was introspected under a name
    nobody declared, the differ saw the declared table as absent, and every
    deploy after the first emitted `create_table` for a table that already
    existed. It failed, the applier refused to call the migration successful,
    and it did so again on the next deploy and every one after. Any table
    whose name is not already lower case could never finish a migration.

    Recovering the case here rather than comparing names case-insensitively
    in the differ is deliberate. The declared name is what the whole package
    quotes into SQL, so introspection has to hand back what was written; a
    case-insensitive comparison would hide the difference and leave every
    other caller holding the folded name.

    Column names are not folded, so nothing else on this engine needs it.
    """
    if ddl is None:
        return stored

    match = _CREATE_TABLE_NAME.search(ddl)
    if match is None:
        return stored

    # Whichever of the four quotings matched. Taking only `quoted` and `bare`
    # would silently fall back to the folded name for a backtick or bracket
    # name, which is the same defect one spelling further along.
    written = next(
        (g for g in match.groupdict().values() if g is not None), stored
    )

    # A DDL that names a different table is not this table's DDL. Fall back
    # rather than trust a match that disagrees with the catalogue.
    return written if written.lower() == stored.lower() else stored


# `IF NOT EXISTS` is optional, the name may be quoted four ways, and the
# capture stops at whitespace or the opening paren. Anchored at the start so
# a `CREATE TABLE` inside a string literal later in the DDL cannot match.
_CREATE_TABLE_NAME = re.compile(
    r'^\s*CREATE\s+(?:TEMP\s+|TEMPORARY\s+)?TABLE\s+'
    r'(?:IF\s+NOT\s+EXISTS\s+)?'
    r'(?:"(?P<quoted>[^"]+)"|`(?P<btick>[^`]+)`|\[(?P<brack>[^\]]+)\]'
    r'|(?P<bare>[^\s(]+))',
    re.IGNORECASE,
)


async def _introspect_table(
    connection: Any,
    table_name: str,
) -> Table:
    """Introspect a single table's structure."""
    columns = await _get_columns(connection, table_name)
    indexes = await _get_indexes(connection, table_name)
    foreign_keys = await _get_foreign_keys(connection, table_name)

    return assemble_table(columns, indexes, foreign_keys)


async def _get_columns(
    connection: Any,
    table_name: str,
) -> dict[str, Column]:
    """Get column definitions for a table."""
    rows = await pragma_table_info(connection, table_name)

    columns = columns_from_pragma_rows(rows)

    unique_cols = await _get_unique_columns(connection, table_name)
    apply_unique_columns(columns, unique_cols)

    # A CHECK is in no PRAGMA on this engine either. Turso reported None for
    # one until 2026-08-17, which is why the differ could not compare a CHECK
    # without looping on it: the SQLite inspector had just learned to read one
    # and this one had not, so enabling the comparison would have rebuilt
    # every Turso table with a CHECK on every deploy.
    cursor = await connection.execute(
        f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'"
    )
    ddl_rows = await cursor.fetchall()
    if ddl_rows and ddl_rows[0][0]:
        for name, expression in checks_from_ddl(ddl_rows[0][0]).items():
            if name in columns:
                columns[name]["check"] = expression

    return columns


async def _get_unique_columns(
    connection: Any,
    table_name: str,
) -> set[str]:
    """Get columns with unique constraints (single-column only)."""
    index_rows = await pragma_index_list(connection, table_name)

    index_info: dict[str, list[tuple]] = {}
    for idx_row in index_rows:
        idx_name = idx_row[1]
        is_unique = bool(idx_row[2])
        origin = idx_row[3]
        if is_unique and origin != "pk":
            index_info[idx_name] = await pragma_index_info(connection, idx_name)

    return unique_cols_from_index_rows(index_rows, index_info)


async def _get_indexes(
    connection: Any,
    table_name: str,
) -> dict[str, Index]:
    """Get non-auto indexes for a table."""
    index_rows = await pragma_index_list(connection, table_name)

    index_info: dict[str, list[tuple]] = {}
    index_sql: dict[str, str | None] = {}

    for idx_row in index_rows:
        idx_name = idx_row[1]
        origin = idx_row[3]

        if origin in ("pk", "u"):
            continue

        index_info[idx_name] = await pragma_index_info(connection, idx_name)

        sql_cursor = await _maybe_await(connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
            (idx_name,),
        ))
        sql_row = await _maybe_await(sql_cursor.fetchone())
        index_sql[idx_name] = sql_row[0] if sql_row else None

    return indexes_from_rows(index_rows, index_info, index_sql)


async def _get_foreign_keys(
    connection: Any,
    table_name: str,
) -> list[dict[str, str]]:
    """Get foreign key constraints for a table."""
    rows = await pragma_foreign_key_list(connection, table_name)
    return fk_list_from_pragma_rows(rows)


async def table_exists(
    connection: Any,
    table_name: str,
) -> bool:
    """Does the engine consider a table of this name to be there?

    `lower()` ON BOTH SIDES. Turso folds `sqlite_master.name`, so `= ?` with
    the name as declared answered False for a table that was there:
    `table_exists(conn, "turso", "UserProfile")` said no while
    `SELECT FROM "UserProfile"` returned rows. Same folding as the migration
    loop `declared_name` fixes above, in a second place the fix did not reach.

    Turso treats the two spellings as one table -- `CREATE TABLE
    "userprofile"` after `"UserProfile"` is refused as already existing -- so
    either spelling has to answer True.
    """
    cursor = await _maybe_await(connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND lower(name) = lower(?)",
        (table_name,),
    ))
    result = await _maybe_await(cursor.fetchone())
    return result is not None


async def get_table_columns(
    connection: Any,
    table_name: str,
) -> dict[str, Any]:
    """Get column definitions for a specific table."""
    if not await table_exists(connection, table_name):
        from declaro_persistum.exceptions import DeclaroError

        raise DeclaroError(f"Table '{table_name}' does not exist")

    return await _get_columns(connection, table_name)


async def introspect_views(
    connection: Any,
) -> dict[str, View]:
    """Introspect views from Turso/libSQL."""
    cursor = await _maybe_await(connection.execute(
        """
        SELECT name, sql
        FROM sqlite_master
        WHERE type = 'view'
          AND name NOT LIKE 'sqlite_%'
          AND name NOT LIKE '_litestream_%'
        ORDER BY name
        """
    ))
    rows = await _maybe_await(cursor.fetchall())

    # The emulated materialized views live in a metadata table, not in
    # sqlite_master. See `views_from_rows`.
    #
    # ABSENCE IS ASKED, NOT CAUGHT — the same fix as `sqlite.matviews_of`,
    # and for the same reason. `except Exception: matviews = {}` gave one
    # answer to two questions: "none have been created" and "this database
    # would not answer me". The second one, reported as the first, tells the
    # differ the user declared no materialized views and every one in the
    # database becomes a drop.
    cursor = await _maybe_await(
        connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            f"AND name = '{MATVIEW_METADATA_TABLE}'"
        )
    )
    matviews: dict[str, tuple[str, str]] = {}
    if await _maybe_await(cursor.fetchall()):
        cursor = await _maybe_await(
            connection.execute(
                "SELECT name, query, refresh_strategy "
                f"FROM {MATVIEW_METADATA_TABLE} ORDER BY name"
            )
        )
        matviews = {
            name: (query, refresh)
            for name, query, refresh in await _maybe_await(cursor.fetchall())
        }

    return views_from_rows(rows, matviews)
