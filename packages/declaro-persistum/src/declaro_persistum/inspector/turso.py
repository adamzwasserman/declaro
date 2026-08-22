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
from collections.abc import Callable
from typing import Any, NoReturn

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
from declaro_persistum.boundary import boundary, helper
from declaro_persistum.exceptions import ConnectionError as DeclaroConnectionError
from declaro_persistum.exceptions import DeclaroError
from declaro_persistum.inspector.shared import (
    apply_unique_columns,
    as_literal,
    assemble_table,
    checks_from_ddl,
    columns_from_pragma_rows,
    fk_list_from_pragma_rows,
    indexes_from_rows,
    table_constraints_from_ddl,
    unique_cols_from_index_rows,
    views_from_rows,
)
from declaro_persistum.types import (
    NOTHING,
    Column,
    DbapiConnection,
    Index,
    Nothing,
    Schema,
    Table,
    View,
    resolved,
)


@helper
def get_dialect() -> str:
    """Return dialect identifier."""
    return "turso"


@boundary
async def introspect(
    connection: DbapiConnection,
    *,
    schema_name: str | Nothing,
    include_views: bool | Nothing,
) -> Schema | tuple[Schema, dict[str, View]]:
    """
    Introspect Turso database schema.

    Uses pragma_compat for PRAGMA calls that may need emulation.
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    # NAMED, NOT USED. Every entry in a dispatch table takes the same
    # arguments, or it is not one function. This engine has no schemas
    # to choose between, and `schema_name` was spelled `_schema_name`
    # here and `schema_name` in the PostgreSQL entry, so no caller
    # could pass it through the table at all.
    _ = resolved(schema_name, 'main')
    include_views = resolved(include_views, False)
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

        # A FOREIGN KEY NAMES ITS TARGET, AND THE PRAGMA FOLDS THAT TOO.
        # `PRAGMA foreign_key_list("Post")` gives back `userprofile` for a
        # column declared `references: UserProfile.id`, so the differ saw a
        # change and emitted drop_foreign_key then add_foreign_key on every
        # deploy. Both go through table reconstruction on this engine, so
        # every deploy copied the whole table into a new one and reported
        # success. SQLite's PRAGMA returns `UserProfile` and needs none of
        # this. Measured 2026-08-18.
        #
        # The names recovered above are the answer, so this runs over the
        # finished dict rather than inside it: a table may reference one read
        # later.
        schema = restore_reference_case(
            {
                declared_name(row[0], row[1]): await _introspect_table(
                    connection, row[0]
                )
                for row in tables
                if row[0] not in backing
            }
        )

        return _WITH_VIEWS[bool(include_views)](schema, views_)

    except Exception as e:
        error_msg = str(e).lower()
        _WHEN_UNREACHABLE[
            any(word in error_msg for word in ("network", "connection", "http"))
        ](e)


@helper
def _schema_alone(
    schema: Schema, _views: dict[str, View]
) -> Schema | tuple[Schema, dict[str, View]]:
    """The caller asked for the tables, so that is what comes back."""
    return schema


@helper
def _schema_and_views(
    schema: Schema, views: dict[str, View]
) -> Schema | tuple[Schema, dict[str, View]]:
    """The caller asked for the views as well, so both come back."""
    return schema, views


_WITH_VIEWS: dict[
    bool, Callable[[Schema, dict[str, View]], Schema | tuple[Schema, dict[str, View]]]
] = {
    True: _schema_and_views,
    False: _schema_alone,
}


@helper
def _as_connection_failure(error: Exception) -> NoReturn:
    """The engine could not be reached, which is a failure of its own kind.

    A caller can then tell "the database is unreachable" from "the database
    says no" by the type, without reading the message.
    """
    raise DeclaroConnectionError(
        f"Failed to connect to Turso database: {error}",
        dialect="turso",
    ) from error


@helper
def _the_error_as_it_came(error: Exception) -> NoReturn:
    """The database answered and said no, so its own error goes back up.

    Raised here rather than returned so that the caller's line is one lookup
    with no `raise ... from` beside it, which would name the error as its own
    cause on this path.
    """
    raise error


# WHETHER THE DATABASE COULD BE REACHED AT ALL. Both entries end the call, one
# with a typed failure and one with the engine's own.
_WHEN_UNREACHABLE: dict[bool, Callable[[Exception], NoReturn]] = {
    True: _as_connection_failure,
    False: _the_error_as_it_came,
}


@helper
def restore_reference_case(schema: Schema) -> Schema:
    """The same schema, with every `references` naming the declared table.

    A table the schema does not hold is left exactly as the engine gave it:
    guessing at a name for a table nobody declared would turn a readable
    mismatch into an invented one.
    """
    declared = {name.lower(): name for name in schema}

    return {
        name: {**table, "columns": _referring_as_declared(table["columns"], declared)}
        for name, table in schema.items()
    }


@helper
def _referring_as_declared(
    columns: dict[str, Column], declared: dict[str, str]
) -> dict[str, Column]:
    """Every column of one table, pointing at names as they were written."""
    return {
        name: _reference_as_declared(column, declared)
        for name, column in columns.items()
    }


@helper
def _reference_as_declared(column: Column, declared: dict[str, str]) -> Column:
    """One column, pointing at the declared spelling of the table it names.

    A column with no `references`, one whose reference names no column, and
    one naming a table outside this schema all come back unchanged.
    """
    targets = [
        f"{declared[head.lower()]}.{tail}"
        for reference in [column.get("references") or ""]
        for head, dot, tail in [reference.partition(".")]
        if dot and head.lower() in declared
    ]
    return [*[_referring_to(column, target) for target in targets], column][0]


@helper
def _referring_to(column: Column, target: str) -> Column:
    """The same column, carrying the reference rewritten as declared."""
    return {**column, "references": target}


@helper
def declared_name(stored: str, ddl: str | None) -> str:
    """Give back the name as it was written, which Turso does not store.

    TURSO FOLDS EVERY OBJECT NAME AND KEEPS EVERY COLUMN NAME. All eight
    places a name comes back out of this engine, measured 2026-08-18 rather
    than found one at a time, after three separate fixes for one thing:

        sqlite_master.name (table)   FOLDS
        sqlite_master.name (index)   FOLDS
        sqlite_master.name (view)    FOLDS
        PRAGMA foreign_key_list      FOLDS
        PRAGMA index_list            FOLDS
        sqlite_master.sql            keeps
        PRAGMA table_info            keeps
        PRAGMA index_info            keeps

    SQLite folds none of the eight. So this reads a CREATE of any kind, not
    only a table: an index and a view fold in exactly the same way and are
    recovered from exactly the same place.

    Measured after `CREATE TABLE "UserProfile" (...)`:

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
    # Whichever of the four quotings matched, out of whatever DDL there was.
    # Taking only `quoted` and `bare` would silently fall back to the folded
    # name for a backtick or bracket name, which is the same defect one
    # spelling further along. No DDL, and DDL that is not a CREATE, both leave
    # the list empty and the stored name stands.
    spellings = [
        group
        for text in [ddl]
        if text is not None
        for match in [_CREATE_TABLE_NAME.search(text)]
        if match is not None
        for group in match.groupdict().values()
        if group is not None
    ]
    written = [*spellings, stored][0]

    # A DDL that names a different table is not this table's DDL. Fall back
    # rather than trust a match that disagrees with the catalogue.
    return written if written.lower() == stored.lower() else stored


# `IF NOT EXISTS` is optional, the name may be quoted four ways, and the
# capture stops at whitespace or the opening paren. Anchored at the start so
# a `CREATE TABLE` inside a string literal later in the DDL cannot match.
_CREATE_TABLE_NAME = re.compile(
    r'^\s*CREATE\s+(?:TEMP\s+|TEMPORARY\s+)?(?:UNIQUE\s+)?'
    r'(?:TABLE|INDEX|VIEW)\s+'
    r'(?:IF\s+NOT\s+EXISTS\s+)?'
    r'(?:"(?P<quoted>[^"]+)"|`(?P<btick>[^`]+)`|\[(?P<brack>[^\]]+)\]'
    r'|(?P<bare>[^\s(]+))',
    re.IGNORECASE,
)


@helper
async def _introspect_table(
    connection: DbapiConnection,
    table_name: str,
) -> Table:
    """Introspect a single table's structure."""
    columns = await _get_columns(connection, table_name)
    indexes = await _get_indexes(connection, table_name)
    foreign_keys = await _get_foreign_keys(connection, table_name)
    constraints = await _get_table_constraints(connection, table_name)

    return assemble_table(columns, indexes, foreign_keys, constraints)


@boundary
async def _get_table_constraints(
    connection: DbapiConnection, table_name: str
) -> dict[str, dict[str, str]]:
    """The named table-level constraints, from the stored CREATE TABLE."""
    cursor = await _maybe_await(connection.execute(
        f"SELECT sql FROM sqlite_master WHERE type='table' "
        f"AND name='{as_literal(table_name)}'"
    ))
    rows = await _maybe_await(cursor.fetchall())
    # No row means no such table, and a row whose `sql` is unset says nothing
    # about constraints. Both read as none declared.
    return {
        name: constraint
        for row in rows[:1]
        if row[0]
        for name, constraint in table_constraints_from_ddl(row[0]).items()
    }


@boundary
async def _get_columns(
    connection: DbapiConnection,
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
        f"SELECT sql FROM sqlite_master WHERE type='table' "
        f"AND name='{as_literal(table_name)}'"
    )
    ddl_rows = await cursor.fetchall()
    checks = _checks_in(ddl_rows)

    return {
        name: _checked(column, [checks[key] for key in [name] if key in checks])
        for name, column in columns.items()
    }


@helper
def _checks_in(ddl_rows: list[tuple]) -> dict[str, str]:
    """The column CHECK expressions in a stored CREATE TABLE.

    The rows are what `sqlite_master` gave back for one table: none when the
    table is not there, and one whose `sql` a caller cannot assume is set.
    Both read as no checks.
    """
    return {
        name: expression
        for row in ddl_rows[:1]
        if row[0]
        for name, expression in checks_from_ddl(row[0]).items()
    }


@helper
def _checked(column: Column, expressions: list[str]) -> Column:
    """The column, carrying the one CHECK the DDL declares for it, if any.

    A CHECK the DDL names for a column no PRAGMA reported belongs to no
    column here, and is dropped rather than invented.
    """
    return [*[_with_check(column, text) for text in expressions], column][0]


@helper
def _with_check(column: Column, expression: str) -> Column:
    """The same column, carrying the CHECK expression read out of the DDL."""
    return {**column, "check": expression}


@helper
async def _get_unique_columns(
    connection: DbapiConnection,
    table_name: str,
) -> set[str]:
    """Get columns with unique constraints (single-column only)."""
    index_rows = await pragma_index_list(connection, table_name)

    # Only a UNIQUE index that the engine did not make for a primary key can
    # mark a column, so only those have their columns fetched.
    index_info: dict[str, list[tuple]] = {
        row[1]: await pragma_index_info(connection, row[1])
        for row in index_rows
        if row[2] and row[3] != "pk"
    }

    return unique_cols_from_index_rows(index_rows, index_info)


async def _get_indexes(
    connection: DbapiConnection,
    table_name: str,
) -> dict[str, Index]:
    """Get non-auto indexes for a table."""
    index_rows = await pragma_index_list(connection, table_name)

    # ONLY `pk` IS SKIPPED HERE NOW. This skipped `u` as well, so a
    # multi-column unique never had its columns fetched and
    # `indexes_from_rows` could not decide anything about it: the index
    # arrived with an empty column list and was dropped for looking like
    # a single-column one.
    #
    # The SQLite inspector does not filter here at all, which is why the
    # same fix landed in `indexes_from_rows` and worked there first.
    wanted = [row[1] for row in index_rows if row[3] != "pk"]

    index_info: dict[str, list[tuple]] = {
        name: await pragma_index_info(connection, name) for name in wanted
    }
    index_sql: dict[str, str | None] = {
        name: await _index_ddl(connection, name) for name in wanted
    }

    indexes = indexes_from_rows(index_rows, index_info, index_sql)

    # `PRAGMA index_list` folds the index name exactly as `sqlite_master`
    # folds a table's, so a declared `IxUserName` came back as `ixusername`
    # and every deploy emitted drop_index then add_index. Both succeeded, so
    # the migration reported success while doing the same work forever. The
    # index's own DDL keeps the original, and that is what `declared_name`
    # reads.
    return {
        declared_name(name, index_sql.get(name)): index
        for name, index in indexes.items()
    }


@boundary
async def _index_ddl(connection: DbapiConnection, index_name: str) -> str | None:
    """The CREATE INDEX text the catalogue stored for one index.

    None for an index the engine made itself, which has no statement behind
    it. `indexes_from_rows` reads the text only to recover a partial index's
    WHERE clause, which appears in no PRAGMA.
    """
    cursor = await _maybe_await(connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
        (index_name,),
    ))
    row = await _maybe_await(cursor.fetchone())
    return row[0] if row else None


@helper
async def _get_foreign_keys(
    connection: DbapiConnection,
    table_name: str,
) -> list[dict[str, str]]:
    """Get foreign key constraints for a table."""
    rows = await pragma_foreign_key_list(connection, table_name)
    return fk_list_from_pragma_rows(rows)


@boundary
async def table_exists(
    connection: DbapiConnection,
    table_name: str,
    *,
    schema_name: str | Nothing,
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
    # NAMED, NOT USED. Every entry in a dispatch table takes the same
    # arguments, or it is not one function. This engine has no schemas to
    # choose between, and the PostgreSQL entry's `schema_name` was reachable
    # from nowhere because the wrapper could not pass an argument two of the
    # three entries would refuse.
    _ = schema_name
    cursor = await _maybe_await(connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' "
        "AND lower(name) = lower(?)",
        (table_name,),
    ))
    result = await _maybe_await(cursor.fetchone())
    return result is not None


@helper
async def get_table_columns(
    connection: DbapiConnection,
    table_name: str,
) -> dict[str, Any]:
    """Get column definitions for a specific table."""
    present = await table_exists(connection, table_name, schema_name=NOTHING)
    return await _get_columns(connection, _WHEN_PRESENT[present](table_name))


@helper
def _refuse_absent_table(table_name: str) -> NoReturn:
    """Nothing of this name is there, and no column list can say so.

    An empty dict is also what a table with no columns would look like, so
    the two answers have to differ.
    """
    raise DeclaroError(f"Table '{table_name}' does not exist")


@helper
def _the_table_name(table_name: str) -> str:
    """The table is there, so its name goes on to be read."""
    return table_name


_WHEN_PRESENT: dict[bool, Callable[[str], str]] = {
    True: _the_table_name,
    False: _refuse_absent_table,
}


@boundary
async def introspect_views(
    connection: DbapiConnection,
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
    present = await _maybe_await(cursor.fetchall())
    matviews: dict[str, tuple[str, str]] = {
        name: (query, refresh)
        for _ in present[:1]
        for name, query, refresh in await _matview_rows(connection)
    }

    # The fifth folding site. A view's name comes back from
    # `sqlite_master.name` like a table's, and its `sql` keeps the original
    # the same way, so the same reading recovers it.
    named = [(declared_name(row[0], row[1]), *row[1:]) for row in rows]

    return views_from_rows(named, matviews)


@boundary
async def _matview_rows(connection: DbapiConnection) -> list[tuple]:
    """Every registered materialized view, read from the metadata table.

    Called only once the table is known to be there, so anything that goes
    wrong here is a real failure and raises.
    """
    cursor = await _maybe_await(
        connection.execute(
            "SELECT name, query, refresh_strategy "
            f"FROM {MATVIEW_METADATA_TABLE} ORDER BY name"
        )
    )
    return await _maybe_await(cursor.fetchall())
