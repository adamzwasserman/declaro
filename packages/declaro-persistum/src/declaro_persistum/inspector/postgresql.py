"""Read a PostgreSQL database's shape back out of it. Functions, not a class.

`PostgreSQLInspector` was stateless — no `__init__`, no fields, methods that
took a connection and returned data. What follows is the same SQL against
information_schema and pg_catalog, with the class removed and `self` gone. The
queries are byte-for-byte the ones that were there: this was a de-classing, not
a rewrite, so nothing about what PostgreSQL is asked has changed.
"""

from collections.abc import Awaitable, Callable
from functools import reduce
from typing import Any, NoReturn

from declaro_persistum.boundary import boundary, helper
from declaro_persistum.exceptions import ConnectionError as DeclaroConnectionError
from declaro_persistum.exceptions import DeclaroError
from declaro_persistum.inspector.shared import normalize_check
from declaro_persistum.inspector.shared import normalize_fk_action as _normalize_fk_action
from declaro_persistum.types import (
    NOTHING,
    Column,
    FetchingConnection,
    Index,
    Nothing,
    Schema,
    Table,
    View,
    resolved,
)


@helper
def _normalize_view_query(query: str) -> str:
    """Normalize view query for consistent comparison.

    RESTORED, NOT REWRITTEN. bf5f7d0 deleted this and kept both lines that call
    it, so PostgreSQL view introspection raised NameError from that commit
    until now. This body is the one that was deleted, recovered with
    `git show bf5f7d0^:...`, rather than a guess at what it must have done —
    the difference matters, because the differ compares this output against a
    stored schema and any other whitespace rule would report every view as
    changed.

    It stays local to this module: both callers are here, and the sibling
    inspectors do not normalize view text.
    """
    return " ".join(query.split())


@helper
def get_engine() -> str:
    """Return engine identifier."""
    return "postgresql"


@helper
async def _the_tables_alone(
    _connection: FetchingConnection, schema: Schema, _schema_name: str
) -> Schema | tuple[Schema, dict[str, View]]:
    """Views were not asked for, so none are read."""
    return schema


@helper
async def _the_tables_and_views(
    connection: FetchingConnection, schema: Schema, schema_name: str
) -> Schema | tuple[Schema, dict[str, View]]:
    """Views were asked for, so they are read and handed back beside the tables."""
    return schema, await introspect_views(connection, schema_name=schema_name)


# Both entries read the same database; only one also reads its views. Typed
# `Any` this said nothing, and mypy could not tell the caller what it had.
_WHEN_VIEWS_ARE_WANTED: dict[
    bool,
    Callable[
        [FetchingConnection, Schema, str],
        Awaitable[Schema | tuple[Schema, dict[str, View]]],
    ],
] = {
    True: _the_tables_and_views,
    False: _the_tables_alone,
}


@helper
def _as_a_connection_failure(error: Exception) -> Schema:
    """The message names the connection, so the failure is reported as one."""
    raise DeclaroConnectionError(
        f"Failed to introspect database: {error}",
        engine="postgresql",
    ) from error


@helper
def _as_it_arrived(error: Exception) -> Schema:
    """Nothing identifies this as a connection failure, so it goes up unchanged."""
    raise error


_WHEN_THE_CONNECTION_IS_NAMED: dict[bool, Callable[[Exception], Schema]] = {
    True: _as_a_connection_failure,
    False: _as_it_arrived,
}


@boundary
async def introspect(
    connection: FetchingConnection,
    *,
    schema_name: str | Nothing,
    include_views: bool | Nothing,
) -> Schema | tuple[Schema, dict[str, View]]:
    """
    Introspect PostgreSQL database schema.

    Uses information_schema for standard metadata and pg_catalog
    for PostgreSQL-specific features (partial indexes, etc.).

    Args:
        connection: asyncpg connection object
        schema_name: PostgreSQL schema to introspect (default: "public")
        include_views: If True, also introspect views and return as second element

    Returns:
        Schema dict, or tuple of (Schema, views dict) if include_views=True
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    schema_name = resolved(schema_name, "public")
    include_views = resolved(include_views, False)
    try:
        # Get all tables in schema
        tables = await connection.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1
              AND table_type = 'BASE TABLE'
              AND table_name NOT LIKE '_declaro_%'
            ORDER BY table_name
            """,
            schema_name,
        )

        schema: Schema = {
            row["table_name"]: await _introspect_table(connection, row["table_name"], schema_name)
            for row in tables
        }

        return await _WHEN_VIEWS_ARE_WANTED[bool(include_views)](connection, schema, schema_name)

    except Exception as e:
        return _WHEN_THE_CONNECTION_IS_NAMED["connection" in str(e).lower()](e)


@helper
async def _introspect_table(
    connection: FetchingConnection,
    table_name: str,
    schema_name: str,
) -> Table:
    """Introspect a single table's structure."""
    columns = await _get_columns(connection, table_name, schema_name)
    primary_key = await _get_primary_key(connection, table_name, schema_name)
    indexes = await _get_indexes(connection, table_name, schema_name)
    foreign_keys = await _get_foreign_keys(connection, table_name, schema_name)

    # Merge foreign key info into columns
    with_keys = reduce(_with_foreign_key, foreign_keys, columns)

    # A CHECK, which nothing read back until 2026-08-17. PostgreSQL keeps
    # these in `pg_constraint`, and `pg_get_constraintdef` returns them as
    # `CHECK ((n > 0))` -- wrapped twice and prefixed -- where SQLite reports
    # the bare expression, so both go through `normalize_check` before anyone
    # compares them.
    #
    # Only a single-column check is attached to its column. One over two
    # columns constrains the ROW, and hanging it on whichever column came
    # first would have the differ propose removing a constraint nobody
    # declared on it. A check named for a column no row reported belongs to
    # no column here and is dropped rather than invented.
    checks = await _get_column_checks(connection, table_name, schema_name)
    checked = {
        **with_keys,
        **{
            name: {**with_keys[name], "check": expression}
            for name, expression in checks.items()
            if name in with_keys
        },
    }

    # THE MULTI-COLUMN CHECKS, WHICH THE COLUMN READ ABOVE SKIPS BY DESIGN.
    # Skipping them attached them nowhere, so a declared table-level
    # constraint was created and then proposed again on every deploy, and the
    # second add fails because the name is already taken. Measured
    # 2026-08-19; the SQLite and Turso inspectors had the same hole and read
    # theirs out of the stored CREATE TABLE, which is the only place those
    # engines record it.
    constraints = await _get_table_constraints(connection, table_name, schema_name)

    # A SINGLE-COLUMN PRIMARY KEY IS ALREADY ON ITS COLUMN, so only a
    # composite one is written here. The other three facts are written only
    # when the engine reports any, because that is how a declared table
    # spells them.
    facts: list[tuple[str, Any]] = [
        ("columns", checked),
        *[("primary_key", primary_key) for keys in [primary_key] if len(keys) > 1],
        *[("indexes", indexes) for declared in [indexes] if declared],
        *[("constraints", constraints) for declared in [constraints] if declared],
    ]
    return dict(facts)  # type: ignore[return-value]


@helper
def _with_foreign_key(columns: dict[str, Column], fk: dict[str, str]) -> dict[str, Column]:
    """The columns, with one key folded onto the column it leaves from.

    A FOLD RATHER THAN A COMPREHENSION, because two keys can leave the same
    column. Each is merged onto the answer the one before it gave, which is
    what the loop this replaces did; building every column from the original
    would let the second key drop the first key's action.
    """
    name = fk["column_name"]
    merged = [(name, _pointing_at(columns[name], fk)) for present in [name] if present in columns]
    return {**columns, **dict(merged)}


@helper
def _pointing_at(column: Column, fk: dict[str, str]) -> Column:
    """One column, carrying the foreign key that leaves it.

    An action the engine reports as NO ACTION, or in words this package has
    no spelling for, is left off rather than written down as something else.
    """
    facts: list[tuple[str, Any]] = [
        *column.items(),
        ("references", f"{fk['foreign_table']}.{fk['foreign_column']}"),
        *[
            ("on_delete", action)
            for action in [_normalize_fk_action(fk.get("delete_rule"))]
            if action
        ],
        *[
            ("on_update", action)
            for action in [_normalize_fk_action(fk.get("update_rule"))]
            if action
        ],
    ]
    return dict(facts)  # type: ignore[return-value]


@boundary
async def _get_columns(
    connection: FetchingConnection,
    table_name: str,
    schema_name: str,
) -> dict[str, Column]:
    """Get column definitions for a table."""
    rows = await connection.fetch(
        """
        SELECT
            column_name,
            data_type,
            udt_name,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = $2
        ORDER BY ordinal_position
        """,
        schema_name,
        table_name,
    )

    columns: dict[str, Column] = {row["column_name"]: _column_from_row(row) for row in rows}

    # A SINGLE-COLUMN PRIMARY KEY IS A FACT ABOUT THE COLUMN. Two or more
    # belong to the table, and `_introspect_table` writes them there.
    pk_columns = await _get_primary_key(connection, table_name, schema_name)
    marked_pk = {
        **columns,
        **{
            name: {**columns[name], "primary_key": True}
            for name in pk_columns[:1]
            if len(pk_columns) == 1 and name in columns
        },
    }

    # UNIQUE, read from the single-column unique indexes.
    unique_cols = await _get_unique_columns(connection, table_name, schema_name)
    return {
        **marked_pk,
        **{name: {**marked_pk[name], "unique": True} for name in unique_cols if name in marked_pk},
    }


@helper
def _cleaned_default(default: str) -> str:
    """A default without its cast. PostgreSQL stores `'x'::text` for `'x'`.

    Splitting on a separator the text does not carry returns the text, so
    a default with no cast comes back as it was stored.
    """
    return default.split("::")[0]


@helper
def _column_from_row(row: dict[str, Any]) -> Column:
    """One column definition, from one information_schema row.

    NULLABLE IS WRITTEN DOWN ONLY WHEN FALSE, and a default only when the
    engine reports one, because that is how the declared side spells them and
    the differ compares the two dicts.
    """
    facts: list[tuple[str, Any]] = [
        ("type", _normalize_type(row)),
        *[("nullable", False) for nullable in [row["is_nullable"]] if nullable == "NO"],
        *[
            ("default", _cleaned_default(default))
            for default in [row["column_default"]]
            if default is not None
        ],
    ]
    return dict(facts)  # type: ignore[return-value]


# PostgreSQL's internal spelling of a type, against the one this package
# declares. A type absent from here keeps whatever `data_type` said.
_TYPE_MAP: dict[str, str] = {
    "character varying": "varchar",
    "character": "char",
    "timestamp without time zone": "timestamp",
    "timestamp with time zone": "timestamptz",
    "time without time zone": "time",
    "time with time zone": "timetz",
    "double precision": "float8",
    "real": "float4",
    "integer": "integer",
    "bigint": "bigint",
    "smallint": "smallint",
    "boolean": "boolean",
    "text": "text",
    "uuid": "uuid",
    "jsonb": "jsonb",
    "json": "json",
    "bytea": "bytea",
}


@helper
def _the_array_element(udt_name: str) -> str:
    """An array's element type. PostgreSQL prefixes the element with `_`."""
    return f"{udt_name.removeprefix('_')}[]"


@helper
def _the_udt_name(udt_name: str) -> str:
    """An enum or another type the user made: its own name is the answer."""
    return udt_name


# The two `data_type` values that name the real type somewhere else. Read
# after `_TYPE_MAP`, which holds neither of these keys.
_FROM_THE_UDT_NAME: dict[str, Callable[[str], str]] = {
    "ARRAY": _the_array_element,
    "USER-DEFINED": _the_udt_name,
}


@helper
def _base_type(data_type: str, udt_name: str) -> str:
    """The type without its length or precision.

    Three answers in order of preference: the canonical name this package
    declares, the name the udt carries for an array or a user-defined type,
    and failing both, what information_schema said.
    """
    return [
        *[_TYPE_MAP[key] for key in [data_type] if key in _TYPE_MAP],
        *[_FROM_THE_UDT_NAME[key](udt_name) for key in [data_type] if key in _FROM_THE_UDT_NAME],
        data_type,
    ][0]


@helper
def _numeric_spelling(precision: Any, scale: Any) -> str:
    """`numeric(p,s)` where the engine reports a scale, `numeric(p)` where not.

    A scale of 0 is reported as no scale here, which is how this has always
    read it and how the declared side spells an integral numeric.
    """
    return [
        *[f"numeric({precision},{scale})" for reported in [scale] if reported],
        f"numeric({precision})",
    ][0]


@helper
def _with_precision(base_type: str, row: dict[str, Any]) -> str:
    """The type, carrying the length or precision that applies to it."""
    return [
        *[
            f"{base_type}({length})"
            for length in [row["character_maximum_length"]]
            if base_type in ("varchar", "char") and length
        ],
        *[
            _numeric_spelling(precision, row["numeric_scale"])
            for precision in [row["numeric_precision"]]
            if base_type == "numeric" and precision
        ],
        base_type,
    ][0]


@helper
def _normalize_type(row: dict[str, Any]) -> str:
    """Normalize PostgreSQL type to canonical form."""
    return _with_precision(_base_type(row["data_type"], row["udt_name"]), row)


@boundary
async def _get_primary_key(
    connection: FetchingConnection,
    table_name: str,
    schema_name: str,
) -> list[str]:
    """Get primary key columns for a table."""
    rows = await connection.fetch(
        """
        SELECT a.attname as column_name
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid
            AND a.attnum = ANY(i.indkey)
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE i.indisprimary
          AND n.nspname = $1
          AND c.relname = $2
        ORDER BY array_position(i.indkey, a.attnum)
        """,
        schema_name,
        table_name,
    )
    return [row["column_name"] for row in rows]


@boundary
async def _get_unique_columns(
    connection: FetchingConnection,
    table_name: str,
    schema_name: str,
) -> set[str]:
    """Get columns with unique constraints (single-column only)."""
    rows = await connection.fetch(
        """
        SELECT a.attname as column_name
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid
            AND a.attnum = ANY(i.indkey)
        JOIN pg_class c ON c.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE i.indisunique
          AND NOT i.indisprimary
          AND array_length(i.indkey, 1) = 1
          AND n.nspname = $1
          AND c.relname = $2
        """,
        schema_name,
        table_name,
    )
    return {row["column_name"] for row in rows}


@boundary
async def _get_indexes(
    connection: FetchingConnection,
    table_name: str,
    schema_name: str,
) -> dict[str, Index]:
    """Get non-primary-key, non-constraint-backed indexes for a table.

    Indexes that implement a PRIMARY KEY, UNIQUE or EXCLUDE constraint
    are owned by the constraint, not independently declared, so they are
    excluded here. A model declares those via ``unique: True`` on the
    column and never as an index entry; surfacing them as indexes would
    put them in the differ's ``current - target`` set and schedule a
    ``drop_index`` for a constraint the model still declares.

    Standalone indexes built with CREATE [UNIQUE] INDEX have no owning
    constraint and are still reported — those are genuinely declared.
    """
    rows = await connection.fetch(
        """
        SELECT
            i.relname as index_name,
            array_agg(a.attname ORDER BY k.n) as columns,
            ix.indisunique as is_unique,
            pg_get_expr(ix.indpred, ix.indrelid) as predicate,
            am.amname as index_method
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_am am ON am.oid = i.relam
        CROSS JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n)
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
        LEFT JOIN pg_constraint c
          ON c.conindid = ix.indexrelid
          AND c.contype IN ('p', 'u', 'x')
        WHERE NOT ix.indisprimary
          -- A CONSTRAINT-BACKED INDEX IS EXCLUDED, EXCEPT A MULTI-COLUMN
          -- UNIQUE. This read `c.oid IS NULL` alone, so every UNIQUE
          -- constraint's index was dropped whether it covered one column or
          -- four, and a table carrying `UNIQUE ("c","d")` introspected with
          -- no index and nothing on either column. Measured 2026-08-19.
          --
          -- The exclusion is right for a single-column unique: a model
          -- declares that as `unique: True` on the column, and reporting the
          -- index as well would put it in the differ's current-minus-target
          -- set and schedule a drop_index for a constraint the model still
          -- declares. A multi-column unique has no column to carry it, so
          -- excluding it lost the fact rather than moving it.
          --
          -- The SQLite and Turso inspectors had the same hole for the same
          -- reason, expressed through PRAGMA index_list's `origin` instead.
          AND (
            c.oid IS NULL
            OR (c.contype = 'u' AND array_length(ix.indkey, 1) > 1)
          )
          AND n.nspname = $1
          AND t.relname = $2
        GROUP BY i.relname, ix.indisunique, ix.indpred, ix.indrelid, am.amname
        ORDER BY i.relname
        """,
        schema_name,
        table_name,
    )

    return {row["index_name"]: _index_from_row(row) for row in rows}


@helper
def _index_from_row(row: dict[str, Any]) -> Index:
    """One index, from one row of the pg_index query above.

    Each of the three optional facts is written down only when it is true of
    the index, because a declared index says nothing about the other two and
    the differ compares the two dicts. btree is what a plain CREATE INDEX
    builds, so naming it would make every ordinary index differ from its
    declaration.
    """
    facts: list[tuple[str, Any]] = [
        ("columns", row["columns"]),
        *[("unique", True) for distinct in [row["is_unique"]] if distinct],
        *[("where", predicate) for predicate in [row["predicate"]] if predicate],
        *[("using", method) for method in [row["index_method"]] if method != "btree"],
    ]
    return dict(facts)  # type: ignore[return-value]


@boundary
async def _get_foreign_keys(
    connection: FetchingConnection,
    table_name: str,
    schema_name: str,
) -> list[dict[str, str]]:
    """Get foreign key constraints for a table."""
    rows = await connection.fetch(
        """
        SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table,
            ccu.column_name AS foreign_column,
            rc.delete_rule,
            rc.update_rule
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        JOIN information_schema.referential_constraints rc
            ON rc.constraint_name = tc.constraint_name
            AND rc.constraint_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = $1
          AND tc.table_name = $2
        """,
        schema_name,
        table_name,
    )

    return [dict(row) for row in rows]


@helper
async def get_table_columns(
    connection: FetchingConnection,
    table_name: str,
) -> dict[str, Column]:
    """One table's columns, or a refusal naming the table.

    See the note on the SQLite twin: this was documented as the inspector's
    and defined on Turso alone.
    """
    present = await table_exists(connection, table_name, schema_name=NOTHING)
    return await _get_columns(connection, _WHEN_PRESENT[present](table_name), "public")


@helper
def _refuse_absent_table(table_name: str) -> NoReturn:
    """Nothing of this name is there, and no column list can say so."""
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
async def table_exists(
    connection: FetchingConnection,
    table_name: str,
    *,
    schema_name: str | Nothing,
) -> bool:
    """Check if a table exists."""
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    schema_name = resolved(schema_name, "public")
    result = await connection.fetchval(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = $1
              AND table_name = $2
              AND table_type = 'BASE TABLE'
        )
        """,
        schema_name,
        table_name,
    )
    return bool(result)


@boundary
async def _get_column_checks(
    connection: FetchingConnection, table_name: str, schema_name: str
) -> dict[str, str]:
    """Column-level CHECK constraints, by column name.

    `conkey` holds the columns a constraint covers. A check over more than one
    is a table constraint and is skipped; a NOT NULL is stored as a check on
    some versions and is skipped by the same rule that its definition is not
    an expression this package declares.
    """
    rows = await connection.fetch(
        """
        SELECT
            att.attname AS column_name,
            pg_get_constraintdef(con.oid) AS definition
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        JOIN unnest(con.conkey) AS k(attnum) ON TRUE
        JOIN pg_attribute att
          ON att.attrelid = rel.oid AND att.attnum = k.attnum
        WHERE rel.relname = $1
          AND nsp.nspname = $2
          AND con.contype = 'c'
          AND array_length(con.conkey, 1) = 1
        """,
        table_name,
        schema_name,
    )
    return {row["column_name"]: normalize_check(row["definition"]) for row in rows}


@boundary
async def _get_table_constraints(
    connection: FetchingConnection, table_name: str, schema_name: str
) -> dict[str, dict[str, str]]:
    """Named CHECK constraints covering more than one column.

    `_get_column_checks` above takes the single-column ones and skips these,
    because a check over two columns belongs to no column and attributing it
    to one would propose removing a column constraint nobody declared.
    Skipping left them invisible: a declared table-level constraint was
    created and then proposed again on every deploy, and PostgreSQL refuses
    the second add because the name is already taken.

    The SQLite and Turso inspectors read the same thing out of the stored
    CREATE TABLE, which is the only place those engines record it.
    """
    rows = await connection.fetch(
        """
        SELECT
            con.conname AS name,
            pg_get_constraintdef(con.oid) AS definition
        FROM pg_constraint con
        JOIN pg_class rel ON rel.oid = con.conrelid
        JOIN pg_namespace nsp ON nsp.oid = rel.relnamespace
        WHERE rel.relname = $1
          AND nsp.nspname = $2
          AND con.contype = 'c'
          AND array_length(con.conkey, 1) > 1
        """,
        table_name,
        schema_name,
    )
    return {
        row["name"]: {
            "type": "check",
            "expression": normalize_check(row["definition"]),
        }
        for row in rows
    }


@boundary
async def introspect_views(
    connection: FetchingConnection,
    *,
    schema_name: str | Nothing,
) -> dict[str, View]:
    """
    Introspect views and materialized views from PostgreSQL.

    Args:
        connection: asyncpg connection object
        schema_name: PostgreSQL schema to introspect (default: "public")

    Returns:
        Dict mapping view names to View definitions
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    schema_name = resolved(schema_name, "public")

    # Regular views
    plain = await connection.fetch(
        """
        SELECT viewname as name, definition as query
        FROM pg_views
        WHERE schemaname = $1
        """,
        schema_name,
    )

    # Materialized views
    stored = await connection.fetch(
        """
        SELECT matviewname as name, definition as query
        FROM pg_matviews
        WHERE schemaname = $1
        """,
        schema_name,
    )

    # The materialized ones are merged second, as they were written second
    # before, so a name pg_views and pg_matviews both report reads as
    # materialized.
    return {
        **{row["name"]: _view_from_row(row, False) for row in plain},
        **{row["name"]: _view_from_row(row, True) for row in stored},
    }


@helper
def _view_from_row(row: dict[str, Any], materialized: bool) -> View:
    """One view, from its catalog row and which catalog it came from."""
    return {
        "name": row["name"],
        "query": _normalize_view_query(row["query"]),
        "materialized": materialized,
    }
