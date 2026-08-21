"""
Core type definitions for declaro_persistum.

All data structures are TypedDict - no classes with state.
This ensures serialization to/from TOML/JSON and compatibility with pure functions.
"""

from typing import Any, Literal, Protocol, TypedDict, runtime_checkable

# THE ENGINES, AS A VOCABULARY RATHER THAN A STRING.
#
# `dialect: str` appeared on 44 signatures. `str` permits every string, so a
# slop audit counted three unexercised input regions behind each one — empty,
# unicode, very long — about 132 in total, or 9% of this package's unexercised
# regions. None of them is a missing test. They are one declaration, wrong in
# 44 places: the vocabulary has exactly three members and always did.
#
# Declaring it is the Rule 13 remedy. The regions do not need exercising; they
# need to stop existing. The boundary that admits a dialect from outside is
# argparse's `choices=` in cli/main.py, which already enforces this same set,
# so the interior can trust the contract rather than re-check it.
Dialect = Literal["postgresql", "sqlite", "turso"]

# =============================================================================
# Extended Schema Objects (Addendum)
# =============================================================================


# =============================================================================
# What a database handle IS
# =============================================================================
#
# MEASURED 2026-08-21: 119 of the package's 158 `Any` parameters were a
# database connection. `Any` declares an unbounded value space, and Umbra
# derives its facets from the declared type, so each one contributed input
# regions no test could ever close.
#
# TWO SHAPES, NOT ONE, read off the call sites rather than guessed. Every
# method this package invokes on a connection:
#
#     execute 92, commit 17, fetch 13, close 8, rollback 7, fetchrow 3,
#     fetchval 3, transaction 3, executemany 1, copy_records_to_table 1
#
# asyncpg answers `fetch`, `fetchrow`, `fetchval` and `transaction`, has no
# cursor, and has no `commit` because a transaction is a context manager.
# aiosqlite and pyturso answer `execute` returning a cursor, plus `commit` and
# `rollback`. One Protocol covering both would be loose enough to say nothing,
# which is what `Any` already did.
#
# These declare what a handle must BE. Nothing inherits from them, and
# `runtime_checkable` is here so a test can hand one a live driver and ask.


@runtime_checkable
class DbapiCursor(Protocol):
    """What `execute` hands back on aiosqlite and pyturso."""

    async def fetchall(self) -> Any: ...

    async def fetchone(self) -> Any: ...


@runtime_checkable
class DbapiConnection(Protocol):
    """aiosqlite and pyturso: a cursor from `execute`, and explicit commits."""

    async def execute(self, *args: Any, **kwargs: Any) -> Any: ...

    async def commit(self) -> Any: ...

    async def rollback(self) -> Any: ...

    async def close(self) -> Any: ...


@runtime_checkable
class FetchingConnection(Protocol):
    """asyncpg: rows come straight back, and a transaction is a scope."""

    async def execute(self, *args: Any, **kwargs: Any) -> Any: ...

    async def fetch(self, *args: Any, **kwargs: Any) -> Any: ...

    async def fetchrow(self, *args: Any, **kwargs: Any) -> Any: ...

    async def fetchval(self, *args: Any, **kwargs: Any) -> Any: ...

    def transaction(self, *args: Any, **kwargs: Any) -> Any: ...

    async def close(self, *args: Any, **kwargs: Any) -> Any: ...


# =============================================================================
# Which tables are ours
# =============================================================================

# THE PREFIXES THIS PACKAGE CREATES FOR ITSELF, and `_dp_enum_` is
# deliberately absent. `expand_schema_enums` puts the enum lookup tables into
# the TARGET schema, so they are on both sides of a diff and migrate like any
# other table; claiming them here would stop them being created at all.
_OUR_PREFIXES = ("_declaro_meta", "_declaro_tmp_", "_dp_materialized_views",
                 "_dp_transfer_state")


def is_our_table(name: str) -> bool:
    """Whether this package made the table for its own bookkeeping.

    The differ compares an introspected schema against a declared one, so an
    introspected table nobody declared reads as a table the author dropped.
    That was always true of these, and the rename detector only hid it: it
    scored a dropped table against an added one and stayed silent below 50%
    column overlap, which these always were.

    Measured 2026-08-21, a database holding `_dp_transfer_state` refused the
    next migration outright:

        Ambiguous changes detected: Table '_dp_transfer_state' removed and
        'orders' added in the same migration.
        Skipping migration - manual intervention required

    `user_tables_sql` already stated the idea for the emptiness check. This is
    the same rule applied where the diff can see it.
    """
    return name.startswith(_OUR_PREFIXES)


class Enum(TypedDict, total=False):
    """
    Enum type definition.

    Attributes:
        name: Enum type name (required)
        values: List of allowed values (required)
        description: Optional description
    """

    name: str
    values: list[str]
    description: str


class Trigger(TypedDict, total=False):
    """
    Trigger definition.

    Attributes:
        name: Trigger name (required)
        timing: When to fire (before, after, instead_of)
        event: Event(s) that fire the trigger (insert, update, delete)
        for_each: Fire for each row or statement
        when: Optional WHEN condition
        body: Inline trigger body (SQL)
        execute: Reference to stored procedure (alternative to body)
    """

    name: str
    timing: Literal["before", "after", "instead_of"]
    event: str | list[str]
    for_each: Literal["row", "statement"]
    when: str
    body: str
    execute: str


class Parameter(TypedDict, total=False):
    """
    Procedure parameter definition.

    Attributes:
        name: Parameter name (required)
        type: SQL type (required)
        default: Default value expression
    """

    name: str
    type: str
    default: str


class Procedure(TypedDict, total=False):
    """
    Stored procedure/function definition (PostgreSQL only).

    Attributes:
        name: Procedure name (required)
        language: Language (sql, plpgsql)
        returns: Return type
        parameters: List of parameters
        body: Procedure body
    """

    name: str
    language: Literal["sql", "plpgsql"]
    returns: str
    parameters: list[Parameter]
    body: str


class View(TypedDict, total=False):
    """
    View definition.

    Attributes:
        name: View name (required)
        query: SELECT query for the view (required)
        materialized: Whether this is a materialized view (PostgreSQL only)
        refresh: Refresh strategy for materialized views
                 PostgreSQL: "on_demand" | "on_commit"
                 SQLite/Turso: "manual" | "trigger" | "hybrid"
        depends_on: List of table/view names this view references (for dependency ordering)
        trigger_sources: Tables to watch for trigger-based auto-refresh (SQLite/Turso only)
    """

    name: str
    query: str
    materialized: bool
    refresh: Literal["on_demand", "on_commit", "manual", "trigger", "hybrid"]
    depends_on: list[str]
    trigger_sources: list[str]


# =============================================================================
# Core Schema Objects
# =============================================================================


class Column(TypedDict, total=False):
    """
    Column definition - all fields optional except type.

    Attributes:
        type: SQL type (text, integer, uuid, timestamptz, etc.)
        nullable: Whether NULL is allowed (default: True)
        default: Default value expression (SQL expression as string)
        primary_key: Whether this column is the primary key (default: False)
        unique: Whether this column has a unique constraint (default: False)
        references: Foreign key target as "table.column"
        on_delete: Foreign key ON DELETE action
        on_update: Foreign key ON UPDATE action
        check: CHECK constraint expression
        literal_values: List of allowed values from Literal type annotation
                        (used to generate enum lookup tables)
        renamed_from: Migration hint - indicates column was renamed from this name
        is_new: Migration hint - confirms this is intentionally a new column
    """

    type: str
    nullable: bool
    default: Any
    primary_key: bool
    unique: bool
    references: str
    on_delete: Literal["cascade", "set null", "restrict", "no action"]
    on_update: Literal["cascade", "set null", "restrict", "no action"]
    check: str
    literal_values: list[str]
    # Migration hints (not persisted to DB)
    renamed_from: str
    is_new: bool


class Index(TypedDict, total=False):
    """
    Index definition.

    Attributes:
        columns: List of column names in the index (required)
        unique: Whether this is a unique index (default: False)
        where: Partial index condition (SQL expression)
        using: Index method (btree, hash, gin, gist, etc.)
    """

    columns: list[str]
    unique: bool
    where: str
    using: str


class Constraint(TypedDict, total=False):
    """
    Named constraint definition.

    Attributes:
        type: Constraint type (check, unique, exclude)
        expression: Constraint expression (for CHECK constraints)
        columns: Columns involved (for UNIQUE constraints)
    """

    type: Literal["check", "unique", "exclude"]
    expression: str
    columns: list[str]


class Table(TypedDict, total=False):
    """
    Table definition.

    Attributes:
        columns: Column name -> Column definition mapping (required)
        primary_key: Composite primary key columns (if not specified in column)
        indexes: Named index definitions
        constraints: Named constraint definitions
        renamed_from: Migration hint - indicates table was renamed from this name
    """

    columns: dict[str, Column]
    primary_key: list[str]
    indexes: dict[str, Index]
    constraints: dict[str, Constraint]
    renamed_from: str


# Complete database schema as table name -> Table mapping
Schema = dict[str, Table]


class Operation(TypedDict):
    """
    A single DDL operation to execute.

    Attributes:
        op: The operation type
        table: The table this operation affects
        details: Operation-specific parameters
    """

    op: Literal[
        "create_table",
        "drop_table",
        "rename_table",
        "add_column",
        "drop_column",
        "rename_column",
        "alter_column",
        "add_index",
        "drop_index",
        "add_constraint",
        "drop_constraint",
        "add_foreign_key",
        "drop_foreign_key",
        "create_view",
        "drop_view",
        # THE SIXTEENTH, AND IT WAS MISSING FOR AS LONG AS IT EXISTED. The
        # differ emits `insert_enum_values` beside every `create_table` that
        # has a lookup table to fill, and all three appliers render it, and
        # this Literal declared fifteen. A type that undercounts the vocabulary
        # cannot catch a misspelt op name, which is the one thing it is for.
        "insert_enum_values",
    ]
    table: str
    details: dict[str, Any]


class Ambiguity(TypedDict):
    """
    An ambiguous change that requires human decision.

    Attributes:
        type: The type of ambiguity (possible_rename or type_change)
        table: The table involved
        from_column: The original column name (for renames)
        to_column: The new column name (for renames)
        column: The column name (for drops)
        confidence: Confidence score for rename detection (0.0-1.0)
        message: Human-readable description of the ambiguity
    """

    type: Literal["possible_rename", "type_change"]
    table: str
    from_column: str | None
    to_column: str | None
    column: str | None
    confidence: float
    message: str


class DiffResult(TypedDict):
    """
    Result of schema diff operation.

    Attributes:
        operations: List of DDL operations to execute
        dependencies: Mapping of operation index -> list of dependency indices
        execution_order: Topologically sorted operation indices
        ambiguities: List of unresolved ambiguous changes
    """

    operations: list[Operation]
    dependencies: dict[int, list[int]]
    execution_order: list[int]
    ambiguities: list[Ambiguity]


class ApplyResult(TypedDict):
    """
    Result of applying migration operations.

    Attributes:
        success: Whether all operations succeeded
        executed_sql: List of SQL statements that were executed
        operations_applied: Number of operations successfully applied
        error: Error message if success is False
        error_operation: Index of the operation that failed
    """

    success: bool
    executed_sql: list[str]
    operations_applied: int
    error: str | None
    error_operation: int | None


class Decision(TypedDict):
    """
    A recorded decision for an ambiguous change.

    Attributes:
        type: The decision type (rename, drop, keep)
        table: The table involved
        from_column: Original column name (for renames)
        to_column: New column name (for renames)
        column: Column name (for drops)
        decided_at: ISO timestamp of when decision was made
    """

    type: Literal["rename", "drop", "keep"]
    table: str
    from_column: str | None
    to_column: str | None
    column: str | None
    decided_at: str


class SnapshotMeta(TypedDict):
    """
    Metadata for a schema snapshot.

    Attributes:
        version: Snapshot format version
        applied_at: ISO timestamp of when snapshot was created
        dialect: Database dialect (postgresql, sqlite, turso)
        applied_by: User/system that created the snapshot
    """

    version: str
    applied_at: str
    dialect: str
    applied_by: str | None


class ConnectionConfig(TypedDict, total=False):
    """
    Parsed database connection configuration.

    Attributes:
        dialect: Database type (postgresql, sqlite, turso)
        host: Database host
        port: Database port
        database: Database name
        user: Username
        password: Password (masked in logs)
        ssl: SSL mode
        path: File path (for SQLite)
        token: Auth token (for Turso)
    """

    dialect: Literal["postgresql", "sqlite", "turso"]
    host: str
    port: int
    database: str
    user: str
    password: str
    ssl: str
    path: str
    token: str


def is_nullable(column: dict[str, Any]) -> bool:
    """Does this column accept NULL, including what the key already said?

    A PRIMARY KEY column is NOT NULL on every engine this package supports, by
    definition rather than by declaration, so a schema writes

        "id": {"type": "text", "primary_key": True}

    and never repeats `nullable: False`.

    `column.get("nullable", True)` reads that omission as nullable, and it
    produced the same defect twice in one day, in two modules:

        differ     alter_column {"nullable": {"from": false, "to": true}}
                   on an unchanged schema, forever, and PostgreSQL refuses to
                   make a primary key nullable so it could never succeed

        validator  "Primary key column should be NOT NULL" on the schema this
                   package ships, asking the author to say it twice

    Two instances of one mistake in two modules is the architecture talking:
    the question was being re-derived by whoever needed it. It is asked here.
    """
    if column.get("primary_key"):
        return False
    return bool(column.get("nullable", True))


def for_dialect(table: dict[str, Any], dialect: str, what: str) -> Any:
    """Read a dispatch table, naming what exists when the key does not.

    THIS WAS TWO CLASSES. `applier/__init__.py` and `inspector/__init__.py`
    each declared `class _Appliers(dict)` / `class _Inspectors(dict)` whose
    only member was `__missing__`, subclassing `dict` to borrow its behaviour
    and override one hook. A wrapper around a dict is allowed when it holds no
    methods; the method was the whole reason each class existed, so it was
    inheritance for code reuse and nothing else.

    The lookup is what varies, so the lookup is the function. The tables stay
    plain dicts, which means they can be printed, merged, compared with `==`
    and passed to anything expecting a dict.

    A bare `KeyError: 'mysql'` tells a caller what they asked for and not what
    was available. `what` names the table so one message can serve every one
    of them.
    """
    if dialect in table:
        return table[dialect]
    raise ValueError(
        f"Unsupported dialect: {dialect}. "
        f"{what} exist for: {', '.join(sorted(table))}"
    )
