"""
Core type definitions for declaro_persistum.

All data structures are TypedDict - no classes with state.
This ensures serialization to/from TOML/JSON and compatibility with pure functions.
"""

from collections.abc import Callable
from types import EllipsisType
from typing import Any, Final, Literal, Protocol, TypedDict, TypeVar, cast, runtime_checkable

# What a generic helper below carries through unchanged. Written down because
# `Any` in and `Any` out says nothing, and these functions do say something:
# whatever you hand them comes back as the same type.
T = TypeVar("T")
V = TypeVar("V")

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

# WHICH WAY AN ORDER RUNS, as a vocabulary rather than a mark on a name.
#
# An order used to be spelled `["-name"]`, a leading minus meaning reverse.
# That is a column name with a directive stuck to the front, so it is neither
# of the two things a string a caller passes is allowed to be: not a declared
# name, because `-name` is not in `model_fields` and has to be cut apart before
# it can be looked up, and not a value, because nothing binds it as a
# parameter. It is the same hole as a WHERE clause given as text, in a smaller
# costume, and this package deleted that one.
#
# An order is `{"name": "ascending"}` instead. Every key is a declared column
# checked against the model, every value is one of two words, and the order the
# keys were written in is the order they break ties in.
Direction = Literal["ascending", "descending"]

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


@runtime_checkable
class ReplicaConnection(Protocol):
    """pyturso's `turso.aio.sync.ConnectionSync`: DBAPI plus replication.

    `turso.aio.connect` and `turso.aio.sync.connect` return different types,
    and every function here took `Any`, so nothing said which one it needed.
    `_pull_until_level` requires `pull`; a local connection has no such method
    and the mistake would surface only at runtime, on a replica, in
    production. Signatures verified against pyturso on 2026-08-23:
    `ConnectionSync` adds `pull`, `push`, `checkpoint` and `stats` to
    `Connection`'s `execute`, `commit`, `rollback` and `close`.
    """

    async def execute(self, *args: Any, **kwargs: Any) -> Any: ...

    async def commit(self) -> Any: ...

    async def rollback(self) -> Any: ...

    async def close(self) -> Any: ...

    async def pull(self) -> bool: ...

    async def push(self) -> Any: ...

    async def checkpoint(self) -> Any: ...


# =============================================================================
# Which tables are ours
# =============================================================================

# THE PREFIXES THIS PACKAGE CREATES FOR ITSELF, and `_dp_enum_` is
# deliberately absent. `expand_schema_enums` puts the enum lookup tables into
# the TARGET schema, so they are on both sides of a diff and migrate like any
# other table; claiming them here would stop them being created at all.
_OUR_PREFIXES = ("_declaro_meta", "_declaro_tmp_", "_dp_materialized_views", "_dp_transfer_state")


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


# =============================================================================
# Absence, written down
# =============================================================================

# AN OMITTED ARGUMENT WAS INDISTINGUISHABLE FROM A CHOSEN ONE. `def
# select(table, where=None, limit=None, from_table=NOTHING, params=NOTHING, order_by=NOTHING, offset=NOTHING, joins=NOTHING, group_by=NOTHING, having=NOTHING)` absorbs the caller's omission:
# afterwards nothing can tell "asked for no limit" from "forgot the limit",
# and the region where the default applies is invisible at every call site,
# so nothing exercises it. Honest Code rule 14 calls the `=` default the
# input-side of catch-and-swallow.
#
# A parameter is declared `X | Nothing` with no default, so a caller who
# wants the absent case says so and the type carries exactly two
# possibilities rather than one plus a silence.
#
# IT IS `Ellipsis` UNDERNEATH, which no schema, column, default or query
# argument can ever be, so it cannot collide with a value somebody meant.
# `None` cannot do this job: NULL is a legal default and a legal column
# value, so a sentinel spelled `None` cannot tell absence from a declared
# NULL. This also matches what the package already reads as "not given":
# `_NO_DEFAULT` in the pydantic loader has listed `...` beside `None` and
# `PydanticUndefined` since the loader was written.
Nothing = EllipsisType
NOTHING: Final[EllipsisType] = ...


def is_nothing(value: Any) -> bool:
    """Whether a caller left this out."""
    return value is NOTHING


def resolved(value: T | Nothing, otherwise: T) -> T:
    """The value a caller passed, or `otherwise` when they passed NOTHING.

    A default hides this inside the signature, where no reader sees it happen
    and no test can reach it. Here it is a line in the body.

    `value or otherwise` is what this replaces in several places and it is a
    different function: it takes the fallback for 0, "" and [], so a declared
    `DEFAULT 0` and an empty WHERE clause became the fallback silently. Only
    NOTHING means absent.
    """
    return _RESOLVED[is_nothing(value)](value, otherwise)


def _the_value(value: T | Nothing, _otherwise: T) -> T:
    """A caller passed something, so that is the answer."""
    return value


def _the_other(_value: T | Nothing, otherwise: T) -> T:
    """A caller passed NOTHING, so the named alternative stands."""
    return otherwise


_RESOLVED: dict[bool, Callable[[Any, Any], Any]] = {
    True: _the_other,
    False: _the_value,
}


# =============================================================================
# What a word gives back
# =============================================================================


class Failure(TypedDict):
    """What went wrong, as a value rather than a raise or a log line.

    A function that logs an error and returns something you cannot tell from
    success has two outputs and only one of them is in the signature. This is
    the other one, written down. Whether anyone hears about it is the
    orchestrator's call, because the function does not know whether it sits
    inside a retry about to succeed, a dry run, or an incident at 3am.

    `what` names the word that could not do its job. `because` carries the
    reason, including the engine's own message when the engine gave one.
    """

    what: str
    because: str


class Answer(TypedDict):
    """What every word in the vocabulary gives back. One shape, six keys.

    THE KEY A WORD FILLS IS THE ONLY THING THAT VARIES, so it is data in the
    table beside the word rather than a return type repeated twenty-three
    times. `count` fills `number`, `exists` fills `truth`, `all` fills `rows`.

    EVERY OTHER KEY IS `NOTHING`, which is what makes absence and failure
    impossible to walk past. A caller reading `["row"]` gets the row or gets
    `NOTHING`, and the two are different values rather than a row and a
    silence. `None` cannot do this job here for the same reason it cannot
    anywhere else in this package: a nullable column holds `None`, so "no such
    row" and "a row whose name is null" would stop being distinguishable.

    `failure` is a key like any other. A word that could not do its job fills
    it and leaves the rest `NOTHING`, so a caller tells success from failure by
    which key is not `NOTHING` rather than by catching something.
    """

    rows: list[Any] | Nothing
    row: Any | Nothing
    number: int | Nothing
    truth: bool | Nothing
    mappings: list[dict[str, Any]] | Nothing
    failure: Failure | Nothing


# The key a word fills. Six kinds of answer, named, so a table row saying
# "count fills number" is checked rather than spelled.
AnswerKey = Literal["rows", "row", "number", "truth", "mappings", "failure"]

_NOTHING_ANSWERED: Final[Answer] = {
    "rows": NOTHING,
    "row": NOTHING,
    "number": NOTHING,
    "truth": NOTHING,
    "mappings": NOTHING,
    "failure": NOTHING,
}


def answered(key: AnswerKey, value: Any) -> Answer:
    """One key filled, the other five left as the absence they are."""
    filled: dict[str, Any] = {**_NOTHING_ANSWERED, key: value}
    return cast(Answer, filled)


def failed(what: str, because: str) -> Answer:
    """A word that could not do its job, saying so as a value."""
    return answered("failure", Failure(what=what, because=because))


def type_of(value: Any) -> type:
    """The type of a value read at runtime.

    This is the one place the package calls `type()`. Input that carries no
    declaration -- hand-written TOML, a column definition a caller built by
    hand -- has to be read somehow, and the dispatch tables that handle it
    are keyed on what this returns. A type the table does not hold is one
    nobody declared, and the caller refuses it by name.
    """
    return type(value)  # honest: ignore HC-P005: reading the type is what this function is for


def _unchanged(value: Any) -> Any:
    """No rule applies to this value, so it comes back as it went in."""
    return value


def written_as(kind: type, value: Any) -> bool:
    """Whether `value` is a `kind`, counting subclasses.

    A library can hand back its own subclass of anything -- asyncpg does that
    with `uuid.UUID` -- so a check for the exact type refuses a value that is,
    for every purpose the caller cares about, what it asked for.

    The order of the arguments matters. `bool` is a subclass of `int`, so
    `written_as(int, True)` is true and `written_as(bool, 1)` is false. A
    view's `materialized` has to be a bool, and 1 is refused.
    """
    return kind in type_of(value).__mro__


def translated_by(rules: dict[type, Callable[[Any], Any]], value: Any) -> Any:
    """Apply the rule `rules` holds for this value's type, or return it as is.

    Subclasses count. Matching the exact type alone lost the one conversion
    `bulk_transfer` makes: asyncpg hands back `asyncpg.pgproto.pgproto.UUID`,
    a subclass of `uuid.UUID` rather than `uuid.UUID` itself, so a PostgreSQL
    to SQLite transfer moved no rows and reported `type
    'asyncpg.pgproto.pgproto.UUID' is not supported`. The nearest match along
    the class hierarchy wins, so an entry for the exact type still beats one
    for a base class.

    A type `rules` does not hold has no conversion and the value comes back
    unchanged. `sqlite_default` is the one caller that refuses instead, and it
    does so with an entry that raises rather than by leaving the type out.
    """
    written = [rules[kind] for kind in type_of(value).__mro__ if kind in rules]
    return [*written, _unchanged][0](value)


def applied_to_text(rule: Callable[[str], Any], value: Any) -> Any:
    """Apply `rule` to a string; return anything else unchanged.

    Only a string can be written more than one way, so only a string needs
    normalising. A number is the same number on every engine.

    Five places wrote this as `if not isinstance(x, str): return x` ahead of
    the normalising, and each was a separate chance to forget it. `_comparable`
    had four such guards, two per rule and one per argument. All three bugs
    `same_default` has been fixed for were a rule that reached one argument
    and not the other.
    """
    return translated_by({str: rule}, value)


def changed_to(value: Any) -> Any:
    """What a property became, whether or not it arrived as a change record.

    The differ writes `{"from": old, "to": new}` for a property that changed,
    and a bare value where it did not go through that path. Nine places read
    that, and on 2026-08-21 they disagreed: the PostgreSQL applier wrote
    `changes["type"]["to"]` and assumed the record, while five other sites
    tested `isinstance(value, dict) and "to" in value` first.

    So a bare value reaching PostgreSQL raised TypeError, and a record
    reaching a site that had forgotten the test went into a column definition
    whole. The second has happened: `_map_type` called `.lower()` on a dict,
    and a CHECK removed from the models rendered as `CHECK (None)`.

    `to: None` IS AN ANSWER, not an absence. A property removed is written as
    a change to None, and reading that as "no value" is what produced the
    None above.

    A dict without a `to` is some other dict. An index definition is a dict
    too, and returning its contents would put the wrong thing in a column.
    """
    return (
        value["to"] if isinstance(value, dict) and "to" in value else value
    )  # honest: ignore HC-P005: the one place that reads a change record


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


def is_nullable(column: Column) -> bool:
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
    # A primary key is NOT NULL whatever the column says, so the two answers
    # are not symmetric and the key wins.
    return not column.get("primary_key") and bool(column.get("nullable", True))


def for_dialect(table: dict[str, V], dialect: str, what: str) -> V:
    """Read a dispatch table, naming what exists when the key does not.

    THIS WAS TWO CLASSES. `applier/__init__.py` and `inspector/__init__.py`
    each declared `class _Appliers(dict)` / `class _Inspectors(dict)` whose
    only member was `__missing__`, subclassing `dict` to borrow its behaviour
    and override one hook. A wrapper around a dict is allowed when it holds no
    methods; the method was the whole reason each class existed, so it was
    inheritance for code reuse and nothing else.

    THE PAIR'S ANSWER IS RETURNED, not discarded. This called the pair for its
    refusal alone and then looked the entry up a second time, so `_the_entry`
    computed a value nothing read.

    The lookup is what varies, so the lookup is the function. The tables stay
    plain dicts, which means they can be printed, merged, compared with `==`
    and passed to anything expecting a dict.

    A bare `KeyError: 'mysql'` tells a caller what they asked for and not what
    was available. `what` names the table so one message can serve every one
    of them.
    """
    return _WHEN_NO_ENTRY[dialect in table](table, dialect, what)


def _the_entry(table: dict[str, V], dialect: str, _what: str) -> V:
    """The table holds this dialect, so this is the entry."""
    return table[dialect]


def _refuse_unknown_dialect(table: dict[str, V], dialect: str, what: str) -> V:
    """No entry, named alongside the ones that exist."""
    raise ValueError(
        f"Unsupported dialect: {dialect}. {what} exist for: {', '.join(sorted(table))}"
    )


# Whether the table holds the dialect asked for.
_WHEN_NO_ENTRY: dict[bool, Callable[[dict[str, Any], str, str], Any]] = {
    True: _the_entry,
    False: _refuse_unknown_dialect,
}
