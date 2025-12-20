"""
Core type definitions for declaro_persistum.

All data structures are TypedDict - no classes with state.
This ensures serialization to/from TOML/JSON and compatibility with pure functions.
"""

from typing import Any, Literal, TypedDict

# =============================================================================
# Extended Schema Objects (Addendum)
# =============================================================================


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
    ]
    table: str
    details: dict[str, Any]


class Ambiguity(TypedDict):
    """
    An ambiguous change that requires human decision.

    Attributes:
        type: The type of ambiguity (possible_rename, confirm_drop, etc.)
        table: The table involved
        from_column: The original column name (for renames)
        to_column: The new column name (for renames)
        column: The column name (for drops)
        confidence: Confidence score for rename detection (0.0-1.0)
        message: Human-readable description of the ambiguity
    """

    type: Literal["possible_rename", "confirm_drop", "type_change"]
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
