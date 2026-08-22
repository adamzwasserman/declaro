"""Apply migration operations to PostgreSQL. Functions, not a class.

De-classed mechanically: methods lifted out, dedented, `self` removed. Every
statement PostgreSQL is asked to run is byte-for-byte what it was.

PostgreSQL supports transactional DDL, so every operation is wrapped in a
single transaction and the batch is atomic. That is the one thing this applier
can promise which the SQLite and Turso ones cannot.
"""

from collections.abc import Callable
from typing import Any

from declaro_persistum.applier.shared import (
    column_definition,
    constraint_clause,
    insert_enum_values_sql,
)
from declaro_persistum.boundary import boundary, helper
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import (
    NOTHING,
    ApplyResult,
    Column,
    Nothing,
    Operation,
    View,
    changed_to,
    resolved,
)

# Type for SQL generator functions
SQLGenerator = Callable[..., str]


@helper
def get_dialect() -> str:
    """Return dialect identifier."""
    return "postgresql"


@helper
def get_transaction_mode() -> str:
    """One failure ends the batch, and the whole batch is rolled back.

    THE OTHER TWO APPLIERS HAVE DECLARED THIS ALL ALONG AND THIS ONE DID NOT.
    `usage.md` documents importing it from here, and that import raised
    ImportError. A note in `test_every_cli_command_can_be_called` recorded the
    absence on 2026-08-17, alongside `generate_sql` missing from the other
    two: a five-member Protocol that no module satisfied, deleted rather than
    filled. Deleting it left this hole documented and open.

    `all_or_nothing` is what this module already does. `apply` runs inside
    `connection.transaction()` and re-raises, so an operation that fails takes
    the batch with it and leaves nothing behind. Turso is the odd one and says
    `per_operation`, because it cannot perform several operations the differ
    emits and refusing a batch for one of them would block the rest forever.
    """
    return "all_or_nothing"




@boundary
async def apply(
    connection: Any,
    operations: list[Operation],
    execution_order: list[int],
    *,
    dry_run: bool | Nothing,
) -> ApplyResult:
    """
    Apply migration operations within a transaction.

    PostgreSQL's transactional DDL means we can safely roll back
    all changes if any operation fails.
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    dry_run = resolved(dry_run, False)
    # ONE ENTRY PER OPERATION, each holding that operation's statements. The
    # dry run prints them joined; the run below executes them one at a time,
    # because an operation is sometimes more than one statement.
    per_operation = [statements_for(operations[i]) for i in execution_order]
    sql_statements = [";\n".join(group) for group in per_operation]

    # WHETHER THE STATEMENTS ARE PRINTED OR RUN. Both entries take the same
    # five arguments and hand back the same shape of result.
    return await _WHEN_DRY_RUN[bool(dry_run)](
        connection, operations, execution_order, per_operation, sql_statements
    )


@helper
async def _preview(
    _connection: Any,
    _operations: list[Operation],
    _execution_order: list[int],
    _per_operation: list[list[str]],
    sql_statements: list[str],
) -> ApplyResult:
    """A dry run asks the database nothing, so the statements are the result."""
    return {
        "success": True,
        "executed_sql": sql_statements,
        "operations_applied": len(sql_statements),
        "error": None,
        "error_operation": None,
    }


@boundary
async def _executed_statements(connection: Any, statements: list[str]) -> list[str]:
    """Run each statement in order and hand back the ones that reached the engine.

    One result comes back per statement, so the count of results is the count
    of statements that ran. A statement the engine refuses takes the
    comprehension with it and nothing comes back at all.
    """
    cursors = [await connection.execute(statement) for statement in statements]
    return statements[: len(cursors)]


@boundary
async def _execute_operation(
    connection: Any,
    operations: list[Operation],
    execution_order: list[int],
    per_operation: list[list[str]],
    sql_statements: list[str],
    position: int,
) -> str:
    """Run one operation's statements and hand back the block that ran."""
    sql = sql_statements[position]
    try:
        return ";\n".join(
            await _executed_statements(connection, per_operation[position])
        )
    except Exception as e:
        raise MigrationError(
            f"Failed to execute operation {position + 1}/{len(execution_order)}",
            operation=operations[execution_order[position]],
            sql=sql,
            original_error=e,
        ) from e


@boundary
async def _run_batch(
    connection: Any,
    operations: list[Operation],
    execution_order: list[int],
    per_operation: list[list[str]],
    sql_statements: list[str],
) -> ApplyResult:
    """Every operation inside one transaction, so the batch is atomic."""
    try:
        # Start transaction
        async with connection.transaction():
            executed = [
                await _execute_operation(
                    connection,
                    operations,
                    execution_order,
                    per_operation,
                    sql_statements,
                    position,
                )
                for position in range(len(execution_order))
            ]

        return {
            "success": True,
            "executed_sql": executed,
            "operations_applied": len(executed),
            "error": None,
            "error_operation": None,
        }

    except MigrationError:
        # Transaction automatically rolled back
        raise
    except Exception as e:
        raise MigrationError(
            f"Migration failed: {e}",
            original_error=e, operation=NOTHING, sql=NOTHING) from e


_WHEN_DRY_RUN: dict[bool, Callable[..., Any]] = {
    True: _preview,
    False: _run_batch,
}


# `generate_sql` was here and nowhere else, so `declaro generate` worked on
# PostgreSQL and raised AttributeError on SQLite and Turso. The loop it ran is
# dialect-independent, so it now lives once in `applier/__init__.py` beside the
# `SQL_GENERATORS` table, and this module keeps only what varies:
# `generate_operation_sql`.




@helper
def statements_for(operation: Operation) -> list[str]:
    """The statements an operation runs, already separated.

    The same shape as `shared.statements_for`, for the same reason: an
    operation is sometimes more than one statement, and joining them into a
    string only to take them apart again is how a trigger body and a string
    literal both got cut in half on the other two dialects.
    """
    op_type = operation["op"]
    generator = _WHEN_HANDLED[op_type in OPERATION_SQL](op_type)
    return generator(operation["table"], operation["details"])


@helper
def _the_generator(op_type: str) -> SQLGenerator:
    """An operation this module renders, so the table names its renderer."""
    return OPERATION_SQL[op_type]


@helper
def _refuse_operation(op_type: str) -> SQLGenerator:
    """An operation this module does not render, named beside the ones it does."""
    raise ValueError(
        f"Unknown operation type: {op_type}. "
        f"Handled: {', '.join(sorted(OPERATION_SQL))}"
    )


_WHEN_HANDLED: dict[bool, Callable[[str], SQLGenerator]] = {
    True: _the_generator,
    False: _refuse_operation,
}


@helper
def generate_operation_sql(operation: Operation) -> str:
    """The same statements as one block, for a person to read.

    This is what `--dry-run` prints. Nothing parses it back, which is why
    joining is safe here and was not safe on the execution path.
    """
    return ";\n".join(statements_for(operation))


@helper
def _create_table_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate CREATE TABLE statement."""
    columns = details.get("columns", {})
    primary_key = details.get("primary_key", [])

    col_defs: list[str] = [
        _column_definition(col_name, col_def)
        for col_name, col_def in columns.items()
    ]

    # A single-column key is written on the column itself, so only a composite
    # one becomes a table-level clause. The filter has to sit above the join,
    # because a comprehension evaluates its clauses in order and `primary_key`
    # can be absent.
    composite_key = [
        f"PRIMARY KEY ({', '.join(chr(34) + c + chr(34) for c in key)})"
        for key in [primary_key]
        if key and len(key) > 1
    ]

    # THE TABLE-LEVEL CONSTRAINTS, WHICH THIS COPY ALSO IGNORED. The shared
    # renderer was given them an hour before this one, and PostgreSQL keeps
    # its own `_create_table_sql`, so the fix landed on two engines of three
    # and this one still dropped a declared constraint on the floor.
    #
    # That is the shape this module's own comment records about `generate_sql`
    # living here and nowhere else: a function copied per dialect goes missing
    # from the copies. `constraint_clause` is shared so the two cannot spell
    # a constraint differently.
    #
    # Unlike SQLite and Turso, PostgreSQL has ADD CONSTRAINT, so one could be
    # added afterwards. Rendering it here keeps the three engines producing
    # the same table from the same declaration, which is what makes a
    # cross-engine transfer land the same shape.
    constraint_defs = [
        constraint_clause(name, definition)
        for name, definition in (details.get("constraints") or {}).items()
    ]

    columns_sql = ",\n    ".join([*col_defs, *composite_key, *constraint_defs])
    return [
        f'CREATE TABLE "{table}" (\n    {columns_sql}\n)'
    ]


@helper
def _column_definition(name: str, col: Column) -> str:
    """One line thick, because the rest is shared.

    This was a near-copy of `applier.shared.column_definition` that differed in
    two places: it used the declared type verbatim while the shared one applied
    SQLite affinity, and it did not double-wrap an expression default. Both are
    now looked up by dialect, so PRIMARY KEY, NOT NULL, UNIQUE, CHECK and
    REFERENCES with its cascade are written once for every engine.
    """
    return column_definition(name, col, "postgresql")

@helper
def _drop_table_sql(table: str, _details: dict[str, Any]) -> list[str]:
    """Generate DROP TABLE statement."""
    return [
        f'DROP TABLE "{table}"'
    ]


@helper
def _rename_table_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE RENAME statement."""
    new_name = details["new_name"]
    return [
        f'ALTER TABLE "{table}" RENAME TO "{new_name}"'
    ]


@helper
def _add_column_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE ADD COLUMN statement."""
    col_name = details["column"]
    col_def = details["definition"]
    col_sql = _column_definition(col_name, col_def)
    return [
        f'ALTER TABLE "{table}" ADD COLUMN {col_sql}'
    ]


@helper
def _drop_column_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE DROP COLUMN statement."""
    col_name = details["column"]
    return [
        f'ALTER TABLE "{table}" DROP COLUMN "{col_name}"'
    ]


@helper
def _rename_column_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE RENAME COLUMN statement."""
    from_col = details["from_column"]
    to_col = details["to_column"]
    return [
        f'ALTER TABLE "{table}" RENAME COLUMN "{from_col}" TO "{to_col}"'
    ]


@helper
def _alter_type_sql(table: str, col_name: str, change: Any) -> list[str]:
    """The column's new type, with a cast so the rows already there convert."""
    new_type = changed_to(change)
    return [
        f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" '
        f'TYPE {new_type} USING "{col_name}"::{new_type}'
    ]


@helper
def _drop_not_null(table: str, col_name: str) -> str:
    """The column accepts NULL."""
    return f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" DROP NOT NULL'


@helper
def _set_not_null(table: str, col_name: str) -> str:
    """The column refuses NULL."""
    return f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" SET NOT NULL'


_NULLABILITY: dict[bool, Callable[[str, str], str]] = {
    True: _drop_not_null,
    False: _set_not_null,
}


@helper
def _alter_nullable_sql(table: str, col_name: str, change: Any) -> list[str]:
    """Whether the column accepts NULL, which is two different statements."""
    return [_NULLABILITY[bool(changed_to(change))](table, col_name)]


@helper
def _drop_default(table: str, col_name: str, _value: Any) -> str:
    """The column has no default any more."""
    return f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" DROP DEFAULT'


@helper
def _set_default(table: str, col_name: str, value: Any) -> str:
    """The column's new default, written as the declaration spells it."""
    return f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" SET DEFAULT {value}'


# A `to: None` MEANS THE DEFAULT IS GONE, which is DROP DEFAULT rather than
# `SET DEFAULT None`.
_DEFAULT_CHANGE: dict[bool, Callable[[str, str, Any], str]] = {
    True: _drop_default,
    False: _set_default,
}


@helper
def _alter_default_sql(table: str, col_name: str, change: Any) -> list[str]:
    """A default set, or a default removed."""
    new_default = changed_to(change)
    return [_DEFAULT_CHANGE[new_default is None](table, col_name, new_default)]


@helper
def _add_unique(table: str, col_name: str, constraint: str) -> str:
    """The column's values must differ from each other."""
    return (
        f'ALTER TABLE "{table}" ADD CONSTRAINT "{constraint}" '
        f'UNIQUE ("{col_name}")'
    )


@helper
def _drop_unique(table: str, _col_name: str, constraint: str) -> str:
    """The column may repeat a value again."""
    return f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{constraint}"'


_UNIQUE_CHANGE: dict[bool, Callable[[str, str, str], str]] = {
    True: _add_unique,
    False: _drop_unique,
}


@helper
def _alter_unique_sql(table: str, col_name: str, change: Any) -> list[str]:
    """UNIQUE IS NOT AN ALTER COLUMN ON THIS ENGINE, it is a table constraint,
    and it was not handled at all. The differ compares `unique` on every
    dialect, so making a column unique produced an alter_column whose
    changes this function did not recognise, it returned "", and asyncpg
    crashed inside its own protocol:

        rendered ''
        AttributeError: 'NoneType' object has no attribute 'decode'

    The constraint is named for the table and column so that dropping it
    later can find it, which is the same convention `_add_constraint_sql`
    uses below.
    """
    constraint = f"{table}_{col_name}_key"
    return [_UNIQUE_CHANGE[bool(changed_to(change))](table, col_name, constraint)]


# WHICH PROPERTIES THIS RENDERER WRITES, in the order it writes them. The
# order of the statements is the order of this table.
_ALTER_COLUMN_SQL: dict[str, Callable[[str, str, Any], list[str]]] = {
    "type": _alter_type_sql,
    "nullable": _alter_nullable_sql,
    "default": _alter_default_sql,
    "unique": _alter_unique_sql,
}


@helper
def _the_alter_statements(
    _table: str, _col_name: str, _changes: dict[str, Any], statements: list[str]
) -> list[str]:
    """A property this renderer writes was named, so these statements run."""
    return statements


@helper
def _refuse_empty_alter_column(
    table: str, col_name: str, changes: dict[str, Any], _statements: list[str]
) -> list[str]:
    """AN OPERATION THAT RENDERS NOTHING IS A BUG, NOT A NO-OP. Returning ""
    sent an empty string to `connection.execute`, and asyncpg does not
    refuse it: it fails deep inside the protocol with an AttributeError
    about `decode`, which names nothing an operator could act on. Saying
    which change was not recognised turns a driver crash into a sentence.
    """
    raise ValueError(
        f"No PostgreSQL statement for an alter_column on "
        f"{table!r}.{col_name!r}: this renderer handles "
        f"{list(_ALTER_COLUMN_SQL)} and the operation "
        f"asked for {sorted(changes)}."
    )


_WHEN_ALTERED: dict[
    bool, Callable[[str, str, dict[str, Any], list[str]], list[str]]
] = {
    True: _the_alter_statements,
    False: _refuse_empty_alter_column,
}


@helper
def _alter_column_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE ALTER COLUMN statement(s)."""
    col_name = details["column"]
    changes = details["changes"]

    statements = [
        statement
        for prop, render in _ALTER_COLUMN_SQL.items()
        if prop in changes
        for statement in render(table, col_name, changes[prop])
    ]

    return [
        "; ".join(
            _WHEN_ALTERED[bool(statements)](table, col_name, changes, statements)
        )
    ]


@helper
def _add_index_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate CREATE INDEX statement."""
    idx_name = details["index"]
    idx_def = details["definition"]
    columns = idx_def.get("columns", [])
    unique = "UNIQUE " if idx_def.get("unique") else ""
    using = f" USING {idx_def['using']}" if idx_def.get("using") else ""
    where = f" WHERE {idx_def['where']}" if idx_def.get("where") else ""

    cols_sql = ", ".join(f'"{c}"' for c in columns)
    return [
        f'CREATE {unique}INDEX "{idx_name}" ON "{table}"{using} ({cols_sql}){where}'
    ]


@helper
def _drop_index_sql(_table: str, details: dict[str, Any]) -> list[str]:
    """Generate DROP INDEX statement."""
    idx_name = details["index"]
    return [
        f'DROP INDEX "{idx_name}"'
    ]


@helper
def _add_check_sql(table: str, const_name: str, const_def: dict[str, Any]) -> list[str]:
    """A CHECK, added to a table that already exists."""
    expr = const_def.get("expression", "")
    return [
        f'ALTER TABLE "{table}" ADD CONSTRAINT "{const_name}" CHECK ({expr})'
    ]


@helper
def _add_unique_sql(table: str, const_name: str, const_def: dict[str, Any]) -> list[str]:
    """A UNIQUE over the columns the declaration names."""
    cols = const_def.get("columns", [])
    cols_sql = ", ".join(f'"{c}"' for c in cols)
    return [
        f'ALTER TABLE "{table}" ADD CONSTRAINT "{const_name}" UNIQUE ({cols_sql})'
    ]


# WHAT EACH CONSTRAINT TYPE IS WRITTEN AS. Two entries, read by subscript.
_ADD_CONSTRAINT_SQL: dict[str, Callable[[str, str, dict[str, Any]], list[str]]] = {
    "check": _add_check_sql,
    "unique": _add_unique_sql,
}


@helper
def _write_added_constraint(
    table: str, const_name: str, kind: Any, const_def: dict[str, Any]
) -> list[str]:
    """A type this renderer writes, so the table names the writer."""
    return _ADD_CONSTRAINT_SQL[kind](table, const_name, const_def)


@helper
def _refuse_added_constraint(
    _table: str, _const_name: str, kind: Any, _const_def: dict[str, Any]
) -> list[str]:
    """A type this renderer does not write, named so the author finds it."""
    raise ValueError(f"Unknown constraint type: {kind}")


_KNOWN_ADDED_CONSTRAINT: dict[
    bool, Callable[[str, str, Any, dict[str, Any]], list[str]]
] = {
    True: _write_added_constraint,
    False: _refuse_added_constraint,
}


@helper
def _add_constraint_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE ADD CONSTRAINT statement."""
    const_name = details["constraint"]
    const_def = details["definition"]
    kind = const_def.get("type")

    return _KNOWN_ADDED_CONSTRAINT[kind in _ADD_CONSTRAINT_SQL](
        table, const_name, kind, const_def
    )


@helper
def _drop_constraint_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE DROP CONSTRAINT statement."""
    const_name = details["constraint"]
    return [
        f'ALTER TABLE "{table}" DROP CONSTRAINT "{const_name}"'
    ]


@helper
def _add_foreign_key_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE ADD FOREIGN KEY statement."""
    col_name = details["column"]
    ref = details["references"]
    ref_table, ref_col = ref.split(".")

    fk_name = f"fk_{table}_{col_name}_{ref_table}"
    sql = (
        f'ALTER TABLE "{table}" ADD CONSTRAINT "{fk_name}" '
        f'FOREIGN KEY ("{col_name}") REFERENCES "{ref_table}"("{ref_col}")'
    )

    # THE TWO CASCADE ACTIONS, each written only when the operation carries
    # one. A declaration that names neither leaves the list empty.
    actions = [
        f" ON {clause} {details[key].upper().replace('_', ' ')}"
        for key, clause in (("on_delete", "DELETE"), ("on_update", "UPDATE"))
        if details.get(key)
    ]

    return [
        "".join([sql, *actions])
    ]


@helper
def _drop_foreign_key_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Drop a foreign key by both names it can have.

    THE CONVENTION NAME IS ONLY HALF OF WHAT EXISTS. This dropped
    `fk_{table}_{column}_{ref_table}`, which `_add_foreign_key_sql` writes, and
    a key created INLINE by CREATE TABLE gets PostgreSQL's own auto-name,
    `{table}_{column}_fkey`. So repointing a foreign key that had been there
    since the table was made failed every time:

        ALTER TABLE "children" DROP CONSTRAINT "fk_children_pid_parents"
        UndefinedObjectError: constraint ... does not exist

    Measured 2026-08-19. Both names are deterministic, so both are dropped
    with IF EXISTS rather than one being guessed at. `IF EXISTS` is safe here
    precisely because two names are tried: without it, the correct one would
    still be dropped and the other would raise.

    The create side names the constraint explicitly now, so new tables carry
    the convention name; the second clause is for tables made before that.
    """
    col_name = details["column"]
    ref = details.get("references", "")
    ref_table = ref.split(".")[0] if "." in ref else ref

    convention = f"fk_{table}_{col_name}_{ref_table}"
    postgres_default = f"{table}_{col_name}_fkey"
    # TWO STATEMENTS, NOT ONE STRING HOLDING A SEMICOLON. These used to be
    # joined with "; " inside a single f-string, which the SQLite and Turso
    # appliers would then have had to split apart again.
    return [
        f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{convention}"',
        f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{postgres_default}"',
    ]


@helper
def _create_view_sql(_table: str, details: dict[str, Any]) -> list[str]:
    """Generate CREATE VIEW or CREATE MATERIALIZED VIEW statement."""
    return generate_create_view(details)  # type: ignore[arg-type]


@helper
def _drop_view_sql(_table: str, details: dict[str, Any]) -> list[str]:
    """Generate DROP VIEW or DROP MATERIALIZED VIEW statement."""
    return [
        generate_drop_view(details["name"], bool(details.get("materialized")))
    ]


# =============================================================================
# Extended Schema Objects (Addendum) - Standalone Functions
# =============================================================================




















@helper
def generate_create_view(view: View) -> list[str]:
    """The statements that create a view, native or materialized.

    THE NAME IS QUOTED. It was interpolated bare, so a view called `order` or
    `Select` produced a syntax error, and a mixed-case name was folded to
    lower case and became a different object from the one declared.
    """
    name = view["name"]
    query = view["query"]

    return [_CREATE_VIEW[bool(view.get("materialized"))](name, query)]


@helper
def _materialized_view_sql(name: str, query: str) -> str:
    """A materialized view holds its rows, so it cannot be replaced in place."""
    return f'CREATE MATERIALIZED VIEW "{name}" AS\n{query}'


@helper
def _plain_view_sql(name: str, query: str) -> str:
    """A plain view is only a query, so it is replaced where it stands."""
    return f'CREATE OR REPLACE VIEW "{name}" AS\n{query}'


_CREATE_VIEW: dict[bool, Callable[[str, str], str]] = {
    True: _materialized_view_sql,
    False: _plain_view_sql,
}


@helper
def generate_drop_view(name: str, materialized: bool) -> str:
    """DROP VIEW or DROP MATERIALIZED VIEW, which are different statements.

    `materialized` IS REQUIRED AND HAS NO DEFAULT. It defaulted to False, so a
    caller that forgot it emitted DROP VIEW against a materialized view, which
    PostgreSQL refuses with WrongObjectTypeError rather than ignoring. The
    differ puts `materialized` in the drop operation's details, so the caller
    always knows it; the default only hid a caller that had not passed it.

    THE NAME IS QUOTED, for the same reason as on the other two dialects. A
    view called `order` is a legal name and unquoted it is a syntax error.

    The SQLite twin of this had both defects and was fixed first. That is the
    fifth pair in this package where a fix reached one copy and not the other.
    """
    kind = "MATERIALIZED VIEW" if materialized else "VIEW"
    return f'DROP {kind} IF EXISTS "{name}"'

# HOISTED OUT OF THE FUNCTION. This table was a local variable rebuilt on
# every call, so no gate could read it and it silently fell one operation
# behind the table in `shared.py`. `declaro` emitted `insert_enum_values`, this
# module answered `Unknown operation type`, and the SQLite tests were green.
#
# `insert_enum_values` renders identically on every engine -- it is an INSERT
# of literal values into a two-column table -- so the entry points at the one
# in `shared.py` rather than at a fourth copy.
OPERATION_SQL: dict[str, SQLGenerator] = {
    "create_table": _create_table_sql,
    "drop_table": _drop_table_sql,
    "rename_table": _rename_table_sql,
    "add_column": _add_column_sql,
    "drop_column": _drop_column_sql,
    "rename_column": _rename_column_sql,
    "alter_column": _alter_column_sql,
    "add_index": _add_index_sql,
    "drop_index": _drop_index_sql,
    "add_constraint": _add_constraint_sql,
    "drop_constraint": _drop_constraint_sql,
    "add_foreign_key": _add_foreign_key_sql,
    "drop_foreign_key": _drop_foreign_key_sql,
    "create_view": _create_view_sql,
    "drop_view": _drop_view_sql,
    "insert_enum_values": insert_enum_values_sql,
}
