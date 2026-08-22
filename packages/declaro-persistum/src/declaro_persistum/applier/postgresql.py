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

    if dry_run:
        return {
            "success": True,
            "executed_sql": sql_statements,
            "operations_applied": len(sql_statements),
            "error": None,
            "error_operation": None,
        }

    executed: list[str] = []

    try:
        # Start transaction
        async with connection.transaction():
            for i, op_idx in enumerate(execution_order):
                sql = sql_statements[i]
                try:
                    for statement in per_operation[i]:
                        await connection.execute(statement)
                    executed.append(sql)
                except Exception as e:
                    raise MigrationError(
                        f"Failed to execute operation {i + 1}/{len(execution_order)}",
                        operation=operations[op_idx],
                        sql=sql,
                        original_error=e,
                    ) from e

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

    generator = OPERATION_SQL.get(op_type)
    if not generator:
        raise ValueError(
            f"Unknown operation type: {op_type}. "
            f"Handled: {', '.join(sorted(OPERATION_SQL))}"
        )

    return generator(operation["table"], operation["details"])


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

    col_defs: list[str] = []

    for col_name, col_def in columns.items():
        col_sql = _column_definition(col_name, col_def)
        col_defs.append(col_sql)

    # Add composite primary key if specified
    if primary_key and len(primary_key) > 1:
        pk_cols = ", ".join(f'"{c}"' for c in primary_key)
        col_defs.append(f"PRIMARY KEY ({pk_cols})")

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
    for name, definition in (details.get("constraints") or {}).items():
        col_defs.append(constraint_clause(name, definition))

    columns_sql = ",\n    ".join(col_defs)
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
def _alter_column_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE ALTER COLUMN statement(s)."""
    col_name = details["column"]
    changes = details["changes"]
    statements: list[str] = []

    if "type" in changes:
        new_type = changed_to(changes["type"])
        statements.append(
            f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" '
            f'TYPE {new_type} USING "{col_name}"::{new_type}'
        )

    if "nullable" in changes:
        if changed_to(changes["nullable"]):
            statements.append(f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" DROP NOT NULL')
        else:
            statements.append(f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" SET NOT NULL')

    if "default" in changes:
        new_default = changed_to(changes["default"])
        if new_default is None:
            statements.append(f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" DROP DEFAULT')
        else:
            statements.append(
                f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" SET DEFAULT {new_default}'
            )

    # UNIQUE IS NOT AN ALTER COLUMN ON THIS ENGINE, it is a table constraint,
    # and it was not handled at all. The differ compares `unique` on every
    # dialect, so making a column unique produced an alter_column whose
    # changes this function did not recognise, it returned "", and asyncpg
    # crashed inside its own protocol:
    #
    #     rendered ''
    #     AttributeError: 'NoneType' object has no attribute 'decode'
    #
    # The constraint is named for the table and column so that dropping it
    # later can find it, which is the same convention `_add_constraint_sql`
    # uses below.
    if "unique" in changes:
        constraint = f"{table}_{col_name}_key"
        if changed_to(changes["unique"]):
            statements.append(
                f'ALTER TABLE "{table}" ADD CONSTRAINT "{constraint}" '
                f'UNIQUE ("{col_name}")'
            )
        else:
            statements.append(
                f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{constraint}"'
            )

    # AN OPERATION THAT RENDERS NOTHING IS A BUG, NOT A NO-OP. Returning ""
    # sent an empty string to `connection.execute`, and asyncpg does not
    # refuse it: it fails deep inside the protocol with an AttributeError
    # about `decode`, which names nothing an operator could act on. Saying
    # which change was not recognised turns a driver crash into a sentence.
    if not statements:
        raise ValueError(
            f"No PostgreSQL statement for an alter_column on "
            f"{table!r}.{col_name!r}: this renderer handles "
            f"{['type', 'nullable', 'default', 'unique']} and the operation "
            f"asked for {sorted(changes)}."
        )

    return [
        "; ".join(statements)
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
def _add_constraint_sql(table: str, details: dict[str, Any]) -> list[str]:
    """Generate ALTER TABLE ADD CONSTRAINT statement."""
    const_name = details["constraint"]
    const_def = details["definition"]

    if const_def.get("type") == "check":
        expr = const_def.get("expression", "")
        return [
            f'ALTER TABLE "{table}" ADD CONSTRAINT "{const_name}" CHECK ({expr})'
        ]
    elif const_def.get("type") == "unique":
        cols = const_def.get("columns", [])
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        return [
            f'ALTER TABLE "{table}" ADD CONSTRAINT "{const_name}" UNIQUE ({cols_sql})'
        ]
    else:
        raise ValueError(f"Unknown constraint type: {const_def.get('type')}")


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

    if details.get("on_delete"):
        sql += f" ON DELETE {details['on_delete'].upper().replace('_', ' ')}"
    if details.get("on_update"):
        sql += f" ON UPDATE {details['on_update'].upper().replace('_', ' ')}"

    return [
        sql
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

    if view.get("materialized"):
        return [f'CREATE MATERIALIZED VIEW "{name}" AS\n{query}']
    return [f'CREATE OR REPLACE VIEW "{name}" AS\n{query}']


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
