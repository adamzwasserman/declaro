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
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import ApplyResult, Column, Operation, View

# Type for SQL generator functions
SQLGenerator = Callable[..., str]


def get_dialect() -> str:
    """Return dialect identifier."""
    return "postgresql"


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




async def apply(
    connection: Any,
    operations: list[Operation],
    execution_order: list[int],
    *,
    dry_run: bool = False,
) -> ApplyResult:
    """
    Apply migration operations within a transaction.

    PostgreSQL's transactional DDL means we can safely roll back
    all changes if any operation fails.
    """
    sql_statements = [
        generate_operation_sql(operations[i]) for i in execution_order
    ]

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
                    await connection.execute(sql)
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
            original_error=e,
        ) from e


# `generate_sql` was here and nowhere else, so `declaro generate` worked on
# PostgreSQL and raised AttributeError on SQLite and Turso. The loop it ran is
# dialect-independent, so it now lives once in `applier/__init__.py` beside the
# `SQL_GENERATORS` table, and this module keeps only what varies:
# `generate_operation_sql`.




def generate_operation_sql(operation: Operation) -> str:
    """Generate SQL for a single operation."""
    op_type = operation["op"]
    table = operation["table"]
    details = operation["details"]

    generator = OPERATION_SQL.get(op_type)
    if not generator:
        raise ValueError(
            f"Unknown operation type: {op_type}. "
            f"Handled: {', '.join(sorted(OPERATION_SQL))}"
        )

    return generator(table, details)


def _create_table_sql(table: str, details: dict[str, Any]) -> str:
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
    return f'CREATE TABLE "{table}" (\n    {columns_sql}\n)'


def _column_definition(name: str, col: Column) -> str:
    """One line thick, because the rest is shared.

    This was a near-copy of `applier.shared.column_definition` that differed in
    two places: it used the declared type verbatim while the shared one applied
    SQLite affinity, and it did not double-wrap an expression default. Both are
    now looked up by dialect, so PRIMARY KEY, NOT NULL, UNIQUE, CHECK and
    REFERENCES with its cascade are written once for every engine.
    """
    return column_definition(name, col, "postgresql")

def _drop_table_sql(table: str, _details: dict[str, Any]) -> str:
    """Generate DROP TABLE statement."""
    return f'DROP TABLE "{table}"'


def _rename_table_sql(table: str, details: dict[str, Any]) -> str:
    """Generate ALTER TABLE RENAME statement."""
    new_name = details["new_name"]
    return f'ALTER TABLE "{table}" RENAME TO "{new_name}"'


def _add_column_sql(table: str, details: dict[str, Any]) -> str:
    """Generate ALTER TABLE ADD COLUMN statement."""
    col_name = details["column"]
    col_def = details["definition"]
    col_sql = _column_definition(col_name, col_def)
    return f'ALTER TABLE "{table}" ADD COLUMN {col_sql}'


def _drop_column_sql(table: str, details: dict[str, Any]) -> str:
    """Generate ALTER TABLE DROP COLUMN statement."""
    col_name = details["column"]
    return f'ALTER TABLE "{table}" DROP COLUMN "{col_name}"'


def _rename_column_sql(table: str, details: dict[str, Any]) -> str:
    """Generate ALTER TABLE RENAME COLUMN statement."""
    from_col = details["from_column"]
    to_col = details["to_column"]
    return f'ALTER TABLE "{table}" RENAME COLUMN "{from_col}" TO "{to_col}"'


def _alter_column_sql(table: str, details: dict[str, Any]) -> str:
    """Generate ALTER TABLE ALTER COLUMN statement(s)."""
    col_name = details["column"]
    changes = details["changes"]
    statements: list[str] = []

    if "type" in changes:
        new_type = changes["type"]["to"]
        statements.append(
            f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" '
            f'TYPE {new_type} USING "{col_name}"::{new_type}'
        )

    if "nullable" in changes:
        if changes["nullable"]["to"]:
            statements.append(f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" DROP NOT NULL')
        else:
            statements.append(f'ALTER TABLE "{table}" ALTER COLUMN "{col_name}" SET NOT NULL')

    if "default" in changes:
        new_default = changes["default"]["to"]
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
        if changes["unique"]["to"]:
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

    return "; ".join(statements)


def _add_index_sql(table: str, details: dict[str, Any]) -> str:
    """Generate CREATE INDEX statement."""
    idx_name = details["index"]
    idx_def = details["definition"]
    columns = idx_def.get("columns", [])
    unique = "UNIQUE " if idx_def.get("unique") else ""
    using = f" USING {idx_def['using']}" if idx_def.get("using") else ""
    where = f" WHERE {idx_def['where']}" if idx_def.get("where") else ""

    cols_sql = ", ".join(f'"{c}"' for c in columns)
    return f'CREATE {unique}INDEX "{idx_name}" ON "{table}"{using} ({cols_sql}){where}'


def _drop_index_sql(_table: str, details: dict[str, Any]) -> str:
    """Generate DROP INDEX statement."""
    idx_name = details["index"]
    return f'DROP INDEX "{idx_name}"'


def _add_constraint_sql(table: str, details: dict[str, Any]) -> str:
    """Generate ALTER TABLE ADD CONSTRAINT statement."""
    const_name = details["constraint"]
    const_def = details["definition"]

    if const_def.get("type") == "check":
        expr = const_def.get("expression", "")
        return f'ALTER TABLE "{table}" ADD CONSTRAINT "{const_name}" CHECK ({expr})'
    elif const_def.get("type") == "unique":
        cols = const_def.get("columns", [])
        cols_sql = ", ".join(f'"{c}"' for c in cols)
        return f'ALTER TABLE "{table}" ADD CONSTRAINT "{const_name}" UNIQUE ({cols_sql})'
    else:
        raise ValueError(f"Unknown constraint type: {const_def.get('type')}")


def _drop_constraint_sql(table: str, details: dict[str, Any]) -> str:
    """Generate ALTER TABLE DROP CONSTRAINT statement."""
    const_name = details["constraint"]
    return f'ALTER TABLE "{table}" DROP CONSTRAINT "{const_name}"'


def _add_foreign_key_sql(table: str, details: dict[str, Any]) -> str:
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

    return sql


def _drop_foreign_key_sql(table: str, details: dict[str, Any]) -> str:
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
    return (
        f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{convention}"; '
        f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{postgres_default}"'
    )


def _create_view_sql(_table: str, details: dict[str, Any]) -> str:
    """Generate CREATE VIEW or CREATE MATERIALIZED VIEW statement."""
    return generate_create_view(details)  # type: ignore[arg-type]


def _drop_view_sql(_table: str, details: dict[str, Any]) -> str:
    """Generate DROP VIEW or DROP MATERIALIZED VIEW statement."""
    return generate_drop_view(details["name"], details.get("materialized", False))


# =============================================================================
# Extended Schema Objects (Addendum) - Standalone Functions
# =============================================================================




















def generate_create_view(view: View) -> str:
    """
    Generate CREATE VIEW or CREATE MATERIALIZED VIEW SQL.

    Args:
        view: View definition

    Returns:
        SQL statement
    """
    name = view["name"]
    query = view["query"]
    materialized = view.get("materialized", False)

    if materialized:
        return f"CREATE MATERIALIZED VIEW {name} AS\n{query}"
    else:
        return f"CREATE OR REPLACE VIEW {name} AS\n{query}"


def generate_drop_view(name: str, materialized: bool = False) -> str:
    """
    Generate DROP VIEW SQL.

    Args:
        name: View name
        materialized: Whether it's a materialized view

    Returns:
        SQL statement
    """
    if materialized:
        return f"DROP MATERIALIZED VIEW IF EXISTS {name}"
    else:
        return f"DROP VIEW IF EXISTS {name}"


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
