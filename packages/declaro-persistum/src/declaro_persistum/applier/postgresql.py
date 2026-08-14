"""Apply migration operations to PostgreSQL. Functions, not a class.

De-classed mechanically: methods lifted out, dedented, `self` removed. Every
statement PostgreSQL is asked to run is byte-for-byte what it was.

PostgreSQL supports transactional DDL, so every operation is wrapped in a
single transaction and the batch is atomic. That is the one thing this applier
can promise which the SQLite and Turso ones cannot.
"""

from collections.abc import Callable
from typing import Any

from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import ApplyResult, Column, Operation, View

# Type for SQL generator functions
SQLGenerator = Callable[..., str]


def get_dialect() -> str:
    """Return dialect identifier."""
    return "postgresql"




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
    sql_statements = generate_sql(operations, execution_order)

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


def generate_sql(
    operations: list[Operation],
    execution_order: list[int],
) -> list[str]:
    """Generate SQL statements in execution order."""
    return [generate_operation_sql(operations[i]) for i in execution_order]


def generate_operation_sql(operation: Operation) -> str:
    """Generate SQL for a single operation."""
    op_type = operation["op"]
    table = operation["table"]
    details = operation["details"]

    generators: dict[str, SQLGenerator] = {
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
    }

    generator = generators.get(op_type)
    if not generator:
        raise ValueError(f"Unknown operation type: {op_type}")

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

    columns_sql = ",\n    ".join(col_defs)
    return f'CREATE TABLE "{table}" (\n    {columns_sql}\n)'


def _column_definition(name: str, col: Column) -> str:
    """Generate column definition for CREATE TABLE."""
    parts = [f'"{name}"', col.get("type", "text")]

    if col.get("primary_key"):
        parts.append("PRIMARY KEY")

    if col.get("nullable") is False:
        parts.append("NOT NULL")

    if col.get("unique"):
        parts.append("UNIQUE")

    if "default" in col:
        parts.append(f"DEFAULT {col['default']}")

    if "check" in col:
        parts.append(f"CHECK ({col['check']})")

    if "references" in col:
        ref = col["references"]
        ref_table, ref_col = ref.split(".")
        fk_sql = f'REFERENCES "{ref_table}"("{ref_col}")'

        if col.get("on_delete"):
            fk_sql += f" ON DELETE {col['on_delete'].upper().replace('_', ' ')}"
        if col.get("on_update"):
            fk_sql += f" ON UPDATE {col['on_update'].upper().replace('_', ' ')}"

        parts.append(fk_sql)

    return " ".join(parts)


def _drop_table_sql(table: str, details: dict[str, Any]) -> str:
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
    """Generate ALTER TABLE DROP FOREIGN KEY statement."""
    col_name = details["column"]
    ref = details["references"]
    ref_table = ref.split(".")[0]
    fk_name = f"fk_{table}_{col_name}_{ref_table}"
    return f'ALTER TABLE "{table}" DROP CONSTRAINT "{fk_name}"'


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




