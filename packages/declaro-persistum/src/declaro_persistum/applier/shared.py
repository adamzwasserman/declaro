"""
Shared pure SQL generation for SQLite-compatible backends (SQLite, Turso/libSQL).

All functions are pure: no I/O, no side effects, no state.
Both SQLiteApplier and TursoApplier delegate SQL generation here.
"""

from typing import Any

from declaro_persistum.types import ApplyResult, Column, Operation, View


# =============================================================================
# Type mapping
# =============================================================================


def map_type(type_str: str) -> str:
    """Map generic types to SQLite type affinity."""
    type_lower = type_str.lower()

    if "int" in type_lower:
        return "INTEGER"
    elif type_lower in ("text", "varchar", "char", "string") or type_lower.startswith(
        "varchar("
    ):
        return "TEXT"
    elif type_lower in ("boolean", "bool"):
        return "INTEGER"
    elif type_lower in (
        "uuid",
        "timestamptz",
        "timestamp",
        "date",
        "datetime",
    ) or type_lower in ("jsonb", "json"):
        return "TEXT"
    elif type_lower in ("float", "double", "real", "float4", "float8") or type_lower.startswith(
        "numeric"
    ):
        return "REAL"
    elif type_lower in ("blob", "bytea"):
        return "BLOB"
    else:
        return type_str.upper()


# =============================================================================
# Column definition
# =============================================================================


def column_definition(name: str, col: Column) -> str:
    """Generate column definition for CREATE TABLE."""
    sql_type = map_type(col.get("type", "text"))
    parts = [f'"{name}"', sql_type]

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


# =============================================================================
# DDL generators — all take (table, details) and return SQL string
# =============================================================================


def create_table_sql(table: str, details: dict[str, Any]) -> str:
    """Generate CREATE TABLE statement."""
    columns = details.get("columns", {})
    primary_key = details.get("primary_key", [])

    col_defs: list[str] = []

    for col_name, col_def in columns.items():
        col_sql = column_definition(col_name, col_def)
        col_defs.append(col_sql)

    if primary_key and len(primary_key) > 1:
        pk_cols = ", ".join(f'"{c}"' for c in primary_key)
        col_defs.append(f"PRIMARY KEY ({pk_cols})")

    columns_sql = ",\n    ".join(col_defs)
    return f'CREATE TABLE "{table}" (\n    {columns_sql}\n)'


def drop_table_sql(table: str, details: dict[str, Any]) -> str:
    """Generate DROP TABLE statement."""
    return f'DROP TABLE "{table}"'


def rename_table_sql(table: str, details: dict[str, Any]) -> str:
    """Generate ALTER TABLE RENAME statement."""
    new_name = details["new_name"]
    return f'ALTER TABLE "{table}" RENAME TO "{new_name}"'


def add_column_sql(table: str, details: dict[str, Any]) -> str:
    """Generate ALTER TABLE ADD COLUMN statement."""
    col_name = details["column"]
    col_def = details["definition"]
    col_sql = column_definition(col_name, col_def)
    return f'ALTER TABLE "{table}" ADD COLUMN {col_sql}'


def drop_column_sql(table: str, details: dict[str, Any]) -> str:
    """Generate DROP COLUMN statement (SQLite 3.35.0+)."""
    col_name = details["column"]
    return f'ALTER TABLE "{table}" DROP COLUMN "{col_name}"'


def rename_column_sql(table: str, details: dict[str, Any]) -> str:
    """Generate RENAME COLUMN statement (SQLite 3.25.0+)."""
    from_col = details["from_column"]
    to_col = details["to_column"]
    return f'ALTER TABLE "{table}" RENAME COLUMN "{from_col}" TO "{to_col}"'


def alter_column_sql(table: str, details: dict[str, Any]) -> str:
    """ALTER COLUMN not supported — requires table reconstruction."""
    raise NotImplementedError(
        "ALTER COLUMN not supported in SQLite-compatible databases. "
        "Use table reconstruction instead."
    )


def add_index_sql(table: str, details: dict[str, Any]) -> str:
    """Generate CREATE INDEX statement."""
    idx_name = details["index"]
    idx_def = details["definition"]
    columns = idx_def.get("columns", [])
    unique = "UNIQUE " if idx_def.get("unique") else ""
    where = f" WHERE {idx_def['where']}" if idx_def.get("where") else ""

    cols_sql = ", ".join(f'"{c}"' for c in columns)
    return f'CREATE {unique}INDEX "{idx_name}" ON "{table}" ({cols_sql}){where}'


def drop_index_sql(table: str, details: dict[str, Any]) -> str:
    """Generate DROP INDEX statement."""
    idx_name = details["index"]
    return f'DROP INDEX "{idx_name}"'


def add_constraint_sql(table: str, details: dict[str, Any]) -> str:
    """ADD CONSTRAINT not supported after table creation."""
    const_name = details["constraint"]
    raise NotImplementedError(
        f"ADD CONSTRAINT not supported after table creation in SQLite-compatible databases. "
        f"Constraint '{const_name}' must be defined in CREATE TABLE."
    )


def drop_constraint_sql(table: str, details: dict[str, Any]) -> str:
    """DROP CONSTRAINT not supported — requires table reconstruction."""
    raise NotImplementedError(
        "DROP CONSTRAINT not supported in SQLite-compatible databases. "
        "Requires table reconstruction."
    )


def add_foreign_key_sql(table: str, details: dict[str, Any]) -> str:
    """ADD FOREIGN KEY not supported — requires table reconstruction."""
    raise NotImplementedError(
        "Adding foreign keys after table creation not supported in SQLite-compatible databases. "
        "Use table reconstruction instead."
    )


def drop_foreign_key_sql(table: str, details: dict[str, Any]) -> str:
    """DROP FOREIGN KEY not supported — requires table reconstruction."""
    raise NotImplementedError(
        "Dropping foreign keys not supported in SQLite-compatible databases. "
        "Use table reconstruction instead."
    )


def create_view_sql(table: str, details: dict[str, Any]) -> str:
    """Generate CREATE VIEW statement."""
    from declaro_persistum.applier.sqlite import generate_create_view

    return generate_create_view(details)  # type: ignore[arg-type]


def drop_view_sql(table: str, details: dict[str, Any]) -> str:
    """Generate DROP VIEW statement."""
    from declaro_persistum.applier.sqlite import generate_drop_view

    return generate_drop_view(details["name"])


# =============================================================================
# Operation dispatch
# =============================================================================

# Maps operation type to pure SQL generator function
_SQL_GENERATORS: dict[str, Any] = {
    "create_table": create_table_sql,
    "drop_table": drop_table_sql,
    "rename_table": rename_table_sql,
    "add_column": add_column_sql,
    "drop_column": drop_column_sql,
    "rename_column": rename_column_sql,
    "alter_column": alter_column_sql,
    "add_index": add_index_sql,
    "drop_index": drop_index_sql,
    "add_constraint": add_constraint_sql,
    "drop_constraint": drop_constraint_sql,
    "add_foreign_key": add_foreign_key_sql,
    "drop_foreign_key": drop_foreign_key_sql,
    "create_view": create_view_sql,
    "drop_view": drop_view_sql,
}


def generate_operation_sql(operation: Operation) -> str:
    """Generate SQL for a single operation."""
    op_type = operation["op"]
    table = operation["table"]
    details = operation["details"]

    generator = _SQL_GENERATORS.get(op_type)
    if not generator:
        raise ValueError(f"Unknown operation type: {op_type}")

    return generator(table, details)


def generate_sql(operations: list[Operation], execution_order: list[int]) -> list[str]:
    """Generate SQL statements in execution order."""
    return [generate_operation_sql(operations[i]) for i in execution_order]


# =============================================================================
# Reconstruction helpers
# =============================================================================

_RECONSTRUCTION_OPS = frozenset(("add_foreign_key", "drop_foreign_key", "alter_column"))


def requires_reconstruction(operation: Operation) -> bool:
    """Check if operation requires table reconstruction."""
    return operation["op"] in _RECONSTRUCTION_OPS


def dry_run_preview(
    operations: list[Operation], execution_order: list[int]
) -> ApplyResult:
    """Generate dry-run preview of operations."""
    sql_statements = []
    for op_idx in execution_order:
        operation = operations[op_idx]
        if requires_reconstruction(operation):
            sql_statements.append(
                f"-- Table reconstruction required for {operation['table']}"
            )
        else:
            sql_statements.append(generate_operation_sql(operation))

    return {
        "success": True,
        "executed_sql": sql_statements,
        "operations_applied": len(sql_statements),
        "error": None,
        "error_operation": None,
    }


def columns_from_pragma_rows(rows: list[tuple]) -> dict[str, Column]:
    """Convert PRAGMA table_info rows to Column dict (pure)."""
    columns: dict[str, Column] = {}
    for row in rows:
        cid, name, type_str, notnull, dflt_value, pk = row

        col_def: Column = {
            "type": type_str or "TEXT",
            "nullable": not bool(notnull),
        }

        if pk:
            col_def["primary_key"] = True

        if dflt_value is not None:
            col_def["default"] = dflt_value

        columns[name] = col_def

    return columns


def apply_reconstruction_changes(
    columns: dict[str, Column], operation: Operation
) -> dict[str, Column]:
    """
    Apply reconstruction operation to columns dict (pure).

    Returns updated columns dict. Raises ValueError for unknown operations.
    """
    op_type = operation["op"]
    details = operation["details"]

    if op_type == "alter_column":
        column = details["column"]
        changes = details["changes"]

        if column not in columns:
            raise ValueError(f"Column '{column}' not found in table '{operation['table']}'")

        for key, value in changes.items():
            if isinstance(value, dict) and "to" in value:
                value = value["to"]
            columns[column][key] = value  # type: ignore

    elif op_type == "add_foreign_key":
        column = details["column"]
        if column not in columns:
            raise ValueError(f"Column '{column}' not found in table '{operation['table']}'")

        ref_table = details["references"]["table"]
        ref_column = details["references"]["column"]
        columns[column]["references"] = f"{ref_table}.{ref_column}"

        if "on_delete" in details:
            columns[column]["on_delete"] = details["on_delete"]
        if "on_update" in details:
            columns[column]["on_update"] = details["on_update"]

    elif op_type == "drop_foreign_key":
        column = details["column"]
        if column not in columns:
            raise ValueError(f"Column '{column}' not found in table '{operation['table']}'")

        columns[column].pop("references", None)
        columns[column].pop("on_delete", None)
        columns[column].pop("on_update", None)

    else:
        raise ValueError(
            f"Operation '{op_type}' does not support reconstruction"
        )

    return columns


def enum_population_sql(
    operation: Operation, target_schema: Any
) -> list[str]:
    """Generate INSERT SQL for enum lookup table population (pure)."""
    if operation["op"] != "create_table" or not target_schema:
        return []

    table_name = operation["table"]
    if not table_name.startswith("_dp_enum_"):
        return []

    table_def = target_schema.get(table_name, {})
    enum_values = table_def.get("_enum_values", [])

    sqls = []
    for value in enum_values:
        escaped_value = value.replace("'", "''")
        sqls.append(f'INSERT INTO "{table_name}" (value) VALUES (\'{escaped_value}\')')

    return sqls
