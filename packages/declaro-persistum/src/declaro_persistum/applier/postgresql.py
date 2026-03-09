"""
PostgreSQL migration applier implementation.

PostgreSQL supports transactional DDL, so all operations are wrapped
in a single transaction for atomic all-or-nothing behavior.
"""

from collections.abc import Callable
from typing import Any, Literal

from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import ApplyResult, Column, Enum, Operation, Procedure, Trigger, View

# Type for SQL generator functions
SQLGenerator = Callable[..., str]


class PostgreSQLApplier:
    """PostgreSQL implementation of MigrationApplier protocol."""

    def get_dialect(self) -> str:
        """Return dialect identifier."""
        return "postgresql"

    def get_transaction_mode(self) -> Literal["all_or_nothing", "per_operation"]:
        """PostgreSQL supports transactional DDL."""
        return "all_or_nothing"

    async def apply(
        self,
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
        sql_statements = self.generate_sql(operations, execution_order)

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
        self,
        operations: list[Operation],
        execution_order: list[int],
    ) -> list[str]:
        """Generate SQL statements in execution order."""
        return [self.generate_operation_sql(operations[i]) for i in execution_order]

    def generate_operation_sql(self, operation: Operation) -> str:
        """Generate SQL for a single operation."""
        op_type = operation["op"]
        table = operation["table"]
        details = operation["details"]

        generators: dict[str, SQLGenerator] = {
            "create_table": self._create_table_sql,
            "drop_table": self._drop_table_sql,
            "rename_table": self._rename_table_sql,
            "add_column": self._add_column_sql,
            "drop_column": self._drop_column_sql,
            "rename_column": self._rename_column_sql,
            "alter_column": self._alter_column_sql,
            "add_index": self._add_index_sql,
            "drop_index": self._drop_index_sql,
            "add_constraint": self._add_constraint_sql,
            "drop_constraint": self._drop_constraint_sql,
            "add_foreign_key": self._add_foreign_key_sql,
            "drop_foreign_key": self._drop_foreign_key_sql,
            "create_view": self._create_view_sql,
            "drop_view": self._drop_view_sql,
        }

        generator = generators.get(op_type)
        if not generator:
            raise ValueError(f"Unknown operation type: {op_type}")

        return generator(table, details)

    def _create_table_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate CREATE TABLE statement."""
        columns = details.get("columns", {})
        primary_key = details.get("primary_key", [])

        col_defs: list[str] = []

        for col_name, col_def in columns.items():
            col_sql = self._column_definition(col_name, col_def)
            col_defs.append(col_sql)

        # Add composite primary key if specified
        if primary_key and len(primary_key) > 1:
            pk_cols = ", ".join(f'"{c}"' for c in primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        columns_sql = ",\n    ".join(col_defs)
        return f'CREATE TABLE "{table}" (\n    {columns_sql}\n)'

    def _column_definition(self, name: str, col: Column) -> str:
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

    def _drop_table_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate DROP TABLE statement."""
        return f'DROP TABLE "{table}"'

    def _rename_table_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate ALTER TABLE RENAME statement."""
        new_name = details["new_name"]
        return f'ALTER TABLE "{table}" RENAME TO "{new_name}"'

    def _add_column_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate ALTER TABLE ADD COLUMN statement."""
        col_name = details["column"]
        col_def = details["definition"]
        col_sql = self._column_definition(col_name, col_def)
        return f'ALTER TABLE "{table}" ADD COLUMN {col_sql}'

    def _drop_column_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate ALTER TABLE DROP COLUMN statement."""
        col_name = details["column"]
        return f'ALTER TABLE "{table}" DROP COLUMN "{col_name}"'

    def _rename_column_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate ALTER TABLE RENAME COLUMN statement."""
        from_col = details["from_column"]
        to_col = details["to_column"]
        return f'ALTER TABLE "{table}" RENAME COLUMN "{from_col}" TO "{to_col}"'

    def _alter_column_sql(self, table: str, details: dict[str, Any]) -> str:
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

    def _add_index_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate CREATE INDEX statement."""
        idx_name = details["index"]
        idx_def = details["definition"]
        columns = idx_def.get("columns", [])
        unique = "UNIQUE " if idx_def.get("unique") else ""
        using = f" USING {idx_def['using']}" if idx_def.get("using") else ""
        where = f" WHERE {idx_def['where']}" if idx_def.get("where") else ""

        cols_sql = ", ".join(f'"{c}"' for c in columns)
        return f'CREATE {unique}INDEX "{idx_name}" ON "{table}"{using} ({cols_sql}){where}'

    def _drop_index_sql(self, details: dict[str, Any]) -> str:
        """Generate DROP INDEX statement."""
        idx_name = details["index"]
        return f'DROP INDEX "{idx_name}"'

    def _add_constraint_sql(self, table: str, details: dict[str, Any]) -> str:
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

    def _drop_constraint_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate ALTER TABLE DROP CONSTRAINT statement."""
        const_name = details["constraint"]
        return f'ALTER TABLE "{table}" DROP CONSTRAINT "{const_name}"'

    def _add_foreign_key_sql(self, table: str, details: dict[str, Any]) -> str:
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

    def _drop_foreign_key_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate ALTER TABLE DROP FOREIGN KEY statement."""
        col_name = details["column"]
        ref = details["references"]
        ref_table = ref.split(".")[0]
        fk_name = f"fk_{table}_{col_name}_{ref_table}"
        return f'ALTER TABLE "{table}" DROP CONSTRAINT "{fk_name}"'

    def _create_view_sql(self, _table: str, details: dict[str, Any]) -> str:
        """Generate CREATE VIEW or CREATE MATERIALIZED VIEW statement."""
        return generate_create_view(details)  # type: ignore[arg-type]

    def _drop_view_sql(self, _table: str, details: dict[str, Any]) -> str:
        """Generate DROP VIEW or DROP MATERIALIZED VIEW statement."""
        return generate_drop_view(details["name"], details.get("materialized", False))


# =============================================================================
# Extended Schema Objects (Addendum) - Standalone Functions
# =============================================================================


def generate_create_enum(enum: Enum) -> str:
    """
    Generate CREATE TYPE ... AS ENUM SQL.

    Args:
        enum: Enum definition

    Returns:
        SQL statement
    """
    name = enum["name"]
    values = ", ".join(f"'{v}'" for v in enum["values"])
    return f"CREATE TYPE {name} AS ENUM ({values})"


def generate_drop_enum(name: str) -> str:
    """
    Generate DROP TYPE SQL.

    Args:
        name: Enum type name

    Returns:
        SQL statement
    """
    return f"DROP TYPE IF EXISTS {name}"


def generate_alter_enum_add_value(name: str, value: str) -> str:
    """
    Generate ALTER TYPE ... ADD VALUE SQL.

    Args:
        name: Enum type name
        value: Value to add

    Returns:
        SQL statement
    """
    return f"ALTER TYPE {name} ADD VALUE '{value}'"


def generate_column_sql(
    col_name: str, col_def: dict[str, Any], enums: set[str] | None = None
) -> str:
    """
    Generate column definition, handling enum types.

    Args:
        col_name: Column name
        col_def: Column definition
        enums: Set of known enum type names

    Returns:
        Column SQL definition
    """
    enums = enums or set()
    col_type = col_def.get("type", "text")

    # Handle enum type reference
    if col_type.startswith("enum:"):
        enum_name = col_type[5:]  # Remove "enum:" prefix
        col_type = enum_name if enum_name in enums else "text"  # Fallback

    parts = [f'"{col_name}"', col_type]

    if col_def.get("nullable") is False:
        parts.append("NOT NULL")

    if "default" in col_def:
        parts.append(f"DEFAULT {col_def['default']}")

    return " ".join(parts)


def generate_trigger_function(table: str, trigger: Trigger) -> str:
    """
    Generate trigger function SQL.

    Args:
        table: Table name
        trigger: Trigger definition

    Returns:
        SQL statement
    """
    name = trigger["name"]
    body = trigger.get("body", "RETURN NEW;")
    func_name = f"{table}_{name}"

    return f"""CREATE OR REPLACE FUNCTION {func_name}()
RETURNS TRIGGER AS $$
BEGIN
    {body}
END;
$$ LANGUAGE plpgsql"""


def generate_create_trigger(table: str, trigger: Trigger) -> str:
    """
    Generate CREATE TRIGGER SQL.

    Args:
        table: Table name
        trigger: Trigger definition

    Returns:
        SQL statement
    """
    name = trigger["name"]
    timing = trigger.get("timing", "before").upper()
    event = trigger.get("event", "insert")
    for_each = trigger.get("for_each", "row").upper()
    func_name = f"{table}_{name}"

    # Handle multiple events
    event_sql = " OR ".join(e.upper() for e in event) if isinstance(event, list) else event.upper()

    sql = f"CREATE TRIGGER {name}\n{timing} {event_sql}\nON {table}\nFOR EACH {for_each}"

    # Add WHEN clause if present
    if trigger.get("when"):
        sql += f"\nWHEN ({trigger['when']})"

    # Execute function
    if trigger.get("execute"):
        sql += f"\nEXECUTE FUNCTION {trigger['execute']}()"
    else:
        sql += f"\nEXECUTE FUNCTION {func_name}()"

    return sql


def generate_drop_trigger(table: str, trigger_name: str) -> str:
    """
    Generate DROP TRIGGER SQL.

    Args:
        table: Table name
        trigger_name: Trigger name

    Returns:
        SQL statement
    """
    return f"DROP TRIGGER IF EXISTS {trigger_name} ON {table}"


def generate_create_function(procedure: Procedure) -> str:
    """
    Generate CREATE FUNCTION SQL.

    Args:
        procedure: Procedure definition

    Returns:
        SQL statement
    """
    name = procedure["name"]
    language = procedure.get("language", "sql")
    returns = procedure.get("returns", "void")
    body = procedure.get("body", "")
    params = procedure.get("parameters", [])

    # Build parameter list
    param_parts = []
    for p in params:
        param_sql = f"{p['name']} {p['type']}"
        if p.get("default"):
            param_sql += f" DEFAULT {p['default']}"
        param_parts.append(param_sql)
    params_sql = ", ".join(param_parts)

    return f"""CREATE OR REPLACE FUNCTION {name}({params_sql})
RETURNS {returns}
LANGUAGE {language}
AS $$
{body}
$$"""


def generate_drop_function(procedure: Procedure) -> str:
    """
    Generate DROP FUNCTION SQL.

    Args:
        procedure: Procedure definition

    Returns:
        SQL statement
    """
    name = procedure["name"]
    params = procedure.get("parameters", [])
    param_types = ", ".join(p["type"] for p in params)
    return f"DROP FUNCTION IF EXISTS {name}({param_types})"


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


def generate_refresh_materialized_view(name: str, concurrently: bool = False) -> str:
    """
    Generate REFRESH MATERIALIZED VIEW SQL.

    Args:
        name: View name
        concurrently: Whether to refresh concurrently

    Returns:
        SQL statement
    """
    if concurrently:
        return f"REFRESH MATERIALIZED VIEW CONCURRENTLY {name}"
    else:
        return f"REFRESH MATERIALIZED VIEW {name}"


async def validate_concurrent_refresh(
    connection: Any,
    view_name: str,
    *,
    schema_name: str = "public",
) -> None:
    """
    Validate that a materialized view can be refreshed concurrently.

    PostgreSQL requires a unique index on the materialized view for
    REFRESH MATERIALIZED VIEW CONCURRENTLY to work. This function
    checks that requirement and provides a helpful error message.

    Args:
        connection: asyncpg connection object
        view_name: Name of the materialized view
        schema_name: PostgreSQL schema (default: "public")

    Raises:
        ValidationError: If view lacks required unique index
    """
    from declaro_persistum.exceptions import ValidationError
    from declaro_persistum.inspector.postgresql import PostgreSQLInspector

    inspector = PostgreSQLInspector()
    has_unique = await inspector.has_unique_index(connection, view_name, schema_name=schema_name)

    if not has_unique:
        raise ValidationError(
            f"Cannot refresh '{view_name}' concurrently: "
            f"materialized view requires a unique index.\n\n"
            f"  Create one with:\n"
            f"    CREATE UNIQUE INDEX ON {view_name} (column_name)",
            table=view_name,
        )
