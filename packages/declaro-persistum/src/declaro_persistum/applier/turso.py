"""
Turso (libSQL) migration applier implementation.

Turso uses libSQL which is SQLite-compatible, so the SQL generation
is nearly identical to SQLite with HTTP/WebSocket transport.
"""

from collections.abc import Callable
from typing import Any, Literal

from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import ApplyResult, Column, Operation, View

# Type for SQL generator functions
SQLGenerator = Callable[..., str]


class TursoApplier:
    """
    Turso implementation of MigrationApplier protocol.

    Turso/libSQL is SQLite-compatible, sharing most SQL generation
    with the SQLite applier. The main difference is connection handling.
    """

    def get_dialect(self) -> str:
        """Return dialect identifier."""
        return "turso"

    def get_transaction_mode(self) -> Literal["all_or_nothing", "per_operation"]:
        """Turso (libSQL) supports transactional DDL."""
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
        Apply migration operations.

        Uses libsql-experimental's synchronous sqlite3-like API.
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
            # Begin transaction
            connection.execute("BEGIN")

            try:
                for i, op_idx in enumerate(execution_order):
                    sql = sql_statements[i]
                    try:
                        # Handle multi-statement SQL
                        for statement in sql.split(";"):
                            statement = statement.strip()
                            if statement:
                                connection.execute(statement)
                        executed.append(sql)
                    except Exception as e:
                        connection.rollback()
                        raise MigrationError(
                            f"Failed to execute operation {i + 1}/{len(execution_order)}",
                            operation=operations[op_idx],
                            sql=sql,
                            original_error=e,
                        ) from e

                connection.commit()

            except MigrationError:
                raise
            except Exception:
                connection.rollback()
                raise

            return {
                "success": True,
                "executed_sql": executed,
                "operations_applied": len(executed),
                "error": None,
                "error_operation": None,
            }

        except MigrationError:
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

        if primary_key and len(primary_key) > 1:
            pk_cols = ", ".join(f'"{c}"' for c in primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        columns_sql = ",\n    ".join(col_defs)
        return f'CREATE TABLE "{table}" (\n    {columns_sql}\n)'

    def _column_definition(self, name: str, col: Column) -> str:
        """Generate column definition for CREATE TABLE."""
        sql_type = self._map_type(col.get("type", "text"))
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

    def _map_type(self, type_str: str) -> str:
        """Map generic types to SQLite/libSQL types."""
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

    def _drop_table_sql(self, table: str) -> str:
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
        """Generate DROP COLUMN statement."""
        col_name = details["column"]
        return f'ALTER TABLE "{table}" DROP COLUMN "{col_name}"'

    def _rename_column_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate RENAME COLUMN statement."""
        from_col = details["from_column"]
        to_col = details["to_column"]
        return f'ALTER TABLE "{table}" RENAME COLUMN "{from_col}" TO "{to_col}"'

    def _alter_column_sql(self, details: dict[str, Any]) -> str:
        """SQLite/libSQL doesn't support ALTER COLUMN."""
        col_name = details["column"]
        changes = details["changes"]
        raise NotImplementedError(
            f"libSQL ALTER COLUMN not directly supported. "
            f"Column '{col_name}' changes require table reconstruction: {changes}"
        )

    def _add_index_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate CREATE INDEX statement."""
        idx_name = details["index"]
        idx_def = details["definition"]
        columns = idx_def.get("columns", [])
        unique = "UNIQUE " if idx_def.get("unique") else ""
        where = f" WHERE {idx_def['where']}" if idx_def.get("where") else ""

        cols_sql = ", ".join(f'"{c}"' for c in columns)
        return f'CREATE {unique}INDEX "{idx_name}" ON "{table}" ({cols_sql}){where}'

    def _drop_index_sql(self, details: dict[str, Any]) -> str:
        """Generate DROP INDEX statement."""
        idx_name = details["index"]
        return f'DROP INDEX "{idx_name}"'

    def _add_constraint_sql(self, details: dict[str, Any]) -> str:
        """SQLite/libSQL doesn't support ADD CONSTRAINT."""
        const_name = details["constraint"]
        raise NotImplementedError(
            f"libSQL doesn't support ADD CONSTRAINT after table creation. "
            f"Constraint '{const_name}' must be defined in CREATE TABLE."
        )

    def _drop_constraint_sql(self, details: dict[str, Any]) -> str:
        """SQLite/libSQL doesn't support DROP CONSTRAINT."""
        const_name = details["constraint"]
        raise NotImplementedError(
            f"libSQL doesn't support DROP CONSTRAINT. "
            f"Removing constraint '{const_name}' requires table reconstruction."
        )

    def _add_foreign_key_sql(self, details: dict[str, Any]) -> str:
        """SQLite/libSQL doesn't support ADD FOREIGN KEY."""
        col_name = details["column"]
        raise NotImplementedError(
            f"libSQL doesn't support adding foreign keys after table creation. "
            f"Foreign key on '{col_name}' requires table reconstruction."
        )

    def _drop_foreign_key_sql(self, details: dict[str, Any]) -> str:
        """SQLite/libSQL doesn't support DROP FOREIGN KEY."""
        col_name = details["column"]
        raise NotImplementedError(
            f"libSQL doesn't support dropping foreign keys. "
            f"Removing foreign key on '{col_name}' requires table reconstruction."
        )

    def _create_view_sql(self, _table: str, details: dict[str, Any]) -> str:
        """Generate CREATE VIEW statement."""
        return generate_create_view(details)  # type: ignore[arg-type]

    def _drop_view_sql(self, _table: str, details: dict[str, Any]) -> str:
        """Generate DROP VIEW statement."""
        return generate_drop_view(details["name"])


def generate_create_view(view: View) -> str:
    """
    Generate CREATE VIEW SQL for Turso/libSQL.

    For materialized views, uses table-based emulation with metadata tracking.
    libSQL is SQLite-compatible, so we use the same emulation strategy.

    Args:
        view: View definition

    Returns:
        SQL statement (or semicolon-separated statements for materialized views)
    """
    from declaro_persistum.abstractions.materialized_views import (
        create_matview_sql,
        generate_refresh_trigger_sql,
    )

    name = view["name"]
    query = view["query"]
    materialized = view.get("materialized", False)

    if not materialized:
        return f'CREATE VIEW IF NOT EXISTS "{name}" AS\n{query}'

    # Use table-based emulation for materialized views
    refresh = view.get("refresh", "manual")
    # Map PostgreSQL strategies to SQLite strategies
    if refresh in ("on_demand", "on_commit"):
        refresh = "manual"

    depends_on = view.get("depends_on")
    trigger_sources = view.get("trigger_sources")

    statements = create_matview_sql(
        name=name,
        query=query,
        refresh_strategy=refresh,  # type: ignore[arg-type]
        depends_on=depends_on,
    )

    # Add triggers if trigger-based refresh
    if refresh in ("trigger", "hybrid") and trigger_sources:
        for source in trigger_sources:
            statements.extend(
                generate_refresh_trigger_sql(
                    matview_name=name,
                    source_table=source,
                    query=query,
                )
            )

    return ";\n".join(statements)


def generate_drop_view(name: str) -> str:
    """Generate DROP VIEW SQL for Turso/libSQL."""
    return f'DROP VIEW IF EXISTS "{name}"'
