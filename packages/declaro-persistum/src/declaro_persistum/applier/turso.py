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
        Apply migration operations asynchronously (protocol compliance).

        Delegates to apply_sync() since Turso Database (pyturso) is synchronous.
        """
        return self.apply_sync(connection, operations, execution_order, dry_run=dry_run)

    def apply_sync(
        self,
        connection: Any,
        operations: list[Operation],
        execution_order: list[int],
        *,
        dry_run: bool = False,
        target_schema: Any = None,
    ) -> ApplyResult:
        """
        Apply migration operations synchronously.

        Turso Database (pyturso) has a synchronous API similar to sqlite3.
        Uses per-operation execution with reconstruction for unsupported operations.

        Args:
            connection: pyturso database connection
            operations: List of operations to apply
            execution_order: Order to execute operations
            dry_run: If True, only generate SQL without executing
            target_schema: Target schema (used for enum value population)

        Returns:
            ApplyResult with success status and executed SQL
        """
        if dry_run:
            # Generate SQL for preview (with reconstruction steps as comments)
            sql_statements = []
            for op_idx in execution_order:
                operation = operations[op_idx]
                if self._requires_reconstruction(operation):
                    sql_statements.append(
                        f"-- Table reconstruction required for {operation['table']}"
                    )
                else:
                    sql_statements.append(self.generate_operation_sql(operation))

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

            # Per-operation execution
            for op_idx in execution_order:
                operation = operations[op_idx]

                try:
                    if self._requires_reconstruction(operation):
                        # Execute with reconstruction
                        self._execute_with_reconstruction_sync(connection, operation)
                        executed.append(f"Table reconstruction for {operation['table']}")
                    else:
                        # Direct SQL execution
                        sql = self.generate_operation_sql(operation)
                        for statement in sql.split(";"):
                            statement = statement.strip()
                            if statement:
                                connection.execute(statement)
                        executed.append(sql)

                    # Handle enum value population for newly created lookup tables
                    if operation["op"] == "create_table" and target_schema:
                        table_name = operation["table"]
                        if table_name.startswith("_dp_enum_"):
                            table_def = target_schema.get(table_name, {})
                            enum_values = table_def.get("_enum_values", [])
                            if enum_values:
                                for value in enum_values:
                                    escaped_value = value.replace("'", "''")
                                    insert_sql = f'INSERT INTO "{table_name}" (value) VALUES (\'{escaped_value}\')'
                                    connection.execute(insert_sql)
                                    executed.append(insert_sql)

                except Exception as e:
                    connection.rollback()
                    raise MigrationError(
                        f"Failed to execute operation",
                        operation=operation,
                        original_error=e,
                    ) from e

            connection.commit()

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
            connection.rollback()
            raise MigrationError(
                f"Migration failed: {e}",
                original_error=e,
            ) from e

    def _requires_reconstruction(self, operation: Operation) -> bool:
        """
        Check if operation requires table reconstruction.

        Turso Database (like SQLite) doesn't support these operations directly:
        - Adding/dropping foreign keys
        - Altering column properties
        - Dropping columns with constraints (in some versions)

        Args:
            operation: The operation to check

        Returns:
            True if reconstruction is needed, False otherwise
        """
        op_type = operation["op"]
        return op_type in ("add_foreign_key", "drop_foreign_key", "alter_column")

    def _execute_with_reconstruction_sync(
        self, connection: Any, operation: Operation
    ) -> None:
        """
        Execute an operation using table reconstruction (synchronous).

        Fresh introspection is performed before each reconstruction to ensure
        we have the latest schema state.

        Args:
            connection: Database connection (pyturso client)
            operation: The operation to execute

        Raises:
            ValueError: If operation type is not supported for reconstruction
        """
        from declaro_persistum.abstractions.reconstruction import execute_reconstruction_sync

        op_type = operation["op"]
        table = operation["table"]
        details = operation["details"]

        # Fresh introspection for current state (direct PRAGMA call - sync)
        cursor = connection.execute(f"PRAGMA table_info('{table}')")
        rows = cursor.fetchall()
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

        # Determine which columns to keep after reconstruction
        if op_type == "alter_column":
            column = details["column"]
            changes = details["changes"]

            if column not in columns:
                raise ValueError(f"Column '{column}' not found in table '{table}'")

            # Apply changes to column definition
            # The differ produces {"from": old, "to": new} dicts for changes
            for key, value in changes.items():
                if isinstance(value, dict) and "to" in value:
                    value = value["to"]
                columns[column][key] = value  # type: ignore

        elif op_type == "add_foreign_key":
            # Add FK to column definition
            column = details["column"]
            if column not in columns:
                raise ValueError(f"Column '{column}' not found in table '{table}'")

            ref_table = details["references"]["table"]
            ref_column = details["references"]["column"]
            columns[column]["references"] = f"{ref_table}.{ref_column}"

            if "on_delete" in details:
                columns[column]["on_delete"] = details["on_delete"]
            if "on_update" in details:
                columns[column]["on_update"] = details["on_update"]

        elif op_type == "drop_foreign_key":
            # Remove FK from column definition
            column = details["column"]
            if column not in columns:
                raise ValueError(f"Column '{column}' not found in table '{table}'")

            columns[column].pop("references", None)
            columns[column].pop("on_delete", None)
            columns[column].pop("on_update", None)

        else:
            raise ValueError(
                f"Operation '{op_type}' does not support reconstruction"
            )

        # Execute reconstruction with updated schema
        execute_reconstruction_sync(connection, table, columns)

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

        # CHECK constraint - Turso now supports CHECK natively
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
        """Generate DROP COLUMN statement."""
        col_name = details["column"]
        return f'ALTER TABLE "{table}" DROP COLUMN "{col_name}"'

    def _rename_column_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate RENAME COLUMN statement."""
        from_col = details["from_column"]
        to_col = details["to_column"]
        return f'ALTER TABLE "{table}" RENAME COLUMN "{from_col}" TO "{to_col}"'

    def _alter_column_sql(self, table: str, details: dict[str, Any]) -> str:
        """
        Generate column alteration SQL.

        Turso Database doesn't support ALTER COLUMN natively.
        This should not be called directly - use _execute_with_reconstruction_sync() instead.
        """
        raise NotImplementedError(
            f"Turso Database doesn't support ALTER COLUMN natively. "
            f"Use _execute_with_reconstruction_sync() instead."
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

    def _drop_index_sql(self, table: str, details: dict[str, Any]) -> str:
        """Generate DROP INDEX statement."""
        idx_name = details["index"]
        return f'DROP INDEX "{idx_name}"'

    def _add_constraint_sql(self, table: str, details: dict[str, Any]) -> str:
        """SQLite/libSQL doesn't support ADD CONSTRAINT."""
        const_name = details["constraint"]
        raise NotImplementedError(
            f"Turso Database doesn't support ADD CONSTRAINT after table creation. "
            f"Constraint '{const_name}' must be defined in CREATE TABLE."
        )

    def _drop_constraint_sql(self, table: str, details: dict[str, Any]) -> str:
        """
        Generate DROP CONSTRAINT SQL.

        Turso Database doesn't support DROP CONSTRAINT.
        This should not be called directly - use _execute_with_reconstruction_sync() instead.
        """
        raise NotImplementedError(
            f"Turso Database doesn't support DROP CONSTRAINT. "
            f"Use _execute_with_reconstruction_sync() instead."
        )

    def _add_foreign_key_sql(self, table: str, details: dict[str, Any]) -> str:
        """
        Generate ADD FOREIGN KEY SQL.

        Turso Database doesn't support ADD CONSTRAINT FOREIGN KEY.
        This should not be called directly - use _execute_with_reconstruction_sync() instead.
        """
        raise NotImplementedError(
            f"Turso Database doesn't support adding foreign keys after table creation. "
            f"Use _execute_with_reconstruction_sync() instead."
        )

    def _drop_foreign_key_sql(self, table: str, details: dict[str, Any]) -> str:
        """
        Generate DROP FOREIGN KEY SQL.

        Turso Database doesn't support dropping foreign keys.
        This should not be called directly - use _execute_with_reconstruction_sync() instead.
        """
        raise NotImplementedError(
            f"Turso Database doesn't support dropping foreign keys. "
            f"Use _execute_with_reconstruction_sync() instead."
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
