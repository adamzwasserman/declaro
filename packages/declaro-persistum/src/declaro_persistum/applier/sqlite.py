"""
SQLite migration applier implementation.

SQLite has limited ALTER TABLE support, so some operations require
table reconstruction (create new, copy data, drop old, rename new).
"""

from collections.abc import Callable
from typing import Any, Literal

from declaro_persistum.errors import NotSupportedError
from declaro_persistum.exceptions import MigrationError
from declaro_persistum.types import ApplyResult, Column, Enum, Operation, Procedure, Trigger, View

# Type for SQL generator functions
SQLGenerator = Callable[..., str]


class SQLiteApplier:
    """SQLite implementation of MigrationApplier protocol."""

    def get_dialect(self) -> str:
        """Return dialect identifier."""
        return "sqlite"

    def get_transaction_mode(self) -> Literal["all_or_nothing", "per_operation"]:
        """SQLite supports transactional DDL."""
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

        SQLite DDL is transactional, allowing safe rollback on failure.
        Uses per-operation execution with reconstruction for unsupported operations.
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
            # Enable foreign keys and start transaction
            await connection.execute("PRAGMA foreign_keys = ON")

            # Per-operation execution
            for op_idx in execution_order:
                operation = operations[op_idx]

                try:
                    if self._requires_reconstruction(operation):
                        # Execute with reconstruction
                        await self._execute_with_reconstruction(connection, operation)
                        executed.append(f"Table reconstruction for {operation['table']}")
                    else:
                        # Direct SQL execution
                        sql = self.generate_operation_sql(operation)
                        for statement in sql.split(";"):
                            statement = statement.strip()
                            if statement:
                                await connection.execute(statement)
                        executed.append(sql)

                except Exception as e:
                    await connection.rollback()
                    raise MigrationError(
                        f"Failed to execute operation",
                        operation=operation,
                        original_error=e,
                    ) from e

            await connection.commit()

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
            await connection.rollback()
            raise MigrationError(
                f"Migration failed: {e}",
                original_error=e,
            ) from e

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
        Apply migration operations synchronously using stdlib sqlite3.

        SQLite DDL is transactional, allowing safe rollback on failure.
        Uses per-operation execution with reconstruction for unsupported operations.

        Args:
            connection: sqlite3.Connection
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
            # Enable foreign keys
            connection.execute("PRAGMA foreign_keys = ON")

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

        SQLite doesn't support these operations directly:
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

    async def _execute_with_reconstruction(
        self, connection: Any, operation: Operation
    ) -> None:
        """
        Execute an operation using table reconstruction.

        Fresh introspection is performed before each reconstruction to ensure
        we have the latest schema state.

        Args:
            connection: Database connection
            operation: The operation to execute

        Raises:
            ValueError: If operation type is not supported for reconstruction
        """
        from declaro_persistum.abstractions.pragma_compat import pragma_table_info
        from declaro_persistum.abstractions.table_reconstruction import (
            alter_column_default,
            alter_column_nullability,
            alter_column_type,
            reconstruct_table,
        )

        op_type = operation["op"]
        table = operation["table"]
        details = operation["details"]

        # Fresh introspection for current state
        rows = await pragma_table_info(connection, table)
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

        # Handle operation-specific reconstruction
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

            # Use specialized functions for single-property changes
            if len(changes) == 1:
                if "nullable" in changes:
                    val = changes["nullable"]
                    if isinstance(val, dict) and "to" in val:
                        val = val["to"]
                    await alter_column_nullability(
                        connection, table, column, val
                    )
                    return
                elif "type" in changes:
                    val = changes["type"]
                    if isinstance(val, dict) and "to" in val:
                        val = val["to"]
                    await alter_column_type(connection, table, column, val)
                    return
                elif "default" in changes:
                    val = changes["default"]
                    if isinstance(val, dict) and "to" in val:
                        val = val["to"]
                    await alter_column_default(
                        connection, table, column, val
                    )
                    return

            # Multiple changes - use general reconstruction
            await reconstruct_table(connection, table, columns)

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

            await reconstruct_table(connection, table, columns)

        elif op_type == "drop_foreign_key":
            # Remove FK from column definition
            column = details["column"]
            if column not in columns:
                raise ValueError(f"Column '{column}' not found in table '{table}'")

            columns[column].pop("references", None)
            columns[column].pop("on_delete", None)
            columns[column].pop("on_update", None)

            await reconstruct_table(connection, table, columns)

        else:
            raise ValueError(
                f"Operation '{op_type}' does not support reconstruction"
            )

    def _execute_with_reconstruction_sync(
        self, connection: Any, operation: Operation
    ) -> None:
        """
        Execute an operation using table reconstruction (synchronous).

        Fresh introspection is performed before each reconstruction to ensure
        we have the latest schema state.

        Args:
            connection: Database connection (sqlite3.Connection)
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

        # Handle operation-specific reconstruction
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

        # Add composite primary key if specified
        if primary_key and len(primary_key) > 1:
            pk_cols = ", ".join(f'"{c}"' for c in primary_key)
            col_defs.append(f"PRIMARY KEY ({pk_cols})")

        columns_sql = ",\n    ".join(col_defs)
        return f'CREATE TABLE "{table}" (\n    {columns_sql}\n)'

    def _column_definition(self, name: str, col: Column) -> str:
        """Generate column definition for CREATE TABLE."""
        # Map types to SQLite types
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
        """Map generic types to SQLite types."""
        type_lower = type_str.lower()

        # SQLite type affinity mapping
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
        """
        Generate DROP COLUMN statement.

        SQLite 3.35.0+ supports ALTER TABLE DROP COLUMN.
        For older versions, table reconstruction would be needed.
        """
        col_name = details["column"]
        return f'ALTER TABLE "{table}" DROP COLUMN "{col_name}"'

    def _rename_column_sql(self, table: str, details: dict[str, Any]) -> str:
        """
        Generate RENAME COLUMN statement.

        SQLite 3.25.0+ supports ALTER TABLE RENAME COLUMN.
        """
        from_col = details["from_column"]
        to_col = details["to_column"]
        return f'ALTER TABLE "{table}" RENAME COLUMN "{from_col}" TO "{to_col}"'

    def _alter_column_sql(self, table: str, details: dict[str, Any]) -> str:
        """
        Generate column alteration SQL.

        SQLite doesn't support ALTER COLUMN natively.
        This should not be called directly - use _execute_with_reconstruction() instead.
        """
        raise NotImplementedError(
            f"ALTER COLUMN not supported directly in SQLite. "
            f"Use _execute_with_reconstruction() instead."
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
        """
        Generate ADD CONSTRAINT SQL.

        SQLite doesn't support ADD CONSTRAINT after table creation.
        Constraints must be defined at CREATE TABLE time.
        """
        const_name = details["constraint"]
        raise NotImplementedError(
            f"SQLite doesn't support ADD CONSTRAINT after table creation. "
            f"Constraint '{const_name}' must be defined in CREATE TABLE."
        )

    def _drop_constraint_sql(self, table: str, details: dict[str, Any]) -> str:
        """
        Generate DROP CONSTRAINT SQL.

        SQLite doesn't support DROP CONSTRAINT.
        """
        const_name = details["constraint"]
        raise NotImplementedError(
            f"SQLite doesn't support DROP CONSTRAINT. "
            f"Removing constraint '{const_name}' requires table reconstruction."
        )

    def _add_foreign_key_sql(self, table: str, details: dict[str, Any]) -> str:
        """
        Generate ADD FOREIGN KEY SQL.

        SQLite doesn't support ADD CONSTRAINT FOREIGN KEY.
        This should not be called directly - use _execute_with_reconstruction() instead.
        """
        raise NotImplementedError(
            f"SQLite doesn't support adding foreign keys after table creation. "
            f"Use _execute_with_reconstruction() instead."
        )

    def _drop_foreign_key_sql(self, table: str, details: dict[str, Any]) -> str:
        """
        Generate DROP FOREIGN KEY SQL.

        SQLite doesn't support dropping foreign keys.
        This should not be called directly - use _execute_with_reconstruction() instead.
        """
        raise NotImplementedError(
            f"SQLite doesn't support dropping foreign keys. "
            f"Use _execute_with_reconstruction() instead."
        )

    def _create_view_sql(self, _table: str, details: dict[str, Any]) -> str:
        """Generate CREATE VIEW statement."""
        return generate_create_view(details)  # type: ignore[arg-type]

    def _drop_view_sql(self, _table: str, details: dict[str, Any]) -> str:
        """Generate DROP VIEW statement."""
        return generate_drop_view(details["name"])


# =============================================================================
# Extended Schema Objects (Addendum) - Standalone Functions
# =============================================================================


def generate_enum_check(column_name: str, enum: Enum) -> str:
    """
    Generate CHECK constraint for enum column (SQLite fallback).

    Args:
        column_name: Column name
        enum: Enum definition

    Returns:
        CHECK constraint SQL
    """
    values = ", ".join(f"'{v}'" for v in enum["values"])
    return f"CHECK ({column_name} IN ({values}))"


def generate_column_sql(
    col_name: str, col_def: dict[str, Any], enums: dict[str, Enum] | None = None
) -> str:
    """
    Generate column definition, handling enum types with CHECK constraints.

    Args:
        col_name: Column name
        col_def: Column definition
        enums: Dict of known enum definitions

    Returns:
        Column SQL definition
    """
    enums = enums or {}
    col_type = col_def.get("type", "text")

    parts = [f'"{col_name}"']

    # Handle enum type reference - SQLite uses TEXT + CHECK
    if col_type.startswith("enum:"):
        enum_name = col_type[5:]  # Remove "enum:" prefix
        parts.append("TEXT")

        if enum_name in enums:
            check_sql = generate_enum_check(col_name, enums[enum_name])
            parts.append(check_sql)
    else:
        parts.append(_map_type(col_type))

    if col_def.get("nullable") is False:
        parts.append("NOT NULL")

    if "default" in col_def:
        parts.append(f"DEFAULT {col_def['default']}")

    return " ".join(parts)


def _map_type(type_str: str) -> str:
    """Map generic types to SQLite types."""
    type_lower = type_str.lower()

    if "int" in type_lower:
        return "INTEGER"
    elif type_lower in ("text", "varchar", "char", "string") or type_lower.startswith("varchar("):
        return "TEXT"
    elif type_lower in ("boolean", "bool"):
        return "INTEGER"
    elif type_lower in ("uuid", "timestamptz", "timestamp", "date", "datetime") or type_lower in (
        "jsonb",
        "json",
    ):
        return "TEXT"
    elif type_lower in ("float", "double", "real", "float4", "float8") or type_lower.startswith(
        "numeric"
    ):
        return "REAL"
    elif type_lower in ("blob", "bytea"):
        return "BLOB"
    else:
        return type_str.upper()


def generate_create_trigger(table: str, trigger: Trigger) -> str:
    """
    Generate SQLite CREATE TRIGGER SQL.

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
    body = trigger.get("body", "")

    # SQLite trigger name convention
    trigger_name = f"{table}_{name}"

    # Handle single event only - SQLite doesn't support multiple events per trigger
    if isinstance(event, list):
        event = event[0]  # Take first event

    event_sql = event.upper()

    sql = f"""CREATE TRIGGER {trigger_name}
{timing} {event_sql}
ON {table}
FOR EACH {for_each}
BEGIN
    {body}
END"""

    return sql


def generate_create_triggers_for_events(table: str, trigger: Trigger) -> list[str]:
    """
    Generate multiple SQLite triggers for multiple events.

    SQLite requires separate triggers for each event type.

    Args:
        table: Table name
        trigger: Trigger definition (may have multiple events)

    Returns:
        List of SQL statements
    """
    event = trigger.get("event", "insert")

    events = [event] if isinstance(event, str) else event

    sqls = []
    for evt in events:
        single_trigger = dict(trigger)
        single_trigger["event"] = evt
        single_trigger["name"] = f"{trigger['name']}_{evt}"
        sqls.append(generate_create_trigger(table, single_trigger))  # type: ignore

    return sqls


def generate_drop_trigger(table: str, trigger_name: str) -> str:
    """
    Generate SQLite DROP TRIGGER SQL.

    Args:
        table: Table name
        trigger_name: Trigger name

    Returns:
        SQL statement
    """
    return f"DROP TRIGGER IF EXISTS {table}_{trigger_name}"


def generate_create_function(procedure: Procedure) -> str:
    """
    SQLite does not support stored procedures.

    Raises:
        NotSupportedError: Always, with helpful alternatives
    """
    raise NotSupportedError(
        f"SQLite does not support stored procedures. "
        f"Function '{procedure.get('name', 'unknown')}' cannot be created.",
        alternatives=[
            "Move the logic to the application layer (Python function)",
            "Use SQLite user-defined functions via connection.create_function()",
            "Switch to PostgreSQL for stored procedure support",
        ],
    )


def generate_create_view(view: View) -> str:
    """
    Generate SQLite CREATE VIEW SQL.

    For materialized views, uses table-based emulation with metadata tracking.

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
    """
    Generate SQLite DROP VIEW SQL.

    Args:
        name: View name

    Returns:
        SQL statement
    """
    return f"DROP VIEW IF EXISTS {name}"
