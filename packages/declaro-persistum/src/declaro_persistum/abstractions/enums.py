"""
Enum abstraction using lookup tables with foreign key constraints.

Replaces CHECK constraints with FK references to lookup tables,
ensuring compatibility across all backends (PostgreSQL, SQLite, Turso, LibSQL).

Pattern:
    Literal["pending", "shipped", "delivered"]

    Becomes:
    - Lookup table: _dp_enum_{table}_{column} with value TEXT PRIMARY KEY
    - FK constraint: {column} REFERENCES _dp_enum_{table}_{column}(value)
"""

from typing import Any


# Prefix for auto-generated enum lookup tables
ENUM_TABLE_PREFIX = "_dp_enum_"


def enum_table_name(table: str, column: str) -> str:
    """Generate lookup table name for an enum column.

    Args:
        table: The table containing the enum column
        column: The enum column name

    Returns:
        Lookup table name like _dp_enum_orders_status
    """
    return f"{ENUM_TABLE_PREFIX}{table}_{column}"


def is_enum_table(table_name: str) -> bool:
    """Check if a table name is an auto-generated enum lookup table."""
    return table_name.startswith(ENUM_TABLE_PREFIX)


def generate_enum_table_schema(
    table: str,
    column: str,
    values: list[str],
) -> dict[str, Any]:
    """Generate schema for an enum lookup table.

    Args:
        table: The parent table name
        column: The enum column name
        values: List of allowed enum values

    Returns:
        Table schema dict for the lookup table
    """
    lookup_name = enum_table_name(table, column)

    return {
        lookup_name: {
            "columns": {
                "value": {
                    "type": "text",
                    "primary_key": True,
                    "nullable": False,
                },
            },
            # Store the enum values as metadata for migration detection
            "_enum_values": values,
            "_enum_for": {"table": table, "column": column},
        }
    }


def create_enum_table_sql(
    table: str,
    column: str,
    values: list[str],
) -> list[str]:
    """Generate SQL to create an enum lookup table with initial values.

    Args:
        table: The parent table name
        column: The enum column name
        values: List of allowed enum values

    Returns:
        List of SQL statements (CREATE TABLE + INSERTs)
    """
    lookup_name = enum_table_name(table, column)
    statements: list[str] = []

    # Create the lookup table
    statements.append(
        f'CREATE TABLE "{lookup_name}" (\n'
        f"    value TEXT PRIMARY KEY\n"
        f")"
    )

    # Insert all values
    if values:
        value_list = ", ".join(f"('{_escape_sql(v)}')" for v in values)
        statements.append(
            f'INSERT INTO "{lookup_name}" (value) VALUES {value_list}'
        )

    return statements


def drop_enum_table_sql(table: str, column: str) -> list[str]:
    """Generate SQL to drop an enum lookup table.

    Args:
        table: The parent table name
        column: The enum column name

    Returns:
        List of SQL statements
    """
    lookup_name = enum_table_name(table, column)
    return [f'DROP TABLE IF EXISTS "{lookup_name}"']


def add_enum_value_sql(table: str, column: str, value: str) -> list[str]:
    """Generate SQL to add a new value to an enum lookup table.

    Args:
        table: The parent table name
        column: The enum column name
        value: The new value to add

    Returns:
        List of SQL statements
    """
    lookup_name = enum_table_name(table, column)
    return [
        f"INSERT INTO \"{lookup_name}\" (value) VALUES ('{_escape_sql(value)}')"
    ]


def remove_enum_value_sql(table: str, column: str, value: str) -> list[str]:
    """Generate SQL to remove a value from an enum lookup table.

    Note: This will fail if any rows reference the value.

    Args:
        table: The parent table name
        column: The enum column name
        value: The value to remove

    Returns:
        List of SQL statements
    """
    lookup_name = enum_table_name(table, column)
    return [
        f"DELETE FROM \"{lookup_name}\" WHERE value = '{_escape_sql(value)}'"
    ]


def get_enum_fk_reference(table: str, column: str) -> str:
    """Get the FK reference clause for an enum column.

    Args:
        table: The parent table name
        column: The enum column name

    Returns:
        Reference string like "_dp_enum_orders_status.value"
    """
    lookup_name = enum_table_name(table, column)
    return f"{lookup_name}.value"


def transform_column_for_enum(
    column_def: dict[str, Any],
    table: str,
    column: str,
) -> dict[str, Any]:
    """Transform a column definition to use FK instead of CHECK for enums.

    Args:
        column_def: Original column definition with literal_values
        table: Table name
        column: Column name

    Returns:
        Modified column definition with FK reference
    """
    # Copy the column def
    new_def = dict(column_def)

    # Remove literal_values (internal marker)
    literal_values = new_def.pop("literal_values", None)

    if literal_values:
        # Add FK reference instead of CHECK constraint
        new_def["references"] = get_enum_fk_reference(table, column)

        # Remove any CHECK constraint that might exist
        new_def.pop("check", None)

    return new_def


def expand_schema_enums(schema: dict[str, Any]) -> dict[str, Any]:
    """Expand all Literal type columns into lookup tables + FK constraints.

    Args:
        schema: Original schema with literal_values in columns

    Returns:
        Expanded schema with:
        - Enum lookup tables added
        - Column definitions updated with FK references
    """
    expanded: dict[str, Any] = {}
    enum_tables: dict[str, Any] = {}

    for table_name, table_def in schema.items():
        # Skip if already an enum table
        if is_enum_table(table_name):
            expanded[table_name] = table_def
            continue

        columns = table_def.get("columns", {})
        new_columns: dict[str, Any] = {}

        for col_name, col_def in columns.items():
            literal_values = col_def.get("literal_values")

            if literal_values:
                # Generate lookup table
                lookup_schema = generate_enum_table_schema(
                    table_name, col_name, literal_values
                )
                enum_tables.update(lookup_schema)

                # Transform column to use FK
                new_columns[col_name] = transform_column_for_enum(
                    col_def, table_name, col_name
                )
            else:
                new_columns[col_name] = col_def

        # Update table with transformed columns
        new_table = dict(table_def)
        new_table["columns"] = new_columns
        expanded[table_name] = new_table

    # Add enum lookup tables first (they need to exist before FK references)
    result: dict[str, Any] = {}
    result.update(enum_tables)
    result.update(expanded)

    return result


def diff_enum_values(
    old_values: list[str] | None,
    new_values: list[str] | None,
) -> tuple[list[str], list[str]]:
    """Compute the difference between two sets of enum values.

    Args:
        old_values: Previous enum values (or None)
        new_values: New enum values (or None)

    Returns:
        Tuple of (values_to_add, values_to_remove)
    """
    old_set = set(old_values or [])
    new_set = set(new_values or [])

    to_add = list(new_set - old_set)
    to_remove = list(old_set - new_set)

    return to_add, to_remove


def _escape_sql(s: str) -> str:
    """Escape single quotes in SQL string."""
    return s.replace("'", "''")
