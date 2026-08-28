"""
Enum abstraction using lookup tables with foreign key constraints.

Replaces CHECK constraints with FK references to lookup tables,
ensuring compatibility across all backends (PostgreSQL, SQLite, Turso).

Pattern:
    Literal["pending", "shipped", "delivered"]

    Becomes:
    - Lookup table: _dp_enum_{table}_{column} with value TEXT PRIMARY KEY
    - FK constraint: {column} REFERENCES _dp_enum_{table}_{column}(value)
"""

from collections.abc import Callable
from typing import Any

from declaro_persistum.types import Schema

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
    statements.append(f'CREATE TABLE "{lookup_name}" (\n    value TEXT PRIMARY KEY\n)')

    # An enum with no values needs no INSERT, so the comprehension over the
    # nought-or-one case is the whole of it.
    statements.extend(
        f'INSERT INTO "{lookup_name}" (value) VALUES '
        + ", ".join(f"('{_escape_sql(v)}')" for v in vs)
        for vs in [values]
        if vs
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


# WIDENING AND NARROWING AN ENUM, one row each. Both took the same three
# arguments, named the same lookup table, escaped the same value and returned
# one statement. The statement was all that differed.
#
# THE ESCAPE IS WHY ONE COPY MATTERS. A value carrying an apostrophe ends the
# statement early if it is not doubled, and two copies of that rule are two
# chances for one of them to be forgotten.
_ENUM_VALUE_SQL: dict[str, tuple[str, str]] = {
    "add_enum_value_sql": (
        "INSERT INTO \"{lookup}\" (value) VALUES ('{value}')",
        "Generate SQL to add a new value to an enum lookup table.\n\n"
        '    A column declared `Literal["a", "b"]` points at a lookup table,\n'
        "    so widening the Literal means inserting a row. Until it is there\n"
        "    the column refuses every row carrying the new value.\n    ",
    ),
    "remove_enum_value_sql": (
        "DELETE FROM \"{lookup}\" WHERE value = '{value}'",
        "Generate SQL to remove a value from an enum lookup table.\n\n"
        "    This fails if any row still references the value, and that is the\n"
        "    foreign key doing its job rather than a defect: narrowing a\n"
        "    Literal while rows still use the value would strand them.\n    ",
    ),
}


def _changes_the_lookup(name: str) -> Callable[[str, str, str], list[str]]:
    """Build the function that writes one kind of change to a lookup table."""

    template, purpose = _ENUM_VALUE_SQL[name]

    def change(table: str, column: str, value: str) -> list[str]:
        lookup = enum_table_name(table, column)
        return [template.format(lookup=lookup, value=_escape_sql(value))]

    change.__name__ = name
    change.__qualname__ = name
    change.__doc__ = purpose
    return change


add_enum_value_sql = _changes_the_lookup("add_enum_value_sql")
remove_enum_value_sql = _changes_the_lookup("remove_enum_value_sql")


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

    # A column declaring Literal values points at the lookup table instead of
    # carrying a CHECK, and one that declares none is unchanged.
    literal_values = new_def.pop("literal_values", None)
    return _AS_ENUM_COLUMN[bool(literal_values)](new_def, table, column)


def _pointing_at_the_lookup(column_def: dict[str, Any], table: str, column: str) -> dict[str, Any]:
    """The FK replaces the CHECK, so any CHECK that was there comes off."""
    return {
        **{k: v for k, v in column_def.items() if k != "check"},
        "references": get_enum_fk_reference(table, column),
    }


def _as_declared(column_def: dict[str, Any], _table: str, _column: str) -> dict[str, Any]:
    """No Literal values, so nothing about this column changes."""
    return column_def


# Whether this column declared a Literal type.
_AS_ENUM_COLUMN: dict[bool, Callable[[dict[str, Any], str, str], dict[str, Any]]] = {
    True: _pointing_at_the_lookup,
    False: _as_declared,
}


def expand_schema_enums(schema: Schema) -> dict[str, Any]:
    """Expand all Literal type columns into lookup tables + FK constraints.

    Args:
        schema: Original schema with literal_values in columns

    Returns:
        Expanded schema with:
        - Enum lookup tables added
        - Column definitions updated with FK references
    """
    # A table this loader already made is carried through untouched; every
    # other table has each of its Literal columns turned into a foreign key
    # and gains a lookup table to point at.
    ours = {name: table for name, table in schema.items() if is_enum_table(name)}
    theirs = {name: table for name, table in schema.items() if not is_enum_table(name)}

    # The filter has to sit above the call, because a comprehension evaluates
    # its clauses in order and `generate_enum_table_schema` would otherwise
    # run for a column that declares no Literal values.
    enum_tables = {
        lookup_name: lookup_table
        for name, table in theirs.items()
        for col_name, col_def in table.get("columns", {}).items()
        if col_def.get("literal_values")
        for lookup_name, lookup_table in generate_enum_table_schema(
            name, col_name, col_def["literal_values"]
        ).items()
    }

    expanded = {
        name: {
            **table,
            "columns": {
                col_name: transform_column_for_enum(col_def, name, col_name)
                for col_name, col_def in table.get("columns", {}).items()
            },
        }
        for name, table in theirs.items()
    }

    # The lookup tables come first, because the foreign keys point at them.
    return {**enum_tables, **ours, **expanded}


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
