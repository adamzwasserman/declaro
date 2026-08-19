"""
Schema validation.

Validates schema definitions for correctness:
- Foreign key references exist
- Index columns exist
- No circular dependencies (without deferrable)
- Type names are valid
"""

from typing import TypedDict

from declaro_persistum.exceptions import ValidationError
from declaro_persistum.types import Column, Index, Schema, is_nullable

# Known SQL types (normalized lowercase)
KNOWN_TYPES = {
    # Standard SQL
    "integer",
    "bigint",
    "smallint",
    "text",
    "varchar",
    "char",
    "boolean",
    "bool",
    "real",
    "float",
    "float4",
    "float8",
    "double precision",
    "numeric",
    "decimal",
    "date",
    "time",
    "timestamp",
    "timestamptz",
    "interval",
    "blob",
    "bytea",
    # PostgreSQL specific
    "uuid",
    "jsonb",
    "json",
    "serial",
    "bigserial",
    "smallserial",
    "money",
    "inet",
    "cidr",
    "macaddr",
    "point",
    "line",
    "lseg",
    "box",
    "path",
    "polygon",
    "circle",
    "tsvector",
    "tsquery",
    "xml",
    # Arrays
    "integer[]",
    "text[]",
    "uuid[]",
    "jsonb[]",
}


class ValidationResult(TypedDict):
    """What a validation found, reached by name.

    Both validators returned `tuple[list[str], list[str]]` as
    `(warnings, errors)`. The annotation is honest about the types and silent
    about the only thing a caller needs, and a swapped pair of same-typed
    lists does not raise: it inverts a check and passes.

    Measured on myself, 2026-08-17. Writing a test against that signature I
    subscripted the result as a dict, then unpacked it as `errors, warnings`.
    The second version asserted the absence of warnings by looking at the
    errors, and it passed, because both lists were empty.
    """

    warnings: list[str]
    errors: list[str]


# WHAT THE RENDERERS CANNOT CARRY, and why each one breaks.
#
#     "   the appliers write an identifier as `"{name}"` at 65 places and
#         none escapes it, so `quo"ted` renders `"quo"ted"` and the engine
#         reads three tokens. Both SQLite and PostgreSQL refuse it.
#
#     ;   the SQLite and Turso appliers split rendered SQL on `;` before
#         executing, so a name holding one is torn in half. PostgreSQL runs
#         the statement whole and takes it without complaint.
#
# Measured 2026-08-18 by declaring one table per name and migrating. Refusing
# here rather than escaping at 65 render sites is deliberate: the boundary is
# one place, and an author hears about the name while writing it rather than
# during a deploy. `column_definition` already makes the same trade for a
# default it cannot spell.
#
# `'`, a space, mixed case and non-ASCII are all rendered correctly and are
# NOT refused. A check that rejected anything unusual would be worse than the
# defect, because it would reject working schemas.
UNRENDERABLE_IN_A_NAME = {
    '"': 'the appliers do not escape a double quote inside a quoted name',
    ";": "the SQLite and Turso appliers split rendered SQL on a semicolon",
}


def _unrenderable(kind: str, owner: str, name: str) -> list[str]:
    """Every character in `name` this package cannot write into SQL."""
    return [
        f"{kind} '{name}' in '{owner}' cannot be migrated: it contains "
        f"'{character}', and {reason}. Rename it."
        for character, reason in UNRENDERABLE_IN_A_NAME.items()
        if character in name
    ]


def validate_schema(schema: Schema) -> ValidationResult:
    """
    Validate a schema for correctness.

    Checks:
    - All foreign key references point to existing tables/columns
    - All index columns exist in the table
    - Type names are recognized
    - No obvious circular dependencies

    Args:
        schema: Schema to validate

    Returns:
        A ValidationResult, whose two lists are reached by name
        - warnings: Non-fatal issues (unknown types, etc.)
        - errors: Fatal issues (invalid references, etc.)
    """
    warnings: list[str] = []
    errors: list[str] = []

    for table_name, table in schema.items():
        errors.extend(_unrenderable("Table", table_name, table_name))

        # Validate columns
        columns = table.get("columns", {})

        for col_name, col_def in columns.items():
            errors.extend(_unrenderable("Column", table_name, col_name))
            column = _validate_column(table_name, col_name, col_def, schema)
            col_warnings, col_errors = column["warnings"], column["errors"]
            warnings.extend(col_warnings)
            errors.extend(col_errors)

        # Validate indexes
        indexes = table.get("indexes", {})
        for idx_name, idx_def in indexes.items():
            idx_errors = _validate_index(table_name, idx_name, idx_def, columns)
            errors.extend(idx_errors)

        # Validate primary key
        pk = table.get("primary_key", [])
        for pk_col in pk:
            if pk_col not in columns:
                errors.append(f"Table '{table_name}': Primary key column '{pk_col}' does not exist")

    # Check for circular foreign key dependencies
    cycle_warnings = _check_circular_dependencies(schema)
    warnings.extend(cycle_warnings)

    return {"warnings": warnings, "errors": errors}


def _validate_column(
    table_name: str,
    col_name: str,
    col_def: Column,
    schema: Schema,
) -> ValidationResult:
    """Validate a single column definition."""
    warnings: list[str] = []
    errors: list[str] = []

    # Check type
    col_type = col_def.get("type", "text")
    type_lower = col_type.lower()

    # Normalize type for checking (remove size specifiers)
    base_type = type_lower.split("(")[0].strip()

    if base_type not in KNOWN_TYPES and not base_type.endswith("[]"):
        # Could be a custom type or enum
        warnings.append(
            f"Table '{table_name}', column '{col_name}': "
            f"Unknown type '{col_type}' (may be custom type or enum)"
        )

    # Check foreign key reference
    if "references" in col_def:
        ref = col_def["references"]
        ref_error = _validate_reference(table_name, col_name, ref, schema)
        if ref_error:
            errors.append(ref_error)

    # A PRIMARY KEY that was DECLARED nullable is a contradiction the author
    # wrote down, and no engine can honour it. An omitted `nullable` is not:
    # declaring the key has already said NOT NULL. This used to read
    # `col_def.get("nullable", True)` and so warned about the schema this
    # package ships, telling the author to repeat themselves.
    if col_def.get("primary_key") and col_def.get("nullable") is True:
        warnings.append(
            f"Table '{table_name}', column '{col_name}': Primary key column should be NOT NULL"
        )

    # Check for default on NOT NULL without default (warning for new columns)
    if col_def.get("is_new") and not is_nullable(col_def) and "default" not in col_def:
        warnings.append(
            f"Table '{table_name}', column '{col_name}': "
            f"New NOT NULL column without default may fail if table has data"
        )

    return {"warnings": warnings, "errors": errors}


def _validate_reference(
    table_name: str,
    col_name: str,
    reference: str,
    schema: Schema,
) -> str | None:
    """Validate a foreign key reference."""
    if "." not in reference:
        return (
            f"Table '{table_name}', column '{col_name}': "
            f"Invalid reference format '{reference}' (expected 'table.column')"
        )

    ref_table, ref_col = reference.split(".", 1)

    if ref_table not in schema:
        return (
            f"Table '{table_name}', column '{col_name}': "
            f"References non-existent table '{ref_table}'"
        )

    ref_table_def = schema[ref_table]
    ref_columns = ref_table_def.get("columns", {})

    if ref_col not in ref_columns:
        return (
            f"Table '{table_name}', column '{col_name}': "
            f"References non-existent column '{ref_table}.{ref_col}'"
        )

    return None


def _validate_index(
    table_name: str,
    idx_name: str,
    idx_def: Index,
    columns: dict[str, Column],
) -> list[str]:
    """Validate an index definition."""
    errors: list[str] = []

    idx_columns = idx_def.get("columns", [])

    if not idx_columns:
        errors.append(
            f"Table '{table_name}', index '{idx_name}': Index must have at least one column"
        )
        return errors

    for col in idx_columns:
        if col not in columns:
            errors.append(
                f"Table '{table_name}', index '{idx_name}': Column '{col}' does not exist"
            )

    return errors


def _check_circular_dependencies(schema: Schema) -> list[str]:
    """
    Check for circular foreign key dependencies.

    Returns warnings (not errors) because circular dependencies
    can be valid with deferrable constraints.
    """
    warnings: list[str] = []

    # Build dependency graph
    deps: dict[str, set[str]] = {table: set() for table in schema}

    for table_name, table in schema.items():
        columns = table.get("columns", {})
        for col_def in columns.values():
            if "references" in col_def:
                ref = col_def["references"]
                ref_table = ref.split(".")[0]
                if ref_table != table_name:  # Ignore self-references
                    deps[table_name].add(ref_table)

    # Find cycles using DFS
    def find_cycle(start: str) -> list[str] | None:
        visited: set[str] = set()
        path: list[str] = []

        def dfs(node: str) -> list[str] | None:
            if node in path:
                cycle_start = path.index(node)
                return path[cycle_start:] + [node]

            if node in visited:
                return None

            visited.add(node)
            path.append(node)

            for dep in deps.get(node, set()):
                result = dfs(dep)
                if result:
                    return result

            path.pop()
            return None

        return dfs(start)

    # Check each table
    checked: set[str] = set()
    for table_key in schema:
        if table_key not in checked:
            cycle = find_cycle(table_key)
            if cycle:
                cycle_str = " -> ".join(cycle)
                warnings.append(
                    f"Circular foreign key dependency detected: {cycle_str}. "
                    f"This may require deferrable constraints."
                )
                # Mark all tables in cycle as checked
                checked.update(cycle[:-1])

    return warnings


def validate_schema_strict(schema: Schema) -> None:
    """
    Validate schema and raise on any error.

    Args:
        schema: Schema to validate

    Raises:
        ValidationError: If any validation errors found
    """
    errors = validate_schema(schema)["errors"]

    if errors:
        error_list = "\n  - ".join(errors)
        raise ValidationError(
            f"Schema validation failed with {len(errors)} error(s):\n  - {error_list}"
        )
