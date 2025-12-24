"""
Pydantic model loader for declaro_persistum.

Loads schema from Pydantic models decorated with @table.
Detects Literal types for automatic enum abstraction.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any, Literal, get_args, get_origin

from declaro_persistum.exceptions import LoaderError
from declaro_persistum.types import Column, Schema, Table


# Python type to SQL type mapping
PYTHON_TO_SQL_TYPE: dict[type | str, str] = {
    str: "text",
    int: "integer",
    float: "real",
    bool: "boolean",
    bytes: "bytea",
    "UUID": "uuid",
    "uuid": "uuid",
    "datetime": "timestamptz",
    "date": "date",
    "time": "time",
    "Decimal": "numeric",
    "decimal": "numeric",
    "dict": "jsonb",
    "list": "jsonb",
}


def is_literal_type(annotation: Any) -> bool:
    """Check if annotation is a Literal type."""
    return get_origin(annotation) is Literal


def extract_literal_values(annotation: Any) -> list[str]:
    """Extract string values from a Literal type annotation.

    Args:
        annotation: A Literal type annotation like Literal["a", "b", "c"]

    Returns:
        List of literal string values

    Raises:
        ValueError: If Literal contains non-string values
    """
    if not is_literal_type(annotation):
        return []

    args = get_args(annotation)
    values: list[str] = []

    for arg in args:
        if not isinstance(arg, str):
            raise ValueError(
                f"Literal enum values must be strings, got {type(arg).__name__}: {arg}"
            )
        values.append(arg)

    return values


def is_optional_type(annotation: Any) -> bool:
    """Check if annotation is Optional (Union with None)."""
    origin = get_origin(annotation)
    if origin is None:
        return False

    # Check for Union types (including X | None syntax)
    origin_name = getattr(origin, "__name__", str(origin))
    if origin_name in ("Union", "UnionType"):
        args = get_args(annotation)
        return type(None) in args

    return False


def unwrap_optional(annotation: Any) -> Any:
    """Unwrap Optional[X] to get X."""
    if not is_optional_type(annotation):
        return annotation

    args = get_args(annotation)
    for arg in args:
        if arg is not type(None):
            return arg

    return annotation


def python_type_to_sql(annotation: Any) -> str:
    """Convert Python type annotation to SQL type string."""
    # Handle Optional types
    if is_optional_type(annotation):
        annotation = unwrap_optional(annotation)

    # Handle Literal types - they become text with enum constraint
    if is_literal_type(annotation):
        return "text"

    # Direct type match
    if annotation in PYTHON_TO_SQL_TYPE:
        return PYTHON_TO_SQL_TYPE[annotation]

    # Check by name for imported types
    type_name = getattr(annotation, "__name__", str(annotation))
    if type_name in PYTHON_TO_SQL_TYPE:
        return PYTHON_TO_SQL_TYPE[type_name]

    # Check string representation for generic types
    type_str = str(annotation)
    if "UUID" in type_str or "uuid" in type_str:
        return "uuid"
    if "datetime" in type_str:
        return "timestamptz"
    if "Decimal" in type_str:
        return "numeric"

    # Default to text
    return "text"


def extract_field_metadata(field_info: Any) -> dict[str, Any]:
    """Extract declaro-specific metadata from Pydantic field.

    Looks for metadata set via field() function like:
        field(primary=True, unique=True, references="users.id")
    """
    meta: dict[str, Any] = {}

    # Check for json_schema_extra (Pydantic v2)
    json_extra = getattr(field_info, "json_schema_extra", None)
    if isinstance(json_extra, dict):
        meta.update(json_extra)

    # Check for metadata attribute
    metadata = getattr(field_info, "metadata", None)
    if metadata:
        for item in metadata:
            if isinstance(item, dict):
                meta.update(item)

    # Check default value
    default = getattr(field_info, "default", None)
    if default is not None and default is not ...:
        # Store default for later processing
        meta["_default_value"] = default

    return meta


def pydantic_model_to_table(model_cls: type) -> tuple[str, Table] | None:
    """Convert a Pydantic model to a Table definition.

    Args:
        model_cls: A Pydantic BaseModel subclass with __tablename__

    Returns:
        Tuple of (table_name, Table) or None if not a table model
    """
    # Check for table decorator
    table_name = getattr(model_cls, "__tablename__", None)
    if table_name is None:
        return None

    # Get annotations and fields
    annotations = getattr(model_cls, "__annotations__", {})
    model_fields = getattr(model_cls, "model_fields", {})

    columns: dict[str, Column] = {}

    for field_name, annotation in annotations.items():
        if field_name.startswith("_"):
            continue

        field_info = model_fields.get(field_name)
        meta = extract_field_metadata(field_info) if field_info else {}

        # Determine nullability
        nullable = is_optional_type(annotation)
        if "nullable" in meta:
            nullable = meta["nullable"]

        # Unwrap Optional for further processing
        unwrapped = unwrap_optional(annotation)

        # Determine SQL type
        sql_type = meta.get("db_type") or python_type_to_sql(unwrapped)

        # Build column definition
        col: Column = {"type": sql_type}

        if nullable:
            col["nullable"] = True
        else:
            col["nullable"] = False

        # Check for Literal type - extract enum values
        if is_literal_type(unwrapped):
            literal_values = extract_literal_values(unwrapped)
            if literal_values:
                col["literal_values"] = literal_values

        # Apply metadata
        if meta.get("primary") or meta.get("primary_key"):
            col["primary_key"] = True

        if meta.get("unique"):
            col["unique"] = True

        if meta.get("references"):
            col["references"] = meta["references"]

        if meta.get("on_delete"):
            col["on_delete"] = meta["on_delete"]

        if meta.get("on_update"):
            col["on_update"] = meta["on_update"]

        if meta.get("check"):
            col["check"] = meta["check"]

        if meta.get("default"):
            col["default"] = meta["default"]
        elif "_default_value" in meta:
            # Convert Python default to SQL default
            default_val = meta["_default_value"]
            if isinstance(default_val, str):
                col["default"] = f"'{default_val}'"
            elif isinstance(default_val, bool):
                col["default"] = "TRUE" if default_val else "FALSE"
            elif isinstance(default_val, (int, float)):
                col["default"] = str(default_val)

        # Migration hints
        if meta.get("renamed_from"):
            col["renamed_from"] = meta["renamed_from"]

        if meta.get("is_new"):
            col["is_new"] = meta["is_new"]

        columns[field_name] = col

    table: Table = {"columns": columns}

    # Check for Meta class with indexes
    meta_cls = getattr(model_cls, "Meta", None)
    if meta_cls:
        indexes = getattr(meta_cls, "indexes", None)
        if indexes:
            table["indexes"] = {idx["name"]: idx for idx in indexes if isinstance(idx, dict)}

        constraints = getattr(meta_cls, "constraints", None)
        if constraints:
            table["constraints"] = constraints

    return table_name, table


def load_models_from_module(module_path: Path) -> Schema:
    """Load all @table decorated Pydantic models from a Python module.

    Args:
        module_path: Path to Python file containing Pydantic models

    Returns:
        Schema dict with table definitions

    Raises:
        LoaderError: If module cannot be loaded
    """
    if not module_path.exists():
        raise LoaderError(f"Module not found: {module_path}", path=str(module_path))

    # Generate unique module name to avoid conflicts
    module_name = f"_dp_model_{module_path.stem}_{id(module_path)}"

    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise LoaderError(f"Cannot load module: {module_path}", path=str(module_path))

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module

    try:
        spec.loader.exec_module(module)
    except Exception as e:
        raise LoaderError(f"Error executing module: {e}", path=str(module_path)) from e

    schema: Schema = {}

    # Find all classes with __tablename__
    for name in dir(module):
        if name.startswith("_"):
            continue

        obj = getattr(module, name)
        if not isinstance(obj, type):
            continue

        if not hasattr(obj, "__tablename__"):
            continue

        result = pydantic_model_to_table(obj)
        if result:
            table_name, table = result
            schema[table_name] = table

    return schema


def load_schema_from_models(models_dir: str | Path) -> Schema:
    """Load schema from Pydantic model files in a directory.

    Args:
        models_dir: Path to directory containing model Python files

    Returns:
        Schema dict with all table definitions

    Raises:
        LoaderError: If directory doesn't exist or no models found
    """
    models_path = Path(models_dir)

    if not models_path.exists():
        raise LoaderError(f"Models directory not found: {models_path}", path=str(models_path))

    schema: Schema = {}

    # Load all .py files (except __init__.py and test files)
    for py_file in models_path.glob("**/*.py"):
        if py_file.name.startswith("_"):
            continue
        if py_file.name.startswith("test_"):
            continue

        try:
            file_schema = load_models_from_module(py_file)
            schema.update(file_schema)
        except LoaderError:
            # Re-raise loader errors
            raise
        except Exception as e:
            # Skip files that can't be imported (might not be model files)
            continue

    if not schema:
        raise LoaderError(
            f"No @table decorated models found in {models_path}",
            path=str(models_path),
        )

    return schema


def get_literal_columns(schema: Schema) -> dict[str, dict[str, list[str]]]:
    """Extract all columns with Literal type values from schema.

    Returns:
        Dict of table_name -> {column_name -> [literal_values]}
    """
    result: dict[str, dict[str, list[str]]] = {}

    for table_name, table in schema.items():
        columns = table.get("columns", {})
        for col_name, col in columns.items():
            literal_values = col.get("literal_values")  # type: ignore[typeddict-item]
            if literal_values:
                if table_name not in result:
                    result[table_name] = {}
                result[table_name][col_name] = literal_values

    return result
