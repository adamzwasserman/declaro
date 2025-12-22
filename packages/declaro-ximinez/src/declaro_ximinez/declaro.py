"""Declaro model validation for declaro-ximinez.

Models are defined as Pydantic classes with @table decorator:

    from pydantic import BaseModel
    from declaro_persistum import table, field

    @table("users")
    class User(BaseModel):
        id: UUID = field(primary=True)
        email: str = field(unique=True)
        name: str | None = None
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from .types import Model, ModelField, ModelRelationship, Violation
from .errors import suggest_similar


# SQL type to Python type mapping for ximinez
SQL_TO_PYTHON_TYPE: dict[str, str] = {
    "text": "str",
    "varchar": "str",
    "char": "str",
    "integer": "int",
    "bigint": "int",
    "smallint": "int",
    "serial": "int",
    "bigserial": "int",
    "real": "float",
    "double precision": "float",
    "float": "float",
    "boolean": "bool",
    "bool": "bool",
    "uuid": "uuid",
    "timestamptz": "datetime",
    "timestamp": "datetime",
    "date": "date",
    "time": "time",
    "decimal": "Decimal",
    "numeric": "Decimal",
    "bytea": "bytes",
    "jsonb": "dict",
    "json": "dict",
}


def pydantic_model_to_model(pydantic_cls: type) -> Model | None:
    """Convert a Pydantic model decorated with @table to a Model TypedDict.

    Args:
        pydantic_cls: A Pydantic model class with __tablename__ attribute.

    Returns:
        Model TypedDict, or None if not a valid table model.
    """
    table_name = getattr(pydantic_cls, "__tablename__", None)
    if table_name is None:
        return None

    fields: dict[str, ModelField] = {}
    relationships: dict[str, ModelRelationship] = {}

    # Import needed for introspection
    try:
        from declaro_persistum.introspection import (
            extract_declaro_meta,
            python_type_to_sql,
            is_nullable_type,
        )
    except ImportError:
        # Fallback if persistum not available - use basic introspection
        return _basic_pydantic_to_model(pydantic_cls, table_name)

    # Get type annotations
    annotations = getattr(pydantic_cls, "__annotations__", {})

    # Parse fields from Pydantic model
    for field_name, field_info in pydantic_cls.model_fields.items():
        annotation = annotations.get(field_name)
        meta = extract_declaro_meta(field_info)

        # Determine SQL type and convert to Python type
        sql_type = meta.get("db_type") or python_type_to_sql(annotation)
        python_type = SQL_TO_PYTHON_TYPE.get(sql_type, "str")

        # Check nullability
        nullable = meta.get("nullable")
        if nullable is None:
            nullable = is_nullable_type(annotation)

        # Check for relationship (foreign key)
        references = meta.get("references")
        if references:
            # This is a foreign key field - create relationship
            target_table, _ = references.split(".")
            relationships[field_name] = {
                "name": field_name,
                "type": "belongs_to",
                "target": target_table,
                "foreign_key": field_name,
            }

        fields[field_name] = {
            "name": field_name,
            "type": python_type,
            "nullable": nullable,
            "validate": [],  # Could parse Pydantic validators
        }

    return {
        "name": pydantic_cls.__name__,
        "table": table_name,
        "fields": fields,
        "relationships": relationships,
    }


def _basic_pydantic_to_model(pydantic_cls: type, table_name: str) -> Model:
    """Basic conversion without declaro_persistum dependency."""
    fields: dict[str, ModelField] = {}

    annotations = getattr(pydantic_cls, "__annotations__", {})
    model_fields = pydantic_cls.model_fields

    for field_name in model_fields:
        annotation = annotations.get(field_name)
        field_info = model_fields.get(field_name) if isinstance(model_fields, dict) else None

        # Basic type mapping
        python_type = "str"
        if annotation:
            origin = getattr(annotation, "__origin__", None)
            if origin is None:
                type_name = getattr(annotation, "__name__", str(annotation))
                python_type = type_name.lower() if type_name in ("str", "int", "float", "bool") else "str"

        # Check nullable from field info
        nullable = False
        if field_info is not None:
            nullable = getattr(field_info, "nullable", False)

        fields[field_name] = {
            "name": field_name,
            "type": python_type,
            "nullable": nullable,
            "validate": [],
        }

    return {
        "name": pydantic_cls.__name__,
        "table": table_name,
        "fields": fields,
        "relationships": {},
    }


def load_models_from_module(module_path: Path) -> dict[str, Model]:
    """Load all @table decorated Pydantic models from a Python module.

    Args:
        module_path: Path to Python file containing Pydantic models.

    Returns:
        Dictionary mapping model names to Model objects.

    Raises:
        FileNotFoundError: If module path doesn't exist.
        ImportError: If module cannot be imported.
    """
    if not module_path.exists():
        raise FileNotFoundError(f"Module path not found: {module_path}")

    # Import the module dynamically
    spec = importlib.util.spec_from_file_location(module_path.stem, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load module: {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_path.stem] = module

    try:
        spec.loader.exec_module(module)
    except Exception as e:
        raise ImportError(f"Error executing module {module_path}: {e}") from e

    models: dict[str, Model] = {}

    # Find all classes with __tablename__ (Pydantic models decorated with @table)
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, type) and hasattr(obj, "__tablename__"):
            model = pydantic_model_to_model(obj)
            if model:
                # Use lowercase key for case-insensitive lookup
                models[model["name"].lower()] = model

    return models


def load_models_from_paths(paths: list[str]) -> dict[str, Model]:
    """Load models from multiple module paths.

    Args:
        paths: List of paths to Python modules or directories.

    Returns:
        Dictionary mapping model names to Model objects.
    """
    all_models: dict[str, Model] = {}

    for path_str in paths:
        path = Path(path_str)
        if not path.exists():
            continue

        if path.is_file() and path.suffix == ".py":
            models = load_models_from_module(path)
            all_models.update(models)
        elif path.is_dir():
            for py_file in path.glob("**/*.py"):
                if py_file.name.startswith("_"):
                    continue
                try:
                    models = load_models_from_module(py_file)
                    all_models.update(models)
                except (ImportError, Exception):
                    # Skip modules that can't be imported
                    continue

    return all_models


def validate_field_access(
    model: Model,
    field_name: str,
    filename: str,
    line: int,
    col: int,
) -> Violation | None:
    """Validate that a field exists on a model.

    Args:
        model: The model to check.
        field_name: The field being accessed.
        filename: Source filename for error messages.
        line: Line number of access.
        col: Column number of access.

    Returns:
        Violation if field doesn't exist, None otherwise.
    """
    if field_name in model["fields"]:
        return None

    if field_name in model["relationships"]:
        return None

    # Field not found - suggest similar
    all_names = list(model["fields"].keys()) + list(model["relationships"].keys())
    suggestion = suggest_similar(field_name, all_names, max_distance=5)

    message = f"'{model['name'].title()}' has no field '{field_name}'"
    if suggestion:
        message += f" (did you mean '{suggestion}'?)"

    return {
        "file": filename,
        "line": line,
        "col": col,
        "message": message,
        "code": "XIM001",
    }


def validate_relationship_access(
    model: Model,
    rel_name: str,
    filename: str,
    line: int,
    col: int,
) -> Violation | None:
    """Validate that a relationship exists on a model.

    Args:
        model: The model to check.
        rel_name: The relationship being accessed.
        filename: Source filename for error messages.
        line: Line number of access.
        col: Column number of access.

    Returns:
        Violation if relationship doesn't exist, None otherwise.
    """
    if rel_name in model["relationships"]:
        return None

    # Not a relationship - maybe it's a field?
    if rel_name in model["fields"]:
        return None

    # Not found - suggest similar
    suggestion = suggest_similar(rel_name, list(model["relationships"].keys()), max_distance=10)

    message = f"'{model['name'].title()}' has no relationship '{rel_name}'"
    if suggestion:
        message += f" (did you mean '{suggestion}'?)"

    return {
        "file": filename,
        "line": line,
        "col": col,
        "message": message,
        "code": "XIM002",
    }


def validate_field_type(
    model: Model,
    field_name: str,
    expected_type: str,
    filename: str,
    line: int,
    col: int,
) -> Violation | None:
    """Validate that a field has the expected type.

    Args:
        model: The model to check.
        field_name: The field being accessed.
        expected_type: The type expected by the code.
        filename: Source filename for error messages.
        line: Line number of access.
        col: Column number of access.

    Returns:
        Violation if types don't match, None otherwise.
    """
    field = model["fields"].get(field_name)
    if field is None:
        return None  # Field doesn't exist - handled by validate_field_access

    actual_type = field["type"]

    # Simple type compatibility check
    # TODO: More sophisticated type compatibility
    if not types_compatible(actual_type, expected_type):
        return {
            "file": filename,
            "line": line,
            "col": col,
            "message": f"'{field_name}' is '{actual_type}', not '{expected_type}'",
            "code": "XIM003",
        }

    return None


def types_compatible(schema_type: str, code_type: str) -> bool:
    """Check if schema type is compatible with code type.

    Args:
        schema_type: Type from model definition.
        code_type: Type from Python code.

    Returns:
        True if compatible.
    """
    # Normalize types
    type_map = {
        "str": {"str", "string"},
        "int": {"int", "integer"},
        "float": {"float", "double"},
        "bool": {"bool", "boolean"},
        "uuid": {"str", "uuid", "UUID"},
        "datetime": {"datetime", "str"},
        "date": {"date", "str"},
        "decimal": {"Decimal", "float", "decimal"},
    }

    schema_normalized = type_map.get(schema_type, {schema_type})
    return code_type.lower() in {t.lower() for t in schema_normalized}


def validate_query_column(
    models: dict[str, Model],
    table_name: str,
    column_name: str,
    filename: str,
    line: int,
    col: int,
) -> Violation | None:
    """Validate that a column exists on a table.

    Args:
        models: All loaded models.
        table_name: The table being queried.
        column_name: The column being referenced.
        filename: Source filename for error messages.
        line: Line number of query.
        col: Column number of query.

    Returns:
        Violation if column doesn't exist, None otherwise.
    """
    # Find model by table name
    model = None
    for m in models.values():
        if m["table"] == table_name:
            model = m
            break

    if model is None:
        return {
            "file": filename,
            "line": line,
            "col": col,
            "message": f"unknown table '{table_name}'",
            "code": "XIM004",
        }

    if column_name in model["fields"]:
        return None

    # Column not found - suggest similar
    # Use higher max_distance to catch partial matches like username -> name
    suggestion = suggest_similar(column_name, list(model["fields"].keys()), max_distance=5)

    message = f"'{table_name}' table has no column '{column_name}'"
    if suggestion:
        message += f" (did you mean '{suggestion}'?)"

    return {
        "file": filename,
        "line": line,
        "col": col,
        "message": message,
        "code": "XIM005",
    }


def validate_insert_fields(
    model: Model,
    provided_fields: set[str],
    filename: str,
    line: int,
    col: int,
) -> list[Violation]:
    """Validate that all required fields are provided for insert.

    Args:
        model: The model being inserted into.
        provided_fields: Set of field names provided.
        filename: Source filename for error messages.
        line: Line number of insert.
        col: Column number of insert.

    Returns:
        List of violations for missing required fields.
    """
    violations: list[Violation] = []

    for field_name, field in model["fields"].items():
        # Skip auto-generated fields (id, uuid types)
        if field_name == "id" or field.get("type") == "uuid":
            continue

        if not field["nullable"] and field_name not in provided_fields:
            # Check if it has a default (we'd need to track this in schema)
            violations.append({
                "file": filename,
                "line": line,
                "col": col,
                "message": f"missing required field '{field_name}' for insert into '{model['table']}'",
                "code": "XIM006",
            })

    return violations
