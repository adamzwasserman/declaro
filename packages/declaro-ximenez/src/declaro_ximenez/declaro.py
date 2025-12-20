"""Declaro model validation for declaro-ximenez."""

from __future__ import annotations

import tomllib
from pathlib import Path

from .types import Model, ModelField, ModelRelationship, Violation
from .errors import suggest_similar


def load_schema(schema_path: Path) -> dict[str, Model]:
    """Load all models from a schema directory.

    Args:
        schema_path: Path to directory containing TOML schema files.

    Returns:
        Dictionary mapping model names to Model objects.

    Raises:
        FileNotFoundError: If schema path doesn't exist.
    """
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema path not found: {schema_path}")

    models: dict[str, Model] = {}

    for toml_file in schema_path.glob("**/*.toml"):
        file_models = load_schema_file(toml_file)
        models.update(file_models)

    return models


def load_schema_file(file_path: Path) -> dict[str, Model]:
    """Load models from a single TOML schema file.

    Args:
        file_path: Path to the TOML file.

    Returns:
        Dictionary mapping model names to Model objects.
    """
    with open(file_path, "rb") as f:
        data = tomllib.load(f)

    models: dict[str, Model] = {}

    for name, config in data.items():
        if not isinstance(config, dict):
            continue

        model = parse_model(name, config)
        if model:
            models[name] = model

    return models


def parse_model(name: str, config: dict) -> Model | None:
    """Parse a model definition from TOML config.

    Args:
        name: The model name.
        config: The model configuration dictionary.

    Returns:
        Parsed Model, or None if invalid.
    """
    if "table" not in config:
        return None

    fields: dict[str, ModelField] = {}
    relationships: dict[str, ModelRelationship] = {}

    # Parse fields
    fields_config = config.get("fields", {})
    for field_name, field_config in fields_config.items():
        if isinstance(field_config, dict):
            fields[field_name] = {
                "name": field_name,
                "type": field_config.get("type", "str"),
                "nullable": field_config.get("nullable", False),
                "validate": field_config.get("validate", []),
            }

    # Parse relationships
    rels_config = config.get("relationships", {})
    for rel_name, rel_config in rels_config.items():
        if isinstance(rel_config, dict):
            relationships[rel_name] = {
                "name": rel_name,
                "type": rel_config.get("type", "has_one"),
                "target": rel_config.get("target", ""),
                "foreign_key": rel_config.get("foreign_key", ""),
            }

    return {
        "name": name,
        "table": config["table"],
        "fields": fields,
        "relationships": relationships,
    }


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
    suggestion = suggest_similar(field_name, all_names)

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
    suggestion = suggest_similar(rel_name, list(model["relationships"].keys()))

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
        schema_type: Type from TOML schema.
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
    suggestion = suggest_similar(column_name, list(model["fields"].keys()))

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
