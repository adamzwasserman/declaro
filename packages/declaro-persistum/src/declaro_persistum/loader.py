"""
Schema file loading and saving.

Handles TOML schema files, snapshots, and decision files.
"""

import re
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import tomli_w

from declaro_persistum.exceptions import LoaderError
from declaro_persistum.types import (
    Column,
    Decision,
    Enum,
    Parameter,
    Procedure,
    Schema,
    Table,
    Trigger,
    View,
)


def load_schema(schema_dir: str | Path) -> Schema:
    """
    Load schema from TOML files in a directory.

    Expects structure:
        schema_dir/
            tables/
                users.toml
                orders.toml
                ...

    Or a single schema.toml file with all tables.

    Args:
        schema_dir: Path to schema directory

    Returns:
        Schema dict with all table definitions

    Raises:
        LoaderError: If files cannot be read or parsed
    """
    schema_path = Path(schema_dir)

    if not schema_path.exists():
        raise LoaderError("Schema directory does not exist", path=str(schema_path))

    schema: Schema = {}

    # Check for tables/ subdirectory
    tables_dir = schema_path / "tables"
    if tables_dir.exists() and tables_dir.is_dir():
        for toml_file in tables_dir.glob("*.toml"):
            table_schema = _load_toml_file(toml_file)
            schema.update(table_schema)

    # Also check for schema.toml in root
    schema_file = schema_path / "schema.toml"
    if schema_file.exists():
        root_schema = _load_toml_file(schema_file)
        schema.update(root_schema)

    # Check for any .toml files in root (except snapshot and pending)
    for toml_file in schema_path.glob("*.toml"):
        if toml_file.name in ("snapshot.toml", "pending.toml"):
            continue
        if toml_file.name == "schema.toml":
            continue  # Already handled
        table_schema = _load_toml_file(toml_file)
        schema.update(table_schema)

    if not schema:
        raise LoaderError(
            f"No schema files found. Expected .toml files in {schema_path} or {schema_path}/tables/",
            path=str(schema_path),
        )

    return schema


def _load_toml_file(path: Path) -> Schema:
    """Load a single TOML file and return its schema."""
    try:
        content = path.read_text()
        data = tomllib.loads(content)
    except tomllib.TOMLDecodeError as e:
        raise LoaderError(f"Invalid TOML syntax: {e}", path=str(path)) from e
    except OSError as e:
        raise LoaderError(f"Cannot read file: {e}", path=str(path)) from e

    # Filter out metadata keys
    schema: Schema = {}
    for key, value in data.items():
        if key.startswith("_"):
            continue
        if isinstance(value, dict) and "columns" in value:
            schema[key] = _normalize_table(value)
        elif isinstance(value, dict) and any(isinstance(v, dict) and "type" in v for v in value.values()):
            # Might be a table without explicit columns section
            # Check if it has column-like structure
            # It's columns directly
            schema[key] = {"columns": _normalize_columns(value)}

    return schema


def _normalize_table(table_data: dict[str, Any]) -> Table:
    """Normalize table data from TOML to Table type."""
    table: Table = {}

    if "columns" in table_data:
        table["columns"] = _normalize_columns(table_data["columns"])

    if "primary_key" in table_data:
        pk = table_data["primary_key"]
        if isinstance(pk, str):
            table["primary_key"] = [pk]
        else:
            table["primary_key"] = list(pk)

    if "indexes" in table_data:
        table["indexes"] = table_data["indexes"]

    if "constraints" in table_data:
        table["constraints"] = table_data["constraints"]

    if "renamed_from" in table_data:
        table["renamed_from"] = table_data["renamed_from"]

    return table


def _normalize_columns(columns_data: dict[str, Any]) -> dict[str, Column]:
    """Normalize column definitions from TOML."""
    columns: dict[str, Column] = {}

    for col_name, col_data in columns_data.items():
        if isinstance(col_data, dict):
            col: Column = {}

            # Required field
            if "type" in col_data:
                col["type"] = col_data["type"]
            else:
                col["type"] = "text"  # Default

            # Optional fields
            if "nullable" in col_data:
                col["nullable"] = col_data["nullable"]

            if "default" in col_data:
                col["default"] = col_data["default"]

            if "primary_key" in col_data:
                col["primary_key"] = col_data["primary_key"]

            if "unique" in col_data:
                col["unique"] = col_data["unique"]

            if "references" in col_data:
                col["references"] = col_data["references"]

            if "on_delete" in col_data:
                col["on_delete"] = col_data["on_delete"]

            if "on_update" in col_data:
                col["on_update"] = col_data["on_update"]

            if "check" in col_data:
                col["check"] = col_data["check"]

            # Migration hints
            if "renamed_from" in col_data:
                col["renamed_from"] = col_data["renamed_from"]

            if "is_new" in col_data:
                col["is_new"] = col_data["is_new"]

            columns[col_name] = col
        elif isinstance(col_data, str):
            # Shorthand: just the type
            columns[col_name] = {"type": col_data}

    return columns


def load_snapshot(schema_dir: str | Path) -> Schema:
    """
    Load the schema snapshot file.

    Args:
        schema_dir: Path to schema directory

    Returns:
        Schema from the snapshot

    Raises:
        LoaderError: If snapshot doesn't exist or is invalid
    """
    snapshot_path = Path(schema_dir) / "snapshot.toml"

    if not snapshot_path.exists():
        raise LoaderError("Snapshot file does not exist", path=str(snapshot_path))

    try:
        content = snapshot_path.read_text()
        data = tomllib.loads(content)
    except tomllib.TOMLDecodeError as e:
        raise LoaderError(f"Invalid snapshot TOML: {e}", path=str(snapshot_path)) from e

    # Extract schema (everything except _meta)
    schema: Schema = {}
    for key, value in data.items():
        if key == "_meta":
            continue
        if isinstance(value, dict):
            schema[key] = _normalize_table(value)

    return schema


def save_snapshot(
    schema_dir: str | Path,
    schema: Schema,
    dialect: str,
    *,
    applied_by: str | None = None,
) -> None:
    """
    Save schema as snapshot file.

    Args:
        schema_dir: Path to schema directory
        schema: Schema to save
        dialect: Database dialect
        applied_by: User/system identifier
    """
    schema_path = Path(schema_dir)
    schema_path.mkdir(parents=True, exist_ok=True)

    snapshot_path = schema_path / "snapshot.toml"

    # Build snapshot data with metadata
    data: dict[str, Any] = {
        "_meta": {
            "version": "1.0.0",
            "applied_at": datetime.now(UTC).isoformat(),
            "dialect": dialect,
        }
    }

    if applied_by:
        data["_meta"]["applied_by"] = applied_by

    # Add all tables
    data.update(schema)

    # Write with header comment
    content = "# AUTO-GENERATED - DO NOT EDIT MANUALLY\n"
    content += f"# Last applied: {data['_meta']['applied_at']}\n\n"
    content += tomli_w.dumps(data)

    snapshot_path.write_text(content)


def load_decisions(schema_dir: str | Path) -> dict[str, Decision]:
    """
    Load pending decisions file.

    Args:
        schema_dir: Path to schema directory

    Returns:
        Dict of decision_id -> Decision
    """
    pending_path = Path(schema_dir) / "migrations" / "pending.toml"

    if not pending_path.exists():
        return {}

    try:
        content = pending_path.read_text()
        data = tomllib.loads(content)
    except (tomllib.TOMLDecodeError, OSError):
        return {}

    decisions: dict[str, Decision] = {}

    for key, value in data.get("decisions", {}).items():
        if isinstance(value, dict):
            decisions[key] = {
                "type": value.get("type", "keep"),
                "table": value.get("table", ""),
                "from_column": value.get("from_column"),
                "to_column": value.get("to_column"),
                "column": value.get("column"),
                "decided_at": value.get("decided_at", ""),
            }

    return decisions


def save_decisions(schema_dir: str | Path, decisions: dict[str, Any]) -> None:
    """
    Save decisions to pending file.

    Args:
        schema_dir: Path to schema directory
        decisions: Decisions to save
    """
    migrations_dir = Path(schema_dir) / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)

    pending_path = migrations_dir / "pending.toml"

    # Add timestamps to decisions
    timestamp = datetime.now(UTC).isoformat()
    for decision in decisions.values():
        if isinstance(decision, dict) and "decided_at" not in decision:
            decision["decided_at"] = timestamp

    data = {"decisions": decisions}

    content = "# Ephemeral decisions - purged after migration applied\n\n"
    content += tomli_w.dumps(data)

    pending_path.write_text(content)


def clear_decisions(schema_dir: str | Path) -> None:
    """
    Clear pending decisions after successful migration.

    Args:
        schema_dir: Path to schema directory
    """
    pending_path = Path(schema_dir) / "migrations" / "pending.toml"

    if pending_path.exists():
        pending_path.unlink()


# =============================================================================
# Extended Schema Objects (Addendum)
# =============================================================================

_VALID_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _is_valid_identifier(name: str) -> bool:
    """Check if name is a valid SQL identifier."""
    return bool(_VALID_IDENTIFIER.match(name))


def parse_enum(data: dict[str, Any]) -> Enum:
    """
    Parse enum definition from TOML data.

    Args:
        data: Dict with name and values

    Returns:
        Enum TypedDict
    """
    enum: Enum = {
        "name": data["name"],
        "values": list(data["values"]),
    }
    if "description" in data:
        enum["description"] = data["description"]
    return enum


def validate_enum(enum: Enum) -> None:
    """
    Validate enum definition.

    Args:
        enum: Enum to validate

    Raises:
        ValueError: If validation fails
    """
    if not enum.get("values"):
        raise ValueError(f"Enum '{enum.get('name', 'unknown')}' must have at least one value")

    values = enum["values"]
    if len(values) != len(set(values)):
        raise ValueError(f"Enum '{enum.get('name', 'unknown')}' has duplicate values")

    name = enum.get("name", "")
    if not _is_valid_identifier(name):
        raise ValueError(f"Enum name '{name}' is not a valid identifier")


def load_enums(enums_dir: str | Path) -> dict[str, Enum]:
    """
    Load enums from schema/types/enums.toml or types/ directory.

    Args:
        enums_dir: Path to types directory

    Returns:
        Dict of enum_name -> Enum
    """
    enums_path = Path(enums_dir)
    enums: dict[str, Enum] = {}

    # Check for enums.toml
    enums_file = enums_path / "enums.toml"
    if enums_file.exists():
        try:
            content = enums_file.read_text()
            data = tomllib.loads(content)
            for name, enum_data in data.items():
                if name.startswith("_"):
                    continue
                if isinstance(enum_data, dict) and "values" in enum_data:
                    enum_data["name"] = name
                    enum = parse_enum(enum_data)
                    validate_enum(enum)
                    enums[name] = enum
        except tomllib.TOMLDecodeError as e:
            raise LoaderError(f"Invalid enums TOML: {e}", path=str(enums_file)) from e

    return enums


def parse_trigger(name: str, data: dict[str, Any]) -> Trigger:
    """
    Parse trigger definition from TOML data.

    Args:
        name: Trigger name
        data: Trigger definition

    Returns:
        Trigger TypedDict
    """
    trigger: Trigger = {
        "name": name,
        "timing": data.get("timing", "before"),
        "event": data.get("event", "insert"),
        "for_each": data.get("for_each", "row"),
    }

    if "when" in data:
        trigger["when"] = data["when"]
    if "body" in data:
        trigger["body"] = data["body"]
    if "execute" in data:
        trigger["execute"] = data["execute"]

    return trigger


def validate_trigger(trigger: Trigger) -> None:
    """
    Validate trigger definition.

    Args:
        trigger: Trigger to validate

    Raises:
        ValueError: If validation fails
    """
    valid_timings = {"before", "after", "instead_of"}
    timing = trigger.get("timing", "")
    if timing not in valid_timings:
        raise ValueError(f"Trigger timing must be one of {valid_timings}, got '{timing}'")

    valid_events = {"insert", "update", "delete"}
    event = trigger.get("event", "")
    if isinstance(event, str):
        if event not in valid_events:
            raise ValueError(f"Trigger event must be one of {valid_events}, got '{event}'")
    elif isinstance(event, list):
        for e in event:
            if e not in valid_events:
                raise ValueError(f"Trigger event must be one of {valid_events}, got '{e}'")

    if not trigger.get("body") and not trigger.get("execute"):
        raise ValueError("Trigger must have either 'body' or 'execute'")


def parse_procedure(name: str, data: dict[str, Any]) -> Procedure:
    """
    Parse procedure definition from TOML data.

    Args:
        name: Procedure name
        data: Procedure definition

    Returns:
        Procedure TypedDict
    """
    procedure: Procedure = {
        "name": name,
        "language": data.get("language", "sql"),
        "returns": data.get("returns", "void"),
        "body": data.get("body", ""),
    }

    if "parameters" in data:
        params: list[Parameter] = []
        for p in data["parameters"]:
            param: Parameter = {
                "name": p["name"],
                "type": p["type"],
            }
            if "default" in p:
                param["default"] = p["default"]
            params.append(param)
        procedure["parameters"] = params

    return procedure


def validate_procedure(procedure: Procedure) -> None:
    """
    Validate procedure definition.

    Args:
        procedure: Procedure to validate

    Raises:
        ValueError: If validation fails
    """
    valid_languages = {"sql", "plpgsql"}
    language = procedure.get("language", "")
    if language not in valid_languages:
        raise ValueError(f"Procedure language must be one of {valid_languages}, got '{language}'")

    if not procedure.get("body"):
        raise ValueError("Procedure must have a 'body'")

    if not procedure.get("returns"):
        raise ValueError("Procedure must have 'returns' type")

    for param in procedure.get("parameters", []):
        if not param.get("name"):
            raise ValueError("Parameter must have a 'name'")
        if not param.get("type"):
            raise ValueError(f"Parameter '{param.get('name')}' must have a 'type'")


def load_procedures(procedures_dir: str | Path) -> dict[str, Procedure]:
    """
    Load procedures from schema/procedures/ directory.

    Args:
        procedures_dir: Path to procedures directory

    Returns:
        Dict of procedure_name -> Procedure
    """
    proc_path = Path(procedures_dir)
    procedures: dict[str, Procedure] = {}

    if not proc_path.exists():
        return procedures

    for toml_file in proc_path.glob("*.toml"):
        try:
            content = toml_file.read_text()
            data = tomllib.loads(content)
            name = toml_file.stem
            procedure = parse_procedure(name, data)
            validate_procedure(procedure)
            procedures[name] = procedure
        except tomllib.TOMLDecodeError as e:
            raise LoaderError(f"Invalid procedure TOML: {e}", path=str(toml_file)) from e

    return procedures


def parse_view(name: str, data: dict[str, Any]) -> View:
    """
    Parse view definition from TOML data.

    Args:
        name: View name
        data: View definition

    Returns:
        View TypedDict
    """
    view: View = {
        "name": name,
        "query": data.get("query", ""),
    }

    if "materialized" in data:
        view["materialized"] = data["materialized"]
    if "refresh" in data:
        view["refresh"] = data["refresh"]
    if "depends_on" in data:
        view["depends_on"] = list(data["depends_on"])

    return view


def validate_view(view: View) -> None:
    """
    Validate view definition.

    Args:
        view: View to validate

    Raises:
        ValueError: If validation fails
    """
    if not view.get("query"):
        raise ValueError("View must have a 'query'")

    name = view.get("name", "")
    if not _is_valid_identifier(name):
        raise ValueError(f"View name '{name}' is not a valid identifier")

    refresh = view.get("refresh")
    if refresh and not view.get("materialized"):
        raise ValueError("View 'refresh' requires 'materialized' to be true")

    # Validate refresh strategy values
    valid_strategies = {"on_demand", "on_commit", "manual", "trigger", "hybrid"}
    if refresh and refresh not in valid_strategies:
        raise ValueError(
            f"View refresh must be one of {valid_strategies}, got '{refresh}'"
        )

    # trigger_sources requires trigger or hybrid strategy
    if view.get("trigger_sources") and refresh not in ("trigger", "hybrid"):
        raise ValueError(
            "View 'trigger_sources' requires refresh='trigger' or 'hybrid'"
        )


def load_views(views_dir: str | Path) -> dict[str, View]:
    """
    Load views from schema/views/ directory.

    Args:
        views_dir: Path to views directory

    Returns:
        Dict of view_name -> View
    """
    views_path = Path(views_dir)
    views: dict[str, View] = {}

    if not views_path.exists():
        return views

    for toml_file in views_path.glob("*.toml"):
        try:
            content = toml_file.read_text()
            data = tomllib.loads(content)
            name = toml_file.stem
            view = parse_view(name, data)
            validate_view(view)
            views[name] = view
        except tomllib.TOMLDecodeError as e:
            raise LoaderError(f"Invalid view TOML: {e}", path=str(toml_file)) from e

    return views
