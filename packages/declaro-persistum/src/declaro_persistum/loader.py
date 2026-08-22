"""
Schema file loading and saving.

Handles TOML schema files, snapshots, and decision files.
"""

import re
import tomllib
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import tomli_w

from declaro_persistum.boundary import boundary, helper, recognizer
from declaro_persistum.exceptions import LoaderError
from declaro_persistum.types import (
    NOTHING,
    Column,
    Decision,
    Dialect,
    Nothing,
    Schema,
    Table,
    View,
    resolved,
    spelling_of,
)


@boundary
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


@boundary
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
            schema[key] = _normalize_table(value, key)
        elif isinstance(value, dict) and any(isinstance(v, dict) and "type" in v for v in value.values()):
            # Might be a table without explicit columns section
            # Check if it has column-like structure
            # It's columns directly
            schema[key] = {"columns": _normalize_columns(value, key)}

    return schema


# A PRIMARY KEY IS ONE NAME OR SEVERAL, AND BOTH MEAN A LIST. Two spellings
# of one thing is a vocabulary, and the boundary is where it is resolved into
# the single form everything downstream reads.
_NAMES_OF: dict[type, Callable[[Any], list[str]]] = {
    str: lambda written: [written],
    list: list,
}

# CARRIED THROUGH UNREAD, so this is a list rather than four `if name in`
# lines that each do the same thing.
_TABLE_FIELDS = ("indexes", "constraints", "renamed_from")




@helper
def _normalize_table(table_data: dict[str, Any], table_name: str) -> Table:
    """Normalize table data from TOML to Table type.

    The name is carried through only so a refusal below can name the table.
    """
    carried = {name: table_data[name] for name in _TABLE_FIELDS if name in table_data}
    read = {
        name: _TABLE_READERS[name](table_data[name], table_name)
        for name in _TABLE_READERS
        if name in table_data
    }
    return cast(Table, carried | read)


# THE OPTIONAL FIELDS, AS A LIST RATHER THAN NINE `if name in col_data`
# LINES. Each one was the same shape, so the vocabulary is what varies and it
# is written once. A key the author did not write is absent from the
# comprehension; a key they did write is carried through unread.
_COLUMN_FIELDS = (
    "nullable",
    "default",
    "primary_key",
    "unique",
    "references",
    "on_delete",
    "on_update",
    "check",
    "renamed_from",
    "is_new",
)


@helper
def _column_from_table(
    col_name: str, table_name: str, col_data: dict[str, Any]
) -> Column:
    """The long spelling: a TOML table carrying `type` and whatever else.

    A MISSING TYPE IS A BROKEN SCHEMA, NOT A TEXT COLUMN. This read
    `col["type"] = "text"  # Default`, and the comment was accurate.

    It is the same fallback three BDD schema helpers were deleted for on
    2026-08-16, recorded in `test_the_bdd_schema_builder_refuses_a_broken_schema`:
    "A schema missing a type is a broken schema, and answering it with TEXT
    means every scenario built on that schema tests a table nobody declared."
    The helper was fixed and the loader it was helping to test was not.

    TEXT is the one type that accepts every value, so a typo -- `typ` for
    `type` -- produced a column that stores integers as strings and complains
    about none of it. Nothing later can tell that from a column the author
    meant to be TEXT.
    """
    _REFUSE_UNTYPED[("type" in col_data)](col_name, table_name, col_data)
    declared: Column = {"type": col_data["type"]}
    declared.update(
        {name: col_data[name] for name in _COLUMN_FIELDS if name in col_data}  # type: ignore[typeddict-item]
    )
    return declared


@helper
def _column_from_type_name(
    _col_name: str, _table_name: str, col_data: str
) -> Column:
    """The short spelling: the value is the type and there is nothing else."""
    return {"type": col_data}


@helper
def _refuse_untyped(col_name: str, table_name: str, col_data: dict[str, Any]) -> None:
    """A column table with no `type` key."""
    raise LoaderError(
        f"Column '{col_name}' in table '{table_name}' declares no type. It "
        f"carries {sorted(col_data)}; a column needs `type`, either as "
        f'`{col_name} = {{ type = "text" }}` or the short form '
        f'`{col_name} = "text"`.', path=NOTHING)


@helper
def _accept_typed(
    _col_name: str, _table_name: str, _col_data: dict[str, Any]
) -> None:
    """A column table carrying `type`, so there is nothing to refuse."""


_REFUSE_UNTYPED: dict[bool, Callable[[str, str, dict[str, Any]], None]] = {
    True: _accept_typed,
    False: _refuse_untyped,
}

# THE TWO SPELLINGS OF A COLUMN, KEYED BY EXACT TYPE. TOML carries no schema,
# so the loader is the only thing that can say which of the two an entry is.
# Reading this by subscript is what makes a third spelling a refusal instead
# of a silence: `.get` with a fallback would put the old behaviour back.
_COLUMN_FROM: dict[type, Callable[[str, str, Any], Column]] = {
    dict: _column_from_table,
    str: _column_from_type_name,
}


@helper
def _refuse_unknown_columns(unknown: dict[str, str], table_name: str) -> None:
    """Columns written in neither spelling, named so the author can find them.

    They used to produce no column, no error and no warning, so the author
    got a table missing a column they had declared and found out when an
    INSERT failed on a name the database had never heard of.
    """
    written = ", ".join(f"'{name}' ({shape})" for name, shape in sorted(unknown.items()))
    raise LoaderError(
        f"Table '{table_name}' declares {written}, which is neither spelling "
        f"of a column. Write either `name = \"text\"` or "
        f"`[{table_name}.columns.name]` with a `type`.", path=NOTHING)


@helper
def _accept_known_columns(_unknown: dict[str, str], _table_name: str) -> None:
    """Every column is written in one of the two spellings."""


_WHEN_UNKNOWN: dict[bool, Callable[[dict[str, str], str], None]] = {
    True: _refuse_unknown_columns,
    False: _accept_known_columns,
}


@helper
def _normalize_columns(
    columns_data: dict[str, Any], table_name: str
) -> dict[str, Column]:
    """Normalize column definitions from TOML.

    THE TABLE NAME IS PASSED SO A REFUSAL CAN SAY WHERE. It was not needed
    while a missing type was answered with a default, which is the point: a
    function that never refuses never needs to say which line to look at.
    """
    unknown = {
        col_name: spelling_of(col_data).__name__
        for col_name, col_data in columns_data.items()
        if spelling_of(col_data) not in _COLUMN_FROM
    }
    _WHEN_UNKNOWN[bool(unknown)](unknown, table_name)

    return {
        col_name: _COLUMN_FROM[spelling_of(col_data)](col_name, table_name, col_data)
        for col_name, col_data in columns_data.items()
    }


@helper
def _primary_key_of(written: Any, _table_name: str) -> list[str]:
    """One name or several, resolved to the list everything downstream reads."""
    return _NAMES_OF[spelling_of(written)](written)


# THE FIELDS THAT ARE READ RATHER THAN CARRIED, each with the function that
# reads it. Both take the declared value and the table name, so a refusal in
# either can say where.
_TABLE_READERS: dict[str, Callable[[Any, str], Any]] = {
    "columns": _normalize_columns,
    "primary_key": _primary_key_of,
}


@boundary
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
            schema[key] = _normalize_table(value, key)

    return schema


@boundary
def save_snapshot(
    schema_dir: str | Path,
    schema: Schema,
    dialect: Dialect,
    *,
    applied_by: str | None | Nothing,
) -> None:
    """
    Save schema as snapshot file.

    Args:
        schema_dir: Path to schema directory
        schema: Schema to save
        dialect: Database dialect
        applied_by: User/system identifier
    """
    # THE VISIBLE RESOLVE. A default would hide this inside the
    # signature, where no reader sees it happen and no test reaches it.
    applied_by = resolved(applied_by, None)
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


@boundary
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
    except tomllib.TOMLDecodeError as broken:
        # AN UNREADABLE FILE IS NOT AN EMPTY ONE. This returned `{}`, which is
        # also what a directory with no pending decisions returns, so the two
        # opposite situations had one answer.
        #
        # Decisions are how a rename is told from a drop. Discarding them
        # silently means every ambiguity is asked again, or resolved the other
        # way, and the other way drops the column and its rows.
        raise LoaderError(
            f"Cannot read the pending decisions: {broken}", path=str(pending_path)
        ) from broken
    except OSError as unreadable:
        raise LoaderError(
            f"Cannot read the pending decisions: {unreadable}",
            path=str(pending_path),
        ) from unreadable

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


@boundary
def clear_decisions(schema_dir: str | Path) -> None:
    """Delete the pending decisions, because they have been applied.

    THIS EXISTED, WAS DELETED IN `162d967` FOR HAVING NO CALLER, AND NOTHING
    TOOK THE JOB OVER. `migrations/pending.toml` survived a successful apply
    and `load_decisions` read it again on the next diff, so a decision made
    once about one rename went on answering for every later rename with the
    same key. A decision is an answer to a question that was asked; keeping it
    after the answer is spent means the next question is not asked at all.

    Deleting a file that is not there is not an error. The caller's intent is
    "there should be no pending decisions", and that is already true.
    """
    pending_path = Path(schema_dir) / "migrations" / "pending.toml"
    pending_path.unlink(missing_ok=True)


@boundary
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

    # TOML HAS NO NULL, AND THE PROMPT WRITES ONE ON EVERY DECISION.
    # `_resolve_ambiguities_interactive` sets every key on every decision and
    # fills the ones that do not apply with None -- a rename carries
    # `column: None`, a drop carries `from_column: None` and `to_column:
    # None`. `tomli_w` refuses them:
    #
    #     TypeError: Object of type 'NoneType' is not TOML serializable
    #
    # So the operator was asked whether a column was renamed or dropped, the
    # question that decides whether that column's rows survive, answered it,
    # and the process died while recording the answer. Nothing was saved, so
    # the next run asked again. Measured 2026-08-20; recorded as declaro-p42.
    #
    # A key with no value is OMITTED rather than written, which is how TOML
    # says the same thing, and `load_decisions` already reads an absent key
    # back as None. Dropping them here is the format's own spelling of the
    # value the writer meant.
    data = {
        "decisions": {
            key: {k: v for k, v in decision.items() if v is not None}
            if isinstance(decision, dict)
            else decision
            for key, decision in decisions.items()
        }
    }

    content = "# Ephemeral decisions - purged after migration applied\n\n"
    content += tomli_w.dumps(data)

    pending_path.write_text(content)




# =============================================================================
# Extended Schema Objects (Addendum)
# =============================================================================

_VALID_IDENTIFIER = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


@recognizer
def _is_valid_identifier(name: str) -> bool:
    """Check if name is a valid SQL identifier."""
    return bool(_VALID_IDENTIFIER.match(name))




















@helper
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
