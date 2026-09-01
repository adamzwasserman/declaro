"""
Schema file loading and saving.

Handles TOML schema files, snapshots, and decision files.
"""

import re
import tomllib
from collections.abc import Callable
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path
from typing import Any, cast

import tomli_w

from declaro_persistum.boundary import boundary, helper, recognizer
from declaro_persistum.exceptions import LoaderError
from declaro_persistum.types import (
    NOTHING,
    Column,
    Decision,
    Engine,
    Nothing,
    Schema,
    Table,
    View,
    resolved,
    type_of,
    written_as,
)

# THE THREE ROOT FILES THAT ARE NOT A DECLARATION. `snapshot.toml` is what
# was last applied and `pending.toml` is an unanswered question, so reading
# either as a declaration would compare a state against itself. `schema.toml`
# is a declaration and is read just before this list is used, so it is here
# only to keep it from being read twice.
_NOT_A_DECLARATION = ("snapshot.toml", "pending.toml", "schema.toml")


@helper
def _refuse_a_missing_directory(schema_path: Path) -> Path:
    """The caller named a directory that is not there."""
    raise LoaderError("Schema directory does not exist", path=str(schema_path))


@helper
def _the_directory(schema_path: Path) -> Path:
    """The directory is there, so it is the one to read."""
    return schema_path


_THE_SCHEMA_DIRECTORY: dict[bool, Callable[[Path], Path]] = {
    True: _the_directory,
    False: _refuse_a_missing_directory,
}


@helper
def _refuse_an_empty_schema(_schema: Schema, schema_path: Path) -> Schema:
    """Every file was read and none of them declared a table."""
    raise LoaderError(
        f"No schema files found. Expected .toml files in {schema_path} or {schema_path}/tables/",
        path=str(schema_path),
    )


@helper
def _the_schema(schema: Schema, _schema_path: Path) -> Schema:
    """At least one table was declared, so this is the schema."""
    return schema


_A_SCHEMA_WAS_FOUND: dict[bool, Callable[[Schema, Path], Schema]] = {
    True: _the_schema,
    False: _refuse_an_empty_schema,
}


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
    named = Path(schema_dir)
    schema_path = _THE_SCHEMA_DIRECTORY[named.exists()](named)

    # THE READING ORDER, AS A LIST. `tables/` first, then `schema.toml`, then
    # every other `.toml` in the root. A later file wins over an earlier one
    # declaring the same table, which is what the three `dict.update` calls
    # this replaces did.
    #
    # `glob` on a directory that is not there yields nothing, and so does
    # `glob` on a file, so `tables/` needs no test before it is read.
    declarations = [
        *(schema_path / "tables").glob("*.toml"),
        *[root for root in [schema_path / "schema.toml"] if root.exists()],
        *[other for other in schema_path.glob("*.toml") if other.name not in _NOT_A_DECLARATION],
    ]

    schema: Schema = {
        table_name: table
        for toml_file in declarations
        for table_name, table in _load_toml_file(toml_file).items()
    }

    return _A_SCHEMA_WAS_FOUND[bool(schema)](schema, schema_path)


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

    return {
        key: _READ_A_TABLE[spelling](value, key)
        for key, value in data.items()
        for spelling in _spelling_of(key, value)
    }


@recognizer
def _declares_a_columns_section(value: Any) -> bool:
    """The long spelling: `[users.columns]` with the columns under it."""
    return written_as(dict, value) and "columns" in value


@recognizer
def _declares_columns_directly(value: Any) -> bool:
    """The short spelling: the entries under `[users]` are the columns."""
    return written_as(dict, value) and any(
        written_as(dict, v) and "type" in v for v in value.values()
    )


# THE TWO SPELLINGS OF A TABLE, IN THE ORDER THEY ARE TRIED. A file entry
# written in the long spelling also looks like the short one when a column is
# called `columns`, so the long spelling is asked first.
_TABLE_SPELLINGS: dict[str, Callable[[Any], bool]] = {
    "columns section": _declares_a_columns_section,
    "columns directly": _declares_columns_directly,
}


@recognizer
def _spelling_of(key: str, value: Any) -> list[str]:
    """Which spelling this file entry is written in, as a list of nought or one.

    A key starting with `_` is metadata rather than a table, and an entry in
    neither spelling is not a table either. Both come back empty, so the
    comprehension reading this passes over them, which is what the `continue`
    and the missing `else` did before.
    """
    return [
        name
        for name, is_written_that_way in _TABLE_SPELLINGS.items()
        if not key.startswith("_") and is_written_that_way(value)
    ][:1]


@helper
def _table_from_a_columns_section(value: Any, key: str) -> Table:
    """Read the long spelling, which carries the table's other fields too."""
    return _normalize_table(value, key)


@helper
def _table_from_columns_alone(value: Any, key: str) -> Table:
    """Read the short spelling, which carries columns and nothing else."""
    return cast(Table, {"columns": _normalize_columns(value, key)})


_READ_A_TABLE: dict[str, Callable[[Any, str], Table]] = {
    "columns section": _table_from_a_columns_section,
    "columns directly": _table_from_columns_alone,
}


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
def _column_from_table(col_name: str, table_name: str, col_data: dict[str, Any]) -> Column:
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
    typed = _REFUSE_UNTYPED[("type" in col_data)](col_name, table_name, col_data)
    declared: Column = {"type": typed["type"]}
    declared.update(
        {name: col_data[name] for name in _COLUMN_FIELDS if name in col_data}  # type: ignore[typeddict-item]
    )
    return declared


@helper
def _column_from_type_name(_col_name: str, _table_name: str, col_data: str) -> Column:
    """The short spelling: the value is the type and there is nothing else."""
    return {"type": col_data}


@helper
def _refuse_untyped(col_name: str, table_name: str, col_data: dict[str, Any]) -> dict[str, Any]:
    """A column table with no `type` key."""
    raise LoaderError(
        f"Column '{col_name}' in table '{table_name}' declares no type. It "
        f"carries {sorted(col_data)}; a column needs `type`, either as "
        f'`{col_name} = {{ type = "text" }}` or the short form '
        f'`{col_name} = "text"`.',
        path=NOTHING,
    )


@helper
def _the_typed_column(_col_name: str, _table_name: str, col_data: dict[str, Any]) -> dict[str, Any]:
    """A column table carrying `type`, handed back to be read."""
    return col_data


_REFUSE_UNTYPED: dict[bool, Callable[[str, str, dict[str, Any]], dict[str, Any]]] = {
    True: _the_typed_column,
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
def _refuse_unknown_columns(unknown: dict[str, str], table_name: str) -> dict[str, str]:
    """Columns written in neither spelling, named so the author can find them.

    They used to produce no column, no error and no warning, so the author
    got a table missing a column they had declared and found out when an
    INSERT failed on a name the database had never heard of.
    """
    written = ", ".join(f"'{name}' ({shape})" for name, shape in sorted(unknown.items()))
    raise LoaderError(
        f"Table '{table_name}' declares {written}, which is neither spelling "
        f'of a column. Write either `name = "text"` or '
        f"`[{table_name}.columns.name]` with a `type`.",
        path=NOTHING,
    )


@helper
def _the_known_columns(unknown: dict[str, str], _table_name: str) -> dict[str, str]:
    """Every column is written in one of the two spellings, so none is unknown."""
    return unknown


_WHEN_UNKNOWN: dict[bool, Callable[[dict[str, str], str], dict[str, str]]] = {
    True: _refuse_unknown_columns,
    False: _the_known_columns,
}


@helper
def _normalize_columns(columns_data: dict[str, Any], table_name: str) -> dict[str, Column]:
    """Normalize column definitions from TOML.

    THE TABLE NAME IS PASSED SO A REFUSAL CAN SAY WHERE. It was not needed
    while a missing type was answered with a default, which is the point: a
    function that never refuses never needs to say which line to look at.
    """
    unknown = {
        col_name: type_of(col_data).__name__
        for col_name, col_data in columns_data.items()
        if type_of(col_data) not in _COLUMN_FROM
    }
    _WHEN_UNKNOWN[bool(unknown)](unknown, table_name)

    return {
        col_name: _COLUMN_FROM[type_of(col_data)](col_name, table_name, col_data)
        for col_name, col_data in columns_data.items()
    }


@helper
def _primary_key_of(written: Any, _table_name: str) -> list[str]:
    """One name or several, resolved to the list everything downstream reads."""
    return _NAMES_OF[type_of(written)](written)


# THE FIELDS THAT ARE READ RATHER THAN CARRIED, each with the function that
# reads it. Both take the declared value and the table name, so a refusal in
# either can say where.
_TABLE_READERS: dict[str, Callable[[Any, str], Any]] = {
    "columns": _normalize_columns,
    "primary_key": _primary_key_of,
}


@helper
def _refuse_a_missing_snapshot(snapshot_path: Path) -> Path:
    """There is no snapshot, so there is no state to compare against."""
    raise LoaderError("Snapshot file does not exist", path=str(snapshot_path))


@helper
def _the_snapshot(snapshot_path: Path) -> Path:
    """The snapshot is there, so it is the file to read."""
    return snapshot_path


_THE_SNAPSHOT_FILE: dict[bool, Callable[[Path], Path]] = {
    True: _the_snapshot,
    False: _refuse_a_missing_snapshot,
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
    named = Path(schema_dir) / "snapshot.toml"
    snapshot_path = _THE_SNAPSHOT_FILE[named.exists()](named)

    try:
        content = snapshot_path.read_text()
        data = tomllib.loads(content)
    except tomllib.TOMLDecodeError as e:
        raise LoaderError(f"Invalid snapshot TOML: {e}", path=str(snapshot_path)) from e

    # Everything except `_meta`, which is the stamp rather than a table.
    return {
        key: _normalize_table(value, key)
        for key, value in data.items()
        if key != "_meta" and isinstance(value, dict)
    }


@boundary
def save_snapshot(
    schema_dir: str | Path,
    schema: Schema,
    engine: Engine,
    *,
    applied_by: str | None | Nothing,
) -> None:
    """
    Save schema as snapshot file.

    Args:
        schema_dir: Path to schema directory
        schema: Schema to save
        engine: Database engine
        applied_by: User/system identifier
    """
    # A default in the signature would decide this where no reader
    # sees it happen and no test reaches it.
    applied_by = resolved(applied_by, None)
    schema_path = Path(schema_dir)
    schema_path.mkdir(parents=True, exist_ok=True)

    snapshot_path = schema_path / "snapshot.toml"

    # Build snapshot data with metadata
    # WHO APPLIED IT IS WRITTEN ONLY WHEN THERE IS A NAME. The comprehension
    # over a list of nought or one is the whole of that.
    data: dict[str, Any] = {
        "_meta": {
            "version": "1.0.0",
            "applied_at": datetime.now(UTC).isoformat(),
            "engine": engine,
            **{field: applied_by for field in ["applied_by"] if applied_by},
        }
    }

    # Add all tables
    data.update(schema)

    # Write with header comment
    content = "# AUTO-GENERATED - DO NOT EDIT MANUALLY\n"
    content += f"# Last applied: {data['_meta']['applied_at']}\n\n"
    content += tomli_w.dumps(data)

    snapshot_path.write_text(content)


@helper
def _no_pending_file(_pending_path: Path) -> dict[str, Decision]:
    """There is no pending file, so nothing is waiting to be decided."""
    return {}


@boundary
def _read_pending_decisions(pending_path: Path) -> dict[str, Decision]:
    """Read the decisions an operator has already answered."""
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

    return {
        key: {
            "type": value.get("type", "keep"),
            "table": value.get("table", ""),
            "from_column": value.get("from_column"),
            "to_column": value.get("to_column"),
            "column": value.get("column"),
            "decided_at": value.get("decided_at", ""),
        }
        for key, value in data.get("decisions", {}).items()
        if isinstance(value, dict)
    }


_THE_PENDING_DECISIONS: dict[bool, Callable[[Path], dict[str, Decision]]] = {
    True: _read_pending_decisions,
    False: _no_pending_file,
}


@helper
def load_decisions(schema_dir: str | Path) -> dict[str, Decision]:
    """
    Load pending decisions file.

    Args:
        schema_dir: Path to schema directory

    Returns:
        Dict of decision_id -> Decision
    """
    pending_path = Path(schema_dir) / "migrations" / "pending.toml"
    return _THE_PENDING_DECISIONS[pending_path.exists()](pending_path)


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

    # Add timestamps to decisions. `setdefault` is the whole of "write it if
    # it is not already there", so the test and the write are one call.
    timestamp = datetime.now(UTC).isoformat()
    _stamped = [
        decision.setdefault("decided_at", timestamp)
        for decision in decisions.values()
        if isinstance(decision, dict)
    ]

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


_VALID_REFRESH_STRATEGIES = {"on_demand", "on_commit", "manual", "trigger", "hybrid"}

# EVERY WAY A VIEW CAN BE WRONG, IN THE ORDER THEY ARE ASKED. Each entry is
# the question and the complaint it produces. Five `if ... raise` blocks said
# the same five things, and each one hid its own answer inside a branch.
_VIEW_FAULTS: tuple[tuple[Callable[[View], bool], Callable[[View], str]], ...] = (
    (
        lambda view: not view.get("query"),
        lambda _view: "View must have a 'query'",
    ),
    (
        lambda view: not _is_valid_identifier(view.get("name", "")),
        lambda view: f"View name '{view.get('name', '')}' is not a valid identifier",
    ),
    (
        lambda view: bool(view.get("refresh")) and not view.get("materialized"),
        lambda _view: "View 'refresh' requires 'materialized' to be true",
    ),
    (
        lambda view: bool(view.get("refresh"))
        and view.get("refresh") not in _VALID_REFRESH_STRATEGIES,
        lambda view: (
            f"View refresh must be one of {_VALID_REFRESH_STRATEGIES}, got '{view.get('refresh')}'"
        ),
    ),
    (
        lambda view: bool(view.get("trigger_sources"))
        and view.get("refresh") not in ("trigger", "hybrid"),
        lambda _view: "View 'trigger_sources' requires refresh='trigger' or 'hybrid'",
    ),
)


@helper
def _refuse_a_broken_view(complaints: list[str]) -> list[str]:
    """The first fault found, named the way it was named before."""
    raise ValueError(complaints[0])


@helper
def _the_valid_view(complaints: list[str]) -> list[str]:
    """No fault was found, so there is nothing to say."""
    return complaints


_A_VIEW_IS_BROKEN: dict[bool, Callable[[list[str]], list[str]]] = {
    True: _refuse_a_broken_view,
    False: _the_valid_view,
}


@helper
def validate_view(view: View) -> None:
    """
    Validate view definition.

    Args:
        view: View to validate

    Raises:
        ValueError: If validation fails
    """
    # `islice` keeps the questions lazy, so a later one is never asked about a
    # view an earlier one has already refused. The five `if ... raise` blocks
    # stopped at the first fault and so does this.
    complaints = list(
        islice(
            (complain(view) for is_wrong, complain in _VIEW_FAULTS if is_wrong(view)),
            1,
        )
    )
    _A_VIEW_IS_BROKEN[bool(complaints)](complaints)
