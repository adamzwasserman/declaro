"""
Pydantic model loader for declaro_persistum.

Loads schema from Pydantic models decorated with @table.
Detects Literal types for automatic enum abstraction.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

from declaro_persistum.boundary import boundary, helper
from declaro_persistum.exceptions import LoaderError
from declaro_persistum.model_columns import (
    _COLUMN_CARRIED as _COLUMN_CARRIED,
)
from declaro_persistum.model_columns import (
    _COLUMN_DEFAULT as _COLUMN_DEFAULT,
)
from declaro_persistum.model_columns import (
    _COLUMN_FACETS as _COLUMN_FACETS,
)
from declaro_persistum.model_columns import (
    _COLUMN_FLAGS as _COLUMN_FLAGS,
)
from declaro_persistum.model_columns import (
    _COLUMN_HINTS as _COLUMN_HINTS,
)
from declaro_persistum.model_columns import (
    _COLUMN_MEMBERS as _COLUMN_MEMBERS,
)
from declaro_persistum.model_columns import (
    _FIELD_METADATA as _FIELD_METADATA,
)
from declaro_persistum.model_columns import (
    _FROM_A_PYTHON_DEFAULT as _FROM_A_PYTHON_DEFAULT,
)
from declaro_persistum.model_columns import (
    _INDEX_OPTIONAL as _INDEX_OPTIONAL,
)
from declaro_persistum.model_columns import (
    _LITERAL_MEMBERS as _LITERAL_MEMBERS,
)
from declaro_persistum.model_columns import (
    _META_ENTRY as _META_ENTRY,
)
from declaro_persistum.model_columns import (
    _META_INDEXES as _META_INDEXES,
)
from declaro_persistum.model_columns import (
    _NO_DEFAULT as _NO_DEFAULT,
)
from declaro_persistum.model_columns import (
    _NULLABILITY as _NULLABILITY,
)
from declaro_persistum.model_columns import (
    _SQL_DEFAULT as _SQL_DEFAULT,
)
from declaro_persistum.model_columns import (
    _TYPE_NAMED_IN_TEXT as _TYPE_NAMED_IN_TEXT,
)
from declaro_persistum.model_columns import (
    _UNION_SPELLINGS as _UNION_SPELLINGS,
)
from declaro_persistum.model_columns import (
    _UNWRAPPED as _UNWRAPPED,
)
from declaro_persistum.model_columns import (
    _VIEW_DESCRIBED as _VIEW_DESCRIBED,
)
from declaro_persistum.model_columns import (
    _VIEW_FIELD_TYPES as _VIEW_FIELD_TYPES,
)
from declaro_persistum.model_columns import (
    _VIEW_MUST_NOT_BE_BLANK as _VIEW_MUST_NOT_BE_BLANK,
)
from declaro_persistum.model_columns import (
    _WHEN_A_DEFAULT_IS_DECLARED as _WHEN_A_DEFAULT_IS_DECLARED,
)
from declaro_persistum.model_columns import (
    _WHEN_A_MEMBER_IS_NOT_A_STRING as _WHEN_A_MEMBER_IS_NOT_A_STRING,
)
from declaro_persistum.model_columns import (
    _WHEN_A_MODULE_HAS_NO_LOADER as _WHEN_A_MODULE_HAS_NO_LOADER,
)
from declaro_persistum.model_columns import (
    _WHEN_A_MODULE_IS_MISSING as _WHEN_A_MODULE_IS_MISSING,
)
from declaro_persistum.model_columns import (
    _WHEN_A_TABLE_IS_DECLARED_TWICE as _WHEN_A_TABLE_IS_DECLARED_TWICE,
)
from declaro_persistum.model_columns import (
    _WHEN_A_VIEW_FIELD_IS_BLANK as _WHEN_A_VIEW_FIELD_IS_BLANK,
)
from declaro_persistum.model_columns import (
    _WHEN_A_VIEW_FIELD_IS_THE_WRONG_TYPE as _WHEN_A_VIEW_FIELD_IS_THE_WRONG_TYPE,
)
from declaro_persistum.model_columns import (
    _WHEN_A_VIEW_KEY_IS_UNKNOWN as _WHEN_A_VIEW_KEY_IS_UNKNOWN,
)
from declaro_persistum.model_columns import (
    _WHEN_AN_INDEX_FIELD_IS_UNKNOWN as _WHEN_AN_INDEX_FIELD_IS_UNKNOWN,
)
from declaro_persistum.model_columns import (
    _WHEN_AN_INDEX_HAS_NO_COLUMNS as _WHEN_AN_INDEX_HAS_NO_COLUMNS,
)
from declaro_persistum.model_columns import (
    _WHEN_AN_INDEX_IS_UNNAMED as _WHEN_AN_INDEX_IS_UNNAMED,
)
from declaro_persistum.model_columns import (
    _WHEN_NO_DECLARED_TYPE as _WHEN_NO_DECLARED_TYPE,
)
from declaro_persistum.model_columns import (
    _WHEN_NO_SQL_SPELLING as _WHEN_NO_SQL_SPELLING,
)
from declaro_persistum.model_columns import (
    _WHEN_NOT_A_MODEL as _WHEN_NOT_A_MODEL,
)
from declaro_persistum.model_columns import (
    _WHEN_NOT_A_TABLE as _WHEN_NOT_A_TABLE,
)
from declaro_persistum.model_columns import (
    _WHEN_NOTHING_IS_DECLARED as _WHEN_NOTHING_IS_DECLARED,
)
from declaro_persistum.model_columns import (
    _WHEN_THE_KEY_IS_COMPOSITE as _WHEN_THE_KEY_IS_COMPOSITE,
)
from declaro_persistum.model_columns import (
    _WHEN_THE_MODELS_DIRECTORY_IS_MISSING as _WHEN_THE_MODELS_DIRECTORY_IS_MISSING,
)
from declaro_persistum.model_columns import (
    _WHEN_UNKNOWN_FACET as _WHEN_UNKNOWN_FACET,
)
from declaro_persistum.model_columns import (
    INDEX_FIELDS as INDEX_FIELDS,
)
from declaro_persistum.model_columns import (
    PYTHON_TO_SQL_TYPE as PYTHON_TO_SQL_TYPE,
)
from declaro_persistum.model_columns import (
    VIEW_OPTIONAL as VIEW_OPTIONAL,
)
from declaro_persistum.model_columns import (
    VIEW_REQUIRED as VIEW_REQUIRED,
)

# The tables and what each entry does. Re-exported so nothing that
# imports from here had to change.
from declaro_persistum.model_columns import (  # noqa: F401
    ColumnFacets as ColumnFacets,
)
from declaro_persistum.model_columns import (
    OnAction as OnAction,
)
from declaro_persistum.model_columns import (
    _check_view as _check_view,
)
from declaro_persistum.model_columns import (
    _column_of as _column_of,
)
from declaro_persistum.model_columns import (
    _named as _named,
)
from declaro_persistum.model_columns import (
    _no_column_default as _no_column_default,
)
from declaro_persistum.model_columns import (
    _no_column_members as _no_column_members,
)
from declaro_persistum.model_columns import (
    _no_declared_entry as _no_declared_entry,
)
from declaro_persistum.model_columns import (
    _no_declared_indexes as _no_declared_indexes,
)
from declaro_persistum.model_columns import (
    _no_literal_members as _no_literal_members,
)
from declaro_persistum.model_columns import (
    _no_metadata_list as _no_metadata_list,
)
from declaro_persistum.model_columns import (
    _no_python_default as _no_python_default,
)
from declaro_persistum.model_columns import (
    _not_a_table as _not_a_table,
)
from declaro_persistum.model_columns import (
    _one_declaration_per_table as _one_declaration_per_table,
)
from declaro_persistum.model_columns import (
    _refuse_a_blank_view_field as _refuse_a_blank_view_field,
)
from declaro_persistum.model_columns import (
    _refuse_a_class_pydantic_did_not_make as _refuse_a_class_pydantic_did_not_make,
)
from declaro_persistum.model_columns import (
    _refuse_a_directory_that_declares_nothing as _refuse_a_directory_that_declares_nothing,
)
from declaro_persistum.model_columns import (
    _refuse_a_member_that_is_not_a_string as _refuse_a_member_that_is_not_a_string,
)
from declaro_persistum.model_columns import (
    _refuse_a_missing_models_directory as _refuse_a_missing_models_directory,
)
from declaro_persistum.model_columns import (
    _refuse_a_missing_module as _refuse_a_missing_module,
)
from declaro_persistum.model_columns import (
    _refuse_a_module_with_no_loader as _refuse_a_module_with_no_loader,
)
from declaro_persistum.model_columns import (
    _refuse_a_value_with_no_sql_spelling as _refuse_a_value_with_no_sql_spelling,
)
from declaro_persistum.model_columns import (
    _refuse_a_view_field_of_the_wrong_type as _refuse_a_view_field_of_the_wrong_type,
)
from declaro_persistum.model_columns import (
    _refuse_an_index_over_no_columns as _refuse_an_index_over_no_columns,
)
from declaro_persistum.model_columns import (
    _refuse_an_unknown_index_field as _refuse_an_unknown_index_field,
)
from declaro_persistum.model_columns import (
    _refuse_an_unknown_view_key as _refuse_an_unknown_view_key,
)
from declaro_persistum.model_columns import (
    _refuse_an_unmapped_annotation as _refuse_an_unmapped_annotation,
)
from declaro_persistum.model_columns import (
    _refuse_an_unnamed_index as _refuse_an_unnamed_index,
)
from declaro_persistum.model_columns import (
    _refuse_two_models_claiming_one_table as _refuse_two_models_claiming_one_table,
)
from declaro_persistum.model_columns import (
    _refuse_unknown_facets as _refuse_unknown_facets,
)
from declaro_persistum.model_columns import (
    _the_annotated_nullability as _the_annotated_nullability,
)
from declaro_persistum.model_columns import (
    _the_annotation_as_written as _the_annotation_as_written,
)
from declaro_persistum.model_columns import (
    _the_column_as_declared as _the_column_as_declared,
)
from declaro_persistum.model_columns import (
    _the_column_members as _the_column_members,
)
from declaro_persistum.model_columns import (
    _the_declared_column_default as _the_declared_column_default,
)
from declaro_persistum.model_columns import (
    _the_declared_entry as _the_declared_entry,
)
from declaro_persistum.model_columns import (
    _the_declared_indexes as _the_declared_indexes,
)
from declaro_persistum.model_columns import (
    _the_declared_nullability as _the_declared_nullability,
)
from declaro_persistum.model_columns import (
    _the_declared_table as _the_declared_table,
)
from declaro_persistum.model_columns import (
    _the_filled_view_fields as _the_filled_view_fields,
)
from declaro_persistum.model_columns import (
    _the_first_declared_type as _the_first_declared_type,
)
from declaro_persistum.model_columns import (
    _the_index_columns as _the_index_columns,
)
from declaro_persistum.model_columns import (
    _the_index_name as _the_index_name,
)
from declaro_persistum.model_columns import (
    _the_known_facets as _the_known_facets,
)
from declaro_persistum.model_columns import (
    _the_known_index_fields as _the_known_index_fields,
)
from declaro_persistum.model_columns import (
    _the_literal_strings as _the_literal_strings,
)
from declaro_persistum.model_columns import (
    _the_loaded_schema as _the_loaded_schema,
)
from declaro_persistum.model_columns import (
    _the_metadata_list as _the_metadata_list,
)
from declaro_persistum.model_columns import (
    _the_models_directory as _the_models_directory,
)
from declaro_persistum.model_columns import (
    _the_module_file as _the_module_file,
)
from declaro_persistum.model_columns import (
    _the_module_loader as _the_module_loader,
)
from declaro_persistum.model_columns import (
    _the_named_table as _the_named_table,
)
from declaro_persistum.model_columns import (
    _the_python_column_default as _the_python_column_default,
)
from declaro_persistum.model_columns import (
    _the_python_default as _the_python_default,
)
from declaro_persistum.model_columns import (
    _the_spelled_column_default as _the_spelled_column_default,
)
from declaro_persistum.model_columns import (
    _the_sql_spelling as _the_sql_spelling,
)
from declaro_persistum.model_columns import (
    _the_type_beside_none as _the_type_beside_none,
)
from declaro_persistum.model_columns import (
    _the_view_as_declared as _the_view_as_declared,
)
from declaro_persistum.model_columns import (
    _the_view_fields_as_declared as _the_view_fields_as_declared,
)
from declaro_persistum.model_columns import (
    _unnamed as _unnamed,
)
from declaro_persistum.model_columns import (
    _without_the_column_primary_key as _without_the_column_primary_key,
)
from declaro_persistum.model_columns import (
    declared_type as declared_type,
)
from declaro_persistum.model_columns import (
    extract_field_metadata as extract_field_metadata,
)
from declaro_persistum.model_columns import (
    extract_literal_values as extract_literal_values,
)
from declaro_persistum.model_columns import (
    field as field,
)
from declaro_persistum.model_columns import (
    index_from_meta as index_from_meta,
)
from declaro_persistum.model_columns import (
    is_literal_type as is_literal_type,
)
from declaro_persistum.model_columns import (
    is_optional_type as is_optional_type,
)
from declaro_persistum.model_columns import (
    sql_default as sql_default,
)
from declaro_persistum.model_columns import (
    table as table,
)
from declaro_persistum.model_columns import (
    unwrap_optional as unwrap_optional,
)
from declaro_persistum.types import (
    Schema,
    Table,
    View,
    type_of,
    written_as,
)


@helper
def pydantic_model_to_table(model_cls: type) -> tuple[str, Table] | None:
    """Convert a Pydantic model to a Table definition.

    Args:
        model_cls: A Pydantic BaseModel subclass with __tablename__

    Returns:
        Tuple of (table_name, Table) or None if not a table model
    """
    table_name = getattr(model_cls, "__tablename__", None)
    declared = _WHEN_NOT_A_TABLE[table_name is None](model_cls, table_name)
    return next(iter(declared), None)


@boundary
def _import_module_file(module_path: Path) -> Any:
    """Import a model file once and hand back the module.

    Tables and views are both read off the same module, so the import is
    separated from what is read out of it. Doing it twice would execute the
    file twice, and a models file is ordinary Python that may do anything at
    import time.
    """
    _WHEN_A_MODULE_IS_MISSING[not module_path.exists()](module_path)

    module_name = f"_dp_model_{module_path.stem}_{id(module_path)}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    loader = _WHEN_A_MODULE_HAS_NO_LOADER[spec is None or spec.loader is None](spec, module_path)

    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[module_name] = module
    try:
        loader.exec_module(module)
    except Exception as e:
        raise LoaderError(f"Error executing module: {e}", path=str(module_path)) from e
    return module


@helper
def load_declarations(module_path: Path) -> tuple[Schema, dict[str, View]]:
    """Everything a models file declares: its tables AND its views.

    ONE IMPORT, BOTH ANSWERS. A models file is ordinary Python and may do
    anything at import time, so reading tables and views through two separate
    entry points would execute it twice. Callers that want only tables still
    have `load_models_from_module`; the apply path wants both.
    """
    module = _import_module_file(module_path)
    return _schema_of(module), views_from_module(module)


@helper
def load_models_from_module(module_path: Path) -> Schema:
    """Load all @table decorated Pydantic models from a Python module.

    Args:
        module_path: Path to Python file containing Pydantic models

    Returns:
        Schema dict with table definitions

    Raises:
        LoaderError: If module cannot be loaded
    """
    return _schema_of(_import_module_file(module_path))


@helper
def _schema_of(module: Any) -> Schema:
    """The tables a loaded module declares."""
    # A module's namespace holds every kind of thing. A table is a class
    # carrying the name `@table` wrote on it, so anything else in there is not
    # a declaration this loader is looking at.
    declared = [
        obj
        for name in dir(module)
        if not name.startswith("_")
        for obj in [getattr(module, name)]
        if written_as(type, obj)
        if hasattr(obj, "__tablename__")
    ]
    return dict(pair for pair in [pydantic_model_to_table(obj) for obj in declared] if pair)


@helper
def _views_of_file(py_file: Path) -> dict[str, View]:
    """The views one file declares, or none when it is not a models file."""
    # NO CATCH. `_import_module_file` raises LoaderError for everything that
    # can go wrong importing -- a missing file, an unloadable spec, a module
    # that throws -- and that names the file to its author. The
    # `except Exception: return {}` that used to sit here reported "no views"
    # for a file that failed, which is the failure this package exists to
    # avoid: a view the differ never sees is a view it drops, and dropping a
    # materialized view discards the contents it exists to hold.
    #
    # It was already unreachable. `test_a_broken_models_file_is_not_a_file_with
    # _no_views` measured that on 2026-08-18: the only route past the
    # re-raise needed a descriptor written to misbehave.
    return views_from_module(_import_module_file(py_file))


# What a `View` value must carry, and what it may. Checked at load time for the
# same reason `index_from_meta` checks index keys: a misspelled key in a plain
# dict is silently dropped, and the view is then missing a property nobody can
# see the absence of.
@boundary
def load_views_from_models(models_dir: str | Path) -> dict[str, View]:
    """Every view declared across a models directory.

    Mirrors `load_schema_from_models`, and skips the same files it skips.
    """
    models_path = Path(models_dir)
    _WHEN_THE_MODELS_DIRECTORY_IS_MISSING[not models_path.exists()](models_path)

    return {
        name: view
        for py_file in models_path.glob("**/*.py")
        if not py_file.name.startswith(("_", "test_"))
        for name, view in _views_of_file(py_file).items()
    }


@helper
def views_from_module(module: Any) -> dict[str, View]:
    """Every `View` value declared at module level.

    A view is data, not a decorated class, because there is no Python object
    for it to decorate: it has a name and a SELECT and no fields. So it is
    written as a dict matching `View` and picked up by shape.

    THE LOADER READ NONE OF THIS UNTIL NOW, and the whole chain around it
    already worked. The inspectors return views when asked, the appliers
    render `create_view` and `drop_view`, and `differ/extended.diff_views`
    computes them — but nothing called it, so no view ever reached the applier
    from a models directory. `usage.md` claimed views worked end to end. They
    did not.
    """
    # A module's namespace holds every kind of thing. A view is written as
    # a mapping carrying both required keys, so anything else in there is not
    # a declaration this loader is looking at.
    declared = [
        obj
        for name in dir(module)
        if not name.startswith("_")
        for obj in [getattr(module, name)]
        if written_as(dict, obj)
        if all(key in obj for key in VIEW_REQUIRED)
    ]
    checked = [
        _WHEN_A_VIEW_KEY_IS_UNKNOWN[bool(unknown)](obj, unknown)
        for obj in declared
        for unknown in [sorted(set(obj) - set(VIEW_REQUIRED) - set(VIEW_OPTIONAL))]
    ]
    return {view["name"]: view for view in checked}


@helper
def _schema_of_file(py_file: Path) -> Schema:
    """The tables one file declares.

    A FILE THAT FAILS TO LOAD IS NOT A FILE THAT DECLARES NOTHING.

    This was `except Exception: continue`, commented "Skip files that can't be
    imported (might not be model files)". The intent is real -- a models
    directory may hold helpers -- but the two cases are different and
    conflating them cost the author the only error they could act on.

    Measured 2026-08-17. A one-letter typo in a Meta index, `"uniqe": True`,
    which `index_from_meta` refuses by name and by design, reached the author
    as:

        LoaderError: No @table decorated models found in models/

    which sends them looking for a missing decorator.

    A helper that imports cleanly and declares no table still contributes
    nothing, which is the case the swallow was reaching for and which needs no
    swallow at all.
    """
    try:  # honest: ignore HC-P002: re-raises naming the file that would not load
        return load_models_from_module(py_file)
    except LoaderError:
        raise
    except Exception as exc:
        raise LoaderError(
            f"{py_file.name} could not be loaded: {type_of(exc).__name__}: {exc}",
            path=str(py_file),
        ) from exc


@boundary
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
    _WHEN_THE_MODELS_DIRECTORY_IS_MISSING[not models_path.exists()](models_path)

    # Load all .py files (except __init__.py and test files), keeping the file
    # each table came from so a collision can name both sides. One filename
    # tells a developer a collision happened without saying where.
    declared = [
        (py_file, table_name, definition)
        for py_file in models_path.glob("**/*.py")
        if not py_file.name.startswith(("_", "test_"))
        for table_name, definition in _schema_of_file(py_file).items()
    ]

    # A COLLISION IS REFUSED, NOT MERGED. This was `schema.update(...)`,
    # and a dict update is last-wins, so two files both declaring
    # `@table("users")` produced one table and which one depended on the
    # order `glob` returned.
    #
    # MEASURED 2026-08-17:
    #
    #     a_users.py  @table("users")  id, email
    #     z_other.py  @table("users")  id, totally_different
    #     loaded: {"users": columns id, email}
    #
    # The losing model's columns are absent from the schema, so the differ
    # compares a database that has them against a target that does not and
    # emits a DROP. A copy-pasted decorator costs a column and its data.
    #
    # It has to be caught BEFORE the merge, because once the merge has run the
    # first declaration is gone and nothing downstream can tell a collision
    # from a single model.
    collisions = [
        (table_name, first.name, py_file)
        for index, (py_file, table_name, _definition) in enumerate(declared)
        for first, earlier, _earlier_definition in declared[:index]
        if earlier == table_name
    ]
    _WHEN_A_TABLE_IS_DECLARED_TWICE[bool(collisions)](collisions)

    schema: Schema = {table_name: definition for _py_file, table_name, definition in declared}
    return _WHEN_NOTHING_IS_DECLARED[not schema](models_path, schema)


@helper
def get_literal_columns(schema: Schema) -> dict[str, dict[str, list[str]]]:
    """Extract all columns with Literal type values from schema.

    Returns:
        Dict of table_name -> {column_name -> [literal_values]}
    """
    found = {
        table_name: {
            col_name: col["literal_values"]
            for col_name, col in declared.get("columns", {}).items()
            if col.get("literal_values")
        }
        for table_name, declared in schema.items()
    }
    # A table whose columns name no members belongs in no answer at all: the
    # old shape only created its entry when the first such column arrived.
    return {table_name: columns for table_name, columns in found.items() if columns}
