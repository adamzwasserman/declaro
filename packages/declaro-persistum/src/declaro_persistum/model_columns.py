"""How a Pydantic annotation becomes a column, and the tables that decide.

pydantic_loader.py reached 1428 lines. This is the dispatch tables taken with
everything they reach: which SQL type an annotation maps to, how a default is
spelled, what each declared facet does to a column. Every name is re-exported
from `pydantic_loader`, so the declaration API is unchanged."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any, get_type_hints

from declaro_persistum.boundary import helper
from declaro_persistum.column_types import (
    _COLUMN_FACETS as _COLUMN_FACETS,
)
from declaro_persistum.column_types import (
    _FIELD_METADATA as _FIELD_METADATA,
)
from declaro_persistum.column_types import (
    _INDEX_OPTIONAL as _INDEX_OPTIONAL,
)
from declaro_persistum.column_types import (
    _LITERAL_MEMBERS as _LITERAL_MEMBERS,
)
from declaro_persistum.column_types import (
    _NO_DEFAULT as _NO_DEFAULT,
)
from declaro_persistum.column_types import (
    _SQL_DEFAULT as _SQL_DEFAULT,
)
from declaro_persistum.column_types import (
    _TYPE_NAMED_IN_TEXT as _TYPE_NAMED_IN_TEXT,
)
from declaro_persistum.column_types import (
    _UNION_SPELLINGS as _UNION_SPELLINGS,
)
from declaro_persistum.column_types import (
    _UNWRAPPED as _UNWRAPPED,
)
from declaro_persistum.column_types import (
    _WHEN_A_DEFAULT_IS_DECLARED as _WHEN_A_DEFAULT_IS_DECLARED,
)
from declaro_persistum.column_types import (
    _WHEN_A_MEMBER_IS_NOT_A_STRING as _WHEN_A_MEMBER_IS_NOT_A_STRING,
)
from declaro_persistum.column_types import (
    _WHEN_AN_INDEX_FIELD_IS_UNKNOWN as _WHEN_AN_INDEX_FIELD_IS_UNKNOWN,
)
from declaro_persistum.column_types import (
    _WHEN_AN_INDEX_HAS_NO_COLUMNS as _WHEN_AN_INDEX_HAS_NO_COLUMNS,
)
from declaro_persistum.column_types import (
    _WHEN_AN_INDEX_IS_UNNAMED as _WHEN_AN_INDEX_IS_UNNAMED,
)
from declaro_persistum.column_types import (
    _WHEN_NO_DECLARED_TYPE as _WHEN_NO_DECLARED_TYPE,
)
from declaro_persistum.column_types import (
    _WHEN_NO_SQL_SPELLING as _WHEN_NO_SQL_SPELLING,
)
from declaro_persistum.column_types import (
    _WHEN_NOT_A_MODEL as _WHEN_NOT_A_MODEL,
)
from declaro_persistum.column_types import (
    _WHEN_UNKNOWN_FACET as _WHEN_UNKNOWN_FACET,
)
from declaro_persistum.column_types import (
    INDEX_FIELDS as INDEX_FIELDS,
)
from declaro_persistum.column_types import (
    PYTHON_TO_SQL_TYPE as PYTHON_TO_SQL_TYPE,
)

# Re-exported so nothing that imports from here had to change.
from declaro_persistum.column_types import (  # noqa: F401
    ColumnFacets as ColumnFacets,
)
from declaro_persistum.column_types import (
    OnAction as OnAction,
)
from declaro_persistum.column_types import (
    _no_literal_members as _no_literal_members,
)
from declaro_persistum.column_types import (
    _no_metadata_list as _no_metadata_list,
)
from declaro_persistum.column_types import (
    _no_python_default as _no_python_default,
)
from declaro_persistum.column_types import (
    _refuse_a_class_pydantic_did_not_make as _refuse_a_class_pydantic_did_not_make,
)
from declaro_persistum.column_types import (
    _refuse_a_member_that_is_not_a_string as _refuse_a_member_that_is_not_a_string,
)
from declaro_persistum.column_types import (
    _refuse_a_value_with_no_sql_spelling as _refuse_a_value_with_no_sql_spelling,
)
from declaro_persistum.column_types import (
    _refuse_an_index_over_no_columns as _refuse_an_index_over_no_columns,
)
from declaro_persistum.column_types import (
    _refuse_an_unknown_index_field as _refuse_an_unknown_index_field,
)
from declaro_persistum.column_types import (
    _refuse_an_unmapped_annotation as _refuse_an_unmapped_annotation,
)
from declaro_persistum.column_types import (
    _refuse_an_unnamed_index as _refuse_an_unnamed_index,
)
from declaro_persistum.column_types import (
    _refuse_unknown_facets as _refuse_unknown_facets,
)
from declaro_persistum.column_types import (
    _the_annotation_as_written as _the_annotation_as_written,
)
from declaro_persistum.column_types import (
    _the_first_declared_type as _the_first_declared_type,
)
from declaro_persistum.column_types import (
    _the_index_columns as _the_index_columns,
)
from declaro_persistum.column_types import (
    _the_index_name as _the_index_name,
)
from declaro_persistum.column_types import (
    _the_known_facets as _the_known_facets,
)
from declaro_persistum.column_types import (
    _the_known_index_fields as _the_known_index_fields,
)
from declaro_persistum.column_types import (
    _the_literal_strings as _the_literal_strings,
)
from declaro_persistum.column_types import (
    _the_metadata_list as _the_metadata_list,
)
from declaro_persistum.column_types import (
    _the_named_table as _the_named_table,
)
from declaro_persistum.column_types import (
    _the_python_default as _the_python_default,
)
from declaro_persistum.column_types import (
    _the_sql_spelling as _the_sql_spelling,
)
from declaro_persistum.column_types import (
    _the_type_beside_none as _the_type_beside_none,
)
from declaro_persistum.column_types import (
    declared_type as declared_type,
)
from declaro_persistum.column_types import (
    extract_field_metadata as extract_field_metadata,
)
from declaro_persistum.column_types import (
    extract_literal_values as extract_literal_values,
)
from declaro_persistum.column_types import (
    field as field,
)
from declaro_persistum.column_types import (
    index_from_meta as index_from_meta,
)
from declaro_persistum.column_types import (
    is_literal_type as is_literal_type,
)
from declaro_persistum.column_types import (
    is_optional_type as is_optional_type,
)
from declaro_persistum.column_types import (
    sql_default as sql_default,
)
from declaro_persistum.column_types import (
    table as table,
)
from declaro_persistum.column_types import (
    unwrap_optional as unwrap_optional,
)
from declaro_persistum.exceptions import LoaderError
from declaro_persistum.types import (
    NOTHING,
    Column,
    Schema,
    Table,
    View,
    type_of,
    written_as,
)


@helper
def _the_declared_nullability(meta: dict[str, Any], _annotation: Any) -> bool:
    """The column said whether it takes NULL, and that answer stands."""
    return bool(meta["nullable"])


@helper
def _the_annotated_nullability(_meta: dict[str, Any], annotation: Any) -> bool:
    """Nothing was said, so an Optional annotation is what allows NULL."""
    return is_optional_type(annotation)


_NULLABILITY: dict[bool, Callable[[dict[str, Any], Any], bool]] = {
    True: _the_declared_nullability,
    False: _the_annotated_nullability,
}


@helper
def _no_column_members(_unwrapped: Any) -> dict[str, list[str]]:
    """Not a Literal, so this column is not one of a fixed set of values."""
    return {}


@helper
def _the_column_members(unwrapped: Any) -> dict[str, list[str]]:
    """A Literal's members, which become the enum lookup table."""
    values = extract_literal_values(unwrapped)
    return {"literal_values": values} if values else {}


_COLUMN_MEMBERS: dict[bool, Callable[[Any], dict[str, list[str]]]] = {
    True: _the_column_members,
    False: _no_column_members,
}


@helper
def _no_column_default(_meta: dict[str, Any]) -> dict[str, Any]:
    """Neither a SQL expression nor a Python value, so no DEFAULT is written."""
    return {}


@helper
def _the_spelled_column_default(meta: dict[str, Any]) -> dict[str, Any]:
    """Pydantic's own default, written as SQL."""
    return {"default": sql_default(meta["_default_value"])}


_FROM_A_PYTHON_DEFAULT: dict[bool, Callable[[dict[str, Any]], dict[str, Any]]] = {
    True: _the_spelled_column_default,
    False: _no_column_default,
}


@helper
def _the_python_column_default(meta: dict[str, Any]) -> dict[str, Any]:
    """No SQL expression was declared, so Pydantic's own default is read.

    It is read only here, which is what keeps `sql_default` from refusing a
    Python default the author already overrode with `field(default=...)`.
    """
    return _FROM_A_PYTHON_DEFAULT["_default_value" in meta](meta)


@helper
def _the_declared_column_default(meta: dict[str, Any]) -> dict[str, Any]:
    """`field(default=...)` is a SQL expression and is used as written."""
    return {"default": meta["default"]}


_COLUMN_DEFAULT: dict[bool, Callable[[dict[str, Any]], dict[str, Any]]] = {
    True: _the_declared_column_default,
    False: _the_python_column_default,
}


# Facets that reach a column as the flag they are, facets that reach it
# carrying their own value, and the two that are hints for the differ. Each is
# written only where the author declared it.
#
# One spelling. The loader accepted `primary` as well, which no caller writes:
# `field()` emits `primary_key`, the Column key is `primary_key`,
# introspection returns `primary_key`, and the six documented
# `field(primary=...)` examples never ran because `field` did not exist. Two
# names for one property is where the drift starts.
_COLUMN_FLAGS = ("primary_key", "unique")
_COLUMN_CARRIED = ("references", "on_delete", "on_update", "check")
_COLUMN_HINTS = ("renamed_from", "is_new")


@helper
def _column_of(annotation: Any, field_info: Any) -> Column:
    """One model field, as the column it declares."""
    meta = extract_field_metadata(field_info) if field_info else {}
    nullable = _NULLABILITY["nullable" in meta](meta, annotation)
    unwrapped = unwrap_optional(annotation)
    sql_type = meta.get("db_type") or declared_type(unwrapped)
    return {
        "type": sql_type,
        "nullable": nullable,
        **_COLUMN_MEMBERS[is_literal_type(unwrapped)](unwrapped),
        **{facet: True for facet in _COLUMN_FLAGS if meta.get(facet)},
        **{facet: meta[facet] for facet in _COLUMN_CARRIED if meta.get(facet)},
        **_COLUMN_DEFAULT[bool(meta.get("default"))](meta),
        **{facet: meta[facet] for facet in _COLUMN_HINTS if meta.get(facet)},
    }


@helper
def _without_the_column_primary_key(col: Column) -> Column:
    """A composite key is declared once on the table, never on a column."""
    return {key: value for key, value in col.items() if key != "primary_key"}


@helper
def _the_column_as_declared(col: Column) -> Column:
    """No composite key, so a column's own primary_key stands."""
    return col


_WHEN_THE_KEY_IS_COMPOSITE: dict[bool, Callable[[Column], Column]] = {
    True: _without_the_column_primary_key,
    False: _the_column_as_declared,
}


@helper
def _the_declared_entry(key: str, value: Any) -> dict[str, Any]:
    """A Meta class declared this, so the table carries it."""
    return {key: value}


@helper
def _no_declared_entry(_key: str, _value: Any) -> dict[str, Any]:
    """A Meta class declared nothing here, so the table says nothing."""
    return {}


_META_ENTRY: dict[bool, Callable[[str, Any], dict[str, Any]]] = {
    True: _the_declared_entry,
    False: _no_declared_entry,
}


@helper
def _no_declared_indexes(_indexes: Any) -> dict[str, Any]:
    """No Meta indexes, so the table declares none."""
    return {}


@helper
def _the_declared_indexes(indexes: Any) -> dict[str, Any]:
    """EVERY DECLARED INDEX, OR A REFUSAL NAMING THE ONE THAT IS NOT ONE.

    This read `if isinstance(idx, dict)`, so an entry written wrongly was
    skipped: the author got a table without that index and no complaint, and
    found out when a query was slow or a uniqueness constraint did not hold.
    """
    return {"indexes": dict(index_from_meta(idx) for idx in indexes)}


_META_INDEXES: dict[bool, Callable[[Any], dict[str, Any]]] = {
    True: _the_declared_indexes,
    False: _no_declared_indexes,
}


@helper
def _not_a_table(_model_cls: type, _table_name: Any) -> list[tuple[str, Table]]:
    """No tables, because this class carries no `__tablename__`.

    NOUGHT OR ONE, NOT A VALUE OR NONE. Both halves answered `tuple | None`,
    which made this one a function whose body was `return None`: a scanner
    cannot tell that from a function nobody finished, and neither can a
    reader. A list says how many tables the class declared, and empty is an
    answer somebody wrote.

    `pydantic_model_to_table` keeps returning `tuple | None`, because a test
    asserts it and its caller filters on it.
    """
    return []


@helper
def _the_declared_table(model_cls: type, table_name: Any) -> list[tuple[str, Table]]:
    """The table one decorated model declares.

    Use typing.get_type_hints rather than reading __annotations__ directly:
    under PEP 563 (`from __future__ import annotations`) and for any string
    forward reference (`name: "User"`), __annotations__ contains *strings*,
    not types. Direct access then makes downstream type lookups (e.g.
    `bool: "boolean"` in PYTHON_TO_SQL_TYPE) miss, silently emitting
    `text` columns where the user wrote `bool`. get_type_hints resolves
    string annotations to actual types using the model's module globals.
    `get_type_hints` resolves a models file's annotations against its
    module globals, so this is the intake of the declarative surface. The
    catch exists to say WHICH model could not be read, and it re-raises as
    LoaderError rather than swallowing.
    """
    try:  # honest: ignore HC-P002: re-raises naming the unreadable model
        annotations = get_type_hints(model_cls)
    except (NameError, TypeError) as unresolvable:
        # THE FALLBACK HERE BLAMED THE WRONG FIELD. It read
        # `annotations = getattr(model_cls, "__annotations__", {})`, with the
        # comment: "Fall back if a forward reference is unresolvable. Caller
        # will see the same wrong-but-consistent behavior as pre-0.1.3 --
        # better than crashing during schema load."
        #
        # Two things moved under it. `declared_type` was taught on 2026-08-17
        # to raise on an annotation it does not recognise rather than quietly
        # emitting `text`, so the load crashes anyway. And under
        # `from __future__ import annotations` the raw `__annotations__` holds
        # every annotation as a STRING, so the first field fails and the field
        # that actually broke resolution is never reached. Measured
        # 2026-08-19 on a model whose second field named a type that does not
        # exist:
        #
        #     LoaderError: cannot tell what column type 'int' means.
        #
        # `int` is the first field and is fine. An author told that changes a
        # line that was never wrong.
        #
        # "Better than crashing" was the trade and it is not on offer, so what
        # is left is crashing with the right name.
        raise LoaderError(
            f"Cannot resolve the annotations on {model_cls.__name__}: "
            f"{unresolvable}. Every field is read as a string until this is "
            f"fixed, so the type of no column can be determined.",
            path=NOTHING,
        ) from unresolvable

    model_fields = getattr(model_cls, "model_fields", {})
    declared = {
        field_name: _column_of(annotation, model_fields.get(field_name))
        for field_name, annotation in annotations.items()
        if not field_name.startswith("_")
    }

    # A Meta class carries indexes, constraints and a composite primary key.
    # `getattr` on a model that has none reads None, and None declares none of
    # the three, so an absent Meta needs no handling of its own.
    meta_cls = getattr(model_cls, "Meta", None)
    primary_key = getattr(meta_cls, "primary_key", None)
    indexes = getattr(meta_cls, "indexes", None)
    constraints = getattr(meta_cls, "constraints", None)

    columns = {
        name: _WHEN_THE_KEY_IS_COMPOSITE[bool(primary_key)](col) for name, col in declared.items()
    }
    built: Table = {
        "columns": columns,
        **_META_ENTRY[bool(primary_key)]("primary_key", primary_key),  # type: ignore[typeddict-item]
        **_META_INDEXES[bool(indexes)](indexes),
        **_META_ENTRY[bool(constraints)]("constraints", constraints),
    }
    return [(table_name, built)]


_WHEN_NOT_A_TABLE: dict[bool, Callable[[type, Any], list[tuple[str, Table]]]] = {
    True: _not_a_table,
    False: _the_declared_table,
}


@helper
def _refuse_a_missing_module(module_path: Path) -> Path:
    """A file that is not there declares nothing, and says so here."""
    raise LoaderError(f"Module not found: {module_path}", path=str(module_path))


@helper
def _the_module_file(module_path: Path) -> Path:
    """The file to import."""
    return module_path


_WHEN_A_MODULE_IS_MISSING: dict[bool, Callable[[Path], Path]] = {
    True: _refuse_a_missing_module,
    False: _the_module_file,
}


@helper
def _refuse_a_module_with_no_loader(_spec: Any, module_path: Path) -> Any:
    """Python built no loader for this file, so nothing can execute it."""
    raise LoaderError(f"Cannot load module: {module_path}", path=str(module_path))


@helper
def _the_module_loader(spec: Any, _module_path: Path) -> Any:
    """The loader that will execute the file."""
    return spec.loader


_WHEN_A_MODULE_HAS_NO_LOADER: dict[bool, Callable[[Any, Path], Any]] = {
    True: _refuse_a_module_with_no_loader,
    False: _the_module_loader,
}


@helper
def _refuse_a_missing_models_directory(models_path: Path) -> Path:
    """A directory that is not there holds no models."""
    raise LoaderError(f"Models directory not found: {models_path}", path=str(models_path))


@helper
def _the_models_directory(models_path: Path) -> Path:
    """The directory to walk."""
    return models_path


_WHEN_THE_MODELS_DIRECTORY_IS_MISSING: dict[bool, Callable[[Path], Path]] = {
    True: _refuse_a_missing_models_directory,
    False: _the_models_directory,
}


VIEW_REQUIRED = ("name", "query")
VIEW_OPTIONAL = ("materialized", "refresh", "depends_on", "trigger_sources")


@helper
def _refuse_an_unknown_view_key(obj: dict[str, Any], unknown: list[str]) -> View:
    """A key a view has no field for, named where it was written."""
    raise LoaderError(
        f"view '{obj['name']}' declares unknown "
        f"{'keys' if len(unknown) > 1 else 'key'} "
        f"{unknown}. A view may carry "
        f"{list(VIEW_REQUIRED + VIEW_OPTIONAL)}.",
        path=NOTHING,
    )


@helper
def _the_view_as_declared(obj: dict[str, Any], _unknown: list[str]) -> View:
    """Every key is one a view has, so the fields themselves are checked next."""
    _check_view(obj)
    return obj  # type: ignore[return-value]


_WHEN_A_VIEW_KEY_IS_UNKNOWN: dict[bool, Callable[[dict[str, Any], list[str]], View]] = {
    True: _refuse_an_unknown_view_key,
    False: _the_view_as_declared,
}


# What each field must BE, not merely that it is present. The keys were checked
# and the values were not, so a view could carry any type at all.
#
# MEASURED 2026-08-17: `{"name": 123, "query": None, "materialized": "yes"}`
# has both required keys, passed, and came back keyed by the integer 123. It
# would have travelled to an applier and rendered `CREATE VIEW 123 AS None`,
# and the failure would have arrived from a SQL parser with no idea which
# models file wrote it.
#
# Rule 13: the boundary declares the shape once so the differ and both
# appliers can treat `name` as a string and `query` as SQL without re-checking.
@helper
def _named(name: str) -> str:
    """The view's own name, which is what a reader is looking for."""
    return name


@helper
def _unnamed(name: Any) -> str:
    """No usable name, so the refusal shows what was written instead."""
    return repr(name)


# WHETHER THE VIEW HAS A NAME TO QUOTE, as a table rather than a conditional
# expression.
_VIEW_DESCRIBED: dict[bool, Callable[[Any], str]] = {
    True: _named,
    False: _unnamed,
}

_VIEW_FIELD_TYPES: dict[str, type] = {
    "name": str,
    "query": str,
    "materialized": bool,
    "refresh": str,
    "depends_on": list,
    "trigger_sources": list,
}

# Fields that must also carry something. An empty name is not a name, and an
# empty query is not a SELECT.
_VIEW_MUST_NOT_BE_BLANK = ("name", "query")


@helper
def _refuse_a_view_field_of_the_wrong_type(
    described: str, wrong: list[tuple[str, Any, type]]
) -> list[tuple[str, Any, type]]:
    """The first field whose value is not what a view's field must be."""
    named, value, expected = wrong[0]
    raise LoaderError(
        f"view {described}: {named!r} is "
        f"{type_of(value).__name__}, and a view's {field!r} must be "
        f"{expected.__name__}.",
        path=NOTHING,
    )


@helper
def _the_view_fields_as_declared(
    _described: str, wrong: list[tuple[str, Any, type]]
) -> list[tuple[str, Any, type]]:
    """Every field present is the type a view's field must be."""
    return wrong


_WHEN_A_VIEW_FIELD_IS_THE_WRONG_TYPE: dict[
    bool, Callable[[str, list[tuple[str, Any, type]]], list[tuple[str, Any, type]]]
] = {
    True: _refuse_a_view_field_of_the_wrong_type,
    False: _the_view_fields_as_declared,
}


@helper
def _refuse_a_blank_view_field(described: str, blank: list[str]) -> list[str]:
    """A name or a query that carries nothing."""
    raise LoaderError(
        f"view {described}: {blank[0]!r} is empty. A view needs a name and a SELECT.",
        path=NOTHING,
    )


@helper
def _the_filled_view_fields(_described: str, blank: list[str]) -> list[str]:
    """The name and the query both carry something."""
    return blank


_WHEN_A_VIEW_FIELD_IS_BLANK: dict[bool, Callable[[str, list[str]], list[str]]] = {
    True: _refuse_a_blank_view_field,
    False: _the_filled_view_fields,
}


@helper
def _check_view(obj: dict[str, Any]) -> None:
    """Refuse a view whose fields are the wrong type or empty. Raises or returns.

    WHAT IS DELIBERATELY NOT CHECKED: whether the query is valid SQL. That
    needs an engine, the engine says so at apply time, and a loader guessing
    at validity would reject dialect-specific syntax it does not know.
    """
    described = _VIEW_DESCRIBED[written_as(str, obj.get("name"))](obj.get("name"))

    # WHAT THE VALUE INHERITS COUNTS, so a mapping or string a library
    # subclassed is still the thing it derives from. The direction is
    # what makes this safe where an `isinstance(x, int)` was not: `bool`
    # derives from `int`, so a flag is written as a number and a number
    # is not written as a flag, and `materialized` refuses 1.
    wrong = [
        (named, obj[named], expected)
        for named, expected in _VIEW_FIELD_TYPES.items()
        if named in obj
        if not written_as(expected, obj[named])
    ]
    _WHEN_A_VIEW_FIELD_IS_THE_WRONG_TYPE[bool(wrong)](described, wrong)

    blank = [named for named in _VIEW_MUST_NOT_BE_BLANK if not obj[named].strip()]
    _WHEN_A_VIEW_FIELD_IS_BLANK[bool(blank)](described, blank)


@helper
def _refuse_two_models_claiming_one_table(
    collisions: list[tuple[str, str, Path]],
) -> list[tuple[str, str, Path]]:
    """Two files declaring one table, named on both sides."""
    table_name, first, py_file = collisions[0]
    raise LoaderError(
        f"Two models declare the table {table_name!r}: "
        f"{first} and {py_file.name}. A schema "
        f"holds one definition per table, so merging them would "
        f"silently keep whichever file loaded last and drop the "
        f"other's columns from the database.",
        path=str(py_file),
    )


@helper
def _one_declaration_per_table(
    collisions: list[tuple[str, str, Path]],
) -> list[tuple[str, str, Path]]:
    """Every table is declared once, so the files can be merged."""
    return collisions


_WHEN_A_TABLE_IS_DECLARED_TWICE: dict[
    bool,
    Callable[[list[tuple[str, str, Path]]], list[tuple[str, str, Path]]],
] = {
    True: _refuse_two_models_claiming_one_table,
    False: _one_declaration_per_table,
}


@helper
def _refuse_a_directory_that_declares_nothing(models_path: Path, _schema: Schema) -> Schema:
    """A directory with no decorated model in it."""
    raise LoaderError(
        f"No @table decorated models found in {models_path}",
        path=str(models_path),
    )


@helper
def _the_loaded_schema(_models_path: Path, schema: Schema) -> Schema:
    """The tables the directory declared."""
    return schema


_WHEN_NOTHING_IS_DECLARED: dict[bool, Callable[[Path, Schema], Schema]] = {
    True: _refuse_a_directory_that_declares_nothing,
    False: _the_loaded_schema,
}
