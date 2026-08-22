"""
Pydantic model loader for declaro_persistum.

Loads schema from Pydantic models decorated with @table.
Detects Literal types for automatic enum abstraction.
"""

from __future__ import annotations

import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, Literal, TypedDict, Unpack, get_args, get_origin, get_type_hints

from pydantic import BaseModel
from pydantic import Field as PydanticField
from pydantic_core import PydanticUndefined

from declaro_persistum.boundary import boundary, helper, recognizer
from declaro_persistum.exceptions import LoaderError
from declaro_persistum.types import (
    NOTHING,
    Column,
    Index,
    Schema,
    Table,
    View,
    type_of,
    written_as,
)

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


@recognizer
def is_literal_type(annotation: Any) -> bool:
    """Check if annotation is a Literal type."""
    return get_origin(annotation) is Literal


@helper
def _no_literal_members(_annotation: Any) -> tuple[Any, ...]:
    """Not a Literal, so it names no members at all."""
    return ()


# WHETHER THERE ARE MEMBERS TO READ. `get_args` on anything else returns an
# empty tuple too, but only for the reason that it is not a Literal, and that
# reason is worth having written down.
_LITERAL_MEMBERS: dict[bool, Callable[[Any], tuple[Any, ...]]] = {
    True: get_args,
    False: _no_literal_members,
}


@helper
def _refuse_a_member_that_is_not_a_string(
    offenders: list[Any], _members: tuple[Any, ...]
) -> list[str]:
    """A Literal member the loader cannot render as a column value."""
    raise ValueError(
        f"Literal enum values must be strings, got "
        f"{type_of(offenders[0]).__name__}: {offenders[0]}"
    )


@helper
def _the_literal_strings(_offenders: list[Any], members: tuple[Any, ...]) -> list[str]:
    """Every member is a string, so every member is a value."""
    return list(members)


_WHEN_A_MEMBER_IS_NOT_A_STRING: dict[
    bool, Callable[[list[Any], tuple[Any, ...]], list[str]]
] = {
    True: _refuse_a_member_that_is_not_a_string,
    False: _the_literal_strings,
}


@helper
def extract_literal_values(annotation: Any) -> list[str]:
    """Extract string values from a Literal type annotation.

    Args:
        annotation: A Literal type annotation like Literal["a", "b", "c"]

    Returns:
        List of literal string values

    Raises:
        ValueError: If Literal contains non-string values
    """
    members = _LITERAL_MEMBERS[is_literal_type(annotation)](annotation)
    offenders = [member for member in members if not written_as(str, member)]
    return _WHEN_A_MEMBER_IS_NOT_A_STRING[bool(offenders)](offenders, members)


# The two spellings of a union. `X | None` reaches `get_origin` as `UnionType`
# and `Optional[X]` as `Union`, and an annotation with no origin at all has no
# `__name__` to read, so `str(origin)` stands in and matches neither.
_UNION_SPELLINGS = ("Union", "UnionType")


@recognizer
def is_optional_type(annotation: Any) -> bool:
    """Check if annotation is Optional (Union with None)."""
    origin = get_origin(annotation)
    spelling = getattr(origin, "__name__", str(origin))
    return spelling in _UNION_SPELLINGS and type(None) in get_args(  # honest: ignore HC-P005: type(None) is how an Optional is spelled
        annotation
    )


@helper
def _the_annotation_as_written(annotation: Any) -> Any:
    """Nothing wraps this, so the annotation itself is the answer."""
    return annotation


@helper
def _the_type_beside_none(annotation: Any) -> Any:
    """The member of the union that is not None.

    A union of nothing but None has no such member, and the annotation comes
    back whole rather than as a member that is not there.
    """
    beside = [arg for arg in get_args(annotation) if arg is not type(None)]  # honest: ignore HC-P005: type(None) is how an Optional is spelled
    return [*beside, annotation][0]


_UNWRAPPED: dict[bool, Callable[[Any], Any]] = {
    True: _the_type_beside_none,
    False: _the_annotation_as_written,
}


@helper
def unwrap_optional(annotation: Any) -> Any:
    """Unwrap Optional[X] to get X."""
    return _UNWRAPPED[is_optional_type(annotation)](annotation)


# TYPES READ OFF THE TEXT OF THE ANNOTATION, for the generics and the forward
# references neither lookup above can match. In the order the chain tested
# them, because more than one can match one annotation.
_TYPE_NAMED_IN_TEXT = (
    ("UUID", "uuid"),
    ("uuid", "uuid"),
    ("datetime", "timestamptz"),
    ("Decimal", "numeric"),
)


@helper
def _refuse_an_unmapped_annotation(_found: list[str], annotation: Any) -> str:
    """An annotation no rule matched, named beside the types that exist."""
    raise LoaderError(
        f"cannot tell what column type {annotation!r} means. Declared types "
        f"come from: {', '.join(sorted(set(PYTHON_TO_SQL_TYPE.values())))}. "
        f"Use field(db_type=...) to declare one this mapping does not know.",
        path=NOTHING,
    )


@helper
def _the_first_declared_type(found: list[str], _annotation: Any) -> str:
    """The first rule that matched wins, which is the order they are built in."""
    return found[0]


_WHEN_NO_DECLARED_TYPE: dict[bool, Callable[[list[str], Any], str]] = {
    True: _refuse_an_unmapped_annotation,
    False: _the_first_declared_type,
}


@recognizer
def declared_type(annotation: Any) -> str:
    """The declared column type this annotation means. NOT SQL.

    `python_type_to_sql` was the old name and it named the wrong arrow. This
    returns the CANONICAL type that goes into the schema dict -- "text",
    "integer", "timestamptz", "jsonb" -- and a second function spells that for
    an engine:

        datetime  ->  timestamptz  ->  sqlite=TEXT  postgresql=timestamptz
        dict      ->  jsonb        ->  sqlite=TEXT  postgresql=jsonb

    So the pipeline is annotation -> declared type -> dialect SQL, and this is
    only the first step. `map_type(declared, dialect)` is the one that writes
    SQL, and it is documented as "Spell a declared type for one engine".
    `declared type` was already the package's word for this layer; only this
    function stood outside the vocabulary.

    AN ANNOTATION IT DOES NOT KNOW IS REFUSED, naming the ones it does. The
    last line used to be `return "text"`, so `set[str]`, `tuple[int, int]`,
    `Any`, `complex`, `object` and `frozenset` all became text columns and
    said nothing. `str` returns "text" too, so the value could not distinguish
    a mapped type from a guess, and no test or reader could tell them apart.
    Measured 2026-08-17 across 23 annotations.

    A caller who wants a column this does not map declares it outright with
    `field(db_type=...)`, which the loader reads first and which is why the
    refusal closes no door.

    THE RULES ARE BUILT IN ORDER AND THE FIRST ONE WINS. A Literal is text
    whatever its members spell, so it is built first: the text match at the
    bottom would otherwise read `Literal["datetime"]` as a timestamp.
    """
    unwrapped = unwrap_optional(annotation)
    literal = ["text"] if is_literal_type(unwrapped) else []
    named = [
        PYTHON_TO_SQL_TYPE[key]
        for key in (unwrapped, getattr(unwrapped, "__name__", str(unwrapped)))
        if key in PYTHON_TO_SQL_TYPE
    ]
    spelled = [sql for text, sql in _TYPE_NAMED_IN_TEXT if text in str(unwrapped)]
    found = [*literal, *named, *spelled]
    return _WHEN_NO_DECLARED_TYPE[not found](found, unwrapped)


OnAction = Literal["cascade", "set null", "restrict", "no action"]


@helper
def _refuse_a_class_pydantic_did_not_make(cls: type, name: str) -> type:
    """A class the loader cannot read, refused where it is declared."""
    raise TypeError(
        f"@table('{name}') expects a Pydantic BaseModel subclass; "
        f"{cls.__name__} is not one. The loader reads `model_fields`, "
        f"so any other class yields a table with no columns."
    )


@helper
def _the_named_table(cls: type, name: str) -> type:
    """A Pydantic model, carrying the name the loader looks for."""
    cls.__tablename__ = name  # type: ignore[attr-defined]
    return cls


_WHEN_NOT_A_MODEL: dict[bool, Callable[[type, str], type]] = {
    True: _refuse_a_class_pydantic_did_not_make,
    False: _the_named_table,
}


@helper
def table(name: str) -> Callable[[type], type]:
    """Declare a Pydantic model as a table.

        @table("users")
        class User(BaseModel):
            id: str = field(primary_key=True)

    Sets `__tablename__`, which is the only thing the loader looks for. The
    decorator existed in the documentation from c1e6ce2 onward and was never
    written; `from declaro_persistum import table, field` raised ImportError
    the whole time.

    It refuses a class the loader cannot read. `pydantic_model_to_table` uses
    `model_fields`, which only a Pydantic model has, so a plain class would
    produce a table with zero columns and no complaint -- a schema that
    silently describes nothing.
    """

    @helper
    def declare(cls: type) -> type:
        # `written_as(type, cls)` asks whether this is a class at all, and
        # `issubclass` may only be asked once that answer is yes.
        is_model = written_as(type, cls) and issubclass(cls, BaseModel)
        return _WHEN_NOT_A_MODEL[not is_model](cls, name)

    return declare


# WHAT A COLUMN MAY DECLARE, AS A BOUNDED VOCABULARY. `total=False` is what
# makes absence explicit: a facet the author did not write is a key that is
# not in the mapping, which is a member of this type rather than a value the
# signature invented. Eleven `= None` parameters used to do this, and each one
# absorbed the author's omission before anything could see it.
class ColumnFacets(TypedDict, total=False):
    """Everything a column can say about itself, and nothing else."""

    primary_key: bool
    unique: bool
    nullable: bool
    references: str
    on_delete: OnAction
    on_update: OnAction
    check: str
    default: str
    db_type: str
    renamed_from: str
    is_new: bool


_COLUMN_FACETS = frozenset(ColumnFacets.__annotations__)


@helper
def field(**declared: Unpack[ColumnFacets]) -> Any:
    """Declare what a column MEANS. The applier decides how it is spelled.

        total: Decimal = field(db_type="numeric(10,2)", check="total >= 0")

    renders `REAL CHECK (total >= 0)` on SQLite and Turso, and
    `numeric(10,2) CHECK (total >= 0)` on PostgreSQL. No engine word appears in
    the model, which is the point: `map_type` owns the spelling and this owns
    the meaning.

    WHY THIS EXISTS RATHER THAN A BARE DICT. The loader reads
    `json_schema_extra`, an untyped dict, so

        Field(json_schema_extra={"primry_key": True})

    is a typo that reaches the schema as a key nothing reads, and the column is
    quietly not a primary key. `ColumnFacets` makes it an error a type checker
    catches at edit time and `_refuse_unknown_facets` catches at import. Same
    defect as `index_from_meta`, on the other side of this loader.

    `default` IS A SQL EXPRESSION, NOT A PYTHON VALUE. `default="now()"` means
    DEFAULT now(); it is not Pydantic's default and does not become the string
    "now()". Pydantic's own default is read separately and quoted as a literal,
    which is why the two cannot share one argument.

    `primary_key`, not `primary`. The documentation wrote `primary=` and the
    loader accepts either, but the schema key, introspection and the SQL all
    say `primary_key`. Nothing can depend on the old spelling, because nothing
    could ever call this.

    NOTHING DEFAULTS. This took eleven keyword arguments each defaulting to
    None, and then dropped every None on the way out, so "not declared" was
    reconstructed from a value the signature had supplied. A facet the author
    did not write never enters now, and there is nothing to drop.
    """
    unknown = sorted(set(declared) - _COLUMN_FACETS)
    _WHEN_UNKNOWN_FACET[bool(unknown)](unknown)
    # Pydantic carries field metadata on its own FieldInfo and offers no
    # other way to attach it. This is the declarative surface an author writes
    # against, so the object IS the contract rather than a leak of one.
    return PydanticField(  # honest: ignore HC-P010: Pydantic's own field type
        json_schema_extra=dict(declared)
    )


@helper
def _refuse_unknown_facets(unknown: list[str]) -> list[str]:
    """A facet no column has, named so the author can see the typo.

    `Unpack[ColumnFacets]` says this at edit time. This says it at import,
    which is where an author who runs no type checker finds out.
    """
    raise LoaderError(
        f"field() was given {unknown}, which no column declares. A column may "
        f"declare {sorted(_COLUMN_FACETS)}.",
        path=NOTHING,
    )


@helper
def _the_known_facets(unknown: list[str]) -> list[str]:
    """Every facet named is one a column has, so none is unknown."""
    return unknown


_WHEN_UNKNOWN_FACET: dict[bool, Callable[[list[str]], list[str]]] = {
    True: _refuse_unknown_facets,
    False: _the_known_facets,
}


# HOW A PYTHON VALUE IS SPELLED IN SQL, read by the value's exact type.
#
# THIS WAS AN if/elif CHAIN WHOSE ORDER WAS LOAD-BEARING. `isinstance(True,
# int)` is True in Python, so testing int before bool spells `True` as `1`,
# and a schema declaring a boolean default then migrates on every deploy
# against an engine that stores it as TRUE. Nothing said the order mattered.
# An exact-type lookup has no order to get wrong.
_SQL_DEFAULT: dict[type, Any] = {
    str: lambda value: f"'{value}'",
    bool: lambda value: "TRUE" if value else "FALSE",
    int: str,
    float: str,
}


@helper
def _refuse_a_value_with_no_sql_spelling(_spelled: list[str], value: Any) -> str:
    """A default the loader cannot write, refused rather than dropped."""
    raise LoaderError(
        f"Cannot write {value!r} as a SQL default: a default may be a "
        f"string, a bool, an int or a float, and this is "
        f"{type_of(value).__name__}.",
        path=NOTHING,
    )


@helper
def _the_sql_spelling(spelled: list[str], _value: Any) -> str:
    """The one spelling the value's own type has."""
    return spelled[0]


_WHEN_NO_SQL_SPELLING: dict[bool, Callable[[list[str], Any], str]] = {
    True: _refuse_a_value_with_no_sql_spelling,
    False: _the_sql_spelling,
}


@helper
def sql_default(value: Any) -> str:
    """The SQL spelling of a Python default.

    A value with no spelling is refused rather than dropped. Returning nothing
    would give the column no default at all, which is a schema quietly
    different from the one the author declared.
    """
    # The exact type IS the lookup key, and a type the table does not hold
    # produces no spelling at all rather than a guessed one.
    spelled = [
        _SQL_DEFAULT[kind](value) for kind in [type_of(value)] if kind in _SQL_DEFAULT
    ]
    return _WHEN_NO_SQL_SPELLING[not spelled](spelled, value)


# THE THREE WAYS A FIELD SAYS IT HAS NO DEFAULT. `PydanticUndefined` is what
# Pydantic puts there; `None` and `...` are what an author writes.
_NO_DEFAULT = (None, ..., PydanticUndefined)


@helper
def _no_metadata_list(_carried: Any) -> tuple[Any, ...]:
    """This field carries no metadata list, so it carries nothing in one."""
    return ()


@helper
def _the_metadata_list(carried: Any) -> tuple[Any, ...]:
    """Pydantic's own metadata list, which may hold declaro's mappings."""
    return tuple(carried)


_FIELD_METADATA: dict[bool, Callable[[Any], tuple[Any, ...]]] = {
    True: _the_metadata_list,
    False: _no_metadata_list,
}


@helper
def _no_python_default(_default: Any) -> dict[str, Any]:
    """The field says it has no default, so nothing travels with it."""
    return {}


@helper
def _the_python_default(default: Any) -> dict[str, Any]:
    """Pydantic's own default, kept for `sql_default` to spell."""
    return {"_default_value": default}


_WHEN_A_DEFAULT_IS_DECLARED: dict[bool, Callable[[Any], dict[str, Any]]] = {
    True: _the_python_default,
    False: _no_python_default,
}


@helper
def extract_field_metadata(field_info: Any) -> dict[str, Any]:
    """Extract declaro-specific metadata from Pydantic field.

    Looks for metadata set via field() function like:
        field(primary_key=True, unique=True, references="users.id")
    """
    # A MAPPING IS HOW DECLARO'S OWN METADATA IS WRITTEN, and Pydantic's
    # `metadata` list also holds constraint objects that are not ours. Reading
    # the spelling is what tells declaro's entries from theirs.
    carried = getattr(field_info, "metadata", None)
    written = [
        item
        for item in (
            getattr(field_info, "json_schema_extra", None),
            *_FIELD_METADATA[bool(carried)](carried),
        )
        if written_as(dict, item)
    ]
    meta = {key: value for mapping in written for key, value in mapping.items()}

    # EVERY SPELLING OF "NO DEFAULT", RESOLVED HERE. This knew two, `None`
    # and `...`, and not Pydantic's own `PydanticUndefined`, which is what a
    # field with no default actually carries. That sentinel was stored as
    # though it were a value, and the chain that spelled a default for SQL had
    # no branch matching it, so absence worked by falling through. It stopped
    # working the moment that chain became a lookup that refuses what it
    # cannot write, which is the right refusal reaching the wrong value.
    default = getattr(field_info, "default", None)
    return {**meta, **_WHEN_A_DEFAULT_IS_DECLARED[default not in _NO_DEFAULT](default)}


# The three an index may leave out; `columns` is not one of them.
_INDEX_OPTIONAL = ("unique", "where", "using")
INDEX_FIELDS = ("columns", *_INDEX_OPTIONAL)


@helper
def _refuse_an_unnamed_index(declared: dict[str, Any]) -> str:
    """An index with no name has no key to be stored under."""
    raise ValueError(
        f"index {declared!r} has no 'name'. The name is the key this index "
        f"is stored under, so there is nothing to store it as."
    )


@helper
def _the_index_name(declared: dict[str, Any]) -> str:
    """The name this index is stored under."""
    return declared["name"]


_WHEN_AN_INDEX_IS_UNNAMED: dict[bool, Callable[[dict[str, Any]], str]] = {
    True: _refuse_an_unnamed_index,
    False: _the_index_name,
}


@helper
def _refuse_an_unknown_index_field(name: str, unknown: list[str]) -> list[str]:
    """A key an index has no field for, named at the moment it is written."""
    raise ValueError(
        f"index {name!r} declares {unknown}, which an index has no such "
        f"field for. An index takes {list(INDEX_FIELDS)}. A key that is "
        f"not one of those reaches the schema, never comes back from "
        f"introspection, and leaves the differ reporting this table as "
        f"changed on every run."
    )


@helper
def _the_known_index_fields(_name: str, unknown: list[str]) -> list[str]:
    """Every key named is one an index has, so none is unknown."""
    return unknown


_WHEN_AN_INDEX_FIELD_IS_UNKNOWN: dict[
    bool, Callable[[str, list[str]], list[str]]
] = {
    True: _refuse_an_unknown_index_field,
    False: _the_known_index_fields,
}


@helper
def _refuse_an_index_over_no_columns(name: str, _declared: dict[str, Any]) -> list[str]:
    """An index over nothing, refused here rather than by the engine."""
    raise ValueError(
        f"index {name!r} has no 'columns'. An index over no columns "
        f"cannot be created, so this fails at DDL instead of here."
    )


@helper
def _the_index_columns(_name: str, declared: dict[str, Any]) -> list[str]:
    """The columns this index covers."""
    return list(declared["columns"])


_WHEN_AN_INDEX_HAS_NO_COLUMNS: dict[
    bool, Callable[[str, dict[str, Any]], list[str]]
] = {
    True: _refuse_an_index_over_no_columns,
    False: _the_index_columns,
}


@helper
def index_from_meta(declared: dict[str, Any]) -> tuple[str, Index]:
    """One Meta index entry, checked against what an Index actually is.

    Returns the name and the index, because the name is the key in the schema
    and never a field inside it — introspection does not report it there, so
    keeping it would make the differ see a mismatch on every run.

    THIS USED TO COPY WHATEVER IT WAS GIVEN. Every key but `name` went straight
    into the schema, so `uniqe=True` became a schema field: the index is not
    unique, nothing says so at load time, and the differ then compares a table
    carrying `uniqe` against a database that never reports it and can never
    reconcile them. The comprehension that did it also produced
    `dict[Any, Any]` where `Index` is required, which is why mypy could not
    see the shape either.

    An unknown key is refused rather than dropped. Dropping it silently trades
    a permanent diff for an index that is quietly missing a property the author
    asked for, and load time is the only moment anyone can act on the typo.
    """
    name = _WHEN_AN_INDEX_IS_UNNAMED[not declared.get("name")](declared)
    unknown = sorted(set(declared) - {"name", *INDEX_FIELDS})
    _WHEN_AN_INDEX_FIELD_IS_UNKNOWN[bool(unknown)](name, unknown)
    columns = _WHEN_AN_INDEX_HAS_NO_COLUMNS[not declared.get("columns")](name, declared)
    index: Index = {
        "columns": columns,
        **{key: declared[key] for key in _INDEX_OPTIONAL if key in declared},  # type: ignore[typeddict-item]
    }
    return name, index


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
def _not_a_table(_model_cls: type, _table_name: Any) -> tuple[str, Table] | None:
    """No `__tablename__`, so this class declares no table to convert.

    The caller filters on the result, so None is the answer rather than an
    absence. `pydantic_model_to_table` is public and a test asserts it.
    """
    return None  # noqa: RET501


@helper
def _the_declared_table(model_cls: type, table_name: Any) -> tuple[str, Table] | None:
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
        name: _WHEN_THE_KEY_IS_COMPOSITE[bool(primary_key)](col)
        for name, col in declared.items()
    }
    table: Table = {
        "columns": columns,
        **_META_ENTRY[bool(primary_key)]("primary_key", primary_key),  # type: ignore[typeddict-item]
        **_META_INDEXES[bool(indexes)](indexes),
        **_META_ENTRY[bool(constraints)]("constraints", constraints),
    }
    return table_name, table


_WHEN_NOT_A_TABLE: dict[bool, Callable[[type, Any], tuple[str, Table] | None]] = {
    True: _not_a_table,
    False: _the_declared_table,
}


@helper
def pydantic_model_to_table(model_cls: type) -> tuple[str, Table] | None:
    """Convert a Pydantic model to a Table definition.

    Args:
        model_cls: A Pydantic BaseModel subclass with __tablename__

    Returns:
        Tuple of (table_name, Table) or None if not a table model
    """
    table_name = getattr(model_cls, "__tablename__", None)
    return _WHEN_NOT_A_TABLE[table_name is None](model_cls, table_name)


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
    loader = _WHEN_A_MODULE_HAS_NO_LOADER[spec is None or spec.loader is None](
        spec, module_path
    )

    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[module_name] = module
    try:
        loader.exec_module(module)
    except Exception as e:
        raise LoaderError(f"Error executing module: {e}", path=str(module_path)) from e
    return module


@boundary
def load_declarations(module_path: Path) -> tuple[Schema, dict[str, View]]:
    """Everything a models file declares: its tables AND its views.

    ONE IMPORT, BOTH ANSWERS. A models file is ordinary Python and may do
    anything at import time, so reading tables and views through two separate
    entry points would execute it twice. Callers that want only tables still
    have `load_models_from_module`; the apply path wants both.
    """
    module = _import_module_file(module_path)
    return _schema_of(module), views_from_module(module)


@boundary
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


@helper
def _refuse_a_missing_models_directory(models_path: Path) -> Path:
    """A directory that is not there holds no models."""
    raise LoaderError(
        f"Models directory not found: {models_path}", path=str(models_path)
    )


@helper
def _the_models_directory(models_path: Path) -> Path:
    """The directory to walk."""
    return models_path


_WHEN_THE_MODELS_DIRECTORY_IS_MISSING: dict[bool, Callable[[Path], Path]] = {
    True: _refuse_a_missing_models_directory,
    False: _the_models_directory,
}


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


_WHEN_A_VIEW_KEY_IS_UNKNOWN: dict[
    bool, Callable[[dict[str, Any], list[str]], View]
] = {
    True: _refuse_an_unknown_view_key,
    False: _the_view_as_declared,
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
        for unknown in [
            sorted(set(obj) - set(VIEW_REQUIRED) - set(VIEW_OPTIONAL))
        ]
    ]
    return {view["name"]: view for view in checked}


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
    field, value, expected = wrong[0]
    raise LoaderError(
        f"view {described}: {field!r} is "
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
        f"view {described}: {blank[0]!r} is empty. A view needs a name "
        f"and a SELECT.",
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
        (field, obj[field], expected)
        for field, expected in _VIEW_FIELD_TYPES.items()
        if field in obj
        if not written_as(expected, obj[field])
    ]
    _WHEN_A_VIEW_FIELD_IS_THE_WRONG_TYPE[bool(wrong)](described, wrong)

    blank = [field for field in _VIEW_MUST_NOT_BE_BLANK if not obj[field].strip()]
    _WHEN_A_VIEW_FIELD_IS_BLANK[bool(blank)](described, blank)


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
def _refuse_a_directory_that_declares_nothing(
    models_path: Path, _schema: Schema
) -> Schema:
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

    schema: Schema = {
        table_name: definition for _py_file, table_name, definition in declared
    }
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
            for col_name, col in table.get("columns", {}).items()
            if col.get("literal_values")
        }
        for table_name, table in schema.items()
    }
    # A table whose columns name no members belongs in no answer at all: the
    # old shape only created its entry when the first such column arrived.
    return {table_name: columns for table_name, columns in found.items() if columns}
