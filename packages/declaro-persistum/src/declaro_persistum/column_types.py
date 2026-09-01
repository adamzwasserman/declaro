"""What a Python annotation means as a column type.

model_columns.py came out of pydantic_loader.py at 1156 lines, still over
the threshold. This half answers what an annotation becomes: the SQL type
it maps to, how a Literal turns into a lookup table, how a default is
spelled. What stays reads a model's `Meta` and builds its indexes and
constraints, and it names this half rather than the other way round.

Every name is re-exported from `model_columns`, and from `pydantic_loader`
above that, so the declaration API is unchanged.
"""

from __future__ import annotations

from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, Literal, TypedDict, Unpack, get_args, get_origin

from pydantic import BaseModel
from pydantic import Field as PydanticField
from pydantic_core import PydanticUndefined

from declaro_persistum.boundary import helper, recognizer
from declaro_persistum.exceptions import LoaderError
from declaro_persistum.types import (
    NOTHING,
    Index,
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
        f"Literal enum values must be strings, got {type_of(offenders[0]).__name__}: {offenders[0]}"
    )


@helper
def _the_literal_strings(_offenders: list[Any], members: tuple[Any, ...]) -> list[str]:
    """Every member is a string, so every member is a value."""
    return list(members)


_WHEN_A_MEMBER_IS_NOT_A_STRING: dict[bool, Callable[[list[Any], tuple[Any, ...]], list[str]]] = {
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
    return spelling in _UNION_SPELLINGS and type(
        None
    ) in get_args(  # honest: ignore HC-P005: type(None) is how an Optional is spelled
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
    beside = [
        arg for arg in get_args(annotation) if arg is not type(None)
    ]  # honest: ignore HC-P005: type(None) is how an Optional is spelled
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

    So the pipeline is annotation -> declared type -> engine SQL, and this is
    only the first step. `map_type(declared, engine)` is the one that writes
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
    """A Pydantic model, carrying the name the loader looks for.

    It is checked for habits first. A model that also does something is
    refused where it is written, which is the only place the writer still has
    the reason in their head.
    """
    found = habits_in(cls)
    return _WHEN_IT_ONLY_DECLARES[not found](cls, name, found)


@helper
def _only_declares(cls: type, name: str, _found: list[str]) -> type:
    """Nothing here but a table name and columns."""
    cls.__tablename__ = name  # type: ignore[attr-defined]
    return cls


@helper
def _refuse_a_model_that_also_does_something(cls: type, name: str, found: list[str]) -> type:
    """Refused with the declaration that replaces each habit.

    EVERY HABIT IS REPORTED, not the first. A writer who fixes one and runs
    again to find the next has been made to work for the checker.
    """
    listed = "\n\n  ".join(found)
    raise TypeError(
        f"@table('{name}'): {cls.__name__} declares a table and also does "
        f"something, and the library reads only the table name and the "
        f"columns. Everything else holds in Python and never reaches the "
        f"database.\n\n  {listed}"
    )


_WHEN_IT_ONLY_DECLARES: dict[bool, Callable[[type, str, list[str]], type]] = {
    True: _only_declares,
    False: _refuse_a_model_that_also_does_something,
}


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


# ---------------------------------------------------------------------------
# A MODEL DECLARES, AND DOES NOTHING ELSE.
#
# The loader reads two things off a model: the table name from `@table` and the
# columns from `model_fields`. Everything else in the class is invisible to it,
# so a rule written as a validator holds in Python and never reaches the
# database. The constraint then exists in one of the two places it should, and
# which one wins depends on whether the write came through your code or a psql
# prompt. That is the two-descriptions bug hiding inside the one file that
# exists to be the single description.
#
# EACH ROW CARRIES BOTH HALVES: how to find the habit on a class, and what to
# say when it is there. Adding a habit is adding a row. `habits_in` reads the
# table and names none of them, which is what keeps the table the rule rather
# than a list the function happens to agree with.
# ---------------------------------------------------------------------------


# What a class with no Pydantic decorators holds: three empty mappings, which
# is the same answer as a class carrying no validators.
_NO_DECORATORS = SimpleNamespace(field_validators={}, validators={}, model_validators={})


@helper
def _validators_on(model: type) -> list[str]:
    """Every validator Pydantic recorded on the class.

    READ THROUGH `getattr`, the way `model_columns` reads `model_fields`.
    `__pydantic_decorators__` is Pydantic's own private attribute and its stubs
    do not declare it, so a direct access is an error mypy is right about.
    """
    written = getattr(model, "__pydantic_decorators__", _NO_DECORATORS)
    return [
        *written.field_validators,
        *written.validators,
        *written.model_validators,
    ]


@helper
def _methods_on(model: type) -> list[str]:
    """Callables written in the class body, which are behavior and not data.

    A NESTED CLASS IS NOT A METHOD, and this counted one because a class is
    callable. `Meta` is a declaration the loader reads (`model_columns.py`
    reads it for table-level constraints), so flagging it called a valid
    declaration a habit and refused three real models.
    """
    return [
        name
        for name, value in vars(model).items()
        if callable(value)
        and not isinstance(value, type)
        and not name.startswith("_")
        and not isinstance(value, (property, classmethod, staticmethod))
    ]


@helper
def _properties_on(model: type) -> list[str]:
    return [name for name, value in vars(model).items() if isinstance(value, property)]


# NOT A HABIT: a Python default. `field(default=...)` is one way to declare a
# column default and `name: str = "unknown"` is the other, and the loader reads
# BOTH -- it turns the Python value into a SQL default, correctly, including
# the three shapes that are easy to get wrong (`0`, `False` and `""`). This
# table listed it as a habit for one commit on the strength of an assumption,
# and `test_a_pydantic_default_reaches_the_database.py` said otherwise.


@helper
def _inherited_by(model: type) -> list[str]:
    """Bases other than BaseModel, which hide where a column came from."""
    return [base.__name__ for base in model.__bases__ if base is not BaseModel]


# name -> (how to find it on a class, what to say about one). THE WORDS ARE IN
# THE ROW. They were three functions with one shape, differing only by the
# sentence each pasted in, which is the thing this file's own table exists to
# avoid. `{named}` is the name found on the class.
HABITS: dict[str, tuple[Callable[[type], list[str]], str]] = {
    "validator": (
        _validators_on,
        "{named!r} is a validator. It runs in Python and never reaches the "
        "database, so the rule holds for writes through your code and not for "
        "any other. Declare it instead: a range becomes "
        'field(check="total >= 0"), "already used" becomes field(unique=True), '
        "a rejected None becomes field(nullable=False), and a related row "
        'becomes field(references="people.id").',
    ),
    "method": (
        _methods_on,
        "{named!r} computes a value from other columns, and a computed value "
        "is not a column. Nothing about it reaches the schema, so the database "
        "never has it. Declare a view if the database should hold it, or keep "
        "the calculation in your own code if it should not.",
    ),
    "property": (
        _properties_on,
        "{named!r} computes a value from other columns, and a computed value "
        "is not a column. Nothing about it reaches the schema, so the database "
        "never has it. Declare a view if the database should hold it, or keep "
        "the calculation in your own code if it should not.",
    ),
    "inheritance": (
        _inherited_by,
        "this model inherits from {named!r}. Columns arriving from somewhere "
        "else are not visible in the file that declares the table, and this "
        "file is meant to be the whole description. Write the columns here.",
    ),
}


@helper
def habits_in(model: type) -> list[str]:
    """Everything in this class the library cannot read, said in full.

    Pure: a class in, a list of sentences out. It reads the table and knows
    nothing about what any row looks for, so a new rule is a new row.
    """
    return [says.format(named=found) for finds, says in HABITS.values() for found in finds(model)]


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
    spelled = [_SQL_DEFAULT[kind](value) for kind in [type_of(value)] if kind in _SQL_DEFAULT]
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


_WHEN_AN_INDEX_FIELD_IS_UNKNOWN: dict[bool, Callable[[str, list[str]], list[str]]] = {
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


_WHEN_AN_INDEX_HAS_NO_COLUMNS: dict[bool, Callable[[str, dict[str, Any]], list[str]]] = {
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
