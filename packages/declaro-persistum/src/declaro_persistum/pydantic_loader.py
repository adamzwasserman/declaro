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
from typing import Any, Literal, get_args, get_origin, get_type_hints

from pydantic import BaseModel
from pydantic import Field as PydanticField
from pydantic_core import PydanticUndefined

from declaro_persistum.boundary import boundary, helper, recognizer
from declaro_persistum.exceptions import LoaderError
from declaro_persistum.types import (
    Column,
    Index,
    Schema,
    Table,
    View,
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
        if not isinstance(arg, str):  # honest: ignore HC-P005: refusing a Literal member the loader cannot render
            raise ValueError(
                f"Literal enum values must be strings, got {type(arg).__name__}: {arg}"  # honest: ignore HC-P005: the class name IS the message
            )
        values.append(arg)

    return values


@recognizer
def is_optional_type(annotation: Any) -> bool:
    """Check if annotation is Optional (Union with None)."""
    origin = get_origin(annotation)
    if origin is None:
        return False

    # Check for Union types (including X | None syntax)
    origin_name = getattr(origin, "__name__", str(origin))
    if origin_name in ("Union", "UnionType"):
        args = get_args(annotation)
        return type(None) in args  # honest: ignore HC-P005: type(None) is how an Optional is spelled

    return False


@helper
def unwrap_optional(annotation: Any) -> Any:
    """Unwrap Optional[X] to get X."""
    if not is_optional_type(annotation):
        return annotation

    args = get_args(annotation)
    for arg in args:
        if arg is not type(None):  # honest: ignore HC-P005: type(None) is how an Optional is spelled
            return arg

    return annotation


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
    """
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

    raise LoaderError(
        f"cannot tell what column type {annotation!r} means. Declared types "
        f"come from: {', '.join(sorted(set(PYTHON_TO_SQL_TYPE.values())))}. "
        f"Use field(db_type=...) to declare one this mapping does not know."
    )


OnAction = Literal["cascade", "set null", "restrict", "no action"]


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
        if not (isinstance(cls, type) and issubclass(cls, BaseModel)):  # honest: ignore HC-P005: @table must refuse a class Pydantic did not make
            raise TypeError(
                f"@table('{name}') expects a Pydantic BaseModel subclass; "
                f"{cls.__name__} is not one. The loader reads `model_fields`, "
                f"so any other class yields a table with no columns."
            )
        cls.__tablename__ = name  # type: ignore[attr-defined]
        return cls

    return declare


@helper
def field(
    *,
    primary_key: bool | None = None,
    unique: bool | None = None,
    nullable: bool | None = None,
    references: str | None = None,
    on_delete: OnAction | None = None,
    on_update: OnAction | None = None,
    check: str | None = None,
    default: str | None = None,
    db_type: str | None = None,
    renamed_from: str | None = None,
    is_new: bool | None = None,
) -> Any:
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
    quietly not a primary key. Named keyword arguments make it a TypeError at
    import. Same defect as `index_from_meta`, on the other side of this loader.

    `default` IS A SQL EXPRESSION, NOT A PYTHON VALUE. `default="now()"` means
    DEFAULT now(); it is not Pydantic's default and does not become the string
    "now()". Pydantic's own default is read separately and quoted as a literal,
    which is why the two cannot share one argument.

    `primary_key`, not `primary`. The documentation wrote `primary=` and the
    loader accepts either, but the schema key, introspection and the SQL all
    say `primary_key`. Nothing can depend on the old spelling, because nothing
    could ever call this.

    Every argument is keyword-only and defaults to None, which here means
    "not declared" rather than a value: a None is dropped, so the schema
    carries only what the model actually said.
    """
    declared = {
        "primary_key": primary_key,
        "unique": unique,
        "nullable": nullable,
        "references": references,
        "on_delete": on_delete,
        "on_update": on_update,
        "check": check,
        "default": default,
        "db_type": db_type,
        "renamed_from": renamed_from,
        "is_new": is_new,
    }
    # Pydantic carries field metadata on its own FieldInfo and offers no
    # other way to attach it. This is the declarative surface an author writes
    # against, so the object IS the contract rather than a leak of one.
    return PydanticField(  # honest: ignore HC-P010: Pydantic's own field type
        json_schema_extra={k: v for k, v in declared.items() if v is not None}
    )


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
def sql_default(value: Any) -> str:
    """The SQL spelling of a Python default.

    A value with no spelling is refused rather than dropped. Returning nothing
    would give the column no default at all, which is a schema quietly
    different from the one the author declared.
    """
    spell = _SQL_DEFAULT.get(type(value))  # honest: ignore HC-P005: the exact type IS the lookup key
    if spell is None:
        raise LoaderError(
            f"Cannot write {value!r} as a SQL default: a default may be a "
            f"string, a bool, an int or a float, and this is "
            f"{type(value).__name__}."  # honest: ignore HC-P005: the class name IS the message
        )
    return spell(value)


# THE THREE WAYS A FIELD SAYS IT HAS NO DEFAULT. `PydanticUndefined` is what
# Pydantic puts there; `None` and `...` are what an author writes.
_NO_DEFAULT = (None, ..., PydanticUndefined)


@helper
def extract_field_metadata(field_info: Any) -> dict[str, Any]:
    """Extract declaro-specific metadata from Pydantic field.

    Looks for metadata set via field() function like:
        field(primary_key=True, unique=True, references="users.id")
    """
    meta: dict[str, Any] = {}

    # Check for json_schema_extra (Pydantic v2)
    # A MAPPING IS HOW DECLARO'S OWN METADATA IS WRITTEN, and Pydantic's
    # `metadata` list also holds constraint objects that are not ours. Reading
    # the spelling is what tells declaro's entries from theirs.
    json_extra = getattr(field_info, "json_schema_extra", None)
    if written_as(dict, json_extra):
        meta.update(json_extra)

    # Check for metadata attribute
    metadata = getattr(field_info, "metadata", None)
    if metadata:
        for item in metadata:
            if written_as(dict, item):
                meta.update(item)

    # EVERY SPELLING OF "NO DEFAULT", RESOLVED HERE. This knew two, `None`
    # and `...`, and not Pydantic's own `PydanticUndefined`, which is what a
    # field with no default actually carries. That sentinel was stored as
    # though it were a value, and the chain that spelled a default for SQL had
    # no branch matching it, so absence worked by falling through. It stopped
    # working the moment that chain became a lookup that refuses what it
    # cannot write, which is the right refusal reaching the wrong value.
    default = getattr(field_info, "default", None)
    if default not in _NO_DEFAULT:
        meta["_default_value"] = default

    return meta


INDEX_FIELDS = ("columns", "unique", "where", "using")


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
    name = declared.get("name")
    if not name:
        raise ValueError(
            f"index {declared!r} has no 'name'. The name is the key this index "
            f"is stored under, so there is nothing to store it as."
        )

    unknown = sorted(set(declared) - {"name", *INDEX_FIELDS})
    if unknown:
        raise ValueError(
            f"index {name!r} declares {unknown}, which an index has no such "
            f"field for. An index takes {list(INDEX_FIELDS)}. A key that is "
            f"not one of those reaches the schema, never comes back from "
            f"introspection, and leaves the differ reporting this table as "
            f"changed on every run."
        )

    columns = declared.get("columns")
    if not columns:
        raise ValueError(
            f"index {name!r} has no 'columns'. An index over no columns "
            f"cannot be created, so this fails at DDL instead of here."
        )

    index: Index = {"columns": list(columns)}
    if "unique" in declared:
        index["unique"] = declared["unique"]
    if "where" in declared:
        index["where"] = declared["where"]
    if "using" in declared:
        index["using"] = declared["using"]
    return name, index


@helper
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

    # Get annotations and fields.
    #
    # Use typing.get_type_hints rather than reading __annotations__ directly:
    # under PEP 563 (`from __future__ import annotations`) and for any string
    # forward reference (`name: "User"`), __annotations__ contains *strings*,
    # not types. Direct access then makes downstream type lookups (e.g.
    # `bool: "boolean"` in PYTHON_TO_SQL_TYPE) miss, silently emitting
    # `text` columns where the user wrote `bool`. get_type_hints resolves
    # string annotations to actual types using the model's module globals.
    # `get_type_hints` resolves a models file's annotations against its
    # module globals, so this is the intake of the declarative surface. The
    # catch exists to say WHICH model could not be read, and it re-raises as
    # LoaderError rather than swallowing.
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
            f"fixed, so the type of no column can be determined."
        ) from unresolvable
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
        sql_type = meta.get("db_type") or declared_type(unwrapped)

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
        # One spelling. This accepted `primary` as well, which no caller
        # writes: `field()` emits `primary_key`, the Column key is
        # `primary_key`, introspection returns `primary_key`, and the six
        # documented `field(primary=...)` examples never ran because `field`
        # did not exist. Two names for one property is where the drift starts.
        if meta.get("primary_key"):
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
            col["default"] = sql_default(meta["_default_value"])

        # Migration hints
        if meta.get("renamed_from"):
            col["renamed_from"] = meta["renamed_from"]

        if meta.get("is_new"):
            col["is_new"] = meta["is_new"]

        columns[field_name] = col

    table: Table = {"columns": columns}

    # Check for Meta class with indexes, constraints, and primary_key
    meta_cls = getattr(model_cls, "Meta", None)
    if meta_cls:
        # Extract composite primary key
        primary_key = getattr(meta_cls, "primary_key", None)
        if primary_key:
            table["primary_key"] = primary_key
            # Remove primary_key flag from individual columns when using composite PK
            for col_name in columns:
                if "primary_key" in columns[col_name]:
                    del columns[col_name]["primary_key"]

        indexes = getattr(meta_cls, "indexes", None)
        if indexes:
            # EVERY DECLARED INDEX, OR A REFUSAL NAMING THE ONE THAT IS NOT
            # ONE. This read `if isinstance(idx, dict)`, so an entry written
            # wrongly was skipped: the author got a table without that index
            # and no complaint, and found out when a query was slow or a
            # uniqueness constraint did not hold.
            table["indexes"] = dict(index_from_meta(idx) for idx in indexes)

        constraints = getattr(meta_cls, "constraints", None)
        if constraints:
            table["constraints"] = constraints

    return table_name, table


@boundary
def _import_module_file(module_path: Path) -> Any:
    """Import a model file once and hand back the module.

    Tables and views are both read off the same module, so the import is
    separated from what is read out of it. Doing it twice would execute the
    file twice, and a models file is ordinary Python that may do anything at
    import time.
    """
    if not module_path.exists():
        raise LoaderError(f"Module not found: {module_path}", path=str(module_path))

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
    schema: Schema = {}

    # Find all classes with __tablename__
    for name in dir(module):
        if name.startswith("_"):
            continue

        obj = getattr(module, name)
        if not isinstance(obj, type):  # honest: ignore HC-P005: reading a module's contents to find the tables
            continue

        if not hasattr(obj, "__tablename__"):
            continue

        result = pydantic_model_to_table(obj)
        if result:
            table_name, table = result
            schema[table_name] = table

    return schema


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
    if not models_path.exists():
        raise LoaderError(
            f"Models directory not found: {models_path}", path=str(models_path)
        )

    views: dict[str, View] = {}
    for py_file in models_path.glob("**/*.py"):
        if py_file.name.startswith(("_", "test_")):
            continue
        try:
            views.update(views_from_module(_import_module_file(py_file)))
        except LoaderError:
            raise
        except Exception:
            continue
    return views


VIEW_REQUIRED = ("name", "query")
VIEW_OPTIONAL = ("materialized", "refresh", "depends_on", "trigger_sources")


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
    views: dict[str, View] = {}
    for name in dir(module):
        if name.startswith("_"):
            continue
        obj = getattr(module, name)
        # A module's namespace holds every kind of thing. A view is written as
        # a mapping, so anything else in there is not a declaration this
        # loader is looking at.
        if not written_as(dict, obj):
            continue
        if not all(k in obj for k in VIEW_REQUIRED):
            continue
        unknown = set(obj) - set(VIEW_REQUIRED) - set(VIEW_OPTIONAL)
        if unknown:
            raise LoaderError(
                f"view '{obj['name']}' declares unknown {'keys' if len(unknown) > 1 else 'key'} "
                f"{sorted(unknown)}. A view may carry "
                f"{list(VIEW_REQUIRED + VIEW_OPTIONAL)}.",
            )
        _check_view(obj)
        views[obj["name"]] = obj
    return views


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
def _check_view(obj: dict[str, Any]) -> None:
    """Refuse a view whose fields are the wrong type or empty. Raises or returns.

    WHAT IS DELIBERATELY NOT CHECKED: whether the query is valid SQL. That
    needs an engine, the engine says so at apply time, and a loader guessing
    at validity would reject dialect-specific syntax it does not know.
    """
    described = _VIEW_DESCRIBED[written_as(str, obj.get("name"))](obj.get("name"))

    for field, expected in _VIEW_FIELD_TYPES.items():
        if field not in obj:
            continue
        value = obj[field]
        # WHAT THE VALUE INHERITS COUNTS, so a mapping or string a library
        # subclassed is still the thing it derives from. The direction is
        # what makes this safe where an `isinstance(x, int)` was not: `bool`
        # derives from `int`, so a flag is written as a number and a number
        # is not written as a flag, and `materialized` refuses 1.
        if not written_as(expected, value):
            raise LoaderError(
                f"view {described}: {field!r} is "
                f"{type(value).__name__}, and a view's {field!r} must be "  # honest: ignore HC-P005: the class name IS the message
                f"{expected.__name__}."
            )

    for field in _VIEW_MUST_NOT_BE_BLANK:
        if not obj[field].strip():
            raise LoaderError(
                f"view {described}: {field!r} is empty. A view needs a name "
                f"and a SELECT."
            )


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

    if not models_path.exists():
        raise LoaderError(f"Models directory not found: {models_path}", path=str(models_path))

    schema: Schema = {}
    # Which file declared each table, so a collision can name both sides. One
    # filename tells a developer a collision happened without saying where.
    declared_in: dict[str, str] = {}

    # Load all .py files (except __init__.py and test files)
    for py_file in models_path.glob("**/*.py"):
        if py_file.name.startswith("_"):
            continue
        if py_file.name.startswith("test_"):
            continue

        try:
            file_schema = load_models_from_module(py_file)
        except LoaderError:
            # Re-raise loader errors
            raise
        except Exception as exc:
            # A FILE THAT FAILS TO LOAD IS NOT A FILE THAT DECLARES NOTHING.
            #
            # This was `except Exception: continue`, commented "Skip files
            # that can't be imported (might not be model files)". The intent
            # is real -- a models directory may hold helpers -- but the two
            # cases are different and conflating them cost the author the only
            # error they could act on.
            #
            # Measured 2026-08-17. A one-letter typo in a Meta index,
            # `"uniqe": True`, which `index_from_meta` refuses by name and by
            # design, reached the author as:
            #
            #     LoaderError: No @table decorated models found in models/
            #
            # which sends them looking for a missing decorator.
            #
            # A helper that imports cleanly and declares no table still
            # contributes nothing, which is the case the swallow was reaching
            # for and which needs no swallow at all.
            raise LoaderError(
                f"{py_file.name} could not be loaded: "
                f"{type(exc).__name__}: {exc}",
                path=str(py_file),
            ) from exc

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
        # It has to be caught HERE because this is the only moment both halves
        # exist. Once the update has run, the first declaration is gone and
        # nothing downstream can tell a collision from a single model.
        for table_name, definition in file_schema.items():
            if table_name in declared_in:
                raise LoaderError(
                    f"Two models declare the table {table_name!r}: "
                    f"{declared_in[table_name]} and {py_file.name}. A schema "
                    f"holds one definition per table, so merging them would "
                    f"silently keep whichever file loaded last and drop the "
                    f"other's columns from the database.",
                    path=str(py_file),
                )
            declared_in[table_name] = py_file.name
            schema[table_name] = definition

    if not schema:
        raise LoaderError(
            f"No @table decorated models found in {models_path}",
            path=str(models_path),
        )

    return schema


@helper
def get_literal_columns(schema: Schema) -> dict[str, dict[str, list[str]]]:
    """Extract all columns with Literal type values from schema.

    Returns:
        Dict of table_name -> {column_name -> [literal_values]}
    """
    result: dict[str, dict[str, list[str]]] = {}

    for table_name, table in schema.items():
        columns = table.get("columns", {})
        for col_name, col in columns.items():
            literal_values = col.get("literal_values")
            if literal_values:
                if table_name not in result:
                    result[table_name] = {}
                result[table_name][col_name] = literal_values

    return result
