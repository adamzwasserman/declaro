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

from declaro_persistum.exceptions import LoaderError
from declaro_persistum.types import Column, Index, Schema, Table, View

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


def is_literal_type(annotation: Any) -> bool:
    """Check if annotation is a Literal type."""
    return get_origin(annotation) is Literal


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
        if not isinstance(arg, str):
            raise ValueError(
                f"Literal enum values must be strings, got {type(arg).__name__}: {arg}"
            )
        values.append(arg)

    return values


def is_optional_type(annotation: Any) -> bool:
    """Check if annotation is Optional (Union with None)."""
    origin = get_origin(annotation)
    if origin is None:
        return False

    # Check for Union types (including X | None syntax)
    origin_name = getattr(origin, "__name__", str(origin))
    if origin_name in ("Union", "UnionType"):
        args = get_args(annotation)
        return type(None) in args

    return False


def unwrap_optional(annotation: Any) -> Any:
    """Unwrap Optional[X] to get X."""
    if not is_optional_type(annotation):
        return annotation

    args = get_args(annotation)
    for arg in args:
        if arg is not type(None):
            return arg

    return annotation


def python_type_to_sql(annotation: Any) -> str:
    """Convert Python type annotation to SQL type string."""
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

    # Default to text
    return "text"


OnAction = Literal["cascade", "set null", "restrict", "no action"]


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

    def declare(cls: type) -> type:
        if not (isinstance(cls, type) and issubclass(cls, BaseModel)):
            raise TypeError(
                f"@table('{name}') expects a Pydantic BaseModel subclass; "
                f"{cls.__name__} is not one. The loader reads `model_fields`, "
                f"so any other class yields a table with no columns."
            )
        cls.__tablename__ = name  # type: ignore[attr-defined]
        return cls

    return declare


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
    return PydanticField(
        json_schema_extra={k: v for k, v in declared.items() if v is not None}
    )


def extract_field_metadata(field_info: Any) -> dict[str, Any]:
    """Extract declaro-specific metadata from Pydantic field.

    Looks for metadata set via field() function like:
        field(primary_key=True, unique=True, references="users.id")
    """
    meta: dict[str, Any] = {}

    # Check for json_schema_extra (Pydantic v2)
    json_extra = getattr(field_info, "json_schema_extra", None)
    if isinstance(json_extra, dict):
        meta.update(json_extra)

    # Check for metadata attribute
    metadata = getattr(field_info, "metadata", None)
    if metadata:
        for item in metadata:
            if isinstance(item, dict):
                meta.update(item)

    # Check default value
    default = getattr(field_info, "default", None)
    if default is not None and default is not ...:
        # Store default for later processing
        meta["_default_value"] = default

    return meta


INDEX_FIELDS = ("columns", "unique", "where", "using")


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
    try:
        annotations = get_type_hints(model_cls)
    except (NameError, TypeError):
        # Fall back if a forward reference is unresolvable. Caller will see
        # the same wrong-but-consistent behavior as pre-0.1.3 — better than
        # crashing during schema load.
        annotations = getattr(model_cls, "__annotations__", {})
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
        sql_type = meta.get("db_type") or python_type_to_sql(unwrapped)

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
            default_val = meta["_default_value"]
            if isinstance(default_val, str):
                col["default"] = f"'{default_val}'"
            elif isinstance(default_val, bool):
                col["default"] = "TRUE" if default_val else "FALSE"
            elif isinstance(default_val, (int, float)):
                col["default"] = str(default_val)

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
            table["indexes"] = dict(
                index_from_meta(idx) for idx in indexes if isinstance(idx, dict)
            )

        constraints = getattr(meta_cls, "constraints", None)
        if constraints:
            table["constraints"] = constraints

    return table_name, table


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


def load_declarations(module_path: Path) -> tuple[Schema, dict[str, View]]:
    """Everything a models file declares: its tables AND its views.

    ONE IMPORT, BOTH ANSWERS. A models file is ordinary Python and may do
    anything at import time, so reading tables and views through two separate
    entry points would execute it twice. Callers that want only tables still
    have `load_models_from_module`; the apply path wants both.
    """
    module = _import_module_file(module_path)
    return _schema_of(module), views_from_module(module)


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


def _schema_of(module: Any) -> Schema:
    """The tables a loaded module declares."""
    schema: Schema = {}

    # Find all classes with __tablename__
    for name in dir(module):
        if name.startswith("_"):
            continue

        obj = getattr(module, name)
        if not isinstance(obj, type):
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
        if not isinstance(obj, dict):
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
        views[obj["name"]] = obj  # type: ignore[assignment]
    return views


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

    # Load all .py files (except __init__.py and test files)
    for py_file in models_path.glob("**/*.py"):
        if py_file.name.startswith("_"):
            continue
        if py_file.name.startswith("test_"):
            continue

        try:
            file_schema = load_models_from_module(py_file)
            schema.update(file_schema)
        except LoaderError:
            # Re-raise loader errors
            raise
        except Exception:
            # Skip files that can't be imported (might not be model files)
            continue

    if not schema:
        raise LoaderError(
            f"No @table decorated models found in {models_path}",
            path=str(models_path),
        )

    return schema


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
