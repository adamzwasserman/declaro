"""How a declared value is spelled for the engine that will read it.

TWO SUBJECTS LIVED IN `shared.py` AND THIS IS ONE OF THEM. A type, a default
and a column definition are spellings: the schema says what a column means
and this says how each engine writes it. What stayed behind is a different
job, turning an operation into DDL, and it calls into here rather than the
other way round.

The split happened at 1009 lines, nine over the threshold, and every name is
re-exported from `shared` so no caller had to change.
"""

from collections.abc import Callable
from types import MappingProxyType, NoneType
from typing import Any

# Re-exported: inspector.shared is the single source of truth for PRAGMA parsing.
from declaro_persistum.boundary import helper
from declaro_persistum.inspector.shared import (
    columns_from_pragma_rows as columns_from_pragma_rows,
)
from declaro_persistum.types import (
    Column,
    Engine,
    type_of,
    written_as,
)

# =============================================================================
# Type mapping
# =============================================================================


TypeMap = Callable[[str], str]


# THE DECLARED TYPES SQLITE STORES, EACH BESIDE THE CLASS IT LANDS IN. This
# was an if/elif chain whose order a reader had to hold in their head to know
# which rule answered. The three tables below are read in the order they are
# written and the first answer wins, which is the only ordering that remains.
_SQLITE_STORAGE_CLASS: dict[str, str] = {
    "text": "TEXT",
    "varchar": "TEXT",
    "char": "TEXT",
    "string": "TEXT",
    "boolean": "INTEGER",
    "bool": "INTEGER",
    "uuid": "TEXT",
    "timestamptz": "TEXT",
    "timestamp": "TEXT",
    "date": "TEXT",
    "datetime": "TEXT",
    "jsonb": "TEXT",
    "json": "TEXT",
    "float": "REAL",
    "double": "REAL",
    "real": "REAL",
    "float4": "REAL",
    "float8": "REAL",
    "blob": "BLOB",
    "bytea": "BLOB",
}

# A NAME THAT CARRIES ITS SIZE. `bigint`, `smallint` and `int4` all hold
# "int", and this fragment is read before the exact names because it always
# was: it was the first arm of the chain.
_SQLITE_BY_FRAGMENT: tuple[tuple[str, str], ...] = (("int", "INTEGER"),)

# A NAME THAT CARRIES A PARAMETER LIST: `varchar(255)`, `numeric(10,2)`.
_SQLITE_BY_PREFIX: tuple[tuple[str, str], ...] = (
    ("varchar(", "TEXT"),
    ("numeric", "REAL"),
)


@helper
def sqlite_type(type_str: str) -> str:
    """SQLite storage classes. Turso shares them.

    Five classes, so uuid, every timestamp and every json flavor all land in
    TEXT. A type the mapper does not recognize is passed through upper-cased
    rather than guessed at, so an engine-specific spelling survives.
    """
    type_lower = type_str.lower()
    spellings = [
        *[storage for fragment, storage in _SQLITE_BY_FRAGMENT if fragment in type_lower],
        *[_SQLITE_STORAGE_CLASS[name] for name in [type_lower] if name in _SQLITE_STORAGE_CLASS],
        *[storage for prefix, storage in _SQLITE_BY_PREFIX if type_lower.startswith(prefix)],
        type_str.upper(),
    ]
    return spellings[0]


@helper
def postgresql_type(type_str: str) -> str:
    """PostgreSQL has a real type for everything the vocabulary declares.

    uuid is uuid, timestamptz is timestamptz, numeric(10,2) keeps its precision.
    Nothing to translate, which is exactly why this being missing was invisible:
    the identity function and no function look the same until another engine
    asks.
    """
    return type_str


TYPE_MAPS: dict[str, TypeMap] = {
    "sqlite": sqlite_type,
    "turso": sqlite_type,
    "postgresql": postgresql_type,
}


@helper
def _one_absence_is_not_the_other(left: Any, right: Any, _engine: Engine) -> bool:
    """One side has no type, so the answer is whether both sides have none."""
    return bool(left is right)


@helper
def _the_same_spelling(left: Any, right: Any, engine: Engine) -> bool:
    """Two declared types, compared as this engine writes them."""
    return bool(map_type(left, engine) == map_type(right, engine))


# WHETHER A TYPE IS MISSING ON EITHER SIDE. Absence is a different question
# from difference, and each has its own answer rather than a shared branch.
_COMPARE_TYPES: dict[bool, Callable[[Any, Any, Engine], bool]] = {
    True: _one_absence_is_not_the_other,
    False: _the_same_spelling,
}


@helper
def same_type(left: str | None, right: str | None, engine: Engine) -> bool:
    """Do two declared types name the same column ON THIS ENGINE?

    `uuid` and `text` are one column on SQLite and two on PostgreSQL, so the
    question cannot be answered without the engine and a string comparison
    cannot answer it at all. The differ compared the raw strings and therefore
    proposed a type change on every migration after the first: introspection
    reports what the engine stored, the models file says what was declared,
    and on SQLite those are never the same word.

    Absent on either side means the caller has no type to compare, which is
    not the same as the types differing; two absences are equal and one is not.
    """
    return _COMPARE_TYPES[left is None or right is None](left, right, engine)


@helper
def _refuse_unmappable_engine(engine: Engine) -> Engine:
    """No engine by this name declares how it spells a type."""
    raise ValueError(f"cannot spell types for engine {engine!r}. Supported: {sorted(TYPE_MAPS)}.")


@helper
def _the_mappable_engine(engine: Engine) -> Engine:
    """An engine with a type mapper, handed back to be looked up."""
    return engine


# The refusal happens before the lookup, so `TYPE_MAPS` is read with [] and a
# engine it does not hold raises here rather than through a `.get` fallback.
_WHEN_UNMAPPABLE: dict[bool, Callable[[Engine], Engine]] = {
    True: _the_mappable_engine,
    False: _refuse_unmappable_engine,
}


@helper
def map_type(type_str: str, engine: Engine) -> str:
    """Spell a declared type for one engine.

    `engine` is required (Rule 14). A default would have meant PostgreSQL
    silently receiving SQLite affinity, which is the defect this replaced.
    """
    mappable = _WHEN_UNMAPPABLE[engine in TYPE_MAPS](engine)
    return TYPE_MAPS[mappable](type_str)


# =============================================================================
# Column definition
# =============================================================================


# How each engine spells the expression defaults a portable schema may declare.
#
# A DEFAULT USED TO BE PASSED THROUGH VERBATIM, so a schema written for one
# engine emitted text another cannot parse. Measured 2026-08-17, running
# `declaro generate` against the schema this package ships:
#
#     "created_at" TEXT DEFAULT now()
#     sqlite3.OperationalError: near "(": syntax error
#
# and the command exited 0. The renderer wrapped parentheses and otherwise
# copied the string, which is a translation table with no entries.
#
# The keys are lowercased and stripped of spaces before lookup, so `NOW()` and
# `now ()` find the same row.
EXPRESSION_DEFAULTS: dict[str, dict[str, str]] = {
    # NO BOOLEAN ROWS. They were here for one day and broke the round trip.
    # Rewriting `false` as `0` meant the schema declared "false", the table was
    # written `DEFAULT 0`, PRAGMA read back "0", and the differ emitted an
    # alter_column on every deploy for a column nobody had touched.
    #
    # Measured on SQLite 3.50.4 and pyturso: both take the keyword.
    #
    #     DEFAULT false -> parses, PRAGMA returns 'false'   (sqlite)
    #     DEFAULT false -> parses, PRAGMA returns 'FALSE'   (turso)
    #
    # Turso uppercases it, which is why `same_default` folds boolean spellings
    # and has since before this table existed. Translating a value the engine
    # already understands turns a working round trip into a migration loop.
    "sqlite": {
        "now()": "(datetime('now'))",
        "current_timestamp": "CURRENT_TIMESTAMP",
    },
    "turso": {
        "now()": "(datetime('now'))",
        "current_timestamp": "CURRENT_TIMESTAMP",
    },
    "postgresql": {},  # PostgreSQL is the language these are written in.
}

# Expressions an engine has NO equivalent for, and what to tell the reader.
#
# THIS IS NOT AN OVERSIGHT IN THE TABLE ABOVE, it is the reason the table has a
# companion. SQLite can produce `lower(hex(randomblob(16)))`, a 32-character
# random hex string that LOOKS like a UUID and is not one -- the version and
# variant bits are wrong, so it is not v4, and anything that parses or
# validates it is being lied to. Translating it would replace a loud syntax
# error with a quiet wrong value.
_NO_UUID = MappingProxyType(
    {
        "gen_random_uuid()": (
            "SQLite has no UUID generator, and a random hex string is not a "
            "v4 UUID. Have the application supply the id"
        ),
        "uuid_generate_v4()": (
            "SQLite has no UUID generator, and a random hex string is not a "
            "v4 UUID. Have the application supply the id"
        ),
    }
)

# WRITTEN ONCE, WHOLE. This was built with sqlite's entry and then mutated on
# the next two lines to add turso and postgresql, so the table a reader sees
# was not the table the code used. Every engine is here, and the mapping
# cannot be changed after import.
NO_EQUIVALENT = MappingProxyType(
    {
        "sqlite": _NO_UUID,
        "turso": _NO_UUID,
        "postgresql": MappingProxyType({}),
    }
)


@helper
def _the_engines_spelling(spelled: dict[str, str], key: str, _value: Any) -> Any:
    """This engine writes the expression its own way."""
    return spelled[key]


@helper
def _as_declared(_spelled: dict[str, str], _key: str, value: Any) -> Any:
    """No engine-specific spelling, so what the author wrote stands."""
    return value


# WHETHER THIS ENGINE RESPELLS THE EXPRESSION. The engine is a closed
# vocabulary and is read by subscript; the expression is not, and both of its
# cases are named rather than one hiding in a `.get` fallback.
_SPELLED_FOR: dict[bool, Callable[[dict[str, str], str, Any], Any]] = {
    True: _the_engines_spelling,
    False: _as_declared,
}


@helper
def _untranslated_default(value: Any, _engine: str, _column: str) -> Any:
    """A number, a flag or nothing: the same in every engine.

    Only text can name an expression, so only text has an engine spelling.
    """
    return value


@helper
def _refuse_unspellable_expression(value: str, engine: str, column: str, refusal: list[str]) -> str:
    """This engine has no way to write the expression the column declares."""
    raise ValueError(
        f"column {column!r} defaults to {value}, which {engine} cannot spell. {refusal[0]}."
    )


@helper
def _the_spellable_expression(value: str, _engine: str, _column: str, _refusal: list[str]) -> str:
    """An expression this engine can write, handed back to be spelled."""
    return value


# WHETHER THIS ENGINE REFUSES THE EXPRESSION OUTRIGHT, read before the
# spelling table, so a refusal names the column rather than passing text the
# engine cannot parse.
_WHEN_NO_EQUIVALENT: dict[bool, Callable[[str, str, str, list[str]], str]] = {
    True: _refuse_unspellable_expression,
    False: _the_spellable_expression,
}


@helper
def _translate_text_default(value: str, engine: str, column: str) -> Any:
    """Text, matched against what this engine can and cannot spell."""
    key = value.strip().lower().replace(" ", "")

    refused = NO_EQUIVALENT[engine]
    refusal = [refused[name] for name in [key] if name in refused]
    spellable = _WHEN_NO_EQUIVALENT[bool(refusal)](value, engine, column, refusal)

    spelled = EXPRESSION_DEFAULTS[engine]
    return _SPELLED_FOR[key in spelled](spelled, key, spellable)


# ONLY TEXT CARRIES An ENGINE SPELLING, and both cases are written out. This
# was a `.get` whose second argument answered for every type the table does
# not name, which reads the same as the fallbacks removed everywhere else
# today. Naming the other case says it is an answer rather than a silence: a
# number is a number in every engine and nothing about it varies.
_TRANSLATE_DEFAULT: dict[bool, Callable[[Any, str, str], Any]] = {
    True: _translate_text_default,
    False: _untranslated_default,
}


@helper
def _translate_default(value: Any, engine: str, column: str) -> Any:
    """The declared default, spelled for `engine`, or a refusal naming both."""
    return _TRANSLATE_DEFAULT[written_as(str, value)](value, engine, column)


@helper
def _null_default(_value: Any) -> str:
    """Nothing declared, which SQLite spells NULL."""
    return "DEFAULT NULL"


@helper
def _literal_default(value: Any) -> str:
    """A number or a flag, written as it stands."""
    return f"DEFAULT {value}"


@helper
def _wrapped_default(value: str) -> str:
    """An expression, wrapped a second time so the round trip agrees.

    PRAGMA table_info returns `now()` for a column declared `DEFAULT (now())`,
    so introspection loses one layer and the differ then sees a change that is
    not one. Two layers go in and one comes back.
    """
    return f"DEFAULT ({value})"


@helper
def _bare_default(value: str) -> str:
    """Text that is not parenthesised: a quoted string, or a bare function."""
    return f"DEFAULT {value}"


# WHETHER THE TEXT ARRIVED PARENTHESISED, as a table rather than an `and`.
_TEXT_DEFAULT: dict[bool, Callable[[str], str]] = {
    True: _wrapped_default,
    False: _bare_default,
}


@helper
def _text_default(value: str) -> str:
    """Text, spelled by whether it is already an expression."""
    return _TEXT_DEFAULT[value.startswith("(")](value)


@helper
def _refuse_unspellable_default(value: Any) -> Any:
    """A default nobody declared a spelling for."""
    raise ValueError(
        f"No SQLite spelling for a default written as "
        f"{type_of(value).__name__}: {value!r}. A default is nothing, "
        f"text, a number or a flag."
    )


# THE SPELLINGS OF A DEFAULT, KEYED BY THE TYPE EACH ANSWERS. This was an
# ordering -- `value is None`, then `isinstance(value, str) and
# value.startswith("(")`, then a fall-through that formatted whatever was left
# into the SQL. `f"DEFAULT {value}"` accepts every Python object, so a value
# with no declared spelling became syntactically valid nonsense in a CREATE
# TABLE, and on a rebuild that statement runs after the real table is dropped.
_SQLITE_DEFAULT: dict[type, Callable[[Any], str]] = {
    NoneType: _null_default,
    str: _text_default,
    bool: _literal_default,
    int: _literal_default,
    float: _literal_default,
}


@helper
def _the_spellable_default(value: Any) -> Any:
    """A default this table has a spelling for, handed back to be spelled."""
    return value


# The refusal happens before the lookup, so the table is read with [] and a
# type it does not hold raises here rather than being answered by a fallback.
# Both entries return the value, so neither function has an empty body.
_WHEN_UNSPELLABLE: dict[bool, Callable[[Any], Any]] = {
    True: _the_spellable_default,
    False: _refuse_unspellable_default,
}


@helper
def sqlite_default(value: Any) -> str:
    """The text that follows DEFAULT in a SQLite CREATE TABLE."""
    spellable = _WHEN_UNSPELLABLE[type_of(value) in _SQLITE_DEFAULT](value)
    return _SQLITE_DEFAULT[type_of(spellable)](spellable)


@helper
def postgresql_default(value: Any) -> str:
    """PostgreSQL returns an expression default as it was written."""
    return "DEFAULT NULL" if value is None else f"DEFAULT {value}"


RenderDefault = Callable[[Any], str]

DEFAULTS: dict[str, RenderDefault] = {
    "sqlite": sqlite_default,
    "turso": sqlite_default,
    "postgresql": postgresql_default,
}


@helper
def _refuse_unrenderable_engine(engine: Engine) -> Engine:
    """No engine by this name declares how it writes a default."""
    raise ValueError(
        f"cannot render a column for engine {engine!r}. Supported: {sorted(DEFAULTS)}."
    )


@helper
def _the_renderable_engine(engine: Engine) -> Engine:
    """An engine with a default renderer, handed back to be looked up."""
    return engine


_WHEN_UNRENDERABLE: dict[bool, Callable[[Engine], Engine]] = {
    True: _the_renderable_engine,
    False: _refuse_unrenderable_engine,
}


@helper
def _foreign_key_clause(col: Column, reference: str) -> str:
    """REFERENCES, with whichever cascade actions the column declares."""
    ref_table, ref_col = reference.split(".")
    return " ".join(
        [
            f'REFERENCES "{ref_table}"("{ref_col}")',
            *[
                f"ON DELETE {action.upper().replace('_', ' ')}"
                for action in [col.get("on_delete")]
                if action
            ],
            *[
                f"ON UPDATE {action.upper().replace('_', ' ')}"
                for action in [col.get("on_update")]
                if action
            ],
        ]
    )


@helper
def column_definition(name: str, col: Column, engine: Engine) -> str:
    """One column, spelled for one engine.

    There used to be two of these, here and in `applier/postgresql.py`, and
    they differed in two lines: the type and the expression default. Both are
    now looked up by engine, so the rest -- PRIMARY KEY, NOT NULL, UNIQUE,
    CHECK, REFERENCES with its cascade -- is written once.

    EACH CLAUSE IS A COMPREHENSION THAT YIELDS IT OR YIELDS NOTHING, so the
    parts a column carries are read down the list rather than accumulated by
    seven branches. `nullable` is tested against False rather than for
    falseness, because a column that never mentions it is not NOT NULL.
    """
    renderable = _WHEN_UNRENDERABLE[engine in DEFAULTS](engine)
    render_default = DEFAULTS[renderable]
    parts = [
        f'"{name}"',
        map_type(col.get("type", "text"), engine),
        *["PRIMARY KEY" for declared in [col.get("primary_key")] if declared],
        *["NOT NULL" for nullable in [col.get("nullable")] if nullable is False],
        *["UNIQUE" for declared in [col.get("unique")] if declared],
        *[
            render_default(_translate_default(col[key], engine, name))
            for key in ["default"]
            if key in col
        ],
        *[f"CHECK ({col[key]})" for key in ["check"] if key in col],
        *[_foreign_key_clause(col, col[key]) for key in ["references"] if key in col],
    ]

    return " ".join(parts)


# =============================================================================
# Table-level constraints
# =============================================================================
#
# MOVED HERE FROM `shared`, because `templates` needs it and `shared` now
# reaches the renderer. How a constraint is spelled is spelling, which is what
# this module is; who renders a statement is rendering, which is not.


@helper
def _check_clause(name: str, definition: dict[str, Any]) -> str:
    """A CHECK, written with the expression the declaration carries."""
    return f'CONSTRAINT "{name}" CHECK ({definition["expression"]})'


@helper
def _unique_clause(name: str, definition: dict[str, Any]) -> str:
    """A UNIQUE over the columns the declaration names."""
    columns = ", ".join(f'"{c}"' for c in definition.get("columns", []))
    return f'CONSTRAINT "{name}" UNIQUE ({columns})'


_CONSTRAINT_CLAUSES: dict[str, Callable[[str, dict[str, Any]], str]] = {
    "check": _check_clause,
    "unique": _unique_clause,
}


@helper
def _write_constraint(name: str, kind: Any, definition: dict[str, Any]) -> str:
    """A type this renderer writes, so the table names the writer."""
    return _CONSTRAINT_CLAUSES[kind](name, definition)


@helper
def _refuse_constraint(name: str, kind: Any, _definition: dict[str, Any]) -> str:
    """A type this renderer does not write, named so the author finds it."""
    raise ValueError(
        f"constraint {name!r} has type {kind!r}; this renderer writes "
        f"{sorted(_CONSTRAINT_CLAUSES)}."
    )


_KNOWN_CONSTRAINT: dict[bool, Callable[[str, Any, dict[str, Any]], str]] = {
    True: _write_constraint,
    False: _refuse_constraint,
}


@helper
def constraint_clause(name: str, definition: dict[str, Any]) -> str:
    """One table-level constraint, as it appears inside a CREATE TABLE.

    Shared by the renderer above and by the reconstruction that rewrites a
    table, so a rebuild cannot spell a constraint differently from the create.
    """
    kind = definition.get("type")
    return _KNOWN_CONSTRAINT[kind in _CONSTRAINT_CLAUSES](name, kind, definition)
