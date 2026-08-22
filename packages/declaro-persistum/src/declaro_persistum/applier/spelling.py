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
    Dialect,
    type_of,
    written_as,
)

# =============================================================================
# Type mapping
# =============================================================================


TypeMap = Callable[[str], str]


@helper
def sqlite_type(type_str: str) -> str:
    """SQLite storage classes. Turso shares them.

    Five classes, so uuid, every timestamp and every json flavour all land in
    TEXT. A type the mapper does not recognise is passed through upper-cased
    rather than guessed at.
    """
    type_lower = type_str.lower()

    if "int" in type_lower:
        return "INTEGER"
    elif type_lower in ("text", "varchar", "char", "string") or type_lower.startswith(
        "varchar("
    ):
        return "TEXT"
    elif type_lower in ("boolean", "bool"):
        return "INTEGER"
    elif type_lower in (
        "uuid",
        "timestamptz",
        "timestamp",
        "date",
        "datetime",
    ) or type_lower in ("jsonb", "json"):
        return "TEXT"
    elif type_lower in ("float", "double", "real", "float4", "float8") or type_lower.startswith(
        "numeric"
    ):
        return "REAL"
    elif type_lower in ("blob", "bytea"):
        return "BLOB"
    else:
        return type_str.upper()


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
def same_type(left: str | None, right: str | None, dialect: Dialect) -> bool:
    """Do two declared types name the same column ON THIS ENGINE?

    `uuid` and `text` are one column on SQLite and two on PostgreSQL, so the
    question cannot be answered without the dialect and a string comparison
    cannot answer it at all. The differ compared the raw strings and therefore
    proposed a type change on every migration after the first: introspection
    reports what the engine stored, the models file says what was declared,
    and on SQLite those are never the same word.

    Absent on either side means the caller has no type to compare, which is
    not the same as the types differing; two absences are equal and one is not.
    """
    if left is None or right is None:
        return left is right
    return map_type(left, dialect) == map_type(right, dialect)


@helper
def map_type(type_str: str, dialect: Dialect) -> str:
    """Spell a declared type for one engine.

    `dialect` is required (Rule 14). A default would have meant PostgreSQL
    silently receiving SQLite affinity, which is the defect this replaced.
    """
    mapper = TYPE_MAPS.get(dialect)
    if mapper is None:
        raise ValueError(
            f"cannot spell types for dialect {dialect!r}. "
            f"Supported: {sorted(TYPE_MAPS)}."
        )
    return mapper(type_str)


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
_NO_UUID = MappingProxyType({
    "gen_random_uuid()": (
        "SQLite has no UUID generator, and a random hex string is not a "
        "v4 UUID. Have the application supply the id"
    ),
    "uuid_generate_v4()": (
        "SQLite has no UUID generator, and a random hex string is not a "
        "v4 UUID. Have the application supply the id"
    ),
})

# WRITTEN ONCE, WHOLE. This was built with sqlite's entry and then mutated on
# the next two lines to add turso and postgresql, so the table a reader sees
# was not the table the code used. Every dialect is here, and the mapping
# cannot be changed after import.
NO_EQUIVALENT = MappingProxyType({
    "sqlite": _NO_UUID,
    "turso": _NO_UUID,
    "postgresql": MappingProxyType({}),
})


@helper
def _the_dialects_spelling(spelled: dict[str, str], key: str, _value: Any) -> Any:
    """This engine writes the expression its own way."""
    return spelled[key]


@helper
def _as_declared(_spelled: dict[str, str], _key: str, value: Any) -> Any:
    """No engine-specific spelling, so what the author wrote stands."""
    return value


# WHETHER THIS ENGINE RESPELLS THE EXPRESSION. The dialect is a closed
# vocabulary and is read by subscript; the expression is not, and both of its
# cases are named rather than one hiding in a `.get` fallback.
_SPELLED_FOR: dict[bool, Callable[[dict[str, str], str, Any], Any]] = {
    True: _the_dialects_spelling,
    False: _as_declared,
}


@helper
def _untranslated_default(value: Any, _dialect: str, _column: str) -> Any:
    """A number, a flag or nothing: the same in every dialect.

    Only text can name an expression, so only text has a dialect spelling.
    """
    return value


@helper
def _translate_text_default(value: str, dialect: str, column: str) -> Any:
    """Text, matched against what this dialect can and cannot spell."""
    key = value.strip().lower().replace(" ", "")

    refusal = NO_EQUIVALENT.get(dialect, {}).get(key)
    if refusal is not None:
        raise ValueError(
            f"column {column!r} defaults to {value}, which {dialect} cannot "
            f"spell. {refusal}."
        )

    spelled = EXPRESSION_DEFAULTS[dialect]
    return _SPELLED_FOR[key in spelled](spelled, key, value)


# ONLY TEXT CARRIES A DIALECT SPELLING, and both cases are written out. This
# was a `.get` whose second argument answered for every type the table does
# not name, which reads the same as the fallbacks removed everywhere else
# today. Naming the other case says it is an answer rather than a silence: a
# number is a number in every dialect and nothing about it varies.
_TRANSLATE_DEFAULT: dict[bool, Callable[[Any, str, str], Any]] = {
    True: _translate_text_default,
    False: _untranslated_default,
}


@helper
def _translate_default(value: Any, dialect: str, column: str) -> Any:
    """The declared default, spelled for `dialect`, or a refusal naming both."""
    return _TRANSLATE_DEFAULT[written_as(str, value)](value, dialect, column)


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
def column_definition(name: str, col: Column, dialect: Dialect) -> str:
    """One column, spelled for one engine.

    There used to be two of these, here and in `applier/postgresql.py`, and
    they differed in two lines: the type and the expression default. Both are
    now looked up by dialect, so the rest -- PRIMARY KEY, NOT NULL, UNIQUE,
    CHECK, REFERENCES with its cascade -- is written once.
    """
    render_default = DEFAULTS.get(dialect)
    if render_default is None:
        raise ValueError(
            f"cannot render a column for dialect {dialect!r}. "
            f"Supported: {sorted(DEFAULTS)}."
        )
    parts = [f'"{name}"', map_type(col.get("type", "text"), dialect)]

    if col.get("primary_key"):
        parts.append("PRIMARY KEY")

    if col.get("nullable") is False:
        parts.append("NOT NULL")

    if col.get("unique"):
        parts.append("UNIQUE")

    if "default" in col:
        parts.append(render_default(_translate_default(col["default"], dialect, name)))

    if "check" in col:
        parts.append(f"CHECK ({col['check']})")

    if "references" in col:
        ref = col["references"]
        ref_table, ref_col = ref.split(".")
        fk_sql = f'REFERENCES "{ref_table}"("{ref_col}")'

        if col.get("on_delete"):
            fk_sql += f" ON DELETE {col['on_delete'].upper().replace('_', ' ')}"
        if col.get("on_update"):
            fk_sql += f" ON UPDATE {col['on_update'].upper().replace('_', ' ')}"

        parts.append(fk_sql)

    return " ".join(parts)


