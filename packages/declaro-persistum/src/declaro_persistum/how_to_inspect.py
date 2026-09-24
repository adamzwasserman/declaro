"""The five questions one engine answers about itself.

    fn how_to_inspect : (engine: Engine) -> Inspecting

`docs/architecture.md:56` and `:60`: `INSPECTORS` is one of the seven tables and
it decides how to read a live schema. Reading it here is what lets
`reader fn schema_inspect` take five questions and a connection and name no
engine at all.

WHY READING A SCHEMA IS ASKING, NOT INTROSPECTING. `architecture.md:34`: the
models say what should be there and the live schema says what is. The library
asks a database what it holds so the two can be compared, never so it can
decide what belongs.

EACH QUESTION IS A `Statement`, so its values stay out of its text. Four of the
five are asked once per table, and the table's name is a value the driver binds
rather than characters pasted into the question. SQLite and Turso answer from
`sqlite_master`; PostgreSQL answers from `information_schema`.

EACH QUESTION CARRIES ITS OWN ENGINE'S PLACEHOLDER, for the same reason
`fn compile_statement` writes `?` for SQLite and Turso and `$1` for PostgreSQL:
all three drivers count their placeholders and read no name. A question written
with `:table` came back refused by every one of them, measured 2026-09-24:
"Binding 1 (':table') is a named parameter, but you supplied a sequence".

TURSO ANSWERS AS SQLITE DOES, AND THAT IS A ROW AND NOT AN ALIAS. Turso is a
Rust rewrite aiming at SQLite compatibility rather than a fork of it, so the day
its catalogue differs is a day one row changes and nothing else does.

EVERY QUESTION NAMES ITS OWN ANSWER, AND THAT IS WHAT MAKES THE READER
ENGINE-FREE. Measured 2026-09-24 against real databases: the columns question
answered under `cid, name, type, notnull, dflt_value, pk` on SQLite and
`column_name, data_type, is_nullable, column_default` on PostgreSQL, and
nullability came back as 0 and 1 on one and as 'YES' and 'NO' on the other. A
reader handed those would have to know which engine it was talking to, which is
the one thing `Inspecting` exists to prevent. So each question aliases its
output to one set of names and casts its answers to one set of words, in the
SQL, where the engine differences already live.

WHAT EVERY ENGINE ANSWERS, QUESTION BY QUESTION:

    tables       name
    columns      name, declared_type, must_hold_something, starts_at, is_key
    indexes      name, is_unique, spelled_out
    constraints  name, kind, spelled_out
    views        name, spelled_out

`must_hold_something` and `is_key` are 0 or 1 everywhere. `spelled_out` is the
engine's own text for a thing it keeps no structured answer about, and
`reader fn schema_inspect` is what reads it.

NO FALLBACK ON THE READ. `THE_FIVE_QUESTIONS[engine]`, never `.get`.
"""

from __future__ import annotations

from declaro_persistum.types import Engine, Inspecting, Statement


def _asked(text: str) -> Statement:
    """One question, with nothing bound into it yet."""
    return {"text": text, "parameters": {}}


def _about_one_table(text: str) -> Statement:
    """One question asked per table, with the table's name bound as a value."""
    return {"text": text, "parameters": {"table": ""}}


# SQLITE AND TURSO READ THEIR OWN CATALOGUE TABLE AND THEIR OWN PRAGMAS.
# `architecture.md:257` warns that an unfetched PRAGMA is a silent no-op, which
# is a fact about running these rather than about writing them down.
_LIKE_SQLITE: Inspecting = {
    "tables": _asked("SELECT name AS name FROM sqlite_master WHERE type = 'table'"),
    "columns": _about_one_table(
        # `notnull` AND `unique` ARE KEYWORDS, so the pragma's own column names
        # are quoted on the way out. Unquoted, SQLite answers `near "notnull":
        # syntax error` and the whole read fails.
        "SELECT name AS name, type AS declared_type, "
        '"notnull" AS must_hold_something, dflt_value AS starts_at, pk AS is_key '
        "FROM pragma_table_info(?)"
    ),
    "indexes": _about_one_table(
        'SELECT name AS name, "unique" AS is_unique, origin AS spelled_out '
        "FROM pragma_index_list(?)"
    ),
    # SQLITE KEEPS NO CATALOGUE OF RULES, only the text it was created with. So
    # the answer is one row carrying the whole CREATE TABLE, and the kind is the
    # word this package uses for that: `reader fn schema_inspect` reads the text.
    "constraints": _about_one_table(
        "SELECT name AS name, 'in_the_ddl' AS kind, sql AS spelled_out "
        "FROM sqlite_master WHERE type = 'table' AND name = ?"
    ),
    "views": _asked(
        "SELECT name AS name, sql AS spelled_out FROM sqlite_master WHERE type = 'view'"
    ),
}

# ONE ROW PER ENGINE, AND ADDING AN ENGINE IS ONE MORE LINE. PostgreSQL keeps
# every answer in `information_schema`, and a CHECK lives in `pg_constraint`
# because `information_schema` does not carry its text.
THE_FIVE_QUESTIONS: dict[Engine, Inspecting] = {
    "sqlite": _LIKE_SQLITE,
    "turso": _LIKE_SQLITE,
    "postgresql": {
        "tables": _asked(
            "SELECT table_name AS name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        ),
        # THE TWO CASTS ARE WHAT MAKE THE ANSWER READ ALIKE. PostgreSQL says
        # 'YES' and 'NO' where SQLite says 0 and 1, and it keeps no per-column
        # key flag at all, so the key is read off `pg_index` and counted.
        "columns": _about_one_table(
            "SELECT c.column_name AS name, c.data_type AS declared_type, "
            "CASE WHEN c.is_nullable = 'NO' THEN 1 ELSE 0 END AS must_hold_something, "
            "c.column_default AS starts_at, "
            "CASE WHEN EXISTS ("
            "  SELECT 1 FROM pg_index i "
            "  JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
            "  WHERE i.indrelid = to_regclass($1) AND i.indisprimary "
            "    AND a.attname = c.column_name"
            ") THEN 1 ELSE 0 END AS is_key "
            "FROM information_schema.columns c "
            "WHERE c.table_schema = 'public' AND c.table_name = $1"
        ),
        "indexes": _about_one_table(
            "SELECT indexname AS name, "
            "CASE WHEN indexdef LIKE 'CREATE UNIQUE%' THEN 1 ELSE 0 END AS is_unique, "
            "indexdef AS spelled_out FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename = $1"
        ),
        "constraints": _about_one_table(
            # `contype` IS A ONE-CHARACTER TYPE AND asyncpg HANDS IT BACK AS BYTES,
            # measured 2026-09-24: `b'c'` rather than `c`, so a reader comparing
            # it against a word never matched and every CHECK was left out of
            # the live schema. The cast makes it text, here, with the rest of
            # what PostgreSQL says differently.
            "SELECT conname AS name, contype::text AS kind, "
            "pg_get_constraintdef(oid) AS spelled_out FROM pg_constraint "
            "WHERE conrelid = to_regclass($1)"
        ),
        "views": _asked(
            "SELECT table_name AS name, view_definition AS spelled_out "
            "FROM information_schema.views WHERE table_schema = 'public'"
        ),
    },
}


def how_to_inspect(engine: Engine) -> Inspecting:
    """Say what to ask this engine about itself."""
    return THE_FIVE_QUESTIONS[engine]
