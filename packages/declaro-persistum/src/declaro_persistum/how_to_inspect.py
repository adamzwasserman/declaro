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

TURSO ANSWERS AS SQLITE DOES, AND THAT IS A ROW AND NOT AN ALIAS. Turso is a
Rust rewrite aiming at SQLite compatibility rather than a fork of it, so the day
its catalogue differs is a day one row changes and nothing else does.

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
    "tables": _asked("SELECT name FROM sqlite_master WHERE type = 'table'"),
    "columns": _about_one_table("SELECT * FROM pragma_table_info(:table)"),
    "indexes": _about_one_table("SELECT * FROM pragma_index_list(:table)"),
    "constraints": _about_one_table(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = :table"
    ),
    "views": _asked("SELECT name, sql FROM sqlite_master WHERE type = 'view'"),
}

# ONE ROW PER ENGINE, AND ADDING AN ENGINE IS ONE MORE LINE. PostgreSQL keeps
# every answer in `information_schema`, and a CHECK lives in `pg_constraint`
# because `information_schema` does not carry its text.
THE_FIVE_QUESTIONS: dict[Engine, Inspecting] = {
    "sqlite": _LIKE_SQLITE,
    "turso": _LIKE_SQLITE,
    "postgresql": {
        "tables": _asked(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        ),
        "columns": _about_one_table(
            "SELECT column_name, data_type, is_nullable, column_default "
            "FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = :table"
        ),
        "indexes": _about_one_table(
            "SELECT indexname, indexdef FROM pg_indexes "
            "WHERE schemaname = 'public' AND tablename = :table"
        ),
        "constraints": _about_one_table(
            "SELECT conname, contype, pg_get_constraintdef(oid) FROM pg_constraint "
            "WHERE conrelid = to_regclass(:table)"
        ),
        "views": _asked(
            "SELECT table_name, view_definition FROM information_schema.views "
            "WHERE table_schema = 'public'"
        ),
    },
}


def how_to_inspect(engine: Engine) -> Inspecting:
    """Say what to ask this engine about itself."""
    return THE_FIVE_QUESTIONS[engine]
