"""An expression default must be spelled in the target engine's language.

WRITTEN BEFORE THE CODE, against SQL that the target database refuses.

MEASURED 2026-08-17, running `declaro generate` against the schema this package
ships in `examples/todo_app_native/schema/todos.toml`, targeting SQLite:

    CREATE TABLE "todos" (
        "id" TEXT PRIMARY KEY DEFAULT gen_random_uuid(),
        "title" TEXT NOT NULL,
        "completed" INTEGER DEFAULT false,
        "created_at" TEXT DEFAULT now(),
        "updated_at" TEXT DEFAULT now()
    )

    sqlite3.OperationalError: near "(": syntax error

`gen_random_uuid()`, `now()` and `false` are PostgreSQL. `sqlite_default` wraps
parentheses and otherwise hands the string through, so a schema written for one
engine emits text the other cannot parse. The command exits 0 and prints it.

WHAT SEPARATES THE THREE, AND WHY THEY ARE NOT ONE FIX:

    now()               every engine has this      translate
    true / false        every engine has this      translate
    gen_random_uuid()   SQLite has NO equivalent   refuse, and say so

The refusal is the part worth defending. SQLite's `lower(hex(randomblob(16)))`
produces a 32-character random hex string, which LOOKS like a UUID and is not
one: the version and variant bits are wrong, so it is not a v4 UUID and any
consumer that parses or validates it is being lied to. Emitting it would turn a
loud syntax error into a quiet wrong value, which is the worse trade. The
column has to be filled by the application, and the error says so.

WHAT THIS DOES NOT COVER: a default that parses on both engines and means
different things. `CURRENT_TIMESTAMP` is UTC on SQLite and local time on some
PostgreSQL configurations. No test here catches that, and this comment is the
only record of it.
"""

from __future__ import annotations

import sqlite3

import pytest

from declaro_persistum.applier.shared import column_definition

pytestmark = pytest.mark.precommit

DIALECTS = ["sqlite", "turso", "postgresql"]

# The vocabulary a portable schema may use. Taken from the shipped example
# schema, which is where the defect was measured, rather than invented.
PORTABLE = ["now()", "true", "false", "CURRENT_TIMESTAMP"]


@pytest.mark.parametrize("default", PORTABLE)
@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_portable_default_is_accepted_by_every_engine(
    default: str, dialect: str
) -> None:
    """It must render, whichever engine is the target."""
    rendered = column_definition("c", {"type": "text", "default": default}, dialect)

    assert "DEFAULT" in rendered, rendered


@pytest.mark.parametrize("default", PORTABLE)
def test_a_portable_default_parses_on_sqlite(default: str) -> None:
    """Rendered is not the same as accepted. This runs the DDL.

    Every assertion that checks a substring of generated SQL passes on text
    the engine refuses; that is exactly how the measured defect survived.
    """
    rendered = column_definition("c", {"type": "text", "default": default}, "sqlite")

    sqlite3.connect(":memory:").execute(f"CREATE TABLE t ({rendered})")


@pytest.mark.parametrize("default", PORTABLE)
def test_a_portable_default_produces_a_value_on_sqlite(default: str) -> None:
    """The floor under the test above.

    `DEFAULT NULL` parses for every one of them and means none of them. The
    column has to come out holding something.
    """
    rendered = column_definition("c", {"type": "text", "default": default}, "sqlite")
    conn = sqlite3.connect(":memory:")
    conn.execute(f"CREATE TABLE t (id INTEGER, {rendered})")
    conn.execute("INSERT INTO t (id) VALUES (1)")

    value = conn.execute("SELECT c FROM t").fetchone()[0]

    assert value is not None, (
        f"{default!r} rendered as {rendered!r}, which parses and stores NULL, "
        f"so the default is decorative"
    )


def test_postgresql_keeps_its_own_spelling() -> None:
    """Translation must not run in the direction that needs no translation."""
    rendered = column_definition(
        "c", {"type": "timestamptz", "default": "now()"}, "postgresql"
    )

    assert "now()" in rendered, rendered


def test_a_default_with_no_equivalent_is_refused_by_name() -> None:
    """The measured case, and the one that must NOT be quietly translated.

    A generated 32-character hex string is not a v4 UUID. Emitting one would
    replace a syntax error with a value that passes every check except the one
    that matters.
    """
    with pytest.raises(ValueError) as caught:
        column_definition(
            "id", {"type": "uuid", "default": "gen_random_uuid()"}, "sqlite"
        )

    message = str(caught.value)
    assert "gen_random_uuid" in message, message
    assert "id" in message, f"the error does not name the column: {message}"


def test_the_refusal_names_the_engine_and_what_to_do() -> None:
    """An error a reader cannot act on is a crash with better manners."""
    with pytest.raises(ValueError) as caught:
        column_definition(
            "id", {"type": "uuid", "default": "gen_random_uuid()"}, "sqlite"
        )

    message = str(caught.value)
    assert "sqlite" in message.lower(), message
    assert "application" in message.lower(), (
        f"the error says the default cannot be spelled and not what the reader "
        f"should do instead: {message}"
    )


def test_postgresql_still_accepts_what_only_it_can_spell() -> None:
    """The refusal is per engine, not a ban on the expression."""
    rendered = column_definition(
        "id", {"type": "uuid", "default": "gen_random_uuid()"}, "postgresql"
    )

    assert "gen_random_uuid()" in rendered, rendered


# ---------------------------------------------------------------------------
# What must keep working
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_literal_default_is_untouched(dialect: str) -> None:
    """Translation applies to the expression vocabulary, not to every string."""
    rendered = column_definition(
        "c", {"type": "text", "default": "'pending'"}, dialect
    )

    assert "'pending'" in rendered, rendered


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_numeric_default_is_untouched(dialect: str) -> None:
    rendered = column_definition("c", {"type": "integer", "default": 0}, dialect)

    assert "DEFAULT 0" in rendered, rendered


@pytest.mark.parametrize("dialect", DIALECTS)
def test_an_absent_default_renders_no_clause(dialect: str) -> None:
    rendered = column_definition("c", {"type": "text"}, dialect)

    assert "DEFAULT" not in rendered, rendered


@pytest.mark.parametrize("dialect", DIALECTS)
def test_an_explicit_null_default_is_still_null(dialect: str) -> None:
    """`default = None` is a declared default of NULL, not an absent one."""
    rendered = column_definition("c", {"type": "text", "default": None}, dialect)

    assert "DEFAULT NULL" in rendered, rendered


def test_sqlite_still_double_wraps_a_parenthesised_default() -> None:
    """The round-trip behaviour the renderer already had, which must survive.

    PRAGMA table_info hands back `now()` for a column declared `DEFAULT
    (now())`, so the parens have to be doubled or the differ sees a change on
    every run.
    """
    rendered = column_definition(
        "c", {"type": "text", "default": "(datetime('now'))"}, "sqlite"
    )

    assert rendered.count("(datetime('now'))") == 1, rendered
    sqlite3.connect(":memory:").execute(f"CREATE TABLE t ({rendered})")
