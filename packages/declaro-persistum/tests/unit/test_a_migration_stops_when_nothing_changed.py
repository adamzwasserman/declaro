"""Diffing a database against the schema it was built from must emit nothing.

WRITTEN BEFORE THE CODE, against a migration that never converges.

MEASURED 2026-08-17 against a real PostgreSQL server. Create one table from a
schema, introspect it straight back, diff the two:

    OP: alter_column widgets {"column": "count",
                              "changes": {"default": {"from": "0", "to": 0}}}
    OP: alter_column widgets {"column": "id",
                              "changes": {"nullable": {"from": false, "to": true}}}

Neither is a change anybody made. They are the two sides of the diff spelling
the same fact differently, so every `declaro apply` against PostgreSQL emits
these forever, and the second one asks PostgreSQL to make a PRIMARY KEY
nullable, which it refuses.

THE INPUTS BELOW ARE THE INTROSPECTED SHAPES, copied from that run rather than
imagined. A round-trip test that invents what the database returns is testing
the invention.

    declared                          introspected
    default: 0        (int)           default: "0"      (str)
    primary_key: true, nullable absent  nullable: false

FIRST DEFECT: the differ compares defaults by identity after folding boolean
literals. `0` and `"0"` are the same default written two ways, and every
engine hands back a string because a default is stored as an expression.

SECOND DEFECT: a PRIMARY KEY column is NOT NULL on every engine, by
definition. The schema does not repeat it, because saying `primary_key: True`
has already said it. The differ read the missing key as `nullable: True` and
proposed an alteration to make a key column nullable.

`_normalize_default` already existed and already folded booleans, for exactly
this reason and after exactly this symptom. Numbers are the same bug in the
same place, which is the sign that folding value by value is the shape that
keeps producing this. What the comparison needs is one question -- do these two
spellings mean the same default on this engine -- rather than a list of the
kinds of literal somebody has been bitten by so far.
"""

from __future__ import annotations

import pytest

from declaro_persistum.differ import diff

pytestmark = pytest.mark.precommit

ENGINES = ["postgresql", "sqlite", "turso"]

# Exactly what `introspect(conn, "postgresql")` returned for a table built
# from DECLARED below. Measured, not written from memory.
INTROSPECTED = {
    "widgets": {
        "columns": {
            "id": {"type": "text", "nullable": False, "primary_key": True},
            "name": {"type": "text", "nullable": False},
            "count": {"type": "integer", "default": "0"},
        }
    }
}

DECLARED = {
    "widgets": {
        "columns": {
            "id": {"type": "text", "primary_key": True},
            "name": {"type": "text", "nullable": False},
            "count": {"type": "integer", "default": 0},
        }
    }
}


@pytest.mark.parametrize("engine", ENGINES)
def test_a_round_trip_emits_no_operations(engine: str) -> None:
    """The measured defect, in one assertion, on every engine."""
    plan = diff(INTROSPECTED, DECLARED, engine=engine, decisions=NOTHING)

    assert plan["operations"] == [], "an unchanged schema still migrates: " + str(
        [(o["op"], o["details"]) for o in plan["operations"]]
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_an_integer_default_matches_the_string_the_engine_returns(
    engine: str,
) -> None:
    """`0` and `"0"` are one default written two ways."""
    current = {"t": {"columns": {"c": {"type": "integer", "default": "0"}}}}
    target = {"t": {"columns": {"c": {"type": "integer", "default": 0}}}}

    assert diff(current, target, engine=engine, decisions=NOTHING)["operations"] == []


@pytest.mark.parametrize("engine", ENGINES)
def test_a_primary_key_is_not_null_without_being_told_twice(
    engine: str,
) -> None:
    """Declaring a primary key has already declared NOT NULL."""
    current = {"t": {"columns": {"c": {"type": "text", "nullable": False, "primary_key": True}}}}
    target = {"t": {"columns": {"c": {"type": "text", "primary_key": True}}}}

    assert diff(current, target, engine=engine, decisions=NOTHING)["operations"] == []


# ---------------------------------------------------------------------------
# The floor: a real change must still be detected
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ENGINES)
def test_a_real_default_change_is_still_emitted(engine: str) -> None:
    """Folding `0` into `"0"` must not fold `0` into `1`."""
    current = {"t": {"columns": {"c": {"type": "integer", "default": "0"}}}}
    target = {"t": {"columns": {"c": {"type": "integer", "default": 1}}}}

    ops = diff(current, target, engine=engine, decisions=NOTHING)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], ops


@pytest.mark.parametrize("engine", ENGINES)
def test_adding_a_default_is_still_emitted(engine: str) -> None:
    """Absent and zero are different, and `0` is falsy in Python."""
    current = {"t": {"columns": {"c": {"type": "integer"}}}}
    target = {"t": {"columns": {"c": {"type": "integer", "default": 0}}}}

    ops = diff(current, target, engine=engine, decisions=NOTHING)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], ops


@pytest.mark.parametrize("engine", ENGINES)
def test_removing_a_default_is_still_emitted(engine: str) -> None:
    current = {"t": {"columns": {"c": {"type": "integer", "default": "0"}}}}
    target = {"t": {"columns": {"c": {"type": "integer"}}}}

    ops = diff(current, target, engine=engine, decisions=NOTHING)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], ops


@pytest.mark.parametrize("engine", ENGINES)
def test_a_real_nullability_change_is_still_emitted(engine: str) -> None:
    """The primary-key rule must not silence an ordinary column."""
    current = {"t": {"columns": {"c": {"type": "text", "nullable": True}}}}
    target = {"t": {"columns": {"c": {"type": "text", "nullable": False}}}}

    ops = diff(current, target, engine=engine, decisions=NOTHING)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], ops


@pytest.mark.parametrize("engine", ENGINES)
def test_dropping_a_primary_key_is_still_a_change(engine: str) -> None:
    """The rule reads nullability off `primary_key`, so losing it must show."""
    current = {"t": {"columns": {"c": {"type": "text", "nullable": False, "primary_key": True}}}}
    target = {"t": {"columns": {"c": {"type": "text", "nullable": True}}}}

    ops = diff(current, target, engine=engine, decisions=NOTHING)["operations"]

    assert ops, "dropping a primary key emitted nothing"


@pytest.mark.parametrize("engine", ENGINES)
def test_a_quoted_string_default_is_not_a_number(engine: str) -> None:
    """`'0'` with quotes is the one-character string, not zero."""
    current = {"t": {"columns": {"c": {"type": "text", "default": "'0'"}}}}
    target = {"t": {"columns": {"c": {"type": "text", "default": 0}}}}

    ops = diff(current, target, engine=engine, decisions=NOTHING)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], ops


@pytest.mark.parametrize("engine", ENGINES)
def test_a_boolean_default_still_folds(engine: str) -> None:
    """The case `_normalize_default` was written for, which must survive."""
    current = {"t": {"columns": {"c": {"type": "boolean", "default": "false"}}}}
    target = {"t": {"columns": {"c": {"type": "boolean", "default": "FALSE"}}}}

    assert diff(current, target, engine=engine, decisions=NOTHING)["operations"] == []


@pytest.mark.parametrize("engine", ENGINES)
def test_a_float_default_matches_its_string(engine: str) -> None:
    current = {"t": {"columns": {"c": {"type": "real", "default": "1.5"}}}}
    target = {"t": {"columns": {"c": {"type": "real", "default": 1.5}}}}

    assert diff(current, target, engine=engine, decisions=NOTHING)["operations"] == []


@pytest.mark.parametrize("engine", ENGINES)
def test_an_expression_default_is_compared_verbatim(engine: str) -> None:
    """`now()` is not a literal and must not be parsed as one."""
    current = {"t": {"columns": {"c": {"type": "timestamptz", "default": "now()"}}}}
    target = {"t": {"columns": {"c": {"type": "timestamptz", "default": "now()"}}}}

    assert diff(current, target, engine=engine, decisions=NOTHING)["operations"] == []


@pytest.mark.parametrize("engine", ENGINES)
def test_a_boolean_is_not_folded_into_the_number_one(engine: str) -> None:
    """The decision `same_default` states, which nothing was holding it to.

    FOUND BY A MUTATION CONTROL. Deleting the `isinstance(value, bool)` guard
    in `_as_number` left all 47 tests passing. Without it Python's own rule
    applies -- `isinstance(True, int)` is True -- so `True` parses as 1.0 and
    folds into the string "1".

    That fold may well be right on SQLite, which stores booleans as integers.
    It is not MEASURED, and `same_default` says so in as many words. A stated
    decision that no test holds is a comment, so this is the test.

    When somebody measures it and decides `true` and `1` are one default on
    SQLite, this test is what they have to change on purpose.
    """
    current = {"t": {"columns": {"c": {"type": "boolean", "default": "1"}}}}
    target = {"t": {"columns": {"c": {"type": "boolean", "default": True}}}}

    ops = diff(current, target, engine=engine, decisions=NOTHING)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], (
        f"a boolean folded into a number, which `same_default` says is "
        f"unmeasured and does not do: {ops}"
    )


# ---------------------------------------------------------------------------
# The round trip through a real database, not through my idea of one
# ---------------------------------------------------------------------------
#
# Every test above hands the differ two dicts I wrote. That catches a
# comparison bug and cannot catch a WRITE-then-READ bug, where the value the
# engine stores is not the value the schema declared.
#
# MEASURED 2026-08-17, and this one is mine. Translating boolean defaults for
# SQLite and Turso was added the same day, to stop `DEFAULT now()` reaching an
# engine that cannot parse it. It also rewrote `false` as `0`:
#
#     declared   default: "false"
#     written    DEFAULT 0
#     read back  "0"
#     diff       alter_column {"default": {"from": "0", "to": "false"}}
#
# so every deploy proposed a change to a column nobody had touched. The fix
# for one engine broke a round trip on the same engine, and only running the
# write and the read together could see it.
#
# THE TRANSLATION WAS NEVER NEEDED. Measured on SQLite 3.50.4 and pyturso:
#
#     DEFAULT false  -> parses, PRAGMA returns 'false', stores 0   (sqlite)
#     DEFAULT false  -> parses, PRAGMA returns 'FALSE'             (turso)
#
# Both engines take the keyword. Turso uppercases it, which is exactly why
# `same_default` folds boolean spellings and has since before today. `now()`
# and `current_timestamp` still need translating; the booleans never did.

import json  # noqa: E402
import os  # noqa: E402
import tempfile  # noqa: E402

import pytest as _pytest  # noqa: E402

from declaro_persistum.types import NOTHING  # noqa: E402

VOCABULARY = {
    "t": {
        "columns": {
            "id": {"type": "text", "primary_key": True},
            "flag": {"type": "boolean", "default": "false"},
            "n": {"type": "integer", "default": 0},
            "amount": {"type": "numeric"},
            "when_": {"type": "timestamptz"},
            "blob_": {"type": "bytea"},
            "ident": {"type": "uuid"},
        }
    }
}


@_pytest.mark.asyncio
@_pytest.mark.sqlite
async def test_a_sqlite_round_trip_emits_no_operations() -> None:
    """Create from a schema, read it back, diff. Nothing may come out."""
    import aiosqlite

    from declaro_persistum.applier import apply
    from declaro_persistum.inspector import introspect

    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        plan = diff({}, VOCABULARY, engine="sqlite", decisions=NOTHING)
        await apply(
            conn,
            plan["operations"],
            plan["execution_order"],
            "sqlite",
            dry_run=False,
        )
        actual = await introspect(conn, "sqlite")
        second = diff({"t": actual["t"]}, VOCABULARY, engine="sqlite", decisions=NOTHING)

        assert second["operations"] == [], (
            "an unchanged schema migrates on every deploy: "
            + json.dumps([(o["op"], o["details"]) for o in second["operations"]], default=str)
        )
    finally:
        await conn.close()
        os.unlink(path)


@_pytest.mark.asyncio
@_pytest.mark.sqlite
async def test_a_boolean_default_survives_the_write_and_the_read() -> None:
    """The measured case, isolated, so the failure names itself."""
    import aiosqlite

    from declaro_persistum.applier import apply
    from declaro_persistum.inspector import introspect

    schema = {"t": {"columns": {"flag": {"type": "boolean", "default": "false"}}}}
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        plan = diff({}, schema, engine="sqlite", decisions=NOTHING)
        await apply(
            conn,
            plan["operations"],
            plan["execution_order"],
            "sqlite",
            dry_run=False,
        )
        actual = await introspect(conn, "sqlite")

        assert (
            diff({"t": actual["t"]}, schema, engine="sqlite", decisions=NOTHING)["operations"] == []
        ), (
            f"declared {schema['t']['columns']['flag']['default']!r}, read back "
            f"{actual['t']['columns']['flag'].get('default')!r}"
        )
    finally:
        await conn.close()
        os.unlink(path)


def test_the_expression_table_translates_only_what_needs_it() -> None:
    """`now()` needs translating on SQLite. `false` does not, and translating
    it is what broke the round trip.
    """
    from declaro_persistum.applier.shared import EXPRESSION_DEFAULTS

    for engine in ("sqlite", "turso"):
        table = EXPRESSION_DEFAULTS[engine]
        assert "now()" in table, engine
        assert "true" not in table and "false" not in table, (
            f"{engine} still rewrites boolean defaults, so a declared "
            f"'false' is written as {table.get('false')!r} and read back as "
            f"something the differ does not recognise as the same default"
        )


# ---------------------------------------------------------------------------
# A default that IS translated, and the comparison that has to know it
# ---------------------------------------------------------------------------
#
# The boolean rows came out of `EXPRESSION_DEFAULTS` earlier today because
# SQLite takes the keyword and translating it broke the round trip. `now()` is
# different: SQLite genuinely cannot parse it, so the translation must stay.
#
#     declared   now()
#     written    (datetime('now'))
#     read back  (datetime('now'))
#     diff       alter_column {"default": {"from": "(datetime('now'))",
#                                          "to": "now()"}}
#
# Measured 2026-08-17 on a real SQLite file, the second migration loop of the
# day from the same cause: a value translated on the way in, and a comparison
# that does not know the translation happened.
#
# `same_type` has had the right shape all along -- it takes an engine and asks
# `map_type` whether two declared types name the same column ON THIS ENGINE.
# `same_default` took no engine and could not ask anything. It does now, and
# the asymmetry between two functions doing the same job one line apart is
# what should have been noticed the first time.


TRANSLATED = {"t": {"columns": {"made_at": {"type": "timestamptz", "default": "now()"}}}}


@pytest.mark.parametrize("engine", ["sqlite", "turso"])
def test_a_translated_default_matches_what_the_engine_stored(engine: str) -> None:
    """The declared spelling and the stored spelling are one default."""
    from declaro_persistum.applier.shared import EXPRESSION_DEFAULTS

    stored = EXPRESSION_DEFAULTS[engine]["now()"]
    current = {"t": {"columns": {"made_at": {"type": "timestamptz", "default": stored}}}}

    assert diff(current, TRANSLATED, engine=engine, decisions=NOTHING)["operations"] == []


def test_postgresql_needs_no_translation_and_still_matches() -> None:
    """The engine whose language these are written in."""
    current = {"t": {"columns": {"made_at": {"type": "timestamptz", "default": "now()"}}}}

    assert diff(current, TRANSLATED, engine="postgresql", decisions=NOTHING)["operations"] == []


def test_a_real_default_change_survives_the_translation() -> None:
    """The floor. Translating both sides into equality would hide a change."""
    current = {"t": {"columns": {"made_at": {"type": "timestamptz", "default": "'2020-01-01'"}}}}

    ops = diff(current, TRANSLATED, engine="sqlite", decisions=NOTHING)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], ops


def test_the_comparison_takes_a_engine_like_its_neighbour() -> None:
    """`same_type` and `same_default` answer the same kind of question.

    One took an engine and one did not, and the one that did not produced two
    migration loops in a day.
    """
    import inspect

    from declaro_persistum.applier.shared import same_type
    from declaro_persistum.differ.core import same_default

    assert "engine" in inspect.signature(same_default).parameters
    assert "engine" in inspect.signature(same_type).parameters


# ---------------------------------------------------------------------------
# What the case fold must NOT reach
# ---------------------------------------------------------------------------
#
# `same_default` folds the case of a BARE keyword, because PostgreSQL stores
# `current_timestamp` as `CURRENT_TIMESTAMP` and comparing them literally
# migrated on every deploy.
#
# FOUND BY A MUTATION CONTROL: widening that fold to every string left every
# test passing, because nothing compared two quoted literals differing only in
# case. `'Pending'` and `'pending'` are two different values, and a schema
# changing one to the other is a change.


@pytest.mark.parametrize("engine", ENGINES)
def test_a_quoted_literal_keeps_its_case(engine: str) -> None:
    current = {"t": {"columns": {"c": {"type": "text", "default": "'Pending'"}}}}
    target = {"t": {"columns": {"c": {"type": "text", "default": "'pending'"}}}}

    ops = diff(current, target, engine=engine, decisions=NOTHING)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], (
        f"'Pending' and 'pending' are different values and the change was folded away: {ops}"
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_a_bare_keyword_still_folds(engine: str) -> None:
    """The floor. Removing the fold entirely would pass the test above."""
    current = {"t": {"columns": {"c": {"type": "timestamptz", "default": "CURRENT_TIMESTAMP"}}}}
    target = {"t": {"columns": {"c": {"type": "timestamptz", "default": "current_timestamp"}}}}

    assert diff(current, target, engine=engine, decisions=NOTHING)["operations"] == []


@pytest.mark.parametrize("engine", ENGINES)
def test_a_space_inside_a_string_default_is_not_cosmetic(engine: str) -> None:
    """The spacing fold must not reach inside a quoted literal.

    Turso stores `datetime('now')` as `datetime ('now')`, so a space before a
    parenthesis is folded away. Inside quotes it is a CHARACTER: `'a ('` and
    `'a('` are two different default values, and folding them together would
    tell a schema that changed one to the other that nothing had changed.

    FOUND BY A MUTATION CONTROL. Deleting the quote guard left everything
    passing, because every default in the suite either had no quotes or no
    space-before-paren inside them.
    """
    current = {"t": {"columns": {"c": {"type": "text", "default": "'a ('"}}}}
    target = {"t": {"columns": {"c": {"type": "text", "default": "'a('"}}}}

    ops = diff(current, target, engine=engine, decisions=NOTHING)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], (
        f"two different string defaults were folded together: {ops}"
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_a_space_outside_quotes_still_folds(engine: str) -> None:
    """The floor. Removing the fold entirely would pass the test above."""
    current = {"t": {"columns": {"c": {"type": "text", "default": "(datetime ('now'))"}}}}
    target = {"t": {"columns": {"c": {"type": "text", "default": "(datetime('now'))"}}}}

    assert diff(current, target, engine=engine, decisions=NOTHING)["operations"] == []
