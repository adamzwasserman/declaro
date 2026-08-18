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

DIALECTS = ["postgresql", "sqlite", "turso"]

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


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_round_trip_emits_no_operations(dialect: str) -> None:
    """The measured defect, in one assertion, on every engine."""
    plan = diff(INTROSPECTED, DECLARED, dialect=dialect)

    assert plan["operations"] == [], (
        "an unchanged schema still migrates: "
        + str([(o["op"], o["details"]) for o in plan["operations"]])
    )


@pytest.mark.parametrize("dialect", DIALECTS)
def test_an_integer_default_matches_the_string_the_engine_returns(
    dialect: str,
) -> None:
    """`0` and `"0"` are one default written two ways."""
    current = {"t": {"columns": {"c": {"type": "integer", "default": "0"}}}}
    target = {"t": {"columns": {"c": {"type": "integer", "default": 0}}}}

    assert diff(current, target, dialect=dialect)["operations"] == []


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_primary_key_is_not_null_without_being_told_twice(
    dialect: str,
) -> None:
    """Declaring a primary key has already declared NOT NULL."""
    current = {"t": {"columns": {"c": {"type": "text", "nullable": False,
                                       "primary_key": True}}}}
    target = {"t": {"columns": {"c": {"type": "text", "primary_key": True}}}}

    assert diff(current, target, dialect=dialect)["operations"] == []


# ---------------------------------------------------------------------------
# The floor: a real change must still be detected
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_real_default_change_is_still_emitted(dialect: str) -> None:
    """Folding `0` into `"0"` must not fold `0` into `1`."""
    current = {"t": {"columns": {"c": {"type": "integer", "default": "0"}}}}
    target = {"t": {"columns": {"c": {"type": "integer", "default": 1}}}}

    ops = diff(current, target, dialect=dialect)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], ops


@pytest.mark.parametrize("dialect", DIALECTS)
def test_adding_a_default_is_still_emitted(dialect: str) -> None:
    """Absent and zero are different, and `0` is falsy in Python."""
    current = {"t": {"columns": {"c": {"type": "integer"}}}}
    target = {"t": {"columns": {"c": {"type": "integer", "default": 0}}}}

    ops = diff(current, target, dialect=dialect)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], ops


@pytest.mark.parametrize("dialect", DIALECTS)
def test_removing_a_default_is_still_emitted(dialect: str) -> None:
    current = {"t": {"columns": {"c": {"type": "integer", "default": "0"}}}}
    target = {"t": {"columns": {"c": {"type": "integer"}}}}

    ops = diff(current, target, dialect=dialect)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], ops


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_real_nullability_change_is_still_emitted(dialect: str) -> None:
    """The primary-key rule must not silence an ordinary column."""
    current = {"t": {"columns": {"c": {"type": "text", "nullable": True}}}}
    target = {"t": {"columns": {"c": {"type": "text", "nullable": False}}}}

    ops = diff(current, target, dialect=dialect)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], ops


@pytest.mark.parametrize("dialect", DIALECTS)
def test_dropping_a_primary_key_is_still_a_change(dialect: str) -> None:
    """The rule reads nullability off `primary_key`, so losing it must show."""
    current = {"t": {"columns": {"c": {"type": "text", "nullable": False,
                                       "primary_key": True}}}}
    target = {"t": {"columns": {"c": {"type": "text", "nullable": True}}}}

    ops = diff(current, target, dialect=dialect)["operations"]

    assert ops, "dropping a primary key emitted nothing"


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_quoted_string_default_is_not_a_number(dialect: str) -> None:
    """`'0'` with quotes is the one-character string, not zero."""
    current = {"t": {"columns": {"c": {"type": "text", "default": "'0'"}}}}
    target = {"t": {"columns": {"c": {"type": "text", "default": 0}}}}

    ops = diff(current, target, dialect=dialect)["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], ops


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_boolean_default_still_folds(dialect: str) -> None:
    """The case `_normalize_default` was written for, which must survive."""
    current = {"t": {"columns": {"c": {"type": "boolean", "default": "false"}}}}
    target = {"t": {"columns": {"c": {"type": "boolean", "default": "FALSE"}}}}

    assert diff(current, target, dialect=dialect)["operations"] == []


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_float_default_matches_its_string(dialect: str) -> None:
    current = {"t": {"columns": {"c": {"type": "real", "default": "1.5"}}}}
    target = {"t": {"columns": {"c": {"type": "real", "default": 1.5}}}}

    assert diff(current, target, dialect=dialect)["operations"] == []


@pytest.mark.parametrize("dialect", DIALECTS)
def test_an_expression_default_is_compared_verbatim(dialect: str) -> None:
    """`now()` is not a literal and must not be parsed as one."""
    current = {"t": {"columns": {"c": {"type": "timestamptz", "default": "now()"}}}}
    target = {"t": {"columns": {"c": {"type": "timestamptz", "default": "now()"}}}}

    assert diff(current, target, dialect=dialect)["operations"] == []


@pytest.mark.parametrize("dialect", DIALECTS)
def test_a_boolean_is_not_folded_into_the_number_one(dialect: str) -> None:
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

    ops = diff(current, target, dialect=dialect)["operations"]

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
        plan = diff({}, VOCABULARY, dialect="sqlite")
        await apply(
            conn, plan["operations"], plan["execution_order"], "sqlite",
            dry_run=False,
        )
        actual = await introspect(conn, "sqlite")
        second = diff({"t": actual["t"]}, VOCABULARY, dialect="sqlite")

        assert second["operations"] == [], (
            "an unchanged schema migrates on every deploy: "
            + json.dumps(
                [(o["op"], o["details"]) for o in second["operations"]], default=str
            )
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
        plan = diff({}, schema, dialect="sqlite")
        await apply(
            conn, plan["operations"], plan["execution_order"], "sqlite",
            dry_run=False,
        )
        actual = await introspect(conn, "sqlite")

        assert diff({"t": actual["t"]}, schema, dialect="sqlite")["operations"] == [], (
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
# `same_type` has had the right shape all along -- it takes a dialect and asks
# `map_type` whether two declared types name the same column ON THIS ENGINE.
# `same_default` took no dialect and could not ask anything. It does now, and
# the asymmetry between two functions doing the same job one line apart is
# what should have been noticed the first time.


TRANSLATED = {
    "t": {"columns": {"made_at": {"type": "timestamptz", "default": "now()"}}}
}


@pytest.mark.parametrize("dialect", ["sqlite", "turso"])
def test_a_translated_default_matches_what_the_engine_stored(dialect: str) -> None:
    """The declared spelling and the stored spelling are one default."""
    from declaro_persistum.applier.shared import EXPRESSION_DEFAULTS

    stored = EXPRESSION_DEFAULTS[dialect]["now()"]
    current = {"t": {"columns": {"made_at": {"type": "timestamptz",
                                            "default": stored}}}}

    assert diff(current, TRANSLATED, dialect=dialect)["operations"] == []


def test_postgresql_needs_no_translation_and_still_matches() -> None:
    """The engine whose language these are written in."""
    current = {"t": {"columns": {"made_at": {"type": "timestamptz",
                                            "default": "now()"}}}}

    assert diff(current, TRANSLATED, dialect="postgresql")["operations"] == []


def test_a_real_default_change_survives_the_translation() -> None:
    """The floor. Translating both sides into equality would hide a change."""
    current = {"t": {"columns": {"made_at": {"type": "timestamptz",
                                            "default": "'2020-01-01'"}}}}

    ops = diff(current, TRANSLATED, dialect="sqlite")["operations"]

    assert [o["op"] for o in ops] == ["alter_column"], ops


def test_the_comparison_takes_a_dialect_like_its_neighbour() -> None:
    """`same_type` and `same_default` answer the same kind of question.

    One took a dialect and one did not, and the one that did not produced two
    migration loops in a day.
    """
    import inspect

    from declaro_persistum.applier.shared import same_type
    from declaro_persistum.differ.core import same_default

    assert "dialect" in inspect.signature(same_default).parameters
    assert "dialect" in inspect.signature(same_type).parameters
