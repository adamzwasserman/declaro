"""A constraint change the differ does not notice is a silent no-op.

MEASURED 2026-08-17, at the table level, on SQLite:

    unique removed    []
    unique added      []
    check removed     []
    on_delete changed []
    type changed      ['alter_column']

`apply` then reports `success: True` and the DDL is unchanged. A user adds
`unique=True` to a column, deploys, is told the migration succeeded, and the
constraint is not there. The same for a CHECK, and for changing a foreign
key's ON DELETE from CASCADE to RESTRICT -- which decides whether deleting a
row destroys its children.

`_compute_column_alterations` compared type, nullable and default.
`_diff_foreign_keys` compared whether a reference exists, not what it does on
delete. Everything else a column can declare was invisible.

HOW IT WAS FOUND, because the route matters more than the defect: dropping a
UNIQUE column fails on SQLite -- the engine refuses -- so I looked for the
workaround, which is to drop the constraint first and the column second. Step
one reported success and changed nothing, so there was no workaround and the
column could not be dropped at all.

    step 1 operations: []
    step 1 applied
    step 2 FAILED: cannot drop UNIQUE column: "spare"

WHAT THIS DOES NOT FIX: dropping a UNIQUE column in ONE migration. SQLite
still refuses that, the failure is loud, names the column, and rolls back, and
the two-step route now works because step one does something. Making it one
step means routing `drop_column` through table reconstruction, which is a
larger change and a separate decision.
"""

from __future__ import annotations

import os
import tempfile

import aiosqlite
import pytest

from declaro_persistum.applier import apply
from declaro_persistum.differ import diff
from declaro_persistum.inspector import introspect
from declaro_persistum.types import NOTHING

pytestmark = pytest.mark.precommit

ENGINES = ["postgresql", "sqlite", "turso"]


def _table(column: dict) -> dict:
    return {"t": {"columns": {"id": {"type": "text", "primary_key": True}, "c": column}}}


def _ops(before: dict, after: dict, engine: str) -> list[str]:
    return [
        o["op"]
        for o in diff(_table(before), _table(after), engine=engine, decisions=NOTHING)["operations"]
    ]


# ---------------------------------------------------------------------------
# unique
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ENGINES)
def test_adding_a_unique_constraint_emits_an_operation(engine: str) -> None:
    assert _ops({"type": "text"}, {"type": "text", "unique": True}, engine) != []


@pytest.mark.parametrize("engine", ENGINES)
def test_removing_a_unique_constraint_emits_an_operation(engine: str) -> None:
    assert _ops({"type": "text", "unique": True}, {"type": "text"}, engine) != []


@pytest.mark.parametrize("engine", ENGINES)
def test_an_unchanged_unique_emits_nothing(engine: str) -> None:
    """The floor. Emitting always would make every migration rebuild."""
    same = {"type": "text", "unique": True}
    assert _ops(same, dict(same), engine) == []


# ---------------------------------------------------------------------------
# check, compared again, and only because all three engines can read one
# ---------------------------------------------------------------------------
#
# Comparing `check` was added, shipped and removed within an hour today: a
# CHECK was created and enforced and invisible to introspection, so the
# declared value was compared against None on every deploy and `alter_column`
# rebuilt the table each time to apply a constraint already there.
#
# What made it safe was not a better comparison. Each inspector learned to
# report a CHECK, and the round trip was MEASURED on each engine before this
# was turned back on:
#
#     SQLite      introspected 'n > 0'   second diff []
#     PostgreSQL  introspected 'n > 0'   second diff []
#     Turso       introspected 'n > 0'   second diff []
#
# Turso is the reason that mattered. SQLite had just learned to read one and
# Turso had not, so enabling this after checking SQLite alone would have
# repeated the identical mistake on a different engine.


# SQLITE ONLY. Turso was here too until the round-trip gate ran against it:
# it stores `c <> ''` as `c != ''` and `length(c) > 2` as `length (c) > 2`, so
# it rewrites an expression as surely as PostgreSQL does. The operator change
# settles it -- no whitespace folding turns `<>` into `!=` without a parser.
# SQLite keeps the CREATE TABLE text verbatim, which is what makes the
# comparison sound there and nowhere else.
TEXT_ROUND_TRIPS = ["sqlite"]


@pytest.mark.parametrize("engine", TEXT_ROUND_TRIPS)
def test_a_check_constraint_change_emits_an_operation(engine: str) -> None:
    assert (
        _ops({"type": "integer", "check": "c > 0"}, {"type": "integer", "check": "c > 10"}, engine)
        != []
    )


@pytest.mark.parametrize("engine", TEXT_ROUND_TRIPS)
def test_removing_a_check_emits_an_operation(engine: str) -> None:
    assert _ops({"type": "integer", "check": "c > 0"}, {"type": "integer"}, engine) != []


@pytest.mark.parametrize("engine", ENGINES)
def test_an_unchanged_check_emits_nothing(engine: str) -> None:
    """The loop, pinned. This is the assertion that was false for an hour."""
    same = {"type": "integer", "check": "c > 0"}
    assert _ops(same, dict(same), engine) == []


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_a_declared_check_survives_the_round_trip() -> None:
    """The loop again, through a real database rather than two dicts.

    Both spellings of the property agreeing in my head is what let the loop
    ship; only a write followed by a read can see it.
    """
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        declared = _table({"type": "integer", "check": "c > 0"})
        plan = diff({}, declared, engine="sqlite", decisions=NOTHING)
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
        await conn.commit()

        actual = await introspect(conn, "sqlite")

        assert actual["t"]["columns"]["c"].get("check") == "c > 0"
        assert (
            diff({"t": actual["t"]}, declared, engine="sqlite", decisions=NOTHING)["operations"]
            == []
        )
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_a_tightened_check_is_enforced_afterwards() -> None:
    """An operation is not a changed database, and a CHECK is not a column."""
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        plan = diff(
            {}, _table({"type": "integer", "check": "c > 0"}), engine="sqlite", decisions=NOTHING
        )
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
        await conn.execute("INSERT INTO t VALUES ('1', 50)")
        await conn.commit()

        actual = await introspect(conn, "sqlite")
        plan = diff(
            {"t": actual["t"]},
            _table({"type": "integer", "check": "c > 10"}),
            engine="sqlite",
            decisions=NOTHING,
        )
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
        await conn.commit()

        cursor = await conn.execute("SELECT c FROM t")
        assert await cursor.fetchall() == [(50,)], "the rows did not survive"

        with pytest.raises(aiosqlite.IntegrityError):
            await conn.execute("INSERT INTO t VALUES ('2', 5)")
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_tightening_onto_data_that_violates_it_fails_loudly() -> None:
    """The reconstruction copies the rows through the new constraint.

    A migration that cannot hold must fail rather than drop the rows or the
    constraint, and this is where that is decided.
    """
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        plan = diff(
            {}, _table({"type": "integer", "check": "c > 0"}), engine="sqlite", decisions=NOTHING
        )
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
        await conn.execute("INSERT INTO t VALUES ('1', 5)")
        await conn.commit()

        actual = await introspect(conn, "sqlite")
        plan = diff(
            {"t": actual["t"]},
            _table({"type": "integer", "check": "c > 10"}),
            engine="sqlite",
            decisions=NOTHING,
        )

        from declaro_persistum.exceptions import MigrationError

        with pytest.raises(MigrationError, match="CHECK"):
            await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)

        cursor = await conn.execute("SELECT c FROM t")
        assert await cursor.fetchall() == [(5,)], "the row was lost"
    finally:
        await conn.close()
        os.unlink(path)


# ---------------------------------------------------------------------------
# the cascade, which decides whether deleting a row destroys its children
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ENGINES)
def test_changing_on_delete_emits_an_operation(engine: str) -> None:
    """CASCADE and RESTRICT are opposite answers to "delete the parent"."""
    before = {"type": "text", "references": "u.id", "on_delete": "cascade"}
    after = {"type": "text", "references": "u.id", "on_delete": "restrict"}

    assert _ops(before, after, engine) != []


@pytest.mark.parametrize("engine", ENGINES)
def test_changing_on_update_emits_an_operation(engine: str) -> None:
    before = {"type": "text", "references": "u.id", "on_update": "cascade"}
    after = {"type": "text", "references": "u.id", "on_update": "set null"}

    assert _ops(before, after, engine) != []


@pytest.mark.parametrize("engine", ENGINES)
def test_an_unchanged_cascade_emits_nothing(engine: str) -> None:
    same = {"type": "text", "references": "u.id", "on_delete": "cascade"}
    assert _ops(same, dict(same), engine) == []


# ---------------------------------------------------------------------------
# Executed, because an operation is not the same as a changed database
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_a_removed_unique_is_gone_from_the_database() -> None:
    """The step that reported success and changed nothing."""
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        with_unique = _table({"type": "text", "unique": True})
        without = _table({"type": "text"})

        plan = diff({}, with_unique, engine="sqlite", decisions=NOTHING)
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
        await conn.execute("INSERT INTO t VALUES ('1', 'x')")
        await conn.commit()

        actual = await introspect(conn, "sqlite")
        plan = diff({"t": actual["t"]}, without, engine="sqlite", decisions=NOTHING)
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
        await conn.commit()

        # The constraint is what matters, not the reported schema.
        await conn.execute("INSERT INTO t VALUES ('2', 'x')")
        await conn.commit()

        cursor = await conn.execute("SELECT COUNT(*) FROM t")
        assert (await cursor.fetchone())[0] == 2
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_an_added_unique_constrains_the_database() -> None:
    """The other direction, and the floor under the test above."""
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        plan = diff({}, _table({"type": "text"}), engine="sqlite", decisions=NOTHING)
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
        await conn.commit()

        actual = await introspect(conn, "sqlite")
        plan = diff(
            {"t": actual["t"]},
            _table({"type": "text", "unique": True}),
            engine="sqlite",
            decisions=NOTHING,
        )
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
        await conn.commit()

        await conn.execute("INSERT INTO t VALUES ('1', 'x')")
        with pytest.raises(aiosqlite.IntegrityError):
            await conn.execute("INSERT INTO t VALUES ('2', 'x')")
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_the_two_step_route_to_dropping_a_unique_column_works() -> None:
    """SQLite refuses to drop a UNIQUE column, so the constraint goes first.

    That route was closed while step one did nothing.
    """
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        plan = diff(
            {}, _table({"type": "text", "unique": True}), engine="sqlite", decisions=NOTHING
        )
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
        await conn.execute("INSERT INTO t VALUES ('1', 'x')")
        await conn.commit()

        actual = await introspect(conn, "sqlite")
        plan = diff(
            {"t": actual["t"]}, _table({"type": "text"}), engine="sqlite", decisions=NOTHING
        )
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
        await conn.commit()

        actual = await introspect(conn, "sqlite")
        gone = {"t": {"columns": {"id": {"type": "text", "primary_key": True}}}}
        plan = diff({"t": actual["t"]}, gone, engine="sqlite", decisions=NOTHING)
        await apply(conn, plan["operations"], plan["execution_order"], "sqlite", dry_run=False)
        await conn.commit()

        cursor = await conn.execute("PRAGMA table_info(t)")
        assert [row[1] for row in await cursor.fetchall()] == ["id"]
        cursor = await conn.execute("SELECT id FROM t")
        assert await cursor.fetchall() == [("1",)], "the rows did not survive"
    finally:
        await conn.close()
        os.unlink(path)


# ---------------------------------------------------------------------------
# The fold, at the inputs where it is observable
# ---------------------------------------------------------------------------
#
# TWO MUTATION CONTROLS PASSED AND BOTH WERE REAL GAPS.
#
# Folding only the CURRENT side changed nothing, because every test above gets
# its current side from introspection, which has already folded. The call
# matters for a schema written by hand -- a snapshot file, or a diff between
# two declared schemas -- where the unique may be on the index and the other
# side puts it on the column.
#
# And nothing exercised a MULTI-COLUMN unique index. Removing the length check
# folds `unique` onto whichever column happens to be first, which would claim
# a single column is unique when only the combination is.


def test_a_unique_index_on_the_current_side_is_folded_too() -> None:
    """Both sides fold, so a hand-written schema compares equal to a declared
    one that spells the same constraint the other way."""
    by_index = {
        "t": {
            "columns": {"id": {"type": "text", "primary_key": True}, "c": {"type": "text"}},
            "indexes": {"i_c": {"columns": ["c"], "unique": True}},
        }
    }
    by_column = {
        "t": {
            "columns": {
                "id": {"type": "text", "primary_key": True},
                "c": {"type": "text", "unique": True},
            },
            "indexes": {"i_c": {"columns": ["c"], "unique": True}},
        }
    }

    assert diff(by_index, by_column, engine="sqlite", decisions=NOTHING)["operations"] == []
    assert diff(by_column, by_index, engine="sqlite", decisions=NOTHING)["operations"] == []


def test_a_multi_column_unique_index_marks_no_single_column_unique() -> None:
    """`UNIQUE (a, b)` says the PAIR is unique, not either column.

    Folding it onto one would claim a constraint the database does not have,
    and the differ would then propose removing a uniqueness nobody declared.
    """
    pair = {
        "t": {
            "columns": {"a": {"type": "text"}, "b": {"type": "text"}},
            "indexes": {"i_ab": {"columns": ["a", "b"], "unique": True}},
        }
    }
    neither = {
        "t": {
            "columns": {"a": {"type": "text"}, "b": {"type": "text"}},
            "indexes": {"i_ab": {"columns": ["a", "b"], "unique": True}},
        }
    }

    assert diff(pair, neither, engine="sqlite", decisions=NOTHING)["operations"] == []

    from declaro_persistum.differ.core import _with_unique_folded

    folded = _with_unique_folded(pair["t"])
    assert "unique" not in folded["a"], folded["a"]
    assert "unique" not in folded["b"], folded["b"]


def test_a_primary_key_is_not_marked_unique_by_its_own_index() -> None:
    """A PRIMARY KEY is already unique, and saying so twice would make the
    differ propose adding a constraint the column has by definition."""
    from declaro_persistum.differ.core import _with_unique_folded

    table = {
        "columns": {"id": {"type": "text", "primary_key": True}},
        "indexes": {"i_id": {"columns": ["id"], "unique": True}},
    }

    assert "unique" not in _with_unique_folded(table)["id"]


# The section that stood here recorded `check` as a gap, after I shipped the
# comparison as a migration loop and took it back out. It is closed: all three
# inspectors read a CHECK now, the round trip is measured on each above, and
# the comparison is back on.


# ---------------------------------------------------------------------------
# PostgreSQL rewrites a CHECK, so its text cannot be compared
# ---------------------------------------------------------------------------
#
# I ENABLED THIS COMPARISON AFTER MEASURING THE ROUND TRIP ON EVERY ENGINE AND
# STILL SHIPPED A LOOP, because I measured every engine with ONE expression.
# `n > 0` is one of the two PostgreSQL happens to leave alone.
#
# Measured across seven expressions, five differ:
#
#     n > 0                     -> n > 0
#     length(s) > 2             -> length(s) > 2
#     n > 0 AND n < 100         -> (n > 0) AND (n < 100)
#     s <> ''                   -> s <> ''::text
#     s IN ('x', 'y')           -> s = ANY (ARRAY['x'::text, 'y'::text])
#     substr(s, 1, 2) <> 'xx'   -> substr(s, 1, 2) <> 'xx'::text
#     (n > 0)                   -> n > 0
#
# PostgreSQL parses the expression and stores its own canonical form. Matching
# that from the declared text means reimplementing PostgreSQL's parser and
# printer, and the differ is pure so it cannot ask the server.
#
# End to end, before this fix:
#
#     declared     n > 0 AND n < 100
#     introspected (n > 0) AND (n < 100)
#     second diff  alter_column {"check": {...}}   on every deploy
#
# SQLite and Turso store the CREATE TABLE text verbatim, so there the declared
# spelling IS what comes back and the comparison is sound. It is engine-
# dependent now, which is the same shape `same_type` and the expression
# defaults already have: a question that only has an answer per engine.


@pytest.mark.parametrize("engine", TEXT_ROUND_TRIPS)
def test_a_check_change_is_noticed_where_the_text_round_trips(engine: str) -> None:
    assert (
        _ops({"type": "integer", "check": "c > 0"}, {"type": "integer", "check": "c > 10"}, engine)
        != []
    )


@pytest.mark.parametrize("engine", ["postgresql", "turso"])
def test_a_check_is_not_compared_where_the_engine_rewrites_it(engine: str) -> None:
    """Both engines store their own form of the expression.

        PostgreSQL  n > 0 AND n < 100  ->  (n > 0) AND (n < 100)
        Turso       c <> ''            ->  c != ''

    A changed CHECK migrates nothing on either, which is where it stood before
    today, and is the alternative to an alter_column on every deploy.
    """
    assert (
        _ops({"type": "integer", "check": "c > 0"}, {"type": "integer", "check": "c > 10"}, engine)
        == []
    )


@pytest.mark.parametrize(
    "declared,stored",
    [
        ("n > 0 AND n < 100", "(n > 0) AND (n < 100)"),
        ("s <> ''", "s <> ''::text"),
        ("s IN ('x', 'y')", "s = ANY (ARRAY['x'::text, 'y'::text])"),
    ],
    ids=["parenthesised", "cast-added", "rewritten-to-any"],
)
def test_postgresql_does_not_loop_on_an_expression_it_rewrote(declared, stored) -> None:
    """The loop I shipped, at the three shapes that cause it.

    Each `stored` value is what a live PostgreSQL returned for that
    declaration, not what I expected it to return.
    """
    current = {"t": {"columns": {"c": {"type": "text", "check": stored}}}}
    target = {"t": {"columns": {"c": {"type": "text", "check": declared}}}}

    assert diff(current, target, engine="postgresql", decisions=NOTHING)["operations"] == []
