"""Every operation the differ emits must apply, and then stop being emitted.

WRITTEN BEFORE ANY FIX, against a matrix measured rather than assumed.

MEASURED 2026-08-18 by wrapping `applier.apply` for the whole suite and
recording every (engine, operation) pair that reached it. Of the 48 cells,
2000 tests reached 12:

                        postgresql   sqlite   turso
    create_table            yes        yes     yes
    add_column               .         yes      .
    drop_column              .         yes      .
    add_index                .         yes      .
    drop_table               .         yes      .
    alter_column             .         yes     yes
    insert_enum_values      yes        yes      .
    everything else          .          .       .

`rename_table`, `rename_column`, `drop_index`, `create_view`, `drop_view`,
`add_constraint`, `drop_constraint`, `add_foreign_key` and `drop_foreign_key`
were applied by nothing, on any engine. A rename that names the wrong table
destroys it, and `rename_table_sql` had never run at all until the renderer
gate went in beside this file.

CONVERGENCE IS THE ORACLE, NOT A STRING I WROTE. The test applies the current
schema, applies the change, introspects, and diffs again. Zero operations is
the only pass. A hand-written expected statement pins whatever the code does
today, including its bugs, which this suite has already done once.

POSTGRESQL RUNS THE SAME CASES AGAINST A REAL SERVER, because it does not
share a renderer with anything. `postgresql.OPERATION_SQL` is its own table of
its own functions, and the matrix above says two of its sixteen entries had
ever been applied. It skips, naming the URL it tried, when no server answers.

TURSO RUNS THEM TOO, and it is the engine most likely to fail them. It shares
the renderers with SQLite, so what is untested there is not the SQL but what
the engine does with it afterwards. Turso rewrites the DDL it stores, turning
`c <> ''` into `c != ''` and `datetime('now')` into `datetime ('now')`, and it
folds the case of a stored table name. Each of those has produced a migration
that runs forever, and the table-name folding dropped and recreated a
mixed-case table on every deploy, destroying its contents each time.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.abstractions.enums import expand_schema_enums
from declaro_persistum.differ import diff_views
from declaro_persistum.differ.core import diff
from declaro_persistum.inspector import introspect
from declaro_persistum.types import NOTHING, Operation
from tests.conftest import probe_url, refuse_without_postgresql

# asyncio mode is AUTO, so async tests are collected without a mark. Marking
# the module would warn on the one synchronous test below.
pytestmark = [pytest.mark.precommit, pytest.mark.sqlite]

USERS: dict[str, Any] = {
    "columns": {
        "id": {"type": "integer", "primary_key": True},
        "name": {"type": "text"},
    }
}


def _users(**overrides: Any) -> dict[str, Any]:
    return {"users": {**USERS, **overrides}}


WITH_EMAIL = {"users": {"columns": {**USERS["columns"], "email": {"type": "text"}}}}
WITH_INDEX = _users(indexes={"ix_name": {"columns": ["name"]}})
POSTS_PLAIN = {
    "users": USERS,
    "posts": {"columns": {"id": {"type": "integer", "primary_key": True}}},
}
POSTS_FK = {
    "users": USERS,
    "posts": {
        "columns": {
            "id": {"type": "integer", "primary_key": True},
            "uid": {"type": "integer", "references": "users.id"},
        }
    },
}

A_VIEW = {"v_names": {"name": "v_names", "query": "SELECT name FROM users"}}
A_MATVIEW = {
    "mv_names": {
        "name": "mv_names",
        "query": "SELECT name FROM users",
        "materialized": True,
        "refresh": "manual",
    }
}
WITH_CHECK = _users(constraints={"ck_id": {"type": "check", "expression": "id > 0"}})

# (label, current, target). A state is a schema, optionally paired with the
# views declared against it, because a view is diffed by `diff_views` and a
# case that carries only a schema can never reach one. Four cells of the
# matrix were empty for exactly that reason.
#
# The operations each case produces are read off the differ below, never
# listed here, so a case that stops producing what it was written for is
# reported rather than silently passing.
CASES: list[tuple[str, dict[str, Any], dict[str, Any]]] = [
    ("create a table", {}, _users()),
    # NOT ALL LOWER CASE, and every other case here is. A uniform input space
    # is how this matrix reached eleven operations on three engines while
    # missing a defect that made any capitalised table fail its migration on
    # Turso forever. See test_a_mixed_case_table_name_converges.py.
    ("create a mixed-case table", {}, {"UserProfile": USERS}),
    # A COMPOSITE KEY IS DECLARED ON THE TABLE AND FELT BY THE COLUMN, and
    # every case above declares its key on a single column. Without the fold
    # in the differ this proposes making a primary key nullable on every
    # deploy, on all three engines, and PostgreSQL refuses it outright.
    (
        "create a table with a composite key",
        {},
        {
            "pairs": {
                "columns": {
                    "left": {"type": "integer"},
                    "right": {"type": "integer"},
                },
                "primary_key": ["left", "right"],
            }
        },
    ),
    # Altering a mixed-case table still loops on Turso, because the engine
    # rewrites the stored DDL and folds the name into it. Held as a strict
    # xfail in test_a_mixed_case_table_name_converges.py rather than here, so
    # this matrix stays a matrix of operations.
    ("drop a table", _users(), {}),
    ("add a column", _users(), WITH_EMAIL),
    ("drop a column", WITH_EMAIL, _users()),
    ("add an index", _users(), WITH_INDEX),
    ("drop an index", WITH_INDEX, _users()),
    ("add a foreign key", POSTS_PLAIN, POSTS_FK),
    ("drop a foreign key", POSTS_FK, POSTS_PLAIN),
    ("rename a table", _users(), {"people": {**USERS, "renamed_from": "users"}}),
    ("create a view", (_users(), {}), (_users(), A_VIEW)),
    ("drop a view", (_users(), A_VIEW), (_users(), {})),
    ("create a materialized view", (_users(), {}), (_users(), A_MATVIEW)),
    ("drop a materialized view", (_users(), A_MATVIEW), (_users(), {})),
    ("add a constraint", _users(), WITH_CHECK),
    ("drop a constraint", WITH_CHECK, _users()),
    (
        "change a column type",
        _users(),
        {
            "users": {
                "columns": {
                    "id": {"type": "integer", "primary_key": True},
                    "name": {"type": "integer"},
                }
            }
        },
    ),
    (
        "rename a table",
        _users(),
        {"customers": {**USERS, "renamed_from": "users"}},
    ),
    (
        "rename a column",
        _users(),
        {
            "users": {
                "columns": {
                    "id": {"type": "integer", "primary_key": True},
                    "full_name": {"type": "text", "renamed_from": "name"},
                }
            }
        },
    ),
    (
        "fill an enum lookup table",
        {},
        expand_schema_enums(
            {
                "t": {
                    "columns": {
                        "id": {"type": "integer", "primary_key": True},
                        "status": {"type": "text", "literal_values": ["a", "b"]},
                    }
                }
            }
        ),
    ),
]


# WHAT SQLITE AND TURSO REFUSE, by label. Neither has ADD CONSTRAINT or DROP
# CONSTRAINT and this package does not yet rebuild a table to stand in for
# one, so the applier raises and says which constraint on which table.
#
# A REFUSAL IS A BEHAVIOR AND IS TESTED AS ONE. Leaving these out of the
# matrix would have said the cells were unreachable, which is what the gate
# above claimed for two years and was wrong about: the differ emits them
# perfectly well, and it is the applier that will not run them.
REFUSED_BY_SQLITE = {"add a constraint", "drop a constraint"}

APPLIED_CASES = [case for case in CASES if case[0] not in REFUSED_BY_SQLITE]


def _operations_of(current: Any, target: Any) -> set[str]:
    """Every operation moving between these two states, tables and views both.

    IT ASKED ONLY ABOUT TABLES. `diff` answers for tables and `diff_views`
    answers for views, so a case carrying a view produced nothing here and the
    view operations went unrecorded. That is why `create_view` and `drop_view`
    were declared out of reach below: nothing could reach them, including this.
    """
    current_schema, current_views = _state(current)
    target_schema, target_views = _state(target)
    return {
        operation["op"]
        for operation in (
            *diff(current_schema, target_schema, engine="sqlite", decisions=NOTHING)["operations"],
            *diff_views(current_views, target_views),
        )
    }


def test_the_cases_cover_the_operations_they_claim_to() -> None:
    """The floor under every test below.

    A case that stops producing its operation would leave that operation
    unapplied and every test here would still be green. This is the assertion
    that makes it a gate: it names what the cases reach and what they do not.
    """
    reached: set[str] = set()
    for _label, current, target in CASES:
        reached |= _operations_of(current, target)

    declared = set(Operation.__annotations__["op"].__args__)

    # NOTHING IS OUT OF REACH ANY MORE, and this used to name five.
    # `create_view`, `drop_view` and `drop_foreign_key` were listed because no
    # case could produce them: the runner applied only table operations, and
    # no case carried a view. `add_constraint` and `drop_constraint` were
    # listed as though the differ could not emit them, and it always could --
    # what refuses them is the SQLite applier, which is a different fact and
    # is asserted as one below.
    #
    # Measured 2026-08-25 by wrapping the applier for a whole suite run:
    # `drop_view` and `drop_constraint` had never been applied to any database
    # on any engine, and `create_view` had never been applied to PostgreSQL. A
    # list of what a gate cannot reach is honest. Leaving five on it when
    # three were waiting for a case and two were mislabelled is not.
    assert reached == declared, (
        f"cases reach {sorted(reached)}. Missing {sorted(declared - reached)}, "
        f"unexpected {sorted(reached - declared)}"
    )


@pytest.mark.parametrize(
    "label,current,target",
    [case for case in CASES if case[0] in REFUSED_BY_SQLITE],
    ids=[case[0].replace(" ", "_") for case in CASES if case[0] in REFUSED_BY_SQLITE],
)
async def test_what_sqlite_refuses_it_refuses_out_loud(
    label: str, current: Any, target: Any
) -> None:
    """A refusal is a behavior, so it is run rather than assumed.

    These two were on the gate's out-of-reach list as though the differ could
    not emit them. It always could. What will not run them is the applier,
    and until this test nothing had ever asked it to: measured 2026-08-25,
    `drop_constraint` had never been applied to any database on any engine.

    The message must name the constraint and the table, because an operator
    reading it has to know which declaration to change.
    """
    import aiosqlite

    path = tempfile.mktemp(suffix=".db")
    connection = await aiosqlite.connect(path)
    try:
        await _apply_plan(connection, {}, current, "sqlite")

        with pytest.raises(Exception) as raised:
            await _apply_plan(connection, current, target, "sqlite")

        assert "ck_id" in str(raised.value) and "users" in str(raised.value), (
            f"{label} was refused without naming what to change: {raised.value}"
        )
    finally:
        await connection.close()
        os.unlink(path)


def _state(case_half: Any) -> tuple[dict, dict]:
    """A case half is a schema, or a schema paired with its views.

    Written as a pair only where a view is the point, so the fifteen cases
    that have nothing to say about views stay as short as they were.
    """
    return case_half if isinstance(case_half, tuple) else (case_half, {})


async def _apply_plan(connection: Any, current: Any, target: Any, engine: str) -> None:
    """Everything that changes between two states: its tables, then its views.

    THE VIEWS WERE NOT APPLIED HERE AT ALL. `diff` answers for tables and
    `diff_views` answers for views, and this helper called only the first, so
    no case could reach a create_view or a drop_view however it was written.
    Measured 2026-08-25 by wrapping the applier for a whole suite run: of the
    48 cells of operation against engine, `drop_view` and `drop_constraint`
    had never been applied to any database, and `create_view` had never been
    applied to PostgreSQL.
    """
    from declaro_persistum.applier import apply

    current_schema, current_views = _state(current)
    target_schema, target_views = _state(target)

    plan = diff(current_schema, target_schema, engine=engine, decisions=NOTHING)
    await apply(
        connection,
        plan["operations"],
        plan["execution_order"],
        engine,
        dry_run=False,
    )

    view_operations = diff_views(current_views, target_views)
    await apply(
        connection,
        view_operations,
        list(range(len(view_operations))),
        engine,
        dry_run=False,
    )


async def _converged(connection: Any, target: Any, engine: str) -> list:
    """What a second migration would still emit. Zero is the only pass."""
    target_schema, _target_views = _state(target)
    actual = await introspect(connection, engine)
    return diff(
        {name: actual[name] for name in target_schema if name in actual},
        target_schema,
        engine=engine,
        decisions=NOTHING,
    )["operations"]


@pytest.mark.parametrize(
    "label,current,target",
    APPLIED_CASES,
    ids=[c[0].replace(" ", "_") for c in APPLIED_CASES],
)
async def test_the_change_applies_and_then_stops_being_emitted(
    label: str, current: dict, target: dict
) -> None:
    """Apply the world, apply the change, read it back, diff again.

    Anything other than zero operations is a migration that runs on every
    deploy forever.
    """
    import aiosqlite

    path = tempfile.mktemp(suffix=".db")
    connection = await aiosqlite.connect(path)
    try:
        await _apply_plan(connection, {}, current, "sqlite")
        await _apply_plan(connection, current, target, "sqlite")

        remaining = await _converged(connection, target, "sqlite")

        assert remaining == [], (
            f"{label} migrates on every deploy: "
            f"{[(o['op'], o['table'], o['details']) for o in remaining]}"
        )
    finally:
        await connection.close()
        os.unlink(path)


@pytest.mark.parametrize(
    "label,current,target",
    APPLIED_CASES,
    ids=[c[0].replace(" ", "_") for c in APPLIED_CASES],
)
async def test_the_change_leaves_the_declared_tables_present(
    label: str, current: dict, target: dict
) -> None:
    """The floor under convergence.

    A diff between two things that are both absent is empty, so a migration
    that dropped everything would converge. `rename_table` is the operation
    that makes this a real risk and it had never been applied by anything.
    """
    import aiosqlite

    path = tempfile.mktemp(suffix=".db")
    connection = await aiosqlite.connect(path)
    try:
        await _apply_plan(connection, {}, current, "sqlite")
        await _apply_plan(connection, current, target, "sqlite")

        actual = await introspect(connection, "sqlite")
        declared, _views = _state(target)

        assert set(declared) <= set(actual), (
            f"{label} left {sorted(set(declared) - set(actual))} missing; "
            f"the database holds {sorted(actual)}"
        )
    finally:
        await connection.close()
        os.unlink(path)


# ---------------------------------------------------------------------------
# The same cases against a real PostgreSQL server
# ---------------------------------------------------------------------------

PROBE_URL = probe_url()

# Every table and view any case names. Dropped before and after each one, so a
# case that fails halfway cannot poison the next. Read off the cases rather
# than listed, because a list here would go stale the moment a case is added.
TOUCHED = sorted(
    {
        name
        for _label, current, target in CASES
        for half in (current, target)
        for part in _state(half)
        for name in part
    }
    | {"_dp_enum_t_status"}
)


# What PostgreSQL calls each kind of object, and the statement that removes
# it. A table read by subscript, so a kind nobody wrote a rule for raises here
# rather than being dropped by the wrong statement.
# IF EXISTS ON EACH, because the list is read once and CASCADE removes
# dependents as the loop runs. A materialized view selecting from `users`
# goes when `users` goes, and the loop then reaches a name pg_class listed a
# moment earlier. IF EXISTS is safe here in a way it was not before: the kind
# is read from the database rather than guessed, so this cannot be a
# wrong-kind drop quietly doing nothing.
_DROP_BY_KIND: dict[str, str] = {
    "r": "DROP TABLE IF EXISTS",
    "p": "DROP TABLE IF EXISTS",
    "v": "DROP VIEW IF EXISTS",
    "m": "DROP MATERIALIZED VIEW IF EXISTS",
}


async def _clean(connection: Any) -> None:
    """Remove everything any case names, by asking what kind each one is.

    A VIEW IS NOT A TABLE AND POSTGRESQL SAYS SO. `DROP TABLE` against a
    materialized view is refused with WrongObjectTypeError and a hint naming
    the statement that would have worked, and `IF EXISTS` does not help:
    PostgreSQL checks the kind before it checks existence, so a wrong-kind
    drop raises for an object that is plainly there.

    Trying all three statements in turn therefore fails on the two that are
    wrong. The kind is a fact the database holds, so it is read rather than
    guessed.
    """
    present = await connection.fetch(
        "SELECT c.relname, c.relkind FROM pg_class c "
        "JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = 'public' AND c.relname = ANY($1::text[])",
        TOUCHED,
    )
    # asyncpg hands `relkind` back as a one-byte value rather than as text,
    # so it is decoded here, at the edge where it arrives, and the table below
    # is keyed by what PostgreSQL's own documentation calls these.
    for name, kind in present:
        statement = _DROP_BY_KIND[kind.decode()]
        await connection.execute(f'{statement} "{name}" CASCADE')


@pytest.mark.postgresql
@pytest.mark.integration
@pytest.mark.parametrize("label,current,target", CASES, ids=[c[0].replace(" ", "_") for c in CASES])
async def test_the_change_applies_and_converges_on_postgresql(
    label: str, current: dict, target: dict
) -> None:
    """PostgreSQL renders through its own table, which nothing had exercised."""
    import asyncpg

    try:
        connection = await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        refuse_without_postgresql(PROBE_URL, unreachable)

    try:
        await _clean(connection)
        await _apply_plan(connection, {}, current, "postgresql")
        await _apply_plan(connection, current, target, "postgresql")

        remaining = await _converged(connection, target, "postgresql")

        assert remaining == [], (
            f"{label} migrates on every deploy: "
            f"{[(o['op'], o['table'], o['details']) for o in remaining]}"
        )
    finally:
        await _clean(connection)
        await connection.close()


@pytest.mark.postgresql
@pytest.mark.integration
@pytest.mark.parametrize("label,current,target", CASES, ids=[c[0].replace(" ", "_") for c in CASES])
async def test_the_change_leaves_the_declared_tables_present_on_postgresql(
    label: str, current: dict, target: dict
) -> None:
    """The same floor. A migration that dropped everything would converge."""
    import asyncpg

    try:
        connection = await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        refuse_without_postgresql(PROBE_URL, unreachable)

    try:
        await _clean(connection)
        await _apply_plan(connection, {}, current, "postgresql")
        await _apply_plan(connection, current, target, "postgresql")

        actual = await introspect(connection, "postgresql")
        declared, _views = _state(target)

        assert set(declared) <= set(actual), (
            f"{label} left {sorted(set(declared) - set(actual))} missing; "
            f"the database holds {sorted(actual)}"
        )
    finally:
        await _clean(connection)
        await connection.close()


# ---------------------------------------------------------------------------
# The same cases on Turso, which rewrites what it stores
# ---------------------------------------------------------------------------


@pytest.mark.turso
@pytest.mark.parametrize(
    "label,current,target",
    APPLIED_CASES,
    ids=[c[0].replace(" ", "_") for c in APPLIED_CASES],
)
async def test_the_change_applies_and_converges_on_turso(
    label: str, current: dict, target: dict
) -> None:
    """A local Turso file. No primary, so nothing here touches the network."""
    import turso.aio

    directory = tempfile.mkdtemp()
    connection = await turso.aio.connect(os.path.join(directory, "ops.db"))
    try:
        await _apply_plan(connection, {}, current, "turso")
        await _apply_plan(connection, current, target, "turso")

        remaining = await _converged(connection, target, "turso")

        assert remaining == [], (
            f"{label} migrates on every deploy: "
            f"{[(o['op'], o['table'], o['details']) for o in remaining]}"
        )
    finally:
        await connection.close()


@pytest.mark.turso
@pytest.mark.parametrize(
    "label,current,target",
    APPLIED_CASES,
    ids=[c[0].replace(" ", "_") for c in APPLIED_CASES],
)
async def test_the_change_leaves_the_declared_tables_present_on_turso(
    label: str, current: dict, target: dict
) -> None:
    """The floor, and on this engine it is the one that has caught a defect.

    Turso folds a stored table name, so a rename can converge while leaving
    the table under a name the declaration never asked for.
    """
    import turso.aio

    directory = tempfile.mkdtemp()
    connection = await turso.aio.connect(os.path.join(directory, "present.db"))
    try:
        await _apply_plan(connection, {}, current, "turso")
        await _apply_plan(connection, current, target, "turso")

        actual = await introspect(connection, "turso")
        declared, _views = _state(target)

        assert set(declared) <= set(actual), (
            f"{label} left {sorted(set(declared) - set(actual))} missing; "
            f"the database holds {sorted(actual)}"
        )
    finally:
        await connection.close()
