"""Every operation the differ emits must apply, and then stop being emitted.

WRITTEN BEFORE ANY FIX, against a matrix measured rather than assumed.

MEASURED 2026-08-18 by wrapping `applier.apply` for the whole suite and
recording every (dialect, operation) pair that reached it. Of the 48 cells,
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


WITH_EMAIL = {
    "users": {"columns": {**USERS["columns"], "email": {"type": "text"}}}
}
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

# (label, current, target). The operations each one produces are read off the
# differ below, never listed here, so a case that stops producing what it was
# written for is reported rather than silently passing.
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


def _operations_of(current: dict[str, Any], target: dict[str, Any]) -> set[str]:
    return {o["op"] for o in diff(current, target, dialect="sqlite", decisions=NOTHING)["operations"]}


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
    # Stated, not silent. Views do not travel through the table introspector,
    # and SQLite refuses to add or drop a named constraint at all, which the
    # renderer gate beside this file asserts it says out loud.
    out_of_reach = {
        "create_view",
        "drop_view",
        "add_constraint",
        "drop_constraint",
        "drop_foreign_key",
    }

    assert reached == declared - out_of_reach, (
        f"cases reach {sorted(reached)}; expected "
        f"{sorted(declared - out_of_reach)}. Missing "
        f"{sorted((declared - out_of_reach) - reached)}, unexpected "
        f"{sorted(reached - (declared - out_of_reach))}"
    )


async def _apply_plan(
    connection: Any, current: dict, target: dict, dialect: str
) -> None:
    from declaro_persistum.applier import apply

    plan = diff(current, target, dialect=dialect, decisions=NOTHING)
    await apply(
        connection,
        plan["operations"],
        plan["execution_order"],
        dialect,
        dry_run=False,
    )


async def _converged(connection: Any, target: dict, dialect: str) -> list:
    actual = await introspect(connection, dialect)
    return diff(
        {name: actual[name] for name in target if name in actual},
        target,
        dialect=dialect, decisions=NOTHING)["operations"]


@pytest.mark.parametrize(
    "label,current,target", CASES, ids=[c[0].replace(" ", "_") for c in CASES]
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
    "label,current,target", CASES, ids=[c[0].replace(" ", "_") for c in CASES]
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

        assert set(target) <= set(actual), (
            f"{label} left {sorted(set(target) - set(actual))} missing; "
            f"the database holds {sorted(actual)}"
        )
    finally:
        await connection.close()
        os.unlink(path)


# ---------------------------------------------------------------------------
# The same cases against a real PostgreSQL server
# ---------------------------------------------------------------------------

PROBE_URL = probe_url()

# Every table any case names. Dropped before and after each one, so a case
# that fails halfway cannot poison the next. Read off the cases rather than
# listed, because a list here would go stale the moment a case is added.
TOUCHED = sorted({name for _l, c, t in CASES for name in (*c, *t)} | {"_dp_enum_t_status"})


async def _clean(connection: Any) -> None:
    for name in TOUCHED:
        await connection.execute(f'DROP TABLE IF EXISTS "{name}" CASCADE')


@pytest.mark.postgresql
@pytest.mark.integration
@pytest.mark.parametrize(
    "label,current,target", CASES, ids=[c[0].replace(" ", "_") for c in CASES]
)
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
@pytest.mark.parametrize(
    "label,current,target", CASES, ids=[c[0].replace(" ", "_") for c in CASES]
)
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

        assert set(target) <= set(actual), (
            f"{label} left {sorted(set(target) - set(actual))} missing; "
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
    "label,current,target", CASES, ids=[c[0].replace(" ", "_") for c in CASES]
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
    "label,current,target", CASES, ids=[c[0].replace(" ", "_") for c in CASES]
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

        assert set(target) <= set(actual), (
            f"{label} left {sorted(set(target) - set(actual))} missing; "
            f"the database holds {sorted(actual)}"
        )
    finally:
        await connection.close()
