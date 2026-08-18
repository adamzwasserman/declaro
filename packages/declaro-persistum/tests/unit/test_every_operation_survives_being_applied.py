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

WHAT THIS DOES NOT COVER: PostgreSQL and Turso. Both are one parametrisation
away and both cost a server or a file per case; SQLite is where the renderer
shared by SQLite and Turso lives, so it is where the gap was widest.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.abstractions.enums import expand_schema_enums
from declaro_persistum.differ.core import diff
from declaro_persistum.inspector import introspect
from declaro_persistum.types import Operation

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
    return {o["op"] for o in diff(current, target, dialect="sqlite")["operations"]}


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


async def _apply_plan(connection: Any, current: dict, target: dict) -> None:
    from declaro_persistum.applier import apply

    plan = diff(current, target, dialect="sqlite")
    await apply(
        connection,
        plan["operations"],
        plan["execution_order"],
        "sqlite",
        dry_run=False,
    )


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
        await _apply_plan(connection, {}, current)
        await _apply_plan(connection, current, target)

        actual = await introspect(connection, "sqlite")
        remaining = diff(
            {name: actual[name] for name in target if name in actual},
            target,
            dialect="sqlite",
        )["operations"]

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
        await _apply_plan(connection, {}, current)
        await _apply_plan(connection, current, target)

        actual = await introspect(connection, "sqlite")

        assert set(target) <= set(actual), (
            f"{label} left {sorted(set(target) - set(actual))} missing; "
            f"the database holds {sorted(actual)}"
        )
    finally:
        await connection.close()
        os.unlink(path)
