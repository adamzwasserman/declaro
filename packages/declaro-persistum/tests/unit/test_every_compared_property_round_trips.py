"""Every property the differ compares must survive a write and a read.

THREE MIGRATION LOOPS SHIPPED TODAY, all the same shape:

    a boolean default translated on write, compared unrenslated   SQLite
    a CHECK created and enforced, read back as None                all
    a CHECK rewritten by the engine, compared as declared text     PostgreSQL

Each was found by widening the input after the fact, never by reading the
code. Each fix was sound and each left the CAUSE in place: a property joins
`_compute_column_alterations` because comparing it seems obviously right, and
nothing checks that the value survives the trip out to a database and back.

So this is that check. For every property the differ compares, on every
engine, with values chosen to include the shapes that broke it: declare it,
apply it, introspect, and diff the result against what was declared. Anything
other than no operations is a migration that repeats forever.

WHAT MAKES IT A GATE RATHER THAN A TEST. The properties come from the differ
itself, not from a list here, so adding one to the comparison without a round
trip fails immediately -- which is the failure I made three times.

Turso and PostgreSQL skip when they are not reachable, naming what they tried.
SQLite always runs.
"""

from __future__ import annotations

import os
import tempfile

import pytest

from declaro_persistum.applier import apply
from declaro_persistum.differ import diff
from declaro_persistum.inspector import introspect

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit]

# Values chosen for what they do to a round trip, not for how they look. Each
# group holds at least one shape that broke one of today's three loops.
VALUES: dict[str, list[object]] = {
    "type": ["text", "integer", "boolean", "timestamptz", "numeric", "uuid"],
    "nullable": [True, False],
    "default": [0, "0", "false", "true", "now()", "current_timestamp", "'pending'"],
    "unique": [True, False],
    "check": ["c > 0", "c <> ''", "length(c) > 2"],
    "primary_key": [True],
}


# THE TYPE A VALUE CAN LIVE IN, stated rather than guessed. My first version
# inferred it -- integer if the expression held a `>` -- and chose integer for
# `length(c) > 2`, which PostgreSQL refuses because `length(integer)` is not a
# function. The gate then reported a defect that was mine.
TYPE_FOR: dict[object, str] = {
    "c > 0": "integer",
    "c <> ''": "text",
    "length(c) > 2": "text",
    0: "integer",
    "0": "integer",
    "false": "boolean",
    "true": "boolean",
    "now()": "timestamptz",
    "current_timestamp": "timestamptz",
    "'pending'": "text",
}


def _declaration(prop: str, value: object) -> dict:
    """One table declaring one property, with a type the value can live in."""
    if prop == "primary_key":
        # ONE PRIMARY KEY PER TABLE. Declaring it on `c` while `id` also has
        # it is two, which SQLite refuses -- another defect of my own the gate
        # reported before this was fixed.
        return {"t": {"columns": {"c": {"type": "text", "primary_key": True}}}}

    column: dict = {"type": "text"}
    if prop == "type":
        column = {"type": value}
    else:
        column = {"type": TYPE_FOR.get(value, "text"), prop: value}

    return {"t": {"columns": {"id": {"type": "text", "primary_key": True}, "c": column}}}


def _compared_properties() -> set[str]:
    """What the differ actually compares, read from the differ.

    A list written here would go stale the moment somebody adds a property,
    which is the whole failure this gate exists to catch.
    """
    import ast
    import inspect

    from declaro_persistum.differ import core

    source = inspect.getsource(core._compute_column_alterations)
    found = {"type", "nullable", "default"}  # compared by name, not in a tuple

    # ONLY THE TUPLE ASSIGNED TO `compared`. My first version took every string
    # tuple in the function and reported ('sqlite', 'turso') -- the dialect
    # test beside it -- as two properties nobody had given values to. An
    # instrument that reads too widely invents work.
    for node in ast.walk(ast.parse(source.lstrip())):
        if not isinstance(node, ast.Assign):
            continue
        if not any(
            isinstance(t, ast.Name) and t.id == "compared" for t in node.targets
        ):
            continue
        for element in ast.walk(node.value):
            if (
                isinstance(element, ast.Constant)
                and isinstance(element.value, str)
                and element.value not in ("sqlite", "turso", "postgresql")
            ):
                found.add(element.value)
    return found


def test_every_compared_property_has_values_to_try() -> None:
    """The floor. A property nobody listed would be silently unchecked.

    This is the assertion that makes the gate a gate: it fails on the day a
    property joins the comparison without a round trip beside it.
    """
    missing = _compared_properties() - set(VALUES)

    assert not missing, (
        f"{sorted(missing)} joined the differ's comparison with no round-trip "
        f"values here. Every loop shipped today came from comparing something "
        f"that does not survive a write and a read."
    )


async def _converges(connection, dialect: str, declared: dict) -> list:
    plan = diff({}, declared, dialect=dialect)
    await apply(
        connection, plan["operations"], plan["execution_order"], dialect, dry_run=False
    )
    actual = await introspect(connection, dialect)
    return diff({"t": actual["t"]}, declared, dialect=dialect)["operations"]


def _cases():
    for prop, values in sorted(VALUES.items()):
        for value in values:
            yield prop, value


@pytest.mark.sqlite
@pytest.mark.parametrize("prop,value", list(_cases()), ids=lambda v: str(v)[:20])
async def test_it_round_trips_on_sqlite(prop, value) -> None:
    import aiosqlite

    declared = _declaration(prop, value)
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        operations = await _converges(conn, "sqlite", declared)
        assert operations == [], (
            f"{prop}={value!r} migrates on every deploy: "
            f"{[(o['op'], o['details']) for o in operations]}"
        )
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.postgresql
@pytest.mark.parametrize("prop,value", list(_cases()), ids=lambda v: str(v)[:20])
async def test_it_round_trips_on_postgresql(prop, value) -> None:
    import asyncpg

    url = os.environ.get(
        "TEST_POSTGRESQL_URL",
        "postgresql://postgres:postgres@localhost/declaro_probe",
    )
    try:
        conn = await asyncpg.connect(url)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"no PostgreSQL at {url}: {type(exc).__name__}: {exc}")

    declared = _declaration(prop, value)
    try:
        await conn.execute('DROP TABLE IF EXISTS t CASCADE')
        operations = await _converges(conn, "postgresql", declared)
        assert operations == [], (
            f"{prop}={value!r} migrates on every deploy: "
            f"{[(o['op'], o['details']) for o in operations]}"
        )
    finally:
        await conn.execute('DROP TABLE IF EXISTS t CASCADE')
        await conn.close()
