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

A COLUMN WAS NOT THE WHOLE OF WHAT THE DIFFER COMPARES, and for four months
this gate read only `_compute_column_alterations`. Two more loops of the same
shape were found on 2026-08-20, both outside that horizon: a foreign key
declared `on_delete="no action"`, which the database can only ever report as
absence, and a partial index whose WHERE clause PostgreSQL renders back with
its own parentheses and quoting. Neither is a column property, so neither
could ever have failed here.

So the gate now walks three objects, and derives each one's properties from
the function that compares them: columns from `_compute_column_alterations`,
foreign keys from `_diff_foreign_keys`, indexes from `_same_index`. Adding a
property to any of the three without a round trip beside it fails.

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
from declaro_persistum.types import NOTHING

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

# A FOREIGN KEY IS NOT A COLUMN PROPERTY. `no action` is here because it is the
# one value in the vocabulary the database cannot report back: SQLite gives
# `NO ACTION` for a plain REFERENCES and for an explicit clause alike, so the
# inspector drops it and the declaration has to fold to match.
FK_VALUES: dict[str, list[object]] = {
    "on_delete": ["cascade", "set null", "restrict", "no action", None],
    "on_update": ["cascade", "restrict", "no action", None],
}

# AN INDEX IS NOT A COLUMN EITHER. The WHERE clauses are written the way an
# author writes them, quoted and unparenthesised, which is exactly what
# PostgreSQL does not give back.
INDEX_VALUES: dict[str, list[object]] = {
    "unique": [True, False],
    "where": [None, '"c" IS NULL', "c IS NOT NULL"],
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


def _fk_properties() -> set[str]:
    """What `_diff_foreign_keys` compares, read from it.

    The loop that got past this gate was `on_delete="no action"`. Reading the
    tuple rather than listing it here means a third referential property
    cannot join the comparison unnoticed.
    """
    import ast
    import inspect

    from declaro_persistum.differ import core

    tree = ast.parse(inspect.getsource(core._diff_foreign_keys).lstrip())
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.For) or not isinstance(node.iter, ast.Tuple):
            continue
        for element in node.iter.elts:
            if isinstance(element, ast.Constant) and isinstance(element.value, str):
                found.add(element.value)
    return found


def _index_properties() -> set[str]:
    """What `_same_index` compares, read from it.

    Every `.get("...")` in the function is a property of the definition it is
    deciding about, which is how `where` would have been caught.
    """
    import ast
    import inspect

    from declaro_persistum.differ import core

    tree = ast.parse(inspect.getsource(core._same_index).lstrip())
    found: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "get"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            found.add(node.args[0].value)
    return found - {"columns"}


def test_every_compared_foreign_key_property_has_values_to_try() -> None:
    """The same floor, for the object whose loop this gate could not see.

    THE NAMES ARE ASSERTED, NOT ONLY THE ABSENCE OF MISSING ONES. A scan that
    matched nothing would report no missing properties and read exactly like a
    scan that found everything, which is how an instrument goes quiet without
    going wrong out loud. Both properties must still be found by name.
    """
    found = _fk_properties()

    assert found == {"on_delete", "on_update"}, (
        f"the scan of _diff_foreign_keys found {sorted(found)}; if the loop it "
        f"reads was rewritten, this gate is no longer reading anything"
    )
    assert not found - set(FK_VALUES), (
        f"{sorted(found - set(FK_VALUES))} joined the foreign key comparison "
        f"with no round-trip values here."
    )


def test_every_compared_index_property_has_values_to_try() -> None:
    """And for the other one, with its found names asserted for the same reason."""
    found = _index_properties()

    assert found == {"unique", "where"}, (
        f"the scan of _same_index found {sorted(found)}; if the function was "
        f"rewritten, this gate is no longer reading anything"
    )
    assert not found - set(INDEX_VALUES), (
        f"{sorted(found - set(INDEX_VALUES))} joined the index comparison "
        f"with no round-trip values here."
    )


def test_every_compared_property_has_values_to_try() -> None:
    """The floor. A property nobody listed would be silently unchecked.

    This is the assertion that makes the gate a gate: it fails on the day a
    property joins the comparison without a round trip beside it.
    """
    found = _compared_properties()

    assert found == {"type", "nullable", "default", "unique", "check"}, (
        f"the scan of _compute_column_alterations found {sorted(found)}; if "
        f"the `compared` tuple was rewritten, this gate reads nothing"
    )
    missing = found - set(VALUES)

    assert not missing, (
        f"{sorted(missing)} joined the differ's comparison with no round-trip "
        f"values here. Every loop shipped today came from comparing something "
        f"that does not survive a write and a read."
    )


async def _converges(connection, dialect: str, declared: dict) -> list:
    plan = diff({}, declared, dialect=dialect, decisions=NOTHING)
    await apply(
        connection, plan["operations"], plan["execution_order"], dialect, dry_run=False
    )
    actual = await introspect(connection, dialect)
    return diff({"t": actual["t"]}, declared, dialect=dialect, decisions=NOTHING)["operations"]


def _cases():
    for prop, values in sorted(VALUES.items()):
        for value in values:
            yield prop, value


def _fk_declaration(prop: str, value: object) -> dict:
    """Two tables and a key between them, carrying one referential action.

    The referenced table is declared here rather than assumed, because a
    foreign key to a table the migration does not create is not a foreign key
    the engine will accept.
    """
    fk: dict = {"type": "integer", "references": "parent.id"}
    if value is not None:
        fk[prop] = value
    return {
        "parent": {"columns": {"id": {"type": "integer", "primary_key": True}}},
        "t": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "c": fk,
            }
        },
    }


def _index_declaration(prop: str, value: object) -> dict:
    """One table and one index over it, carrying one index property.

    A partial index needs a column its clause can name, so `c` is nullable and
    the WHERE clauses talk about it.
    """
    definition: dict = {"columns": ["c"]}
    if prop == "unique":
        definition["unique"] = value
    elif value is not None:
        definition["where"] = value
    return {
        "t": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "c": {"type": "text"},
            },
            "indexes": {"ix_c": definition},
        }
    }


def _object_cases():
    for prop, values in sorted(FK_VALUES.items()):
        for value in values:
            yield "fk", prop, value
    for prop, values in sorted(INDEX_VALUES.items()):
        for value in values:
            yield "index", prop, value


_DECLARE = {"fk": _fk_declaration, "index": _index_declaration}


async def _object_converges(connection, dialect: str, declared: dict) -> list:
    """The same trip as `_converges`, over every table the declaration names.

    `_converges` compares one table because a column case only ever declares
    one. A foreign key needs two, and comparing only `t` would miss a parent
    the migration got wrong.
    """
    plan = diff({}, declared, dialect=dialect, decisions=NOTHING)
    await apply(
        connection, plan["operations"], plan["execution_order"], dialect, dry_run=False
    )
    actual = await introspect(connection, dialect)
    current = {name: actual[name] for name in declared if name in actual}
    return diff(current, declared, dialect=dialect, decisions=NOTHING)["operations"]


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


@pytest.mark.turso
@pytest.mark.parametrize("prop,value", list(_cases()), ids=lambda v: str(v)[:20])
async def test_it_round_trips_on_turso(prop, value) -> None:
    """The engine this gate exists for as much as any.

    Turso is where the CHECK comparison would have looped: SQLite had just
    learned to read one and Turso had not, and enabling the comparison after
    checking SQLite alone would have repeated the same defect on a different
    engine. It was caught by hand that time. This is the check that does not
    depend on my remembering.
    """
    import turso.aio

    declared = _declaration(prop, value)
    directory = tempfile.mkdtemp()
    conn = await turso.aio.connect(os.path.join(directory, "roundtrip.db"))
    try:
        operations = await _converges(conn, "turso", declared)
        assert operations == [], (
            f"{prop}={value!r} migrates on every deploy: "
            f"{[(o['op'], o['details']) for o in operations]}"
        )
    finally:
        await conn.close()


@pytest.mark.sqlite
@pytest.mark.parametrize("kind,prop,value", list(_object_cases()), ids=lambda v: str(v)[:20])
async def test_an_object_property_round_trips_on_sqlite(kind, prop, value) -> None:
    """Foreign keys and indexes, which a column round trip cannot reach."""
    import aiosqlite

    declared = _DECLARE[kind](prop, value)
    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        operations = await _object_converges(conn, "sqlite", declared)
        assert operations == [], (
            f"{kind} {prop}={value!r} migrates on every deploy: "
            f"{[(o['op'], o['details']) for o in operations]}"
        )
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.postgresql
@pytest.mark.parametrize("kind,prop,value", list(_object_cases()), ids=lambda v: str(v)[:20])
async def test_an_object_property_round_trips_on_postgresql(kind, prop, value) -> None:
    """The engine both of 2026-08-20's loops were measured on."""
    import asyncpg

    url = os.environ.get(
        "TEST_POSTGRESQL_URL",
        "postgresql://postgres:postgres@localhost/declaro_probe",
    )
    try:
        conn = await asyncpg.connect(url)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"no PostgreSQL at {url}: {type(exc).__name__}: {exc}")

    declared = _DECLARE[kind](prop, value)
    try:
        for table in ("t", "parent"):
            await conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        operations = await _object_converges(conn, "postgresql", declared)
        assert operations == [], (
            f"{kind} {prop}={value!r} migrates on every deploy: "
            f"{[(o['op'], o['details']) for o in operations]}"
        )
    finally:
        for table in ("t", "parent"):
            await conn.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        await conn.close()


@pytest.mark.turso
@pytest.mark.parametrize("kind,prop,value", list(_object_cases()), ids=lambda v: str(v)[:20])
async def test_an_object_property_round_trips_on_turso(kind, prop, value) -> None:
    """Turso, where a fix landing on only one of a pair has bitten four times."""
    import turso.aio

    declared = _DECLARE[kind](prop, value)
    directory = tempfile.mkdtemp()
    conn = await turso.aio.connect(os.path.join(directory, "objects.db"))
    try:
        operations = await _object_converges(conn, "turso", declared)
        assert operations == [], (
            f"{kind} {prop}={value!r} migrates on every deploy: "
            f"{[(o['op'], o['details']) for o in operations]}"
        )
    finally:
        await conn.close()
