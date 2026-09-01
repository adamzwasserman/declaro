"""A model field with a default value had never been loaded.

MEASURED 2026-08-19 with `coverage json` over the whole suite. The four lines
in `pydantic_loader` that turn a Python default into a SQL one had never run:

    if isinstance(default_val, str):    col["default"] = f"'{default_val}'"
    elif isinstance(default_val, bool): col["default"] = "TRUE"/"FALSE"
    elif isinstance(default_val, (int, float)): col["default"] = str(...)

Defaults are what this package has got wrong most often. A boolean default
translated on write and compared untranslated looped on SQLite; an expression
default copied verbatim emitted `now()` to SQLite and exited 0; a removed
default assigned rather than popped rendered `DEFAULT None`. All three were
found by round-tripping, and none of them came through this path, because
nothing ever declared a default in a model.

IT IS CORRECT, WHICH IS THE RESULT. Every shape produces what it should,
including the three that are easy to get wrong:

    count: int = 0        `'0'`, not skipped for being falsy
    hidden: bool = False   `'FALSE'`, not `'0'` and not skipped
    blank: str = ""        `"''"`, not skipped

`bool` is a subclass of `int` in Python, so a bool checked after int would
render `True` as `1`. The branches are in the order that avoids it.

A FIELD WITH NO DEFAULT CARRIES NO KEY, which matters more than it looks.
`column_definition` renders `DEFAULT NULL` for a `default` key present and
None, so a primary key that picked one up would be declared `PRIMARY KEY NOT
NULL DEFAULT NULL`. The loader omits the key rather than setting it to None.

The round trip is the real test. A default that renders is not a default the
engine keeps, and every loop above passed a rendering check.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Any

import pytest

from declaro_persistum.applier import apply
from declaro_persistum.applier.shared import column_definition
from declaro_persistum.differ.core import diff
from declaro_persistum.inspector import introspect
from declaro_persistum.pydantic_loader import load_schema_from_models
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.precommit]

MODELS = """
from pydantic import BaseModel
from declaro_persistum import table, field


@table("widgets")
class Widget(BaseModel):
    id: int = field(primary_key=True)
    name: str = "unnamed"
    count: int = 0
    ratio: float = 1.5
    active: bool = True
    hidden: bool = False
    blank: str = ""
    note: str | None = None
"""

EXPECTED = {
    "name": "'unnamed'",
    "count": "0",
    "ratio": "1.5",
    "active": "TRUE",
    "hidden": "FALSE",
    "blank": "''",
}


def _columns() -> dict[str, Any]:
    directory = Path(tempfile.mkdtemp())
    (directory / "models.py").write_text(MODELS)
    return load_schema_from_models(str(directory))["widgets"]["columns"]


COLUMNS = _columns()


@pytest.mark.parametrize("column", sorted(EXPECTED))
def test_the_default_is_spelled_for_sql(column: str) -> None:
    assert COLUMNS[column].get("default") == EXPECTED[column], COLUMNS[column]


def test_a_false_default_is_not_mistaken_for_no_default() -> None:
    """Stated on its own because falsy defaults are where this goes wrong.

    `if meta.get("default")` above these branches is a truthiness test, so a
    declared `False`, `0` or `""` reaching it would be read as absent.
    """
    for column in ("hidden", "count", "blank"):
        assert "default" in COLUMNS[column], column


def test_a_boolean_is_not_rendered_as_a_number() -> None:
    """`bool` is a subclass of `int`, so branch order decides this."""
    assert COLUMNS["active"]["default"] == "TRUE"
    assert COLUMNS["hidden"]["default"] == "FALSE"


@pytest.mark.parametrize("column", ["id", "note"])
def test_a_field_with_no_default_carries_no_default_key(column: str) -> None:
    """Absent, not present and None.

    `column_definition` renders `DEFAULT NULL` for a key present and None, so
    a primary key that picked one up would be `PRIMARY KEY NOT NULL DEFAULT
    NULL`.
    """
    assert "default" not in COLUMNS[column], COLUMNS[column]


def test_the_primary_key_renders_without_a_default() -> None:
    """The consequence of the test above, asserted where it would show."""
    rendered = column_definition("id", COLUMNS["id"], "sqlite")

    assert "DEFAULT" not in rendered, rendered


@pytest.mark.parametrize("engine", ["sqlite", "turso"])
async def test_the_defaults_survive_a_round_trip(engine: str) -> None:
    """The real test. A default that renders is not one the engine keeps.

    Every default defect this package has shipped passed a rendering check
    and failed here: the table was written, read back, and diffed against the
    declaration, and the diff was not empty.
    """
    directory = Path(tempfile.mkdtemp())
    (directory / "models.py").write_text(MODELS)
    declared = load_schema_from_models(str(directory))

    if engine == "turso":
        import turso.aio

        connection = await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "d.db"))
    else:
        import aiosqlite

        connection = await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "d.db"))

    try:
        plan = diff({}, declared, engine=engine, decisions=NOTHING)
        await apply(
            connection,
            plan["operations"],
            plan["execution_order"],
            engine,
            dry_run=False,
        )

        actual = await introspect(connection, engine)
        remaining = diff(
            {n: actual[n] for n in declared if n in actual},
            declared,
            engine=engine,
            decisions=NOTHING,
        )["operations"]

        assert remaining == [], (
            f"the declared defaults migrate on every deploy: "
            f"{[(o['op'], o['details']) for o in remaining]}"
        )
    finally:
        await connection.close()


@pytest.mark.parametrize("engine", ["sqlite", "turso"])
async def test_the_defaults_actually_apply_to_a_row(engine: str) -> None:
    """The floor under convergence.

    `DEFAULT NULL` parses for every one of these and means none of them, and
    a schema that converged on shape while storing NULL would pass above.
    """
    directory = Path(tempfile.mkdtemp())
    (directory / "models.py").write_text(MODELS)
    declared = load_schema_from_models(str(directory))

    if engine == "turso":
        import turso.aio

        connection = await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "r.db"))
    else:
        import aiosqlite

        connection = await aiosqlite.connect(os.path.join(tempfile.mkdtemp(), "r.db"))

    try:
        plan = diff({}, declared, engine=engine, decisions=NOTHING)
        await apply(
            connection,
            plan["operations"],
            plan["execution_order"],
            engine,
            dry_run=False,
        )
        # TWO DRIVERS, TWO CURSOR SHAPES. pyturso hands back a cursor object
        # that holds the results; aiosqlite returns a cursor FROM execute.
        # Passing the connection for both got `'Connection' object has no
        # attribute 'fetchall'`.
        select = 'SELECT name, count, ratio, active, hidden, blank FROM "widgets"'
        if engine == "turso":
            cursor = connection.cursor()
            await cursor.execute('INSERT INTO "widgets" (id) VALUES (1)')
            await connection.commit()
            await cursor.execute(select)
            row = (await cursor.fetchall())[0]
        else:
            await connection.execute('INSERT INTO "widgets" (id) VALUES (1)')
            await connection.commit()
            cursor = await connection.execute(select)
            row = (await cursor.fetchall())[0]

        assert row[0] == "unnamed", row
        assert row[1] == 0, row
        assert row[2] == 1.5, row
        assert row[5] == "", row
    finally:
        await connection.close()
