"""Most of the TOML loader's vocabulary had never been read.

MEASURED 2026-08-19 with `coverage json` over the whole suite. Unrun in
`loader.py`:

    invalid TOML syntax                 -> LoaderError
    a file that cannot be read          -> LoaderError
    a table written as a bare column map, with no [x.columns] section
    a primary_key given as one string rather than a list
    a constraints block
    renamed_from on a table
    renamed_from and is_new on a column

That is most of what the format offers. A schema using any of it was going
through code nothing had executed.

THE CONSTRAINTS BLOCK IS THE ONE THAT MATTERS MOST TODAY. Table-level
constraints were fixed this morning across four modules -- they were never
created, never read back, dropped by both rebuilds, and missing from the
PostgreSQL renderer -- and the TOML path that declares one in the first place
was never run.

THE MIGRATION HINTS ARE THE OTHER. `renamed_from` is how an author answers
the differ's rename-or-drop question, and answering it wrong loses a column's
rows. It is read here and nowhere else for a TOML schema.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from declaro_persistum.exceptions import LoaderError
from declaro_persistum.loader import load_schema

pytestmark = [pytest.mark.precommit]


def _dir(toml: str, name: str = "tables.toml") -> str:
    directory = Path(tempfile.mkdtemp())
    (directory / name).write_text(toml)
    return str(directory)


def test_invalid_toml_is_refused_by_name() -> None:
    """The file is named, because a schema directory holds several."""
    with pytest.raises(LoaderError) as caught:
        load_schema(_dir("this is not [ valid toml", name="broken.toml"))

    assert "broken.toml" in str(caught.value), caught.value


def test_a_table_written_as_a_bare_column_map_is_read() -> None:
    """The short form: no `[users.columns]` header, the columns directly."""
    schema = load_schema(
        _dir(
            """
[users]
id = { type = "integer", primary_key = true }
name = { type = "text" }
"""
        )
    )

    assert sorted(schema["users"]["columns"]) == ["id", "name"]


def test_a_primary_key_given_as_one_string_becomes_a_list() -> None:
    """`primary_key = "id"` and `primary_key = ["id"]` are the same key.

    Everything downstream indexes into it, so a bare string would be read as
    a list of its characters.
    """
    schema = load_schema(
        _dir(
            """
[users]
primary_key = "id"
[users.columns]
id = { type = "integer" }
"""
        )
    )

    assert schema["users"]["primary_key"] == ["id"]


def test_a_composite_primary_key_is_read_as_given() -> None:
    schema = load_schema(
        _dir(
            """
[pairs]
primary_key = ["left", "right"]
[pairs.columns]
left = { type = "integer" }
right = { type = "integer" }
"""
        )
    )

    assert schema["pairs"]["primary_key"] == ["left", "right"]


def test_a_constraints_block_is_read() -> None:
    """The declaration for everything fixed across four modules this morning."""
    schema = load_schema(
        _dir(
            """
[widgets]
[widgets.columns]
id = { type = "integer", primary_key = true }
lo = { type = "integer" }
hi = { type = "integer" }
[widgets.constraints.ck_order]
type = "check"
expression = "lo < hi"
"""
        )
    )

    assert schema["widgets"]["constraints"]["ck_order"] == {
        "type": "check",
        "expression": "lo < hi",
    }


def test_a_declared_constraint_reaches_a_database() -> None:
    """The floor. Reading it into a dict is not the same as it being created.

    Everything between the TOML and the engine was broken this morning, so
    this runs the whole way rather than stopping at the loader.
    """
    import asyncio
    import os

    import aiosqlite

    from declaro_persistum.applier import apply
    from declaro_persistum.differ.core import diff

    schema = load_schema(
        _dir(
            """
[widgets]
[widgets.columns]
id = { type = "integer", primary_key = true }
lo = { type = "integer" }
hi = { type = "integer" }
[widgets.constraints.ck_order]
type = "check"
expression = "lo < hi"
"""
        )
    )

    async def run() -> str:
        connection = await aiosqlite.connect(
            os.path.join(tempfile.mkdtemp(), "t.db")
        )
        try:
            plan = diff({}, schema, dialect="sqlite")
            await apply(
                connection,
                plan["operations"],
                plan["execution_order"],
                "sqlite",
                dry_run=False,
            )
            cursor = await connection.execute(
                "SELECT sql FROM sqlite_master WHERE name='widgets'"
            )
            return str((await cursor.fetchone())[0])
        finally:
            await connection.close()

    assert "CHECK" in asyncio.run(run()).upper()


def test_a_table_rename_hint_is_read() -> None:
    """`renamed_from` is how an author answers the rename-or-drop question.

    Answering it wrong drops the table and its rows, so the hint has to
    arrive.
    """
    schema = load_schema(
        _dir(
            """
[customers]
renamed_from = "users"
[customers.columns]
id = { type = "integer", primary_key = true }
"""
        )
    )

    assert schema["customers"]["renamed_from"] == "users"


def test_a_column_rename_hint_is_read() -> None:
    schema = load_schema(
        _dir(
            """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
full_name = { type = "text", renamed_from = "name" }
"""
        )
    )

    assert schema["users"]["columns"]["full_name"]["renamed_from"] == "name"


def test_a_new_column_hint_is_read() -> None:
    """`is_new` is the other half of the answer: this one really is new."""
    schema = load_schema(
        _dir(
            """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
note = { type = "text", is_new = true }
"""
        )
    )

    assert schema["users"]["columns"]["note"]["is_new"] is True


def test_an_ordinary_schema_carries_no_hints_it_was_not_given() -> None:
    """The control. A loader that invented hints would answer the differ's
    question on the author's behalf, which is the one thing it must not do."""
    schema = load_schema(
        _dir(
            """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
"""
        )
    )

    assert "renamed_from" not in schema["users"]
    assert "renamed_from" not in schema["users"]["columns"]["id"]
    assert "is_new" not in schema["users"]["columns"]["id"]
