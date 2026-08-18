"""Test rows are either specified or generated, and the builder must say which.

WRITTEN BEFORE THE CODE. `todo()`, `user()` and `order()` were deleted on
2026-08-16 for fifteen fallbacks of this shape:

    def todo(**overrides):
        return {"id": overrides.get("id", str(uuid.uuid4())), ...}

ONE FUNCTION ANSWERED TWO DIFFERENT QUESTIONS. "Give me a row with this exact
id" and "give me any row" are not the same request, and `**overrides` made
them the same call. A caller who misspelled a key got a generated value and no
error: `todo(titel="x")` returned a row with a random title, and the scenario
asserting on "x" failed somewhere far away with no hint that the key was wrong.

SO THERE ARE TWO FUNCTIONS. `a_todo(...)` takes every field and generates
nothing; `some_todo()` generates every field and takes nothing. A misspelled
key is a TypeError at the call site in the first, and impossible in the
second. `todos(n)` builds n generated rows, which is what every caller of the
old `todos(n)` actually wanted.

GENERATED ROWS MUST BE DISTINCT, and that is asserted rather than assumed. The
old `todos(n, **overrides)` passed the same overrides to every row, so
`todos(5, id="x")` produced five rows sharing a primary key. Every scenario
that inserted them would fail on the second row.
"""

from __future__ import annotations

import pytest

from tests.bdd.factories.data_factory import (
    a_todo,
    a_user,
    some_todo,
    some_user,
    todos,
    users,
)

pytestmark = pytest.mark.precommit


# ---------------------------------------------------------------------------
# Specified rows: every field given, nothing generated
# ---------------------------------------------------------------------------


def test_a_specified_todo_is_exactly_what_was_asked_for() -> None:
    row = a_todo(id="t1", title="port it", completed=0, created_at="2026-08-16")

    assert row == {
        "id": "t1",
        "title": "port it",
        "completed": 0,
        "created_at": "2026-08-16",
    }


def test_a_specified_todo_refuses_a_missing_field() -> None:
    """The whole point. A field left out used to be filled in silently."""
    with pytest.raises(TypeError):
        a_todo(id="t1", title="port it")  # type: ignore[call-arg]


def test_a_specified_todo_refuses_a_misspelled_field() -> None:
    """`titel` used to return a row with a generated title and no complaint."""
    with pytest.raises(TypeError):
        a_todo(  # type: ignore[call-arg]
            id="t1", titel="port it", completed=0, created_at="2026-08-16"
        )


def test_a_specified_user_is_exactly_what_was_asked_for() -> None:
    row = a_user(
        id="u1",
        email="a@b.c",
        name="Alice",
        status="active",
        age=30,
        created_at="2026-08-16",
    )

    assert row["email"] == "a@b.c"
    assert row["age"] == 30
    assert set(row) == {"id", "email", "name", "status", "age", "created_at"}


def test_a_specified_user_refuses_a_missing_field() -> None:
    with pytest.raises(TypeError):
        a_user(id="u1", email="a@b.c")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Generated rows: every field generated, nothing taken
# ---------------------------------------------------------------------------


def test_a_generated_todo_fills_every_column_the_schema_declares() -> None:
    """The columns come from `simple_todos_schema`, so a drifted schema fails."""
    from tests.bdd.factories.schema_factory import simple_todos_schema

    declared = set(simple_todos_schema()["todos"]["columns"])

    assert set(some_todo()) == declared, (
        f"a generated todo has {sorted(set(some_todo()))} and the schema "
        f"declares {sorted(declared)}"
    )


def test_a_generated_user_fills_every_column_the_schema_declares() -> None:
    from tests.bdd.factories.schema_factory import simple_users_schema

    declared = set(simple_users_schema()["users"]["columns"])

    assert set(some_user()) == declared


def test_a_generated_todo_takes_no_arguments() -> None:
    """`some_todo(id="x")` would be the old conflation coming back."""
    with pytest.raises(TypeError):
        some_todo(id="x")  # type: ignore[call-arg]


def test_two_generated_todos_differ() -> None:
    assert some_todo()["id"] != some_todo()["id"]


# ---------------------------------------------------------------------------
# Batches, which is what the scenarios actually call
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("count", [0, 1, 2, 1000])
def test_a_batch_has_the_count_it_was_asked_for(count: int) -> None:
    """1000 because `Scenario: Large dataset` inserts exactly that many."""
    assert len(todos(count)) == count
    assert len(users(count)) == count


def test_every_row_in_a_batch_has_its_own_primary_key() -> None:
    """`todos(5, id="x")` used to give five rows sharing one id.

    Every scenario inserting them would fail on the second row, with a
    constraint violation that says nothing about the builder that caused it.
    """
    rows = todos(50)
    ids = [r["id"] for r in rows]

    assert len(set(ids)) == 50, (
        f"{50 - len(set(ids))} of 50 generated todos share an id"
    )


def test_every_generated_user_has_its_own_email() -> None:
    """`email` is UNIQUE in `simple_users_schema`, so a collision is a failure."""
    emails = [r["email"] for r in users(50)]

    assert len(set(emails)) == 50, (
        f"{50 - len(set(emails))} of 50 generated users share an email, and "
        f"the column is declared UNIQUE"
    )


def test_a_batch_takes_no_overrides() -> None:
    with pytest.raises(TypeError):
        todos(3, id="x")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# The rows have to survive the database they are built for
# ---------------------------------------------------------------------------


def test_a_generated_todo_inserts_into_the_table_its_schema_builds() -> None:
    """The end of the chain, in one test and no database file.

    The builder, the schema and the DDL are three separate pieces, and each
    was tested alone. This is the seam: a row whose keys do not match the
    columns is a fixture that passes every unit test and fails every scenario.
    """
    import sqlite3

    from tests.bdd.factories.connection_factory import create_table_sql
    from tests.bdd.factories.schema_factory import simple_todos_schema

    schema = simple_todos_schema()
    conn = sqlite3.connect(":memory:")
    conn.execute(create_table_sql("todos", schema["todos"], "sqlite"))

    rows = todos(3)
    columns = ", ".join(rows[0])
    marks = ", ".join("?" for _ in rows[0])
    for row in rows:
        conn.execute(f"INSERT INTO todos ({columns}) VALUES ({marks})", tuple(row.values()))

    assert conn.execute("SELECT COUNT(*) FROM todos").fetchone()[0] == 3
