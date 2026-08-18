"""Seven pure functions the reachability scan found called by nothing.

The retry backoff, the reconstruction copy list, and five introspection
assemblers. All pure -- data in, data out -- so the only reason they were
untested is that nobody went looking.

THE PRAGMA ROWS COME FROM SQLITE. A tuple I write tests my idea of the format,
and I read position 5 of `PRAGMA foreign_key_list` as ON DELETE earlier in
this session when it is ON UPDATE. The fixture asks the engine.

`delay_before` is the exception: it takes a `Retry` record, and the two the
package ships are `NO_RETRY` and `ON_CONTENTION`, so those are what it is
asked about rather than numbers I invented.
"""

from __future__ import annotations

import sqlite3

import pytest

from declaro_persistum.abstractions.table_reconstruction import columns_to_copy
from declaro_persistum.inspector.shared import (
    apply_unique_columns,
    assemble_table,
    indexes_from_rows,
    unique_cols_from_index_rows,
    views_from_rows,
)
from declaro_persistum.retry import NO_RETRY, ON_CONTENTION, delay_before

pytestmark = pytest.mark.precommit


# ---------------------------------------------------------------------------
# The backoff
# ---------------------------------------------------------------------------


def test_the_first_attempt_does_not_wait() -> None:
    """Nothing has gone wrong yet, so sleeping would be pure latency."""
    assert delay_before(ON_CONTENTION, 1) == 0.0


def test_the_wait_doubles_with_each_attempt() -> None:
    """Backing off at a constant rate is what makes contention worse."""
    waits = [delay_before(ON_CONTENTION, n) for n in (2, 3, 4)]

    assert waits == [0.01, 0.02, 0.04], waits


def test_the_wait_stops_at_the_ceiling() -> None:
    """Doubling without a cap reaches minutes, and a write queue holds a
    caller for every second of it."""
    assert delay_before(ON_CONTENTION, 20) == ON_CONTENTION["max_delay_s"]


def test_a_policy_that_does_not_retry_never_waits() -> None:
    assert [delay_before(NO_RETRY, n) for n in (1, 2, 3)] == [0.0, 0.0, 0.0]


def test_the_backoff_is_pure() -> None:
    """No clock and no sleep, which is why this file can assert on it at all."""
    assert delay_before(ON_CONTENTION, 3) == delay_before(ON_CONTENTION, 3)


# ---------------------------------------------------------------------------
# Which columns a rebuild copies
# ---------------------------------------------------------------------------


def test_a_rebuild_copies_the_columns_both_shapes_have() -> None:
    assert columns_to_copy(["id", "email"], ["id", "email"]) == [
        ("id", "id"),
        ("email", "email"),
    ]


def test_a_dropped_column_is_not_copied() -> None:
    assert columns_to_copy(["id", "spare"], ["id"]) == [("id", "id")]


def test_an_added_column_has_nothing_to_copy_from() -> None:
    """Copying it would be a SELECT of a column the old table lacks."""
    assert columns_to_copy(["id"], ["id", "fresh"]) == [("id", "id")]


def test_a_column_whose_case_changed_is_still_copied() -> None:
    """SQLite folds an identifier and Python's `in` does not.

    This is a measured data loss: a rebuild dropped a column's contents
    because the stored name differed in case from the declared one.
    """
    assert columns_to_copy(["ID", "Email"], ["id", "email"]) == [
        ("ID", "id"),
        ("Email", "email"),
    ]


def test_nothing_in_common_copies_nothing() -> None:
    assert columns_to_copy(["a"], ["b"]) == []


# ---------------------------------------------------------------------------
# Assembling what introspection read
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE users (
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  team TEXT
);
CREATE INDEX idx_users_team ON users(team);
CREATE UNIQUE INDEX idx_users_email ON users(email);
CREATE TABLE pair (a TEXT, b TEXT);
CREATE UNIQUE INDEX idx_pair ON pair(a, b);
CREATE VIEW active AS SELECT id FROM users WHERE team IS NOT NULL;
"""


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(SCHEMA)
    return conn


def _index_inputs(conn, table: str):
    rows = conn.execute(f"PRAGMA index_list({table})").fetchall()
    info = {r[1]: conn.execute(f"PRAGMA index_info({r[1]})").fetchall() for r in rows}
    sql = {
        name: statement
        for name, statement in conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='index'"
        )
    }
    return rows, info, sql


def test_an_index_keeps_its_columns_and_its_uniqueness() -> None:
    conn = _conn()

    indexes = indexes_from_rows(*_index_inputs(conn, "users"))

    assert indexes["idx_users_team"] == {"columns": ["team"]}
    assert indexes["idx_users_email"]["unique"] is True


def test_a_single_column_unique_index_marks_its_column() -> None:
    conn = _conn()
    rows, info, _sql = _index_inputs(conn, "users")

    assert unique_cols_from_index_rows(rows, info) == {"email"}


def test_a_two_column_unique_index_marks_neither() -> None:
    """`UNIQUE (a, b)` says the PAIR is unique, not either column.

    Marking one would claim a constraint the database does not have, and the
    differ would then propose removing a uniqueness nobody declared.
    """
    conn = _conn()
    rows, info, _sql = _index_inputs(conn, "pair")

    assert unique_cols_from_index_rows(rows, info) == set()


def test_marking_unique_columns_leaves_a_primary_key_alone() -> None:
    """A primary key is unique by definition, and saying it twice makes the
    differ propose adding what the column already has."""
    columns = {"id": {"type": "text", "primary_key": True}, "email": {"type": "text"}}

    apply_unique_columns(columns, {"id", "email"})

    assert "unique" not in columns["id"], columns["id"]
    assert columns["email"]["unique"] is True


def test_a_table_is_assembled_from_its_three_parts() -> None:
    columns = {"id": {"type": "text", "primary_key": True}}
    indexes = {"i": {"columns": ["id"]}}
    keys = [{"from": "id", "table": "u", "to": "id"}]

    table = assemble_table(columns, indexes, keys)

    assert table["columns"] == columns
    assert table["indexes"] == indexes


def test_a_table_with_nothing_extra_carries_nothing_extra() -> None:
    """An empty `indexes` key that introspection would not report is a
    difference on every run."""
    table = assemble_table({"id": {"type": "text"}}, {}, [])

    assert table["columns"] == {"id": {"type": "text"}}
    assert not table.get("indexes")


def test_a_view_is_read_with_its_query() -> None:
    conn = _conn()
    rows = conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type='view'"
    ).fetchall()

    views = views_from_rows(rows, {})

    assert "active" in views
    assert "SELECT" in views["active"]["query"].upper()
    assert views["active"].get("materialized") is not True


def test_a_materialized_view_is_marked_as_one() -> None:
    """The distinction is why the function takes two arguments: a plain view
    is recreated freely and a materialized one holds data."""
    views = views_from_rows([], {"stats": ("SELECT count(*) FROM users", "manual")})

    assert views["stats"]["materialized"] is True
