"""The introspection layer, fifteen functions no test had ever called.

These decide whether a migration converges: the differ compares what a schema
declares against what these report, so a parser that drops a detail proposes
an operation on every deploy, and one that invents a detail proposes the
opposite.

MEASURED 2026-08-17. Of 284 public functions in `src/`, 43 are called by
nothing and dispatched by nothing, and the largest block is here:

    inspector.shared   apply_unique_columns  assemble_table
                       columns_from_pragma_rows  extract_view_query
                       fk_list_from_pragma_rows  indexes_from_rows
                       normalize_fk_action  normalize_sqlite_type
    inspector.sqlite   columns_of  foreign_keys_of  indexes_of
                       matviews_of  table_of

EVERY INPUT COMES FROM SQLITE, not from a hand-written tuple. The pure parsers
take PRAGMA rows, and a row I invent tests my idea of the format: I got the
shape of `PRAGMA foreign_key_list` wrong once already this session by reading
position 5 as ON DELETE when it is ON UPDATE. So the tests that exercise the
parsers directly take their rows from a real database first.

Three round trips were broken today and all three lived on this boundary --
a boolean default, an expression default, and a primary key's nullability --
so what is asserted below is mostly that a declaration survives being written
and read back.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile

import aiosqlite
import pytest

from declaro_persistum.inspector.shared import (
    columns_from_pragma_rows,
    extract_view_query,
    fk_list_from_pragma_rows,
    normalize_fk_action,
    normalize_sqlite_type,
)

pytestmark = pytest.mark.precommit

SCHEMA = """
CREATE TABLE users (
  id TEXT PRIMARY KEY,
  email TEXT NOT NULL UNIQUE,
  age INTEGER DEFAULT 0,
  bio VARCHAR(255),
  score REAL,
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE orders (
  id TEXT PRIMARY KEY,
  user_id TEXT REFERENCES users(id) ON DELETE CASCADE ON UPDATE SET NULL,
  total NUMERIC(10,2)
);
CREATE INDEX idx_orders_user ON orders(user_id);
CREATE UNIQUE INDEX idx_users_email ON users(email);
CREATE VIEW active AS SELECT id FROM users WHERE age > 0;
"""


def _rows(sql: str) -> list[tuple]:
    """PRAGMA output from SQLite itself, not from my idea of the format."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(SCHEMA)
    return conn.execute(sql).fetchall()


# ---------------------------------------------------------------------------
# Columns
# ---------------------------------------------------------------------------


def test_a_column_keeps_its_type_its_nullability_and_its_default() -> None:
    columns = columns_from_pragma_rows(_rows("PRAGMA table_info(users)"))

    assert columns["id"] == {"type": "text", "nullable": False, "primary_key": True}
    assert columns["email"] == {"type": "text", "nullable": False}
    assert columns["age"] == {"type": "integer", "default": "0"}


def test_a_primary_key_is_reported_not_null() -> None:
    """The differ reads nullability off the key too, and the two must agree."""
    columns = columns_from_pragma_rows(_rows("PRAGMA table_info(users)"))

    assert columns["id"]["primary_key"] is True
    assert columns["id"]["nullable"] is False


def test_a_column_with_no_default_carries_no_default_key() -> None:
    """Absent and NULL are different, and `"default" in col` is how the
    differ tells them apart."""
    columns = columns_from_pragma_rows(_rows("PRAGMA table_info(users)"))

    assert "default" not in columns["bio"], columns["bio"]


def test_an_expression_default_keeps_its_parentheses() -> None:
    """PRAGMA strips one layer, so the round trip needs them doubled.

    Without this the differ sees a change on every run for a column nobody
    touched, which is one of three migration loops measured today.
    """
    columns = columns_from_pragma_rows(_rows("PRAGMA table_info(users)"))

    assert columns["created_at"]["default"] == "(datetime('now'))"


@pytest.mark.parametrize(
    "declared,expected",
    [
        ("TEXT", "text"),
        ("VARCHAR(255)", "text"),
        ("INTEGER", "integer"),
        ("REAL", "real"),
        ("BLOB", "blob"),
        ("NUMERIC(10,2)", "numeric"),
        ("BOOLEAN", "numeric"),
    ],
)
def test_a_stored_type_normalises_to_the_vocabulary(declared, expected) -> None:
    """SQLite stores what you wrote; the differ compares canonical names."""
    assert normalize_sqlite_type(declared) == expected


# ---------------------------------------------------------------------------
# Foreign keys, where I read the wrong column once already
# ---------------------------------------------------------------------------


def test_on_delete_and_on_update_are_not_swapped() -> None:
    """`PRAGMA foreign_key_list` puts ON UPDATE at 5 and ON DELETE at 6.

    Reading them the other way round turns `ON DELETE CASCADE` into
    `ON UPDATE CASCADE`, so deleting a user would leave its orders behind and
    renaming its id would destroy them. The declaration below is deliberately
    asymmetric so a swap cannot pass.
    """
    keys = fk_list_from_pragma_rows(_rows("PRAGMA foreign_key_list(orders)"))

    assert len(keys) == 1, keys
    assert keys[0]["on_delete"] == "CASCADE"
    assert keys[0]["on_update"] == "SET NULL"


def test_a_foreign_key_names_both_ends() -> None:
    keys = fk_list_from_pragma_rows(_rows("PRAGMA foreign_key_list(orders)"))

    assert keys[0]["from"] == "user_id"
    assert keys[0]["table"] == "users"
    assert keys[0]["to"] == "id"


@pytest.mark.parametrize(
    "action,expected",
    [
        ("CASCADE", "cascade"),
        ("SET NULL", "set null"),
        ("RESTRICT", "restrict"),
        ("NO ACTION", None),
        ("NONE", None),
        (None, None),
    ],
)
def test_a_cascade_action_normalises(action, expected) -> None:
    """`NO ACTION` is SQLite's default, and reporting it would make every
    table with a plain foreign key look changed."""
    assert normalize_fk_action(action) == expected


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------


def test_a_view_query_is_taken_out_of_its_create_statement() -> None:
    statement = _rows("SELECT sql FROM sqlite_master WHERE type='view'")[0][0]

    query = extract_view_query(statement)

    assert query.strip().upper().startswith("SELECT")
    assert "CREATE" not in query.upper()


# ---------------------------------------------------------------------------
# The whole table, through the async path a migration uses
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_a_declared_table_reads_back_whole() -> None:
    """`table_of` is what the differ compares against, so this is the claim
    that matters: everything declared comes back."""
    from declaro_persistum.inspector.sqlite import table_of

    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        await conn.executescript(SCHEMA)
        await conn.commit()

        table = await table_of(conn, "orders")

        assert set(table["columns"]) == {"id", "user_id", "total"}
        assert table["columns"]["id"]["primary_key"] is True
        assert "idx_orders_user" in table["indexes"], table["indexes"]
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_a_unique_index_marks_its_column_unique() -> None:
    """`apply_unique_columns` folds a single-column unique index into the
    column, because that is how the schema declares it."""
    from declaro_persistum.inspector.sqlite import table_of

    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        await conn.executescript(SCHEMA)
        await conn.commit()

        table = await table_of(conn, "users")

        assert table["columns"]["email"].get("unique") is True, table["columns"]["email"]
        assert table["columns"]["age"].get("unique") is not True
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_the_implicit_indexes_sqlite_creates_are_not_reported() -> None:
    """A PRIMARY KEY and a UNIQUE make `sqlite_autoindex_*` entries.

    Reporting them would put indexes in the introspected schema that no
    declaration mentions, and the differ would emit DROP INDEX for them on
    every run.
    """
    from declaro_persistum.inspector.sqlite import indexes_of

    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        await conn.executescript(SCHEMA)
        await conn.commit()

        indexes = await indexes_of(conn, "users")

        assert not [n for n in indexes if n.startswith("sqlite_autoindex")], indexes
        assert "idx_users_email" in indexes
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_a_database_with_no_materialized_views_reads_back_empty() -> None:
    """The metadata table does not exist until a matview is declared."""
    from declaro_persistum.inspector.sqlite import matviews_of

    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        await conn.executescript(SCHEMA)
        await conn.commit()

        assert await matviews_of(conn) == {}
    finally:
        await conn.close()
        os.unlink(path)
