"""The SQL migrations runs, asserted as text rather than by running it.

WRITTEN BEFORE THE CODE. This exists because of a measured failure. Five
mutations were made to `migrations.py` and run against two test suites:

    mutation                              dict double   real database
    _stamp_key drops the hash  (Python)   CAUGHT        CAUGHT
    ON CONFLICT -> DO NOTHING  (SQL)      missed        CAUGHT
    ON CONFLICT removed        (SQL)      missed        CAUGHT
    key stops being PRIMARY KEY (DDL)     missed        CAUGHT
    value becomes nullable      (DDL)     missed        CAUGHT

The double caught the one Python mutation and missed every SQL one. The line
is exact and it has a cause: `_store_hash` built its statement and executed it
in the same function, so the statement was never a value anybody could look at.
The only seam left was the connection, and the connection is what gives SQL its
meaning, so a double there is a re-implementation of a database.

THE PACKAGE ALREADY KNEW THE OTHER SHAPE. `applier/shared.py` has fourteen
functions that return SQL and none that execute it, and they are tested by
equality. `migrations.py` had one and five. These tests are for the five, split.

A REAL DATABASE IS STILL USED, in `test_schema_hash_key_isolation.py`, and
these do not replace it. Text says the clause is written; execution says the
engine accepts it. Neither substitutes for the other, and the reason the SQL
tests are here is that a wrong clause should fail on the clause rather than
three layers away.
"""

from __future__ import annotations

import pytest

from declaro_persistum.migrations import (
    META_TABLE,
    meta_table_sql,
    store_hash_sql,
    stored_hash_sql,
    user_tables_sql,
)

pytestmark = pytest.mark.precommit

ENGINES = ["sqlite", "postgresql"]


# ---------------------------------------------------------------------------
# The table the stamp lives in
# ---------------------------------------------------------------------------


def test_the_meta_table_names_key_as_its_primary_key() -> None:
    """The upsert's conflict target. Without it every restamp inserts a row.

    Mutation `key TEXT PRIMARY KEY -> key TEXT` failed 7 tests against a real
    database and 0 against the dict. It fails here on the text.
    """
    sql = meta_table_sql()

    assert "key TEXT PRIMARY KEY" in sql, sql
    assert f'"{META_TABLE}"' in sql, sql
    assert sql.startswith("CREATE TABLE IF NOT EXISTS"), (
        "the statement is not idempotent, so a second service booting against "
        "the same database fails on the create rather than reading the stamp"
    )


def test_the_meta_table_requires_a_value() -> None:
    """A stamp is not optional. `value TEXT -> ` was the fifth mutation."""
    assert "value TEXT NOT NULL" in meta_table_sql()


def test_the_meta_table_records_when_it_was_stamped() -> None:
    assert "updated_at TEXT NOT NULL" in meta_table_sql()


# ---------------------------------------------------------------------------
# Reading a stamp
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ENGINES)
def test_reading_a_stamp_selects_by_key(engine: str) -> None:
    sql = stored_hash_sql(engine)

    assert sql.startswith("SELECT value FROM"), sql
    assert "WHERE key =" in sql, sql


def test_the_two_engines_differ_only_in_the_placeholder() -> None:
    """SQLite binds `?` and asyncpg binds `$1`, and nothing else changes.

    Written as a comparison rather than two literals because that is the whole
    difference, and a second divergence appearing silently is what a table of
    statements is meant to prevent.
    """
    sqlite = stored_hash_sql("sqlite")
    postgres = stored_hash_sql("postgresql")

    assert sqlite != postgres
    assert sqlite.replace("?", "$1") == postgres, (
        f"the engines differ by more than the placeholder:\n"
        f"  sqlite:     {sqlite}\n"
        f"  postgresql: {postgres}"
    )


# ---------------------------------------------------------------------------
# Writing a stamp: the clause four mutations lived in
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ENGINES)
def test_writing_a_stamp_is_an_upsert(engine: str) -> None:
    """`ON CONFLICT (key)` or a second boot raises UNIQUE constraint failed."""
    sql = store_hash_sql(engine)

    assert "INSERT INTO" in sql, sql
    assert "ON CONFLICT (key)" in sql, (
        f"no conflict target on the {engine} upsert, so restamping inserts a "
        f"duplicate or raises: {sql}"
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_the_upsert_updates_rather_than_doing_nothing(engine: str) -> None:
    """`DO NOTHING` keeps one row and never refreshes the stamp.

    It is the mutation that slipped past a real database too, on the first
    attempt, because the row count is one either way and the timestamps were
    compared with `>=`. On the text there is nothing to slip past.
    """
    sql = store_hash_sql(engine)

    assert "DO UPDATE SET" in sql, f"the {engine} upsert does not update: {sql}"
    assert "DO NOTHING" not in sql, sql
    assert "value =" in sql and "updated_at =" in sql, (
        f"the {engine} upsert refreshes neither the value nor the timestamp: {sql}"
    )


# ---------------------------------------------------------------------------
# Asking whether the database has anything in it
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("engine", ENGINES)
def test_asking_for_user_tables_excludes_our_own(engine: str) -> None:
    """`_declaro_meta` is ours. A database holding only it is empty.

    Counting it would make every fresh database look populated, and
    `apply_migrations` reads that to decide whether it is bootstrapping.
    """
    sql = user_tables_sql(engine)

    assert META_TABLE in sql, (
        f"the {engine} query does not mention {META_TABLE}, so it cannot exclude it: {sql}"
    )


def test_sqlite_reads_the_catalogue_and_postgres_reads_its_own() -> None:
    """The one place the two genuinely differ in more than a placeholder."""
    assert "sqlite_master" in user_tables_sql("sqlite")
    assert "pg_tables" in user_tables_sql("postgresql")


def test_sqlite_excludes_its_internal_tables_too() -> None:
    """`sqlite_sequence` appears the moment a table uses AUTOINCREMENT."""
    assert "sqlite_%" in user_tables_sql("sqlite")


# ---------------------------------------------------------------------------
# The vocabulary is bounded
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("builder", [stored_hash_sql, store_hash_sql, user_tables_sql])
def test_an_unknown_engine_raises_and_names_the_ones_that_exist(builder) -> None:
    """A dispatch on a bounded set must not fall through.

    These were `if hasattr(conn, "fetch")` inside each function: one branch for
    asyncpg, one for everything else. A third engine took the SQLite arm
    silently, because "everything else" is not a decision.
    """
    with pytest.raises(ValueError, match="mysql"):
        builder("mysql")
