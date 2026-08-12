"""Introspection is functions dispatched on dialect, against a real database.

`create_inspector(dialect)` was ALREADY a dispatch table. It just dispatched
on classes:

    INSPECTORS = {"postgresql": PostgreSQLInspector,
                  "sqlite": SQLiteInspector,
                  "turso": TursoInspector}
    return INSPECTORS[dialect]()

Every one of those classes was stateless — no `__init__`, no fields, methods
that took a connection and returned data. `get_dialect()` returned a string
literal. The dispatch was correct all along; the classes were a wrapper around
the functions the dispatch should have held directly.

So this is not a redesign. It is deleting the wrapper and letting the table
name functions.

NO FAKES. Every case here runs against a real SQLite database created in the
test. A fake connection returning canned PRAGMA rows would prove that the
parsing works on rows we invented, which is the one thing already covered by
`inspector/shared.py`'s pure functions. What is worth testing here is that the
SQL we send to a real engine comes back the shape we expect.

The dialect vocabulary is bounded — postgresql, sqlite, turso — so every
member is asserted present rather than one being sampled.
"""

from __future__ import annotations

import pytest

from declaro_persistum.inspector import INSPECTORS, introspect

DIALECTS = ["postgresql", "sqlite", "turso"]


@pytest.mark.parametrize("dialect", DIALECTS)
def test_every_dialect_has_an_inspect_function(dialect):
    """The bounded vocabulary, each member present and callable."""
    assert dialect in INSPECTORS, f"no inspector registered for {dialect}"
    assert callable(INSPECTORS[dialect]), (
        f"{dialect} maps to {type(INSPECTORS[dialect]).__name__}, not a function — "
        f"the table used to hold classes and instantiate them on the way out"
    )


def test_an_unknown_dialect_fails_loudly():
    """A dict lookup on a bounded vocabulary must not fall through."""
    with pytest.raises(ValueError, match="postgres"):
        INSPECTORS["mysql"]


class TestAgainstARealSqliteDatabase:
    """Real engine, real PRAGMAs, no canned rows."""

    @pytest.mark.asyncio
    async def test_it_finds_a_table_and_its_columns(self, tmp_path):
        import aiosqlite

        async with aiosqlite.connect(tmp_path / "t.db") as conn:
            await conn.execute(
                "CREATE TABLE users ("
                "  id INTEGER PRIMARY KEY,"
                "  email TEXT NOT NULL,"
                "  age INTEGER"
                ")"
            )
            await conn.commit()

            schema = await introspect(conn, "sqlite")

        assert "users" in schema, schema
        columns = schema["users"]["columns"]
        assert set(columns) == {"id", "email", "age"}

        # `nullable` is written ONLY when False. A nullable column omits the
        # key entirely, so `"nullable" not in col` is the assertion, not
        # `col["nullable"] is True`. That is the library's existing contract
        # (inspector/shared.py::columns_from_pragma_rows) and this test asserts
        # it rather than a shape I would have preferred.
        assert columns["email"]["nullable"] is False
        assert "nullable" not in columns["age"]

        # A PRIMARY KEY is implicitly NOT NULL in SQL even though
        # PRAGMA table_info reports notnull=0 for it.
        assert columns["id"]["nullable"] is False
        assert columns["id"]["primary_key"] is True

    @pytest.mark.asyncio
    async def test_it_skips_sqlite_and_declaro_internal_tables(self, tmp_path):
        """Internal bookkeeping is not part of the user's schema.

        `sqlite_%` is the engine's and `_declaro_%` is ours. A schema that
        included either would make the differ propose dropping them.
        """
        import aiosqlite

        async with aiosqlite.connect(tmp_path / "t.db") as conn:
            await conn.execute("CREATE TABLE real_one (id INTEGER PRIMARY KEY)")
            await conn.execute("CREATE TABLE _declaro_meta (k TEXT)")
            await conn.commit()

            schema = await introspect(conn, "sqlite")

        assert set(schema) == {"real_one"}, (
            f"introspection returned internal tables: {sorted(schema)}"
        )

    @pytest.mark.asyncio
    async def test_an_empty_database_is_an_empty_schema(self, tmp_path):
        """The boundary case, asserted rather than assumed."""
        import aiosqlite

        async with aiosqlite.connect(tmp_path / "t.db") as conn:
            assert await introspect(conn, "sqlite") == {}

    @pytest.mark.asyncio
    async def test_it_reads_indexes(self, tmp_path):
        import aiosqlite

        async with aiosqlite.connect(tmp_path / "t.db") as conn:
            await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, k TEXT)")
            await conn.execute("CREATE INDEX idx_k ON t(k)")
            await conn.commit()

            schema = await introspect(conn, "sqlite")

        # `indexes` is a dict keyed by index name, not a list of records.
        indexes = schema["t"]["indexes"]
        assert "idx_k" in indexes, indexes
        assert indexes["idx_k"]["columns"] == ["k"]
