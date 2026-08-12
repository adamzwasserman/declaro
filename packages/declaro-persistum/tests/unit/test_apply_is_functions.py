"""Applying migrations is functions dispatched on dialect, against a real database.

`create_applier(dialect)` was the same shape as `create_inspector`: a dispatch
table over CLASSES that instantiated the winner on the way out. The classes
were stateless, and the SQL GENERATORS beside them were already module-level
functions — `generate_column_sql`, `generate_create_view`,
`generate_create_trigger` and the rest. Only `apply()` lived on the class, and
it did nothing a function could not: take a connection and a list of
operations, and run them.

So the split was already right and drawn in the wrong place. The pure half was
outside the class; the I/O half was inside it for no reason.

NO FAKES. A fake connection would record the SQL we sent and prove we sent
what we meant to send, which is what the generator tests already establish.
What is worth testing here is that a real engine ACCEPTS it — that
`CREATE TABLE` produces a table you can then introspect.

Round-tripping through `introspect` is deliberate: it asserts the two halves
agree. A test that only checked "no exception raised" would pass against an
applier that silently applied nothing.

`execution_order` is passed explicitly at every call. It is the differ's
topological sort — indices into `operations` — and it is required rather than
defaulted, because declaration order is not dependency order.
"""

from __future__ import annotations

import pytest

from declaro_persistum.applier import APPLIERS, apply
from declaro_persistum.inspector import introspect

DIALECTS = ["postgresql", "sqlite", "turso"]


@pytest.mark.parametrize("dialect", DIALECTS)
def test_every_dialect_has_an_apply_function(dialect):
    assert dialect in APPLIERS, f"no applier registered for {dialect}"
    assert callable(APPLIERS[dialect]), (
        f"{dialect} maps to {type(APPLIERS[dialect]).__name__}, not a function"
    )


def test_an_unknown_dialect_fails_loudly():
    with pytest.raises(ValueError, match="postgres"):
        APPLIERS["mysql"]


class TestAgainstARealSqliteDatabase:
    @pytest.mark.asyncio
    async def test_create_table_produces_a_table_you_can_read_back(self, tmp_path):
        """The round trip. Applier and inspector must agree."""
        import aiosqlite

        # The real Operation shape: `op`, `table`, `details`. My first version
        # of this test invented "type" and "definition"; the TypedDict in
        # types.py is the contract and the test follows it, not the reverse.
        operations = [
            {
                "op": "create_table",
                "table": "widgets",
                "details": {
                    "columns": {
                        "id": {"type": "integer", "primary_key": True},
                        "name": {"type": "text", "nullable": False},
                    }
                },
            }
        ]

        async with aiosqlite.connect(tmp_path / "t.db") as conn:
            await apply(conn, operations, [0], "sqlite")
            schema = await introspect(conn, "sqlite")

        assert "widgets" in schema, schema
        assert set(schema["widgets"]["columns"]) == {"id", "name"}
        assert schema["widgets"]["columns"]["name"]["nullable"] is False

    @pytest.mark.asyncio
    async def test_an_empty_operation_list_changes_nothing(self, tmp_path):
        """The boundary case. Applying nothing must not be an error."""
        import aiosqlite

        async with aiosqlite.connect(tmp_path / "t.db") as conn:
            await apply(conn, [], [], "sqlite")
            assert await introspect(conn, "sqlite") == {}

    @pytest.mark.asyncio
    async def test_drop_table_removes_it(self, tmp_path):
        import aiosqlite

        async with aiosqlite.connect(tmp_path / "t.db") as conn:
            await conn.execute("CREATE TABLE gone (id INTEGER PRIMARY KEY)")
            await conn.commit()

            await apply(conn, [{"op": "drop_table", "table": "gone", "details": {}}], [0], "sqlite")

            assert "gone" not in await introspect(conn, "sqlite")
