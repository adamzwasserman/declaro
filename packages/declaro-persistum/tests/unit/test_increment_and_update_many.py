"""
Tests for atomic ``increment(...)`` and Prisma ``update_many``.

Pure SQL-emission tests assert the shape of the UPDATE statement. Integration
tests use a real SQLite pool — no mocks — to verify the operation is
genuinely atomic at the storage layer and that the returned row count
reflects rows actually modified.
"""

from typing import Any

import pytest

from declaro_persistum import increment, Increment, table_factory
from declaro_persistum.pool import ConnectionPool
from declaro_persistum.query.table import table
from declaro_persistum.query.update import UpdateQuery


SCHEMA: dict[str, Any] = {
    "tags": {
        "columns": {
            "tag_id": {"type": "text", "primary_key": True, "nullable": False},
            "card_count": {"type": "integer", "nullable": False},
            "last_touched": {"type": "text", "nullable": True},
        },
    },
}


# ---------------------------------------------------------------------------
# Pure SQL-emission tests
# ---------------------------------------------------------------------------


class TestIncrementSQL:
    def test_positive_delta_emits_col_plus_param(self):
        tags = table("tags", SCHEMA, pool=None)
        uq = UpdateQuery(
            "tags",
            SCHEMA,
            {"card_count": increment(1)},
            tags._columns,
        )
        sql, params = uq.to_sql("sqlite")
        assert "card_count = card_count + :inc_card_count" in sql
        assert params == {"inc_card_count": 1}

    def test_negative_delta_binds_signed_value(self):
        """Negative delta works without special-casing subtraction —
        the SQL stays ``col + :param`` with the negative value bound."""
        tags = table("tags", SCHEMA, pool=None)
        uq = UpdateQuery(
            "tags",
            SCHEMA,
            {"card_count": increment(-1)},
            tags._columns,
        )
        sql, params = uq.to_sql("sqlite")
        assert "card_count = card_count + :inc_card_count" in sql
        assert params == {"inc_card_count": -1}

    def test_increment_composes_with_data(self):
        """data= and increment= both appear in the same SET clause."""
        tags = table("tags", SCHEMA, pool=None)
        uq = UpdateQuery(
            "tags",
            SCHEMA,
            {"card_count": increment(1), "last_touched": "2026-05-13"},
            tags._columns,
        )
        sql, params = uq.to_sql("sqlite")
        assert "card_count = card_count + :inc_card_count" in sql
        assert "last_touched = :upd_last_touched" in sql
        assert params == {
            "inc_card_count": 1,
            "upd_last_touched": "2026-05-13",
        }

    def test_increment_factory_returns_increment_instance(self):
        marker = increment(5)
        assert isinstance(marker, Increment)
        assert marker.delta == 5


# ---------------------------------------------------------------------------
# Integration tests against real SQLite — atomic semantics + row count
# ---------------------------------------------------------------------------


@pytest.fixture
async def tags_pool(tmp_path):
    """SQLite pool with a tags table seeded with three rows."""
    db_path = str(tmp_path / "increment_test.db")
    pool = await ConnectionPool.sqlite(db_path)
    async with pool.acquire() as conn:
        await conn.execute(
            "CREATE TABLE tags ("
            "  tag_id TEXT PRIMARY KEY,"
            "  card_count INTEGER NOT NULL,"
            "  last_touched TEXT"
            ")"
        )
        await conn.execute(
            "INSERT INTO tags (tag_id, card_count) VALUES "
            "('a', 0), ('b', 5), ('c', 10)"
        )
        await conn.commit()
    yield pool
    await pool.close()


class TestSingleRowIncrement:
    @pytest.mark.asyncio
    async def test_update_one_increment_increments_atomically(self, tags_pool):
        """update_one(increment={...}) modifies the row without an explicit read."""
        tags = table("tags", SCHEMA, pool=tags_pool)
        await tags.update_one(
            where={"tag_id": "a"},
            increment={"card_count": 1},
        )
        row = await tags.find_one(where={"tag_id": "a"})
        assert row["card_count"] == 1

    @pytest.mark.asyncio
    async def test_update_one_increment_negative(self, tags_pool):
        tags = table("tags", SCHEMA, pool=tags_pool)
        await tags.update_one(
            where={"tag_id": "b"},
            increment={"card_count": -2},
        )
        row = await tags.find_one(where={"tag_id": "b"})
        assert row["card_count"] == 3

    @pytest.mark.asyncio
    async def test_update_one_combines_data_and_increment(self, tags_pool):
        tags = table("tags", SCHEMA, pool=tags_pool)
        await tags.update_one(
            where={"tag_id": "c"},
            data={"last_touched": "2026-05-13"},
            increment={"card_count": 5},
        )
        row = await tags.find_one(where={"tag_id": "c"})
        assert row["card_count"] == 15
        assert row["last_touched"] == "2026-05-13"


class TestUpdateMany:
    @pytest.mark.asyncio
    async def test_update_many_applies_increment_to_in_clause(self, tags_pool):
        tags = table("tags", SCHEMA, pool=tags_pool)
        count = await tags.update_many(
            where={"tag_id": {"in": ["a", "b"]}},
            increment={"card_count": 100},
        )
        assert count == 2
        rows = sorted(
            await tags.find_many(where={"tag_id": {"in": ["a", "b", "c"]}}),
            key=lambda r: r["tag_id"],
        )
        # a: 0 + 100, b: 5 + 100, c: untouched 10
        assert [r["card_count"] for r in rows] == [100, 105, 10]

    @pytest.mark.asyncio
    async def test_update_many_returns_zero_for_no_matches(self, tags_pool):
        tags = table("tags", SCHEMA, pool=tags_pool)
        count = await tags.update_many(
            where={"tag_id": {"in": ["does_not_exist"]}},
            increment={"card_count": 1},
        )
        assert count == 0

    @pytest.mark.asyncio
    async def test_update_many_with_data_only(self, tags_pool):
        tags = table("tags", SCHEMA, pool=tags_pool)
        count = await tags.update_many(
            where={"tag_id": {"in": ["a", "b", "c"]}},
            data={"last_touched": "now"},
        )
        assert count == 3
        rows = await tags.find_many(where={"tag_id": {"in": ["a", "b", "c"]}})
        assert all(r["last_touched"] == "now" for r in rows)


class TestErrorCases:
    @pytest.mark.asyncio
    async def test_update_requires_data_or_increment(self, tags_pool):
        tags = table("tags", SCHEMA, pool=tags_pool)
        with pytest.raises(ValueError, match="requires data= or increment="):
            await tags.update_one(where={"tag_id": "a"})

    @pytest.mark.asyncio
    async def test_update_many_requires_data_or_increment(self, tags_pool):
        tags = table("tags", SCHEMA, pool=tags_pool)
        with pytest.raises(ValueError, match="requires data= or increment="):
            await tags.update_many(where={"tag_id": {"in": ["a"]}})

    @pytest.mark.asyncio
    async def test_column_in_both_data_and_increment_rejected(self, tags_pool):
        tags = table("tags", SCHEMA, pool=tags_pool)
        with pytest.raises(ValueError, match="appears in both data and increment"):
            await tags.update_one(
                where={"tag_id": "a"},
                data={"card_count": 0},
                increment={"card_count": 1},
            )

    @pytest.mark.asyncio
    async def test_unknown_column_in_increment_is_caught(self, tags_pool):
        tags = table("tags", SCHEMA, pool=tags_pool)
        with pytest.raises(AttributeError, match="has no column 'nonexistent'"):
            await tags.update_one(
                where={"tag_id": "a"},
                increment={"nonexistent": 1},
            )


class TestHookIntegration:
    """Hooks fire on update_one / update_many like any other write."""

    @pytest.mark.asyncio
    async def test_post_hook_sees_returned_rows_for_update_many(self, tags_pool):
        captured: list[list[dict[str, Any]]] = []

        def post(rows, meta):
            captured.append(rows)
            return rows

        get_table = table_factory(SCHEMA, tags_pool, post=post)
        tags = get_table("tags")
        count = await tags.update_many(
            where={"tag_id": {"in": ["a", "b"]}},
            increment={"card_count": 1},
        )
        assert count == 2
        assert len(captured) == 1
        assert len(captured[0]) == 2  # post-hook saw the two RETURNING rows

    @pytest.mark.asyncio
    async def test_post_hook_filtering_affects_returned_count(self, tags_pool):
        """If a post-hook drops rows, update_many's returned count reflects
        what the hook actually let through — consistent with the documented
        post-hook semantics."""

        def post(rows, meta):
            return [r for r in rows if r["tag_id"] == "a"]

        get_table = table_factory(SCHEMA, tags_pool, post=post)
        tags = get_table("tags")
        count = await tags.update_many(
            where={"tag_id": {"in": ["a", "b"]}},
            increment={"card_count": 1},
        )
        # DB modified 2 rows; post-hook kept 1; returned count is 1.
        assert count == 1
