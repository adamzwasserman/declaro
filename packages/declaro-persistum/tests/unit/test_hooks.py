"""
Tests for query hooks — pre/post function injection.

Unit tests rely on pure-function assertions. Integration tests use real
in-memory SQLite to exercise the end-to-end hook flow.

Hook design:
    pre_hook:  (query_object) -> query_object      # runs before SQL is built
    post_hook: (rows, QueryMeta) -> rows           # runs after DB returns
"""

from typing import Any

import pytest

from declaro_persistum import PostHook, PreHook, QueryMeta, table_factory
from declaro_persistum.pool import ConnectionPool
from declaro_persistum.query.delete import DeleteQuery
from declaro_persistum.query.insert import InsertQuery
from declaro_persistum.query.select import SelectQuery
from declaro_persistum.query.table import TableProxy, table
from declaro_persistum.query.update import UpdateQuery


# ---------------------------------------------------------------------------
# Minimal schema used across tests (no DB needed to construct/inspect queries)
# ---------------------------------------------------------------------------

SCHEMA = {
    "items": {
        "columns": {
            "id": {"type": "integer", "primary_key": True, "nullable": False},
            "name": {"type": "text", "nullable": False},
            "owner": {"type": "text", "nullable": True},
            "deleted_at": {"type": "text", "nullable": True},
        },
    },
}


# ---------------------------------------------------------------------------
# Pure tests — no DB, no pool required
# ---------------------------------------------------------------------------


class TestFactory:
    """table_factory returns a closure producing hook-wired TableProxy instances."""

    def test_factory_returns_callable(self):
        get_table = table_factory(SCHEMA, pool=None)
        assert callable(get_table)

    def test_factory_produces_table_proxy(self):
        get_table = table_factory(SCHEMA, pool=None)
        items = get_table("items")
        assert isinstance(items, TableProxy)

    def test_factory_wires_pre_hook_onto_proxy(self):
        def pre(q):
            return q

        get_table = table_factory(SCHEMA, pool=None, pre=pre)
        items = get_table("items")
        assert items._pre is pre

    def test_factory_wires_post_hook_onto_proxy(self):
        def post(rows, meta):
            return rows

        get_table = table_factory(SCHEMA, pool=None, post=post)
        items = get_table("items")
        assert items._post is post

    def test_plain_table_also_accepts_hooks(self):
        def pre(q):
            return q

        items = table("items", SCHEMA, pool=None, pre=pre)
        assert items._pre is pre


class TestHookPropagation:
    """Hooks flow from TableProxy → query builders → chained queries."""

    def test_pre_propagates_to_select(self):
        def pre(q):
            return q

        items = table("items", SCHEMA, pool=None, pre=pre)
        q = items.select()
        assert q._pre is pre
        assert q._OPERATION == "select"

    def test_post_propagates_to_select(self):
        def post(rows, meta):
            return rows

        items = table("items", SCHEMA, pool=None, post=post)
        q = items.select()
        assert q._post is post

    def test_pre_propagates_through_select_chain(self):
        """A SelectQuery rebuilds itself on every chain method — hooks must survive."""

        def pre(q):
            return q

        items = table("items", SCHEMA, pool=None, pre=pre)
        q = items.select(items.id).where(items.id == 1).limit(10).offset(5)
        assert q._pre is pre

    def test_pre_propagates_to_insert(self):
        def pre(q):
            return q

        items = table("items", SCHEMA, pool=None, pre=pre)
        q = items.insert(name="x")
        assert q._pre is pre
        assert q._OPERATION == "insert"

    def test_pre_propagates_to_update(self):
        def pre(q):
            return q

        items = table("items", SCHEMA, pool=None, pre=pre)
        q = items.update(name="x").where(items.id == 1)
        assert q._pre is pre
        assert q._OPERATION == "update"

    def test_pre_propagates_to_delete(self):
        def pre(q):
            return q

        items = table("items", SCHEMA, pool=None, pre=pre)
        q = items.delete().where(items.id == 1)
        assert q._pre is pre
        assert q._OPERATION == "delete"

    def test_alias_preserves_hooks(self):
        def pre(q):
            return q

        items = table("items", SCHEMA, pool=None, pre=pre)
        aliased = items.alias("i2")
        assert aliased._pre is pre

    def test_objects_carries_hooks(self):
        def pre(q):
            return q

        items = table("items", SCHEMA, pool=None, pre=pre)
        qs = items.objects
        assert qs._pre is pre

    def test_prisma_carries_hooks(self):
        def pre(q):
            return q

        items = table("items", SCHEMA, pool=None, pre=pre)
        prisma = items.prisma
        assert prisma._pre is pre


class TestPreHookBuildsWhere:
    """A pre-hook that adds a WHERE clause emits the injected condition in SQL."""

    def test_pre_hook_can_add_where_clause(self):
        def inject_owner(q):
            if isinstance(q, SelectQuery):
                items_proxy = table("items", SCHEMA, pool=None)
                return q.where(items_proxy.owner == "alice")
            return q

        items = table("items", SCHEMA, pool=None, pre=inject_owner)
        q = items.select(items.id)
        transformed = inject_owner(q)
        sql, params = transformed.to_sql("sqlite")

        assert "owner" in sql
        assert "alice" in params.values()


class TestDeleteToUpdateRewrite:
    """Pre-hook can structurally rewrite DELETE into UPDATE (soft delete)."""

    def test_delete_hook_returns_update_query(self):
        def soft_delete(q):
            if isinstance(q, DeleteQuery):
                items_proxy = table("items", SCHEMA, pool=None)
                return items_proxy.update(deleted_at="now").where(
                    q._where if q._where else items_proxy.id == items_proxy.id
                )
            return q

        items = table("items", SCHEMA, pool=None, pre=soft_delete)
        delete_q = items.delete().where(items.id == 1)

        rewritten = soft_delete(delete_q)
        assert isinstance(rewritten, UpdateQuery)

        sql, _ = rewritten.to_sql("sqlite")
        assert sql.startswith("UPDATE")
        assert "deleted_at" in sql


# ---------------------------------------------------------------------------
# Execution tests — real SQLite pool, no mocks
# ---------------------------------------------------------------------------


@pytest.fixture
async def sqlite_pool(tmp_path):
    """Real file-backed SQLite pool with a seeded items table.

    File-backed (not ``:memory:``) so the schema is visible across the
    multiple short-lived connections the pool hands out per execute.
    """
    db_path = str(tmp_path / "hooks_test.db")
    pool = await ConnectionPool.sqlite(db_path)
    async with pool.acquire() as conn:
        await conn.execute(
            "CREATE TABLE items ("
            "  id INTEGER PRIMARY KEY,"
            "  name TEXT NOT NULL,"
            "  owner TEXT,"
            "  deleted_at TEXT"
            ")"
        )
        await conn.execute(
            "INSERT INTO items (id, name, owner) VALUES "
            "(1, 'a', 'alice'),"
            "(2, 'b', 'bob'),"
            "(3, 'c', 'alice')"
        )
        await conn.commit()
    yield pool
    await pool.close()


class TestPreHookExecute:
    """Pre-hook actually runs during .execute()."""

    @pytest.mark.asyncio
    async def test_pre_hook_restricts_rows(self, sqlite_pool):
        def only_alice(q):
            if isinstance(q, SelectQuery):
                proxy = table("items", SCHEMA, pool=sqlite_pool)
                return q.where(proxy.owner == "alice")
            return q

        items = table("items", SCHEMA, pool=sqlite_pool, pre=only_alice)
        rows = await items.select().execute()

        owners = {r["owner"] for r in rows}
        assert owners == {"alice"}
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_execute_level_pre_replaces_factory_pre(self, sqlite_pool):
        def always_bob(q):
            if isinstance(q, SelectQuery):
                proxy = table("items", SCHEMA, pool=sqlite_pool)
                return q.where(proxy.owner == "bob")
            return q

        def only_alice(q):
            if isinstance(q, SelectQuery):
                proxy = table("items", SCHEMA, pool=sqlite_pool)
                return q.where(proxy.owner == "alice")
            return q

        items = table("items", SCHEMA, pool=sqlite_pool, pre=always_bob)
        rows = await items.select().execute(pre=only_alice)

        owners = {r["owner"] for r in rows}
        assert owners == {"alice"}  # execute-level pre won

    @pytest.mark.asyncio
    async def test_without_hooks_bypasses_pre(self, sqlite_pool):
        def only_alice(q):
            if isinstance(q, SelectQuery):
                proxy = table("items", SCHEMA, pool=sqlite_pool)
                return q.where(proxy.owner == "alice")
            return q

        items = table("items", SCHEMA, pool=sqlite_pool, pre=only_alice)
        rows = await items.select().execute(without_hooks=True)

        owners = {r["owner"] for r in rows}
        assert owners == {"alice", "bob"}  # bypass returned full set
        assert len(rows) == 3


class TestPostHookExecute:
    """Post-hook actually runs, receives list[dict] and QueryMeta."""

    @pytest.mark.asyncio
    async def test_post_hook_filters_rows(self, sqlite_pool):
        def only_alice(rows, meta):
            return [r for r in rows if r["owner"] == "alice"]

        items = table("items", SCHEMA, pool=sqlite_pool, post=only_alice)
        rows = await items.select().execute()

        owners = {r["owner"] for r in rows}
        assert owners == {"alice"}

    @pytest.mark.asyncio
    async def test_post_hook_receives_list_dict_on_execute_one(self, sqlite_pool):
        """Post-hook always sees list[dict]. Mode reduction runs after."""
        captured: list[list[dict[str, Any]]] = []

        def capture(rows, meta):
            captured.append(rows)
            return rows

        items = table("items", SCHEMA, pool=sqlite_pool, post=capture)
        _ = await items.select().execute_one()

        assert len(captured) == 1
        assert isinstance(captured[0], list)
        assert all(isinstance(r, dict) for r in captured[0])

    @pytest.mark.asyncio
    async def test_post_hook_meta_reflects_executed_sql(self, sqlite_pool):
        """QueryMeta.sql/params reflect the SQL that actually ran (post-pre-hook)."""
        captured: list[QueryMeta] = []

        def capture(rows, meta):
            captured.append(meta)
            return rows

        def pre_add_where(q):
            if isinstance(q, SelectQuery):
                proxy = table("items", SCHEMA, pool=sqlite_pool)
                return q.where(proxy.owner == "alice")
            return q

        items = table("items", SCHEMA, pool=sqlite_pool, pre=pre_add_where, post=capture)
        _ = await items.select().execute()

        assert len(captured) == 1
        meta = captured[0]
        assert meta["operation"] == "select"
        assert meta["table"] == "items"
        assert "owner" in meta["sql"]  # pre-hook's WHERE is visible
        assert "alice" in meta["params"].values()

    @pytest.mark.asyncio
    async def test_post_hook_filtering_affects_execute_one(self, sqlite_pool):
        """A post-hook that removes all rows makes .execute_one() return None."""

        def drop_all(rows, meta):
            return []

        items = table("items", SCHEMA, pool=sqlite_pool, post=drop_all)
        result = await items.select().where(items.id == 1).execute_one()

        assert result is None


class TestPreHookReturnsNone:
    """Pre-hook returning None is a hard error — no silent no-op."""

    @pytest.mark.asyncio
    async def test_pre_none_raises_typeerror(self, sqlite_pool):
        def buggy(q):
            return None

        items = table("items", SCHEMA, pool=sqlite_pool, pre=buggy)
        with pytest.raises(TypeError, match="pre-hook returned None"):
            await items.select().execute()
