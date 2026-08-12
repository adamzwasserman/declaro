"""The SQLAlchemy compatibility shim: Session-like surface over the pool.

`compat/sqlalchemy_shim.py` carried 10 branches at 0% coverage — part of
the Slop Audit L1.19 gap (declaro-xu0).

The shim exists so code written against SQLAlchemy runs on a
declaro-persistum pool with minimal edits. Two of its behaviours are the
ones a caller can actually trip over, and both get tests:

- `SessionLocal()` refuses to construct before `configure_database`,
  rather than failing later with an obscure NoneType error.
- `execute()` outside the context manager raises rather than silently
  doing nothing on a connection that was never acquired.

`commit` and `rollback` are deliberate no-ops — the pool owns transaction
boundaries. That is asserted, so nobody "fixes" them into real calls
without meeting a failing test first.

The module holds a global `_pool`, so every test restores it.
"""

import pytest

from declaro_persistum.compat import sqlalchemy_shim as shim
from declaro_persistum.compat.sqlalchemy_shim import (
    Base,
    SessionLocal,
    configure_database,
    get_db,
)


@pytest.fixture(autouse=True)
def _restore_global_pool():
    before = shim._pool
    yield
    shim._pool = before


class _FakeConn:
    def __init__(self):
        self.executed: list[tuple] = []

    async def execute(self, query, params=None):
        self.executed.append((query, params))
        return "result"


class _FakePool:
    def __init__(self):
        self.conn = _FakeConn()
        self.released: list = []

    async def acquire(self):
        return self.conn

    async def release(self, conn):
        self.released.append(conn)


class TestConfigureDatabase:
    def test_a_session_before_configuration_refuses_to_construct(self):
        shim._pool = None
        with pytest.raises(RuntimeError, match="Database not configured"):
            SessionLocal()

    def test_configuring_lets_a_session_be_built(self):
        configure_database(_FakePool())
        assert SessionLocal() is not None

    def test_configuring_again_replaces_the_pool(self):
        first, second = _FakePool(), _FakePool()
        configure_database(first)
        configure_database(second)
        assert SessionLocal()._pool is second


class TestDeclarativeBase:
    def test_a_model_with_a_tablename_is_registered(self):
        class Widget(Base):
            __tablename__ = "widgets"

        assert type(Base)._registry["widgets"] is Widget

    def test_base_itself_is_not_registered(self):
        assert "Base" not in type(Base)._registry

    def test_a_subclass_without_a_tablename_is_not_registered(self):
        before = dict(type(Base)._registry)

        class Anonymous(Base):
            pass

        assert type(Base)._registry == before

    def test_keyword_arguments_become_attributes(self):
        class Widget(Base):
            __tablename__ = "widgets_kwargs"

        assert Widget(name="bolt", size=3).name == "bolt"

    def test_no_arguments_constructs_an_empty_instance(self):
        class Widget(Base):
            __tablename__ = "widgets_empty"

        assert Widget() is not None

    def test_to_dict_returns_the_assigned_attributes(self):
        class Widget(Base):
            __tablename__ = "widgets_dict"

        assert Widget(name="bolt", size=3).to_dict() == {"name": "bolt", "size": 3}

    def test_to_dict_omits_private_attributes(self):
        class Widget(Base):
            __tablename__ = "widgets_private"

        assert "_secret" not in Widget(name="a", _secret="s").to_dict()

    def test_to_dict_omits_methods(self):
        class Widget(Base):
            __tablename__ = "widgets_methods"

        assert "to_dict" not in Widget(name="a").to_dict()


class TestSessionLocal:
    @pytest.mark.asyncio
    async def test_entering_acquires_a_connection(self):
        pool = _FakePool()
        configure_database(pool)
        async with SessionLocal() as session:
            assert session.connection is pool.conn

    @pytest.mark.asyncio
    async def test_leaving_releases_the_connection(self):
        pool = _FakePool()
        configure_database(pool)
        async with SessionLocal():
            pass
        assert pool.released == [pool.conn]

    @pytest.mark.asyncio
    async def test_the_connection_is_cleared_on_exit(self):
        configure_database(_FakePool())
        session = SessionLocal()
        async with session:
            pass
        assert session.connection is None

    @pytest.mark.asyncio
    async def test_the_connection_is_released_even_when_the_body_raises(self):
        pool = _FakePool()
        configure_database(pool)
        with pytest.raises(ValueError):
            async with SessionLocal():
                raise ValueError("boom")
        assert pool.released == [pool.conn]

    @pytest.mark.asyncio
    async def test_exiting_twice_does_not_release_twice(self):
        pool = _FakePool()
        configure_database(pool)
        session = SessionLocal()
        async with session:
            pass
        await session.__aexit__(None, None, None)
        assert pool.released == [pool.conn]

    def test_the_connection_is_none_before_entering(self):
        configure_database(_FakePool())
        assert SessionLocal().connection is None

    @pytest.mark.asyncio
    async def test_execute_outside_the_context_manager_raises(self):
        """Better than executing against a connection nobody acquired."""
        configure_database(_FakePool())
        with pytest.raises(RuntimeError, match="Session not active"):
            await SessionLocal().execute("SELECT 1")

    @pytest.mark.asyncio
    async def test_execute_passes_the_query_and_params_through(self):
        pool = _FakePool()
        configure_database(pool)
        async with SessionLocal() as session:
            assert await session.execute("SELECT :x", {"x": 1}) == "result"
        assert pool.conn.executed == [("SELECT :x", {"x": 1})]

    @pytest.mark.asyncio
    async def test_omitted_params_become_an_empty_dict(self):
        pool = _FakePool()
        configure_database(pool)
        async with SessionLocal() as session:
            await session.execute("SELECT 1")
        assert pool.conn.executed == [("SELECT 1", {})]

    @pytest.mark.asyncio
    async def test_commit_and_rollback_are_deliberate_no_ops(self):
        """The pool owns transaction boundaries. Do not 'fix' these without
        a failing test that says what they should do instead."""
        pool = _FakePool()
        configure_database(pool)
        async with SessionLocal() as session:
            assert await session.commit() is None
            assert await session.rollback() is None
        assert pool.conn.executed == []


class TestGetDb:
    @pytest.mark.asyncio
    async def test_it_yields_an_entered_session(self):
        pool = _FakePool()
        configure_database(pool)
        async with get_db() as session:
            assert isinstance(session, SessionLocal)
            assert session.connection is pool.conn

    @pytest.mark.asyncio
    async def test_the_connection_is_released_when_the_dependency_finishes(self):
        pool = _FakePool()
        configure_database(pool)
        async with get_db():
            pass
        assert pool.released == [pool.conn]

    @pytest.mark.asyncio
    async def test_an_unconfigured_pool_raises_on_entry(self):
        shim._pool = None
        with pytest.raises(RuntimeError, match="Database not configured"):
            async with get_db():
                pass
