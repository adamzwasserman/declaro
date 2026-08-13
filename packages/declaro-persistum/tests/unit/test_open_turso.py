"""Opening a Turso database, against a real engine.

`database.py` defines the `Database` shape and the functions over it, but
every callable in it is injected — `connect`, `close_connection`,
`replicate_once`, `refresh_once`, `release`, `sleep`. Nothing built one, so
`replication.py` sat orphaned: 279 lines of push-and-backoff logic with no
caller to inject it into.

`open_turso` is that factory. It is the only place that knows pyturso exists.

THE ENGINE CHOICE IS MADE HERE AND NOWHERE ELSE, from one fact:

    primary set  ->  replicated  ->  WAL,  writers serialised
    no primary   ->  local       ->  MVCC, writers concurrent

Measured 2026-08-12 against a real replica: under MVCC on a replicated
database, once writer zero has written anything no second connection can open
("database tape error: database is busy", 3 of 3 preludes, 15 retries over
35s). Under WAL many connections open but the engine rejects the second
WRITER — 8 open, no lock, 1 of 8 writes landed. So neither mode gives a
replicated database concurrent writers, and WAL is the one that fails safely.

DDL GOES THROUGH `migrating`, WHICH IS ALWAYS WAL. Measured 2026-08-12: a
table created on an MVCC connection is invisible to every other connection
("Parse error: no such table"), while its INSERTs cross perfectly once the
schema exists. The blind spot is DDL only, and `migrating` is the answer to
it.

No fakes. These run against real local Turso files.
"""

from __future__ import annotations

import pytest

from declaro_persistum.database import is_replicated, reading, writing
from declaro_persistum.turso_database import migrating, open_turso

pytestmark = pytest.mark.turso


class TestALocalDatabase:
    @pytest.mark.asyncio
    async def test_it_is_not_replicated(self, tmp_path):
        db = await open_turso(str(tmp_path / "t.db"))
        assert is_replicated(db) is False
        assert db["primary"] is None

    @pytest.mark.asyncio
    async def test_it_takes_no_write_lock(self, tmp_path):
        """MVCC exists so writers run concurrently. A lock would throw that away."""
        db = await open_turso(str(tmp_path / "t.db"))
        assert db["serialise"] is None

    @pytest.mark.asyncio
    async def test_a_write_is_readable_afterwards(self, tmp_path):
        db = await open_turso(str(tmp_path / "t.db"))

        # DDL goes through `migrating`, which is WAL. Creating the table on an
        # MVCC connection makes it INVISIBLE to every later connection —
        # measured, and the reason `migrating` exists.
        conn = await migrating(db)
        await conn.execute("CREATE TABLE t (v TEXT)")
        await conn.commit()
        await conn.close()

        async with writing(db) as conn:
            await conn.execute("INSERT INTO t VALUES (?)", ("hello",))
            await conn.commit()

        async with reading(db) as conn:
            cur = await conn.execute("SELECT v FROM t")
            assert (await cur.fetchone())[0] == "hello"

    @pytest.mark.asyncio
    async def test_replicating_a_local_database_is_a_no_op(self, tmp_path):
        """No primary means nothing to send. True by vacuity, not by success."""
        from declaro_persistum.database import flush, replicate

        db = await open_turso(str(tmp_path / "t.db"))
        assert await replicate(db) is True
        await flush(db)  # must not hang or raise

    @pytest.mark.asyncio
    async def test_writing_is_the_single_writer_door(self, tmp_path):
        """`writing(db)` opens a connection per block. That is CORRECT for one
        writer and is NOT the concurrent path.

        Concurrency comes from the crew, where each drainer holds one
        connection for its whole life — see `crew.py` and `test_crew.py`.
        Calling `writing` in a loop is connection-per-write, which this
        package has arrived at three separate times and which throws away the
        larger of the two throughput levers.

        Sequential writes through `writing` all land, which is what this
        asserts. It deliberately does not assert anything about twenty of them
        at once: that is the crew's job, and a laptop cannot say what a server
        would do anyway.
        """
        db = await open_turso(str(tmp_path / "t.db"))
        conn = await migrating(db)
        await conn.execute("CREATE TABLE t (v INT)")
        await conn.commit()
        await conn.close()

        for i in range(5):
            async with writing(db) as conn:
                await conn.execute("INSERT INTO t VALUES (?)", (i,))
                await conn.commit()

        async with reading(db) as conn:
            cur = await conn.execute("SELECT count(*) FROM t")
            assert (await cur.fetchone())[0] == 5


class TestTheEngineChoiceIsNotTheCallers:
    def test_open_turso_takes_no_engine_argument(self):
        """`mvcc` and `pooled_writes` were caller parameters, and `mvcc`
        defaulted to True — so omitting it on a replicated database selected
        the configuration that strands writes. 0.1.29 was yanked for it."""
        import inspect

        params = set(inspect.signature(open_turso).parameters)
        assert not params & {"mvcc", "pooled_writes", "journal_mode"}, params

    @pytest.mark.asyncio
    async def test_a_local_database_gets_no_serialisation(self, tmp_path):
        db = await open_turso(str(tmp_path / "t.db"))
        assert db["serialise"] is None, (
            "a local database serialised its writers, which throws away the "
            "concurrency MVCC is enabled for"
        )
