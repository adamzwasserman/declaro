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
from declaro_persistum.types import NOTHING

pytestmark = pytest.mark.turso


class TestALocalDatabase:
    @pytest.mark.asyncio
    async def test_it_is_not_replicated(self, tmp_path):
        db = await open_turso(str(tmp_path / "t.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING)
        assert is_replicated(db) is False
        assert db["primary"] is None

    @pytest.mark.asyncio
    async def test_it_takes_no_write_lock(self, tmp_path):
        """MVCC exists so writers run concurrently. A lock would throw that away."""
        db = await open_turso(str(tmp_path / "t.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING)
        assert db["serialise"] is None

    @pytest.mark.asyncio
    async def test_a_write_is_readable_afterwards(self, tmp_path):
        db = await open_turso(str(tmp_path / "t.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING)

        # DDL goes through `migrating`, which is WAL. Creating the table on an
        # MVCC connection makes it INVISIBLE to every later connection —
        # measured, and the reason `migrating` exists.
        async with migrating(db) as conn:
            await conn.execute("CREATE TABLE t (v TEXT)")
            await conn.commit()

        async with writing(db) as conn:
            await conn.execute("INSERT INTO t VALUES (?)", ("hello",))
            await conn.commit()

        async with reading(db) as conn:
            cur = await conn.execute("SELECT v FROM t")
            assert (await cur.fetchone())[0] == "hello"

    @pytest.mark.asyncio
    async def test_replicating_a_local_database_raises_rather_than_pretending(
        self, tmp_path
    ):
        """"True by vacuity" cannot be told apart from a replication that worked.

        This asserted `replicate(db) is True` for a database with no primary,
        and its own docstring conceded the value meant nothing: "True by
        vacuity, not by success". That is the failure mode this package keeps
        paying for — a call reporting success while doing nothing, which no
        caller can distinguish from one that did the job.

        A caller asking a database with no primary to reach its primary has
        made a mistake, and hearing it at the call site is the difference
        between learning it now and believing for months that replication was
        happening.
        """
        from declaro_persistum.database import flush, replicate

        db = await open_turso(str(tmp_path / "t.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING)

        with pytest.raises(ValueError, match="no primary"):
            await replicate(db)

        # flush stays quiet for a local database: nothing was promised, so
        # there is nothing to fail to deliver. Only the direct ask raises.
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
        db = await open_turso(str(tmp_path / "t.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING)
        async with migrating(db) as conn:
            await conn.execute("CREATE TABLE t (v INT)")
            await conn.commit()

        for i in range(5):
            async with writing(db) as conn:
                await conn.execute("INSERT INTO t VALUES (?)", (i,))
                await conn.commit()

        async with reading(db) as conn:
            cur = await conn.execute("SELECT count(*) FROM t")
            assert (await cur.fetchone())[0] == 5


class TestTheEngineChoiceIsNotTheCallers:
    def test_open_turso_takes_no_engine_argument(self):
        """`mvcc` and `journal_mode` were caller parameters, and `mvcc`
        defaulted to True — so omitting it on a replicated database selected
        the configuration that strands writes. 0.1.29 was yanked for it."""
        import inspect

        params = set(inspect.signature(open_turso).parameters)
        assert not params & {"mvcc", "journal_mode"}, params

    @pytest.mark.asyncio
    async def test_a_local_database_gets_no_serialisation(self, tmp_path):
        db = await open_turso(str(tmp_path / "t.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING)
        assert db["serialise"] is None, (
            "a local database serialised its writers, which throws away the "
            "concurrency MVCC is enabled for"
        )


class TestTheShutdownPolicyIsActedOn:
    """A policy that is stored and never read is a swallowed argument.

    `shutdown="replicate"` has to DO something at the point the database is
    opened. If a caller must also know to call `trap_shutdown` by hand, then
    the argument is decoration and the guarantee is still "whoever remembers" —
    the shape this package has already paid for more than once.
    """

    @pytest.mark.asyncio
    async def test_a_local_database_cannot_ask_to_replicate(self, tmp_path):
        """CHANGED 2026-08-17. This opened a LOCAL database asking to
        replicate and asserted the SIGTERM trap was installed.

        That open is refused now: a database with no primary has nowhere to
        replicate to, and the failure would otherwise arrive at SIGTERM. So
        the policy IS acted on at open, which is what this test was for.
        """
        with pytest.raises(ValueError, match="primary"):
            await open_turso(str(tmp_path / "t.db"), shutdown="replicate", primary=NOTHING, token=NOTHING)

    async def test_the_trap_is_installed_for_a_database_that_can_replicate(self):
        """The original intent, kept, without opening a replica.

        Opening one reaches the remote -- a fake primary answers `status=404,
        Host not found` -- so the trap is asserted against the record it reads
        rather than against a network I would have to stand up.
        """
        import signal

        from declaro_persistum.shutdown import trap_shutdown

        before = signal.getsignal(signal.SIGTERM)
        try:
            trapped = trap_shutdown({"shutdown": "replicate", "path": "x.db"})
            assert signal.getsignal(signal.SIGTERM) is not before, (
                'a database carrying shutdown="replicate" installed no '
                "handler; the policy would be stored and ignored"
            )
            # THE UNDO TRAVELS IN THE RECORD. It used to come back as a loose
            # callable, and both openers dropped it, so nothing ever removed
            # the handler.
            assert trapped["untrap"]() == 2, (
                "the database does not carry a working way to stop listening"
            )
        finally:
            signal.signal(signal.SIGTERM, before)

    async def test_no_trap_is_installed_when_the_policy_says_exit(self):
        """The floor. Always trapping would pass the test above."""
        import signal

        from declaro_persistum.shutdown import trap_shutdown

        before = signal.getsignal(signal.SIGTERM)
        try:
            trap_shutdown({"shutdown": "exit_immediately", "path": "x.db"})
            assert signal.getsignal(signal.SIGTERM) is before
        finally:
            signal.signal(signal.SIGTERM, before)

    @pytest.mark.asyncio
    async def test_exit_immediately_installs_nothing(self, tmp_path):
        import signal

        before = signal.getsignal(signal.SIGTERM)
        await open_turso(str(tmp_path / "t2.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING)
        assert signal.getsignal(signal.SIGTERM) is before, (
            "a database that asked to exit immediately installed a handler anyway"
        )
