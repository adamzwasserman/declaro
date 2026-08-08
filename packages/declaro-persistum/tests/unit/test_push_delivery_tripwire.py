"""The push delivery tripwire must actually read the sync revision.

The tripwire exists to catch one failure: a push that reports success while
delivering none of the write connection's frames. That is silent data loss,
and it is the risk taken on by moving the push to its own connection.

It was wired wrong. On the async connection, stats() is a coroutine
function. The pool called it without awaiting, so getattr(coroutine,
"revision", None) returned None every time, _check_push_delivered returned
early on every push, and the alarm could never fire. Python said so —
"coroutine 'ConnectionSync.stats' was never awaited" — but only under a
soak, because nothing asserted the tripwire could see a moving revision.

A tripwire that always reads None either never fires or fires always.
Neither is an alarm. These tests assert it reads real, moving values.
"""

import asyncio
import warnings

import pytest

from declaro_persistum.pool import TursoPool


class _AsyncStatsConn:
    """A connection whose stats() is a coroutine, as turso.aio.sync's is."""

    def __init__(self, revisions: list) -> None:
        self._revisions = list(revisions)
        self.stats_calls = 0

    async def stats(self):
        self.stats_calls += 1
        value = self._revisions[min(self.stats_calls - 1, len(self._revisions) - 1)]
        return type("Stats", (), {"revision": value})()


class _SyncStatsConn:
    """A connection whose stats() returns directly, as turso.sync's does."""

    def __init__(self, revisions: list) -> None:
        self._revisions = list(revisions)
        self.stats_calls = 0

    def stats(self):
        self.stats_calls += 1
        value = self._revisions[min(self.stats_calls - 1, len(self._revisions) - 1)]
        return type("Stats", (), {"revision": value})()


class _Holder:
    def __init__(self, conn) -> None:
        self.conn = conn

    async def connect_async(self) -> None:
        pass

    async def push(self) -> None:
        pass

    async def pull(self) -> None:
        pass


def _pool(tmp_path, conn):
    db = tmp_path / "r.db"
    db.write_bytes(b"x" * 64)
    pool = TursoPool(str(db), remote_url="https://example.turso.io", auth_token="t")
    pool._push_holder = _Holder(conn)  # type: ignore[assignment]
    pool._write_holder = _Holder(conn)  # type: ignore[assignment]
    return pool


class TestRevisionIsActuallyRead:
    """The revision read must produce a value, not a coroutine."""

    @pytest.mark.asyncio
    async def test_reads_a_real_value_from_async_stats(self, tmp_path):
        """stats() is awaited, so a number comes back rather than None."""
        conn = _AsyncStatsConn([41])
        pool = _pool(tmp_path, conn)

        revision = await pool._sync_revision(pool._push_holder)

        assert revision == 41, (
            "the revision read returned None — stats() was probably not awaited, "
            "which makes the tripwire blind"
        )

    @pytest.mark.asyncio
    async def test_reads_a_real_value_from_sync_stats(self, tmp_path):
        """A non-coroutine stats() must work too, for the fallback path."""
        conn = _SyncStatsConn([7])
        pool = _pool(tmp_path, conn)

        assert await pool._sync_revision(pool._push_holder) == 7

    @pytest.mark.asyncio
    async def test_never_leaves_a_coroutine_unawaited(self, tmp_path):
        """An un-awaited coroutine is the exact defect this file exists for."""
        conn = _AsyncStatsConn([1, 2])
        pool = _pool(tmp_path, conn)

        with warnings.catch_warnings():
            warnings.simplefilter("error", RuntimeWarning)
            await pool._sync_revision(pool._push_holder)
            await pool._push_once()

    @pytest.mark.asyncio
    async def test_missing_stats_reads_as_none(self, tmp_path):
        """A connection without stats() must not raise."""

        class _NoStats:
            pass

        pool = _pool(tmp_path, _NoStats())
        assert await pool._sync_revision(pool._push_holder) is None


class TestTripwireFiresOnlyWhenDeliveryStalls:
    """It must stay silent on a moving revision and speak on a static one."""

    @pytest.mark.asyncio
    async def test_silent_when_the_revision_moves(self, tmp_path, caplog):
        """A healthy push must not warn."""
        conn = _AsyncStatsConn([10, 11])
        pool = _pool(tmp_path, conn)
        pool._writes_since_push = 3

        with caplog.at_level("WARNING"):
            await pool._push_once()

        assert "revision did not change" not in caplog.text
        assert pool._pushes_without_revision_change == 0

    @pytest.mark.asyncio
    async def test_warns_when_the_revision_is_static_with_writes_pending(
        self, tmp_path, caplog
    ):
        """Writes pending and a revision that never moves is the alarm case."""
        conn = _AsyncStatsConn([10, 10])
        pool = _pool(tmp_path, conn)
        pool._writes_since_push = 5

        with caplog.at_level("WARNING"):
            await pool._push_once()

        assert "revision did not change" in caplog.text
        assert pool._pushes_without_revision_change == 1

    @pytest.mark.asyncio
    async def test_silent_when_nothing_was_pending(self, tmp_path, caplog):
        """No writes means nothing to deliver, so a static revision is correct."""
        conn = _AsyncStatsConn([10, 10])
        pool = _pool(tmp_path, conn)
        pool._writes_since_push = 0

        with caplog.at_level("WARNING"):
            await pool._push_once()

        assert "revision did not change" not in caplog.text
        assert pool._pushes_without_revision_change == 0


class TestUnderConcurrency:
    """The soak ran the tripwire against a live push loop."""

    @pytest.mark.asyncio
    async def test_repeated_pushes_do_not_warn_while_the_revision_advances(
        self, tmp_path, caplog
    ):
        conn = _AsyncStatsConn(list(range(100)))
        pool = _pool(tmp_path, conn)

        with caplog.at_level("WARNING"):
            for _ in range(10):
                pool._writes_since_push = 1
                await pool._push_once()

        assert "revision did not change" not in caplog.text
        assert pool._pushes_without_revision_change == 0
        await asyncio.sleep(0)
