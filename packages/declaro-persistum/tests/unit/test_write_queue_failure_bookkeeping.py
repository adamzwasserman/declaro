"""The failure bookkeeping must not leak, and shutdown must not lose writes silently.

Two defects on the same few lines of ``_write_retry`` / ``_flush``.

D1 — ``_first_failure_time`` grows without bound.
``_check_critical_threshold`` runs in the except handler *outside* the
``if key in self._queue`` guard. Both places that pop the key
(``remove_entry`` and ``_quarantine``) have already run by then if the
entry left the queue during the write's awaits. The threshold check
re-inserts the key afterwards, and the retry loop then exits on
``while key in self._queue``, so nothing pops it ever again. One leaked
float per race, for the lifetime of the process.

The race is reachable: ``_flush`` and the supervisor's ``_write_retry``
tasks both drain the same queue, and a write attempt awaits three times
between the queue check and the except handler.

D2 — ``_flush`` swallows every exception with a bare ``except: pass``.
It is the last drain before shutdown. When it fails and no persistence
path is configured, the write is gone and nothing is written to the log
saying so. That is exactly the silent failure this package exists to
prevent.
"""

import logging

import pytest

from declaro_persistum.write_queue import WriteQueue


class _Conn:
    async def execute(self, *_a, **_k):
        raise AssertionError("the fake pool fails before any statement runs")


class _FailingAcquire:
    """An async context manager that fails on enter, after a callback."""

    def __init__(self, on_enter) -> None:
        self._on_enter = on_enter

    async def __aenter__(self):
        self._on_enter()
        raise RuntimeError("remote unreachable")

    async def __aexit__(self, *_a):
        return False


class _RacingPool:
    """Removes the entry from the queue during the write, then fails.

    This is what a concurrent _flush success looks like from inside
    _write_retry: by the time the exception is handled, the key is gone.
    """

    def __init__(self) -> None:
        self.queue: WriteQueue | None = None
        self.table = ""
        self.pk = None

    def acquire_write(self):
        def _remove_mid_flight() -> None:
            assert self.queue is not None
            self.queue.remove_entry(self.table, self.pk)

        return _FailingAcquire(_remove_mid_flight)

    # _write_retry does getattr(pool, "acquire_write", pool.acquire), and the
    # fallback is evaluated eagerly, so this must exist even though the
    # write path never reaches it.
    acquire = acquire_write


class _AlwaysFailingPool:
    def acquire_write(self):
        return _FailingAcquire(lambda: None)

    acquire = acquire_write


def _enqueue(queue: WriteQueue, table: str, pk: int) -> None:
    queue.enqueue(
        table=table,
        pk_column="id",
        pk_value=pk,
        op="update",
        data={"id": pk},
        sql="UPDATE t SET x = 1 WHERE id = ?",
        params=(pk,),
        dialect="sqlite",
    )


class TestFailureTimesNeverOutliveTheirEntry:
    """D1: no key may sit in _first_failure_time without a queue entry."""

    @pytest.mark.asyncio
    async def test_a_key_removed_mid_write_does_not_leak(self):
        pool = _RacingPool()
        queue = WriteQueue(pool)
        pool.queue, pool.table, pool.pk = queue, "users", 1

        _enqueue(queue, "users", 1)
        await queue._write_retry("users:1", queue._queue["users:1"])

        assert queue._queue == {}, "the entry should have been removed"
        assert queue._first_failure_time == {}, (
            f"leaked {list(queue._first_failure_time)} — these keys have no "
            f"queue entry, and neither remove_entry nor _quarantine will ever "
            f"run for them again"
        )

    @pytest.mark.asyncio
    async def test_the_leak_accumulates_across_rows(self):
        """One leaked float per raced row is unbounded growth, not a one-off."""
        pool = _RacingPool()
        queue = WriteQueue(pool)
        pool.queue = queue

        for pk in range(25):
            pool.table, pool.pk = "users", pk
            _enqueue(queue, "users", pk)
            await queue._write_retry(f"users:{pk}", queue._queue[f"users:{pk}"])

        assert len(queue._first_failure_time) == 0, (
            f"{len(queue._first_failure_time)} entries retained after 25 raced "
            f"writes; the dict tracks rows that no longer exist"
        )

    @pytest.mark.asyncio
    async def test_a_genuinely_failing_key_is_still_tracked(self):
        """The fix must not throw away the tracking it is there to do.

        A key that stays in the queue and keeps failing must keep its first
        failure time, because that is what the critical threshold measures.
        """
        queue = WriteQueue(_AlwaysFailingPool(), max_drain_attempts=2)
        _enqueue(queue, "users", 7)

        await queue._write_retry("users:7", queue._queue["users:7"])

        # Quarantined after 2 attempts, so it is cleaned up on that path...
        assert queue.dead_letters(), "the entry should have been dead-lettered"
        assert queue._first_failure_time == {}, "quarantine must clean up too"

    @pytest.mark.asyncio
    async def test_first_failure_time_is_recorded_while_the_entry_lives(self):
        """Positive control: tracking still happens for a live entry."""
        queue = WriteQueue(_AlwaysFailingPool())
        _enqueue(queue, "users", 9)
        entry = queue._queue["users:9"]

        queue._check_critical_threshold("users:9", entry, RuntimeError("x"))

        assert "users:9" in queue._first_failure_time, (
            "a live failing entry must still be tracked"
        )


class TestShutdownFlushDoesNotFailSilently:
    """D2: a lost write on shutdown must be reported."""

    @pytest.mark.asyncio
    async def test_a_failed_flush_is_logged(self, caplog):
        queue = WriteQueue(_AlwaysFailingPool())
        _enqueue(queue, "users", 1)

        with caplog.at_level(logging.ERROR, logger="declaro_persistum.write_queue"):
            await queue._flush()

        assert caplog.records, (
            "the shutdown flush failed and logged nothing; with no persistence "
            "path configured this write is gone and no operator can know"
        )
        message = " ".join(r.getMessage() for r in caplog.records)
        assert "users" in message and "1" in message, (
            f"the log must name the row that was lost, got: {message!r}"
        )

    @pytest.mark.asyncio
    async def test_a_clean_flush_logs_no_error(self, caplog, tmp_path):
        """Negative control, against a real pool so the write truly lands.

        A fake connection is rejected by _prepare_query, which makes the
        flush genuinely fail -- so a fake here would prove nothing about
        the quiet path.
        """
        from declaro_persistum import ConnectionPool

        pool = await ConnectionPool.sqlite(str(tmp_path / "q.db"))
        async with pool.acquire() as conn:
            await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
            await conn.execute("INSERT INTO t (id, x) VALUES (1, 0)")
            await conn.commit()

        queue = WriteQueue(pool)
        _enqueue(queue, "t", 1)

        with caplog.at_level(logging.ERROR, logger="declaro_persistum.write_queue"):
            await queue._flush()

        assert queue._queue == {}, "the entry should have drained"
        assert not caplog.records, (
            f"a successful flush must stay quiet, logged: "
            f"{[r.getMessage() for r in caplog.records]}"
        )
        await pool.close()
