"""Measures whether a cloud push blocks consumer reads and writes.

The pool used to serve reads, writes and the background push from one
connection under one lock (_conn_lock), and _push_once holds that lock
across `await push()` — the whole cloud round trip. Every consumer
operation therefore waited on the network while a push was in flight.

Both are now fixed. Reads take their own plain local connection. The push
takes its own sync connection. Neither takes _conn_lock, so no consumer
operation waits on a cloud round trip.

That the push may run on a separate connection was not assumed. It was
verified under free-threaded CPython with the GIL off: 1353 writes on one
connection, 40 pushes on another, and a fresh third connection pulled all
1353 rows back from cloud. A push on the separate connection delivers the
write connection's frames in full.

These tests stub push() with a measurable delay rather than reaching the
cloud, so they quantify the contention deterministically. The push holder
is injected directly; a real connect against the fake remote would fail,
fall back to the write connection, and make these pass for the wrong
reason.
"""

import asyncio

import pytest

from declaro_persistum.pool import TursoPool

# How long the stubbed cloud round trip takes. Real Turso Cloud pushes were
# measured downstream at ~1.35s; this is scaled down to keep the suite fast
# while staying far above scheduler noise.
PUSH_SECONDS = 0.30

# A consumer operation is "blocked" if it waits for a meaningful fraction of
# the push. Well clear of both scheduler jitter and the full push duration.
BLOCKED_THRESHOLD = PUSH_SECONDS / 2


class _SlowPushHolder:
    """Connection holder whose push() takes a measurable amount of time."""

    def __init__(self) -> None:
        self.conn = object()
        self.push_calls = 0

    async def connect_async(self) -> None:
        pass

    async def push(self) -> None:
        self.push_calls += 1
        await asyncio.sleep(PUSH_SECONDS)

    async def pull(self) -> None:
        pass


async def _seed_real_db(path: str) -> None:
    """Create a genuine local database, so read connections can open it."""
    import turso.aio

    conn = await turso.aio.connect(path)
    await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    await conn.commit()
    await conn.close()


async def _pool_with_slow_push(tmp_path):
    """A cloud-configured pool whose push is slow and whose holder is stubbed."""
    db = tmp_path / "replica.db"
    await _seed_real_db(str(db))

    pool = TursoPool(
        str(db),
        remote_url="https://example.turso.io",
        auth_token="tok",
        background_pull=True,
    )
    pool._write_holder = _SlowPushHolder()  # type: ignore[assignment]
    # The push runs on its own connection. Injected directly so the test
    # never attempts a real connect against the fake remote — a failed
    # connect would fall back to the write holder and make this pass for the
    # wrong reason.
    pool._push_holder = _SlowPushHolder()  # type: ignore[assignment]
    pool._mvcc = False
    return pool


async def _time_it(coro_fn) -> float:
    loop = asyncio.get_running_loop()
    start = loop.time()
    await coro_fn()
    return loop.time() - start


async def _noop() -> None:
    return None


class TestReadsRunDuringAPush:
    """Reads take their own connection, so a push cannot stall them."""

    @pytest.mark.asyncio
    async def test_read_does_not_wait_for_an_in_flight_push(self, tmp_path):
        """A read arriving mid-push must not wait for the cloud round trip."""
        pool = await _pool_with_slow_push(tmp_path)

        push = asyncio.create_task(pool._push_once())
        await asyncio.sleep(0.02)  # let the push take the lock

        async def _read():
            async with pool.acquire():
                pass

        elapsed = await _time_it(_read)
        await push

        assert elapsed < BLOCKED_THRESHOLD, (
            f"read waited {elapsed:.3f}s on a {PUSH_SECONDS}s push — "
            f"it is blocking on the cloud round trip"
        )

    @pytest.mark.asyncio
    async def test_many_reads_are_not_serialised_behind_one_push(self, tmp_path):
        """Concurrent reads must not queue behind a single push."""
        pool = await _pool_with_slow_push(tmp_path)

        push = asyncio.create_task(pool._push_once())
        await asyncio.sleep(0.02)

        async def _read():
            async with pool.acquire():
                pass

        elapsed = await _time_it(lambda: asyncio.gather(*(_read() for _ in range(5))))
        await push

        assert elapsed < BLOCKED_THRESHOLD, (
            f"5 concurrent reads took {elapsed:.3f}s during a {PUSH_SECONDS}s push"
        )


@pytest.mark.asyncio
async def test_write_does_not_wait_for_an_in_flight_push(tmp_path):
    """A write arriving mid-push must not wait for the cloud round trip."""
    pool = await _pool_with_slow_push(tmp_path)

    push = asyncio.create_task(pool._push_once())
    await asyncio.sleep(0.02)

    async def _write():
        async with pool.acquire_write(concurrent=False) as conn:
            conn.commit = _noop  # type: ignore[method-assign]

    elapsed = await _time_it(_write)
    await push

    assert elapsed < BLOCKED_THRESHOLD, (
        f"write waited {elapsed:.3f}s on a {PUSH_SECONDS}s push — "
        f"it is blocking on the cloud round trip"
    )
