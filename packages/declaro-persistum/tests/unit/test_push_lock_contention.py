"""Measures whether a cloud push blocks consumer reads and writes.

The pool serves reads, writes and the background push from one connection
guarded by one lock (_conn_lock). _push_once holds that lock across
`await push()` — the whole cloud round trip — so anything a consumer does
while a push is in flight waits for the network.

Reported downstream as write latency tracking the push duration instead of
staying local sub-ms. Reads were assumed unaffected; acquire() takes the
same lock, so they are not.

These tests stub push() with a measurable delay rather than reaching the
cloud, so they quantify the contention deterministically and act as a
regression guard for any fix.
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


def _pool_with_slow_push(tmp_path):
    """A cloud-configured pool whose push is slow and whose holder is stubbed."""
    db = tmp_path / "replica.db"
    db.write_bytes(b"x" * 64)

    pool = TursoPool(
        str(db),
        remote_url="https://example.turso.io",
        auth_token="tok",
        background_pull=True,
    )
    pool._write_holder = _SlowPushHolder()  # type: ignore[assignment]
    pool._mvcc = False
    return pool


async def _time_it(coro_fn) -> float:
    loop = asyncio.get_running_loop()
    start = loop.time()
    await coro_fn()
    return loop.time() - start


_KNOWN_DEFECT = pytest.mark.xfail(
    strict=True,
    reason=(
        "Reads, writes and the push share one connection guarded by one lock, "
        "and _push_once holds it across the cloud round trip, so consumer "
        "operations wait on the network. Measured here rather than argued. "
        "The fix is structural — separate connections — and is gated on "
        "whether concurrent sync connections to one replica are safe, which "
        "is unproven. strict=True so that whoever fixes it is forced to "
        "update this file instead of the tests quietly starting to pass."
    ),
)


@_KNOWN_DEFECT
class TestPushBlocksConsumers:
    """Each of these fails while the push holds _conn_lock across the network."""

    @pytest.mark.asyncio
    async def test_read_does_not_wait_for_an_in_flight_push(self, tmp_path):
        """A read arriving mid-push must not wait for the cloud round trip."""
        pool = _pool_with_slow_push(tmp_path)

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
    async def test_write_does_not_wait_for_an_in_flight_push(self, tmp_path):
        """A write arriving mid-push must not wait for the cloud round trip."""
        pool = _pool_with_slow_push(tmp_path)

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

    @pytest.mark.asyncio
    async def test_many_reads_are_not_serialised_behind_one_push(self, tmp_path):
        """Concurrent reads should not queue behind a single push."""
        pool = _pool_with_slow_push(tmp_path)

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


async def _noop() -> None:
    return None
