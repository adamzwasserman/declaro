"""A synced replica opens ONE sync connection, however many writes it takes.

THE REGRESSION THIS HOLDS SHUT, tracked as declaro-dna. 0.1.28 pooled write
connections and reused them. 0.3.0 opened one and closed it per write:

    holder = await self._retry_while_busy(self._open_write_connection, ...)
    finally:
        await holder.conn.close(); holder.conn = None

Every pyturso connection is its own OS worker thread, and on a synced database
every open is a `turso.aio.sync` handshake against the replica tape. So a burst
that cost about five held connections became one open, one thread and one tape
acquisition PER WRITE. A consumer seeding 200 signups on 0.1.x died at 20
concurrent on 0.3.0 with SIGKILL 137.

IT CONTRADICTED A MEASUREMENT TAKEN THE SAME DAY: a synced replica takes ONE
sync connection, and opening a second answers `database tape error: database
is busy`. Pooling was what kept 0.1.28 clear of that.

THIS COUNTS THE OPENS. Reading the source would tell you `connect` returns a
held connection, and that is what I checked first; it does not tell you
whether anything else opens one on the way through a write. So this wraps
`turso.aio.sync.connect` and counts, which is the same thing I did by hand
against the real primary before writing it:

    sync connections opened at open():      1
    sync connections opened by 8 writes:    0
    distinct connection objects across 8:   1
"""

from __future__ import annotations

import os
import pathlib
import tempfile

import pytest

pytestmark = [pytest.mark.network, pytest.mark.turso]


def _the_primary() -> tuple[str, str]:
    url = os.environ.get("TEST_TURSO_URL", "")
    token = os.environ.get("TEST_TURSO_AUTH_TOKEN", "")
    assert url and token, "TEST_TURSO_URL and TEST_TURSO_AUTH_TOKEN must be set"
    return url, token


async def test_eight_writes_open_no_new_sync_connection(monkeypatch) -> None:
    """One connection at open, and none after, however many writes follow."""
    import turso.aio.sync

    from declaro_persistum import close, open_turso, writing

    url, token = _the_primary()
    opened: list[str] = []
    real = turso.aio.sync.connect

    async def counting(*args, **kwargs):
        # COUNTING, NOT REPLACING. The real driver still runs and still opens
        # the connection; this only records that it was asked.
        opened.append(str(args[0] if args else kwargs.get("path")))
        return await real(*args, **kwargs)

    monkeypatch.setattr(turso.aio.sync, "connect", counting)

    path = str(pathlib.Path(tempfile.mkdtemp()) / "app.db")
    db = await open_turso(path, primary=url, token=token, shutdown="exit_immediately")
    at_open = len(opened)
    connections: list[int] = []
    try:
        for _ in range(8):
            async with writing(db) as conn:
                connections.append(id(conn))
    finally:
        await close(db)

    assert at_open == 1, f"opening the database took {at_open} sync connections"
    assert len(opened) - at_open == 0, (
        f"eight writes opened {len(opened) - at_open} more sync connections. "
        f"Each is an OS thread and a tape handshake, and the second one "
        f"answers 'database tape error: database is busy' under load."
    )
    assert len(set(connections)) == 1, (
        f"eight writes ran on {len(set(connections))} different connections; "
        f"a replicated database holds one for its life"
    )
