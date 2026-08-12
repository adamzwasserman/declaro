"""
Durability regression tests for the Turso embedded-replica sync path.

Covers the FK-push silent-data-loss bug (BUG_REPORT_fk_push_silent_data_loss.md)
and its defense-in-depth remediation (W1-W5).

The pyturso replica connection + network is replaced by an in-process fake holder
(_FakeHolder) so the pool's *orchestration* logic — the code under repair — is
tested deterministically with no cloud DB. The real-cloud enforcement mechanism
for W1 (PRAGMA foreign_keys = ON causing an IntegrityError at commit) was proven
directly against the production libsql engine; these tests lock in the pool-side
contract that drives it.
"""

import pytest

import declaro_persistum.turso_pool as pool_mod  # TursoPool's own module since declaro-tvx split pool.py
from declaro_persistum.pool import TursoPool, _TursoConnectionHolder








@pytest.fixture
def fake_holder(monkeypatch):
    """Patch the pool to build _FakeHolder instances; return the live instance."""
    created = {}

    created["holders"] = []

    def factory(database_path, remote_url=None, auth_token=None):
        h = _FakeHolder(database_path, remote_url, auth_token)
        created["holders"].append(h)
        # The first holder built is the write holder. A cloud pool also opens
        # a second, dedicated push connection, so these tests must keep
        # pointing at the write holder rather than at whichever was made last.
        created.setdefault("holder", h)
        return h

    _FakeHolder.push_error = None
    _FakeHolder.shared_events = []
    monkeypatch.setattr(pool_mod, "_TursoConnectionHolder", factory)
    return created




async def _connect_no_loop(pool, path, remote_url, token):
    """Attach a connected _FakeHolder without starting the background push loop."""
    holder = _FakeHolder(path, remote_url, token)
    await holder.connect_async()
    pool._write_holder = holder
    # The push runs on its own connection and no longer borrows the write
    # connection, so a test that exercises pushes must supply one.
    push_holder = _FakeHolder(path, remote_url, token)
    await push_holder.connect_async()
    pool._push_holder = push_holder
    return holder






