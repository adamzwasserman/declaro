"""A closed sync pool raises PoolClosedError, not NameError.

Regression for a bug introduced by the eight-module split of pool.py
(commit 03c1112): `sync_pool.py` raised `PoolClosedError` in two places and
never imported it. Every caller hitting a closed sync pool got

    NameError: name 'PoolClosedError' is not defined

instead of the typed exception, and no test exercised the path.

The bound vocabulary here is small enough to exhaust: two sync pool classes,
one closed state, one operation. Both members run.
"""

import pytest

from declaro_persistum.exceptions import PoolClosedError
from declaro_persistum.sync_pool import SyncSQLitePool, SyncTursoPool

POOLS = [SyncSQLitePool, SyncTursoPool]


@pytest.mark.parametrize("pool_cls", POOLS)
def test_acquire_after_close_raises_pool_closed_error(pool_cls, tmp_path):
    """The typed exception, not NameError, and not a bare Exception."""
    pool = pool_cls(str(tmp_path / "db"))
    pool.close()
    with pytest.raises(PoolClosedError, match="Pool has been closed"):
        pool.acquire()


@pytest.mark.parametrize("pool_cls", POOLS)
def test_closed_reports_closed(pool_cls, tmp_path):
    pool = pool_cls(str(tmp_path / "db"))
    assert pool.closed is False
    pool.close()
    assert pool.closed is True


@pytest.mark.parametrize("pool_cls", POOLS)
def test_close_is_idempotent(pool_cls, tmp_path):
    """Closing twice must not raise — a caller cannot always know."""
    pool = pool_cls(str(tmp_path / "db"))
    pool.close()
    pool.close()
    assert pool.closed is True


@pytest.mark.parametrize("pool_cls", POOLS)
def test_every_name_the_module_raises_is_importable(pool_cls):
    """The class of bug this file exists for: a name used but never imported.

    `raise SomeError(...)` only fails when the line executes, so an untested
    branch hides it indefinitely. This asserts the module's namespace has the
    name, independently of reaching the branch.
    """
    import declaro_persistum.sync_pool as mod

    assert hasattr(mod, "PoolClosedError"), (
        "sync_pool raises PoolClosedError; it must be importable in that module"
    )
    assert mod.PoolClosedError is PoolClosedError
