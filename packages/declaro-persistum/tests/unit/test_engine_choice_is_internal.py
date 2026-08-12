"""The consumer never names an engine mode. persistum decides.

`mvcc` and `pooled_writes` were parameters on `ConnectionPool.turso`, so the
choice between a safe and an unsafe engine configuration sat on the caller's
call site. Worse, `mvcc` defaulted to True, which on a REPLICATED pool is the
configuration that strands writes — a caller who simply omitted the argument
got the dangerous one.

The rule persistum now applies internally, with no way for a caller to override
it:

    remote_url set  ->  replicated  ->  MVCC OFF
    no remote_url   ->  local   ->  MVCC ON

The REASON changed on 2026-08-12 even though the rule did not. It is not that
MVCC cannot run on a replica — it can, measured 4 of 4 runs, and 20 sequential
writes under it reached the primary intact. The measured constraint is that a
replica takes ONE replica connection, and MVCC is the mode in which the
pool stops serialising writers. Turning it off on a replicated pool is what keeps
one connection live at a time, so the rule is right by consequence.

The input space here is bounded and tiny — replicated or not — so both members run.
"""

import inspect

import pytest

from declaro_persistum.pool import ConnectionPool
from declaro_persistum.turso_pool import TursoPool

FORBIDDEN = ("mvcc", "pooled_writes")


@pytest.mark.parametrize("name", FORBIDDEN)
def test_factory_does_not_accept_an_engine_parameter(name):
    """Set-based: every forbidden name is checked, not a sample."""
    params = inspect.signature(ConnectionPool.turso).parameters
    assert name not in params, (
        f"{name} is on the consumer surface; the engine choice is persistum's"
    )


@pytest.mark.parametrize("name", FORBIDDEN)
def test_pool_class_does_not_accept_an_engine_parameter(name):
    params = inspect.signature(TursoPool.__init__).parameters
    assert name not in params, f"{name} is still constructible by a caller"


@pytest.mark.parametrize("name", FORBIDDEN)
def test_passing_it_is_rejected_loudly(name, tmp_path):
    """A caller who still passes it gets a TypeError, not silent acceptance."""
    with pytest.raises(TypeError):
        TursoPool(str(tmp_path / "db"), **{name: False})


def test_a_replicated_pool_never_requests_mvcc(tmp_path):
    """MVCC on a replica is the stranding defect. It must be impossible."""
    pool = TursoPool(str(tmp_path / "db"), remote_url="https://x.turso.io",
                     auth_token="t")
    assert pool._mvcc_requested is False


def test_a_local_pool_requests_mvcc(tmp_path):
    """Local is where MVCC is safe and where the throughput is."""
    pool = TursoPool(str(tmp_path / "db"))
    assert pool._mvcc_requested is True


def test_the_choice_follows_remote_url_and_nothing_else(tmp_path):
    """Both members of the bounded vocabulary, asserted together."""
    replicated = TursoPool(str(tmp_path / "a"), remote_url="https://x.turso.io")
    local = TursoPool(str(tmp_path / "b"))
    assert (replicated._mvcc_requested, local._mvcc_requested) == (False, True)


def test_writes_are_never_pooled(tmp_path):
    """The two competing write strategies are gone; one shape remains."""
    pool = TursoPool(str(tmp_path / "db"))
    assert not hasattr(pool, "_pooled_writes")
