"""The replication stand-ins, held against pyturso's own signatures.

THIS SUITE HAS BEEN BURNED BY A STAND-IN TWICE, IN OPPOSITE DIRECTIONS.

Once loudly: a scratch double returned a SYNCHRONOUS cursor from `execute`, a
reconstruction run through it raised `TypeError: 'list' object can't be
awaited`, and that was read as a pyturso defect. A tolerance went into two
source files over a comment asserting a measurement nobody had taken. Measured
afterwards against a real replica, pyturso's `execute` and `fetchall` are both
awaitable. The defect never existed and the fix landed in correct code.

Once quietly, and this is the worse half: `_FakeSync` declared

    async def connect(path, remote_url=None, auth_token=None)

under a comment claiming it mirrored `turso.aio.sync.connect`. It does not.
The real one is

    connect(path, remote_url, *, auth_token=None, ...)

so `remote_url` is REQUIRED and `auth_token` is KEYWORD-ONLY. The stand-in
accepted two call shapes the driver refuses, so a regression in
`replicate_path` could have gone green against the double and failed against
pyturso. A stand-in that is MORE permissive than the driver hides a defect
rather than inventing one, and only that kind leaves a passing suite.

SO A COMMENT ASSERTING A CONTRACT IS NOT EVIDENCE OF ONE. These tests compare
the stand-ins against the installed driver by `inspect`, so a driver that
tightens or adds fails here instead of at a customer.

NO NETWORK. Every assertion below reads signatures and attribute names off the
installed package. The live replica was used once, by hand on 2026-08-16, to
establish which methods exist and that all of them are coroutine functions;
those findings are pinned here as constants so the suite never dials out.
"""

from __future__ import annotations

import inspect

import pytest

pytestmark = pytest.mark.precommit

turso_sync = pytest.importorskip(
    "turso.aio.sync", reason="pyturso is not installed, so there is no contract to check"
)


# Measured against a live replica on 2026-08-16, opened with TEST_TURSO_URL:
# every one present, every one a coroutine function.
REPLICA_METHODS = ("push", "pull", "close", "commit", "rollback", "execute")

# The methods `replicate_path` and `_pull_until_level` actually call. A stand-in
# needs these; the rest of the surface it may leave alone.
METHODS_UNDER_TEST = ("push", "pull", "close")


def _stand_in():
    from tests.bdd.steps.test_replication_steps import new_recorder, new_replica_stand_in

    return new_replica_stand_in(new_recorder(batches=0, failures=0))


# ---------------------------------------------------------------------------
# connect
# ---------------------------------------------------------------------------


def test_remote_url_is_required_by_the_real_connect() -> None:
    """The exact fact the old stand-in got wrong."""
    parameters = inspect.signature(turso_sync.connect).parameters

    assert "remote_url" in parameters, sorted(parameters)
    assert parameters["remote_url"].default is inspect.Parameter.empty, (
        "pyturso's `connect` now defaults `remote_url`. The stand-in in "
        "test_replication_steps.py requires it, which is stricter and "
        "therefore safe, but the comment there should be corrected."
    )


def test_auth_token_is_keyword_only_in_the_real_connect() -> None:
    parameters = inspect.signature(turso_sync.connect).parameters

    assert parameters["auth_token"].kind is inspect.Parameter.KEYWORD_ONLY, (
        f"auth_token is {parameters['auth_token'].kind}, so passing it "
        f"positionally now works against the driver and the stand-in should "
        f"stop refusing it"
    )


def test_the_stand_in_connect_refuses_what_the_driver_refuses() -> None:
    """Both call shapes the old double wrongly accepted, asserted against ours.

    Written as a call rather than a signature comparison because that is how
    the mistake would recur: someone relaxes the stand-in to make a step
    easier, and nothing notices until pyturso rejects the same call.
    """
    import asyncio

    from tests.bdd.steps.test_replication_steps import new_recorder, new_sync_connect

    made: list[str] = []
    rec = new_recorder(batches=0, failures=0)
    stand_in_connect = new_sync_connect(rec, made)

    with pytest.raises(TypeError):
        asyncio.run(stand_in_connect("/tmp/x.db"))  # remote_url omitted

    with pytest.raises(TypeError):
        asyncio.run(stand_in_connect("/tmp/x.db", "libsql://p", "token"))  # positional

    assert made == [], "a refused call still ran"
    conn = asyncio.run(stand_in_connect("/tmp/x.db", "libsql://p", auth_token="t"))
    assert made == ["/tmp/x.db"], made
    assert hasattr(conn, "push"), "the accepted call returned no replica"


# ---------------------------------------------------------------------------
# the connection the stand-in returns
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("method", METHODS_UNDER_TEST)
def test_the_stand_in_has_every_method_the_code_under_test_calls(method: str) -> None:
    """`replicate_path` calls push then pull then close. All three or nothing."""
    assert hasattr(_stand_in(), method), (
        f"the replica stand-in has no {method}, so any scenario reaching it "
        f"fails with an AttributeError that names our test rather than the bug"
    )


@pytest.mark.parametrize("method", METHODS_UNDER_TEST)
def test_the_stand_in_is_awaitable_wherever_the_driver_is(method: str) -> None:
    """The first burn, in one assertion.

    A stand-in that returns a plain value where the driver returns a coroutine
    makes correct code raise `TypeError: 'list' object can't be awaited`, and
    that reads as a driver-compatibility defect.
    """
    assert inspect.iscoroutinefunction(getattr(_stand_in(), method)), (
        f"the stand-in's {method} is synchronous; pyturso's is a coroutine "
        f"function, measured against a live replica on 2026-08-16"
    )


def test_the_recorded_method_list_still_matches_the_installed_driver() -> None:
    """Guards the constant above from going stale.

    `REPLICA_METHODS` was measured by hand against a live replica. The class
    the driver returns is importable without connecting, so the names can be
    re-checked here on every run and the measurement never has to be trusted.
    """
    from turso.lib_sync import ConnectionSync

    missing = [m for m in REPLICA_METHODS if not hasattr(ConnectionSync, m)]

    assert missing == [], (
        f"pyturso's ConnectionSync no longer has {missing}. The stand-in was "
        f"built from that list, so re-measure before trusting any replication "
        f"scenario."
    )


def test_the_stand_in_never_offers_more_than_the_driver() -> None:
    """The quiet half. A permissive double hides regressions instead of finding them."""
    from turso.lib_sync import ConnectionSync

    offered = {n for n in vars(_stand_in()) if not n.startswith("_")}
    real = {n for n in dir(ConnectionSync) if not n.startswith("_")}

    invented = sorted(offered - real)
    assert invented == [], (
        f"the stand-in offers {invented}, which pyturso does not. A scenario "
        f"using one would pass here and fail against the driver."
    )
