"""pyturso's own signatures, held against what this package calls.

WHAT THIS FILE USED TO BE. It checked a stand-in for the driver against the
driver. That stand-in is gone: the scenarios it served now run against the
real primary, and once they did, nothing used it but the tests here. A
stand-in whose only remaining reader is the test that checks it is two things
verifying each other.

WHAT SURVIVES IS THE HALF THAT WAS ALWAYS ABOUT THE DRIVER. These three read
`inspect.signature` off the installed pyturso and fail when it moves under us.
They are the ones that would have caught either of the two stand-in failures
this suite has already paid for.

Once loudly: a scratch double returned a SYNCHRONOUS cursor from `execute`, a
reconstruction run through it raised `TypeError: 'list' object can't be
awaited`, and that was read as a pyturso defect. A tolerance went into two
source files over a comment asserting a measurement nobody had taken. Measured
afterwards against a real replica, pyturso's `execute` and `fetchall` are both
awaitable. The defect never existed and the fix landed in correct code.

Once quietly, and this is the worse half: a double declared

    async def connect(path, remote_url=None, auth_token=None)

under a comment claiming it mirrored `turso.aio.sync.connect`. It does not.
The real one takes `remote_url` positionally and `auth_token` keyword-only, so
the double accepted two call shapes the driver refuses.

A THIRD, ON 2026-08-28, IS WHY THE STAND-IN WENT. Its `connect` recorded the
path it was handed and returned. The real one opens a file, and a path whose
folder does not exist fails. Our code never made that folder, and the scenario
aimed at exactly that case had been green for months. The signature was right;
what a signature cannot carry is that the real thing touches a disk.
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


# ---------------------------------------------------------------------------
# the connection the stand-in returns
# ---------------------------------------------------------------------------


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
