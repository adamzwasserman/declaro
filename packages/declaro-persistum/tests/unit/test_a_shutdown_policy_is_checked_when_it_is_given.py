"""An opener must refuse a shutdown policy it cannot honour.

`shutdown` is required on all three openers and has no default, because the
usual deployment has ephemeral disk and durability rests on replicating at
SIGTERM. It is the one argument a caller cannot be allowed to leave to chance.

MEASURED 2026-08-17. No opener checks it:

    open_sqlite(shutdown="replicate")           accepted
    open_postgresql(shutdown="replicate")       accepted
    open_turso(no primary, shutdown="replicate") accepted
    open_sqlite(shutdown="typo")                accepted
    open_postgresql(shutdown="typo")            accepted
    open_turso(shutdown="typo")                 accepted

`ShutdownPolicy` is `Literal["replicate", "exit_immediately"]`, and Python
does not enforce a Literal, so a typo is a plain string that reaches the
record.

THE TYPO IS THE EXPENSIVE ONE. `trap_shutdown` reads
`if db["shutdown"] != "replicate": return` -- so anything misspelt means "do
not replicate". Someone who meant to replicate, and typed it wrong, installs
no signal handler, is told nothing, and loses the writes on the next deploy.
The value that was supposed to guarantee durability silently switches it off.

`replicate` on a database with no primary is the other half: there is nowhere
to replicate to, and the failure would arrive at SIGTERM, which is the worst
moment to discover it.
"""

from __future__ import annotations

import tempfile

import pytest

from declaro_persistum import close, open_sqlite, open_turso
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit]


def _path() -> str:
    return tempfile.mktemp(suffix=".db")


# ---------------------------------------------------------------------------
# A value outside the vocabulary
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("policy", ["typo", "replicate_then_exit", "", "REPLICATE"])
async def test_sqlite_refuses_a_policy_that_is_not_one(policy: str) -> None:
    with pytest.raises(ValueError) as caught:
        await open_sqlite(_path(), shutdown=policy, busy_timeout_s=5.0)

    message = str(caught.value)
    assert "exit_immediately" in message, (
        f"the error does not name what it takes: {message}"
    )


@pytest.mark.parametrize("policy", ["typo", "replicate_then_exit", ""])
async def test_turso_refuses_a_policy_that_is_not_one(policy: str) -> None:
    with pytest.raises(ValueError) as caught:
        await open_turso(_path(), shutdown=policy, primary=NOTHING, token=NOTHING)

    assert "exit_immediately" in str(caught.value)


def test_the_vocabulary_is_the_one_the_type_declares() -> None:
    """The list in the openers and the Literal must not drift apart."""
    import typing

    from declaro_persistum.database import ShutdownPolicy

    assert set(typing.get_args(ShutdownPolicy)) == {"replicate", "exit_immediately"}


# ---------------------------------------------------------------------------
# A policy this database cannot honour
# ---------------------------------------------------------------------------


async def test_sqlite_refuses_to_replicate_at_shutdown() -> None:
    """There is no primary. The failure would otherwise arrive at SIGTERM."""
    with pytest.raises(ValueError) as caught:
        await open_sqlite(_path(), shutdown="replicate", busy_timeout_s=5.0)

    message = str(caught.value)
    assert "replicate" in message and "primary" in message.lower(), message


async def test_turso_refuses_to_replicate_with_no_primary() -> None:
    """A local Turso file has nowhere to send anything."""
    with pytest.raises(ValueError) as caught:
        await open_turso(_path(), shutdown="replicate", primary=NOTHING, token=NOTHING)

    assert "primary" in str(caught.value).lower()


# ---------------------------------------------------------------------------
# What must keep working
# ---------------------------------------------------------------------------


async def test_sqlite_opens_with_the_policy_it_can_honour() -> None:
    db = await open_sqlite(_path(), shutdown="exit_immediately", busy_timeout_s=5.0)
    try:
        assert db["shutdown"] == "exit_immediately"
    finally:
        await close(db)


async def test_turso_opens_locally_without_replicating() -> None:
    db = await open_turso(_path(), shutdown="exit_immediately", primary=NOTHING, token=NOTHING)
    try:
        assert db["shutdown"] == "exit_immediately"
    finally:
        await close(db)


def test_the_guard_permits_replicate_when_there_is_a_primary() -> None:
    """The whole point of the policy, and the case that must not be refused.

    THROUGH THE GUARD, NOT THROUGH `open_turso`. I wrote this as an open with
    a fake primary and the docstring claimed no network call is made, which is
    not true:

        turso.lib.DatabaseError: sync engine operation failed:
        remote server returned an error: status=404, Host not found

    Opening a replica reaches the remote. Asserting the guard directly keeps
    this a test of the policy check rather than of a network I would have to
    fake, and the opener's own network behaviour is a separate question.
    """
    from declaro_persistum.database import check_shutdown

    check_shutdown("replicate", can_replicate=True, opener="open_turso")


def test_the_guard_refuses_replicate_with_nothing_to_replicate_to() -> None:
    """The other half, at the same level."""
    from declaro_persistum.database import check_shutdown

    with pytest.raises(ValueError, match="primary"):
        check_shutdown("replicate", can_replicate=False, opener="open_turso")
