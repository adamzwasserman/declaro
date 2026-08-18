"""`replicate` moves the data, and the order it moves it in is load-bearing.

Every test of the replication loop monkeypatched this function, so the one
that actually pushes and pulls rested on reading the code.

UP FIRST, THEN DOWN. Bringing the primary's changes down first could overwrite
a local commit that has been waiting since the last shutdown -- and on
ephemeral disk that commit is the only copy. Sending up first makes the local
copy the primary's problem before the primary becomes the local copy's.

THE CALLABLES HERE ARE THE DATABASE'S OWN INJECTION POINTS, not stand-ins for
a driver. `replicate_once` and `refresh_once` are fields of the `Database`
record that `open_turso` fills in, so recording them records the real seam.
A test asserts they are the fields a real opener populates, because a stub
whose contract has drifted from the library has manufactured two false bugs in
this package already.
"""

from __future__ import annotations

import pytest

from declaro_persistum.database import replicate

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit]


def _replica(order: list[str], *, push_ok: bool = True) -> dict:
    async def push(_db):
        order.append("up")
        return push_ok

    async def pull(_db):
        order.append("down")
        return True

    return {
        "closed": False,
        "primary": "libsql://example",
        "serialise": {"waiting": 0},
        "replicate_once": push,
        "refresh_once": pull,
        "path": "x.db",
    }


async def test_it_sends_before_it_receives() -> None:
    """The ordering, which is the whole reason this function exists as one."""
    order: list[str] = []

    assert await replicate(_replica(order)) is True
    assert order == ["up", "down"], order


async def test_a_failed_send_does_not_pull() -> None:
    """Pulling after a failed push is what overwrites the local commit.

    The push failed, so the primary does not have the local writes yet.
    Bringing its state down now discards them.
    """
    order: list[str] = []

    assert await replicate(_replica(order, push_ok=False)) is False
    assert order == ["up"], order


async def test_a_local_database_is_refused_rather_than_told_it_worked() -> None:
    """It returned True for a database with no primary, and its own docstring
    said the value meant "true by vacuity rather than by success".

    A caller cannot tell that apart from a replication that worked.
    """
    order: list[str] = []
    db = _replica(order)
    db["primary"] = None

    with pytest.raises(ValueError) as caught:
        await replicate(db)

    assert "no primary" in str(caught.value)
    assert order == [], "it moved data for a database with nowhere to send it"


async def test_a_closed_database_is_refused() -> None:
    """Replicating through a closed connection is a use-after-close."""
    order: list[str] = []
    db = _replica(order)
    db["closed"] = True

    with pytest.raises(Exception):
        await replicate(db)

    assert order == []


async def test_both_directions_run_in_one_pass() -> None:
    """`replicate` pushed only, once, while its comment claimed the pair
    brought the copies into conformity.

    A caller that only ever called it had a local copy that learned nothing
    about its primary for the life of the process.
    """
    order: list[str] = []

    await replicate(_replica(order))

    assert "up" in order and "down" in order


def test_the_injected_names_are_the_ones_an_opener_fills() -> None:
    """The stub above is only worth anything if it names the real seam.

    A stand-in whose contract has drifted from the library has manufactured
    two false bugs in this package, so the field names are asserted against
    the record `open_turso` builds rather than trusted.
    """
    from declaro_persistum.database import Database

    fields = set(Database.__annotations__)

    # AGAINST THE TYPE, NOT THE SOURCE TEXT. My first version searched
    # `turso_database.py` for `"replicate_once"` and found nothing, because
    # the opener passes them as keyword arguments rather than quoted keys. The
    # detector was wrong and the code was right, which is the shape of half
    # the false reports I have made today.
    for field in ("replicate_once", "refresh_once", "closed", "primary", "serialise"):
        assert field in fields, (
            f"{field} is not a field of Database, so the record in this file "
            f"is testing a shape the package does not build"
        )


# ---------------------------------------------------------------------------
# The far side of the seam
# ---------------------------------------------------------------------------
#
# The tests above record the injection points and assert they are fields of
# `Database`. That verifies the seam and not what a real opener puts through
# it, which I recorded as a gap. This closes the local half of it: a local
# `open_turso` is opened for real and its injected callables are inspected.
#
# The replicated half stays open. Opening a replica reaches the remote, and a
# fake primary answers `status=404, Host not found`.


@pytest.mark.turso
async def test_a_local_open_injects_callables_of_the_right_shape(tmp_path) -> None:
    """A real `open_turso`, not a record I wrote."""
    import inspect

    from declaro_persistum import close as close_db
    from declaro_persistum import open_turso

    db = await open_turso(str(tmp_path / "t.db"), shutdown="exit_immediately")
    try:
        for field in ("replicate_once", "refresh_once"):
            fn = db[field]
            assert inspect.iscoroutinefunction(fn), f"{field} is not awaitable"
            assert list(inspect.signature(fn).parameters) == ["_db"], (
                f"{field} does not take a Database"
            )
    finally:
        await close_db(db)


@pytest.mark.turso
async def test_a_local_database_never_consults_them(tmp_path) -> None:
    """`nothing_to_replicate` returns True, which would read as success.

    Every caller guards with `is_replicated` first -- `replicate` raises,
    `flush` and the shutdown path return early -- so that True is never read.
    This asserts the guard rather than the placeholder, because if a guard
    were dropped the placeholder would report a replication that never
    happened, on a local database, at shutdown.
    """
    from declaro_persistum import close as close_db
    from declaro_persistum import open_turso
    from declaro_persistum.database import flush, replicate

    db = await open_turso(str(tmp_path / "t.db"), shutdown="exit_immediately")
    consulted = False

    async def _tripwire(_db):
        nonlocal consulted
        consulted = True
        return True

    db["replicate_once"] = _tripwire
    try:
        await flush(db)
        with pytest.raises(ValueError, match="no primary"):
            await replicate(db)

        assert not consulted, (
            "a local database consulted its replication callable, so the "
            "placeholder's True would be read as a successful replication"
        )
    finally:
        await close_db(db)
