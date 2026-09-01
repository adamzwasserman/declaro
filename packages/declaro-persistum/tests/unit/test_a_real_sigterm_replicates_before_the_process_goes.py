"""SIGTERM must replicate, and nothing had ever sent one.

WHY THIS MATTERS MORE THAN ITS SIZE. The usual deployment has ephemeral disk.
A process that exits without replicating loses whatever it wrote since the last
push, so `shutdown="replicate"` is the whole durability story and the signal
handler is the whole of `shutdown="replicate"`.

WHAT WAS TESTED BEFORE. That the openers refuse a policy they cannot honor,
which is a different question. No test raised a signal, so no test ever ran the
handler, and the handler carried a defect nobody could see:

    loop = asyncio.get_event_loop()
    ...
    _ON_THE_LOOP[loop.is_running()](loop, coro)

`get_event_loop` answers with whatever loop object is lying around, including a
closed one. A closed loop reports `is_running() == False`, which read as
"stopped, so run the work to completion here", and `run_until_complete` on a
closed loop raises. It also CREATES a loop when there is none and nothing
closes it, which the suite reported as an unraisable exception from
`BaseEventLoop.__del__` attributed to whichever unrelated test was running when
the collector fired.

`get_running_loop` returns a running loop or raises, and can do neither.

THIS RAISES A REAL SIGNAL at the process running the tests. That is safe here
and nowhere else: `trap_shutdown` installs our handler before the signal is
sent and restores the previous one afterwards, so the default terminate
behavior is not in play. If the install ever fails the test kills its own
runner, which is a failure nobody could mistake for a pass.
"""

from __future__ import annotations

import asyncio
import signal
import tempfile
from pathlib import Path
from typing import Any

import pytest

from declaro_persistum.database import nothing_was_trapped
from declaro_persistum.shutdown import trap_shutdown

pytestmark = pytest.mark.precommit


def _a_database_that_records_its_replication(replicated: list[str], slept: list[float]) -> Any:
    """A Database whose replicate step says it ran, and nothing else.

    The keys are the ones `replicate_on_shutdown` reads, taken from
    `database.py` rather than guessed: a stub that gets the shape wrong tests
    the stub.
    """

    async def _replicate_once(_db: Any) -> bool:
        replicated.append("replicated")
        return True

    async def _refresh_once(_db: Any) -> int:
        return 1

    async def _sleep(seconds: float) -> float:
        """Records the wait rather than taking it, and hands the wait back.

        `_replicate_once` says the primary has everything on its first call,
        so nothing should ever wait here. An empty body would have returned
        None to a caller that cannot tell "not written" from "nothing to do";
        this returns what it was asked for, and the tests assert the list
        stayed empty, which turns an unexercised key into a claim.
        """
        slept.append(seconds)
        return seconds

    return {
        "path": str(Path(tempfile.mkdtemp()) / "t.db"),
        "shutdown": "replicate",
        "primary": "libsql://somewhere",
        "replicate_once": _replicate_once,
        "refresh_once": _refresh_once,
        "sleep": _sleep,
        "retry_delay_s": 0.0,
    }


def test_a_signal_with_no_running_loop_replicates() -> None:
    """The deployment case: a synchronous process taking SIGTERM."""
    replicated: list[str] = []
    slept: list[float] = []
    restore = trap_shutdown(_a_database_that_records_its_replication(replicated, slept))["untrap"]
    try:
        signal.raise_signal(signal.SIGTERM)
    finally:
        restore()

    assert replicated == ["replicated"], (
        "SIGTERM arrived and nothing replicated, so a deploy would lose "
        "whatever this process had written"
    )
    assert slept == [], f"the primary was current and the handler waited: {slept}"


def test_a_signal_inside_a_running_loop_replicates() -> None:
    """The other case: the process is already serving requests."""
    replicated: list[str] = []

    slept: list[float] = []

    async def _take_the_signal() -> None:
        restore = trap_shutdown(_a_database_that_records_its_replication(replicated, slept))[
            "untrap"
        ]
        try:
            signal.raise_signal(signal.SIGTERM)
            # The handler hands the work to the running loop as a task, so it
            # has not run yet at the moment the signal returns.
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        finally:
            restore()

    asyncio.run(_take_the_signal())

    assert replicated == ["replicated"], (
        "SIGTERM arrived while a loop was running and nothing replicated"
    )
    assert slept == [], f"the primary was current and the handler waited: {slept}"


def test_a_closed_loop_is_not_mistaken_for_a_stopped_one() -> None:
    """The defect this file was written for, stated as its own case.

    A closed loop left as the current one is what a test suite leaves behind
    all the time. `get_event_loop` hands it back, `is_running()` says False,
    and the old code ran `run_until_complete` on it, which raises.
    """
    closed = asyncio.new_event_loop()
    closed.close()
    asyncio.set_event_loop(closed)
    try:
        replicated: list[str] = []
        slept: list[float] = []
        restore = trap_shutdown(_a_database_that_records_its_replication(replicated, slept))[
            "untrap"
        ]
        try:
            signal.raise_signal(signal.SIGTERM)
        finally:
            restore()

        assert replicated == ["replicated"], (
            "a closed loop was left current and the handler failed to "
            "replicate, which is the shape that raises at the worst moment"
        )
    finally:
        asyncio.set_event_loop(None)


async def test_closing_stops_the_listening_it_started() -> None:
    """The handler comes off with the database, and nothing has to remember.

    MEASURED 2026-08-27, BEFORE THE FIX. `trap_shutdown` handed its undo back
    as a loose callable and both openers threw it away, so `close` had no way
    to reach it and never tried:

        closed flag: True
        handler still ours after close: True
        replicate ran on the CLOSED database: True

    The handler's closure holds the value as it was at open, so it cannot see
    the `closed` flag either. A process that opened and closed three databases
    kept three handlers, none of which ever came off, and a SIGTERM after any
    close drove a database whose connections had been released.
    """
    from declaro_persistum.database import close

    replicated: list[str] = []
    slept: list[float] = []
    db = _a_database_that_records_its_replication(replicated, slept)
    db = {
        **db,
        "flush": _did_nothing,
        "release": _did_nothing,
        "closed": False,
        "untrap": nothing_was_trapped,
    }

    # A HARMLESS CATCHER STANDS IN FOR THE HOST'S OWN HANDLER, and it has to
    # be installed BEFORE the trap. The signal raised below arrives after the
    # database is closed and ours is gone, so without this the default action
    # runs and terminates the test runner. It did, once, on the first version
    # of this test: the run stopped at 75% with no failure and no output.
    caught: list[str] = []
    original = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGTERM, lambda *_: caught.append("host"))
    host = signal.getsignal(signal.SIGTERM)
    try:
        trapped = trap_shutdown(db)
        assert signal.getsignal(signal.SIGTERM) is not host, "nothing trapped"

        closed = await close(trapped)

        assert signal.getsignal(signal.SIGTERM) is host, (
            "close left this database's signal handler installed"
        )
        assert closed["untrap"] is nothing_was_trapped, (
            "the closed value still claims to have something to untrap, so a "
            "second close would remove a handler it does not own"
        )

        replicated.clear()
        signal.raise_signal(signal.SIGTERM)
        assert replicated == [], "a SIGTERM after close replicated a database that was released"
        assert caught == ["host"], "the host's own handler did not run, so the signal went nowhere"
    finally:
        signal.signal(signal.SIGTERM, original)


async def _did_nothing(_db: object) -> int:
    """Close calls flush and release; neither is what this test is about."""
    return 0
