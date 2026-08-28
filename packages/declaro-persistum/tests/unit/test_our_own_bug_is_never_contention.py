"""A bug of ours is not the database saying "not now".

`is_contention` decides whether a failed write is retried. It classifies by
MESSAGE TEXT:

    return "busy" in text or "sqlite_busy" in text or "conflict" in text

MEASURED 2026-08-17:

    KeyError('busy')                -> True
    AttributeError('busy')          -> True
    TypeError('conflict')           -> True
    NameError('conflict resolution')-> True

Every one of those is a bug in this package. Each is deterministic, so the
retry loop in `write_queue.py` runs it again, gets the identical exception,
and runs it again, for `retry["attempts"]` rounds -- and then reports a
database contention failure for something the database never saw.

ASKING THE TYPE IS THE FIX, and this package already has the answer:
`exceptions.A_BUG_IN_THIS_PACKAGE` is AttributeError, IndexError, KeyError,
NameError and TypeError, and five other places catch and re-raise it ahead of
any broad handler. This is the sixth.

WHAT REMAINS TEXT-BASED, stated so the fix is not read as bigger than it is: a
genuine driver error is still recognised by its message, because the three
drivers raise three unrelated types for the same condition and this package
supports all three. Narrowing that is a separate job. What is fixed here is
the half that could never be right: our own exception types are never the
engine reporting contention, whatever they happen to say.
"""

from __future__ import annotations

import pytest

from declaro_persistum.exceptions import A_BUG_IN_THIS_PACKAGE
from declaro_persistum.retry import is_contention

pytestmark = pytest.mark.precommit

# Each carries a message the text check matches, which is the whole point.
OUR_BUGS = [
    KeyError("busy"),
    AttributeError("busy"),
    IndexError("conflict"),
    NameError("conflict resolution"),
    TypeError("database is busy"),
]


@pytest.mark.parametrize("error", OUR_BUGS, ids=lambda e: type(e).__name__)
def test_a_bug_of_ours_is_not_contention(error: BaseException) -> None:
    """The measured defect, in one assertion."""
    assert is_contention(error) is False, (
        f"{type(error).__name__}({error}) was classified as contention, so a "
        f"deterministic bug is retried until the attempts run out and then "
        f"reported as the database being busy"
    )


def test_the_bug_types_are_the_ones_the_package_already_names() -> None:
    """Not a second list. The same tuple five other handlers use."""
    assert {type(e) for e in OUR_BUGS} <= set(A_BUG_IN_THIS_PACKAGE)


# ---------------------------------------------------------------------------
# What must keep being retried
# ---------------------------------------------------------------------------

REAL_CONTENTION = [
    Exception("database is busy"),
    Exception("SQLITE_BUSY: database is locked"),
    Exception("database tape error: database is busy"),
    RuntimeError("commit conflict detected"),
]


@pytest.mark.parametrize("error", REAL_CONTENTION, ids=lambda e: str(e)[:28])
def test_the_engine_saying_not_now_is_still_contention(error: BaseException) -> None:
    """The floor. Returning False for everything passes every test above."""
    assert is_contention(error) is True, error


def test_an_unrelated_failure_is_still_not_contention() -> None:
    assert is_contention(OSError("connection reset")) is False


@pytest.mark.parametrize("error", OUR_BUGS, ids=lambda e: type(e).__name__)
def test_a_bug_that_says_nothing_about_busy_is_also_refused(
    error: BaseException,
) -> None:
    """The rule is the TYPE, so the message must not be able to rescue it."""
    assert is_contention(type(error)("anything at all")) is False


# ---------------------------------------------------------------------------
# The consequence, in the loop that reads it
# ---------------------------------------------------------------------------
#
# `_append_one` re-runs a write while `is_contention` says so. A KeyError of
# ours is deterministic, so classifying it as contention meant running the
# identical failing call `retry["attempts"]` times, sleeping between each, and
# then handing back a receipt saying the engine stayed busy.


@pytest.mark.asyncio
async def test_a_bug_is_attempted_once_and_raised() -> None:
    """Counted, because "does not retry" is a claim about how many times."""
    from declaro_persistum.retry import ON_CONTENTION
    from declaro_persistum.write_queue import _append_one

    attempts = 0

    async def always_our_bug(_write):
        nonlocal attempts
        attempts += 1
        raise KeyError("busy")

    with pytest.raises(KeyError):
        await _append_one("t1", {"sql": "x", "params": ()}, always_our_bug, ON_CONTENTION)

    assert attempts == 1, f"a deterministic bug was run {attempts} times before being reported"


@pytest.mark.asyncio
async def test_real_contention_is_still_retried_to_the_budget() -> None:
    """The floor. Never retrying anything would pass the test above.

    `ON_CONTENTION` sleeps between attempts, so this asserts the count rather
    than the wall clock.
    """
    from declaro_persistum.retry import ON_CONTENTION
    from declaro_persistum.write_queue import _append_one

    attempts = 0

    async def always_busy(_write):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("database is busy")

    receipt = await _append_one(
        "t1",
        {"sql": "x", "params": ()},
        always_busy,
        {**ON_CONTENTION, "attempts": 3, "base_delay_s": 0.0, "max_delay_s": 0.0},
    )

    assert attempts == 3, attempts
    assert receipt["ok"] is False
    assert "busy" in receipt["error"]
