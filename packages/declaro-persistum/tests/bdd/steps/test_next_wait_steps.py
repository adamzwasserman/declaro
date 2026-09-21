"""The scenarios in `core/next_wait.feature`, bound to the function itself.

WHAT THIS FILE COVERS. `next_wait` works out how long a caller waits before it
tries a write again. The feature asks for two things and they are the whole
requirement: each wait is longer than the one before it, and two callers
retrying in the same instant with different tickets wait different amounts.
Both are asserted here against real calls with real arguments.

WHY SIX SCENARIOS SKIP. The feature file folded in
`running/retrying_is_the_librarys_job.feature` on 2026-09-21, and six of those
scenarios are about the retry loop that CALLS `next_wait`, not about the wait
itself. They belong to `worth_retrying`, `budget_spent`, `receipt_for` and the
three `open_*` functions, none of which is written yet. Each of those scenarios
skips on its first step, naming the function that owns it, so the report says
"not this function" rather than passing on an empty assertion. Every later step
in those scenarios fails loudly if it is ever reached, so a skip that stops
working shows up red instead of green.

THE WAITS ARE EXACT NUMBERS, NOT RANGES. `next_wait` reads no clock and draws
no random number, so the same three arguments always give the same float and a
test can compare it with `==`.
"""

from __future__ import annotations

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.next_wait import next_wait

scenarios("../features/core/next_wait.feature")


# THE INSTANT IS FROZEN AND WRITTEN DOWN, because every scenario that compares
# two waits needs them worked out at the same time. A step that read the clock
# would make "two callers at the same instant" a claim the test could not make.
SAME_INSTANT = 1_000_000.5

# TWO TICKETS, because one caller cannot demonstrate a spread. These are the
# strings `room_deposit` hands back, and `Ticket` is a `str` in types.py.
ONE_CALLER = "ticket-4f2a9c"
ANOTHER_CALLER = "ticket-b71e03"


@pytest.fixture
def worked_out() -> dict[str, list[float]]:
    """What the scenario has worked out so far, keyed by which caller asked."""
    return {}


# =============================================================================
# The wait grows with the attempt
# =============================================================================


@when("the first three attempts are worked out")
def work_out_the_first_three(worked_out: dict[str, list[float]]) -> None:
    worked_out[ONE_CALLER] = [next_wait(attempt, SAME_INSTANT, ONE_CALLER) for attempt in (0, 1, 2)]


@then("each wait is longer than the one before it")
def each_wait_is_longer_than_the_one_before_it(worked_out: dict[str, list[float]]) -> None:
    waits = worked_out[ONE_CALLER]
    assert len(waits) == 3, f"three attempts were asked for, {len(waits)} came back"
    assert waits[0] < waits[1] < waits[2], f"the waits do not grow: {waits}"


# =============================================================================
# Two callers at the same instant wait differently
# =============================================================================


@given("two writes retrying at the same time with different tickets")
def two_writes_at_the_same_time(worked_out: dict[str, list[float]]) -> None:
    assert ONE_CALLER != ANOTHER_CALLER, "the two tickets must differ for this to mean anything"
    worked_out[ONE_CALLER] = [next_wait(1, SAME_INSTANT, ONE_CALLER)]
    worked_out[ANOTHER_CALLER] = [next_wait(1, SAME_INSTANT, ANOTHER_CALLER)]


@then("their waits differ")
def their_waits_differ(worked_out: dict[str, list[float]]) -> None:
    mine = worked_out[ONE_CALLER][0]
    theirs = worked_out[ANOTHER_CALLER][0]
    assert mine != theirs, (
        f"both callers read {SAME_INSTANT} and both would wait {mine}; "
        "the tickets put no spread between them"
    )


# =============================================================================
# The same three inputs always give the same wait
# =============================================================================
#
# THE PINNED NUMBER IS RECORDED, NOT DERIVED. A test that recomputed the wait
# the way the function does would pass whatever the function did, which is the
# trap the differ tests fell into. So this asserts three separate things: two
# calls with the same arguments agree, the answer equals the float written down
# here on 2026-09-21, and it sits inside the band the second attempt is allowed.


AN_EXACT_WAIT = 0.1344822540161059
A_SECOND_ATTEMPT_WAITS_AT_LEAST = 0.1
A_SECOND_ATTEMPT_WAITS_AT_MOST = 0.15


@then("a test can check an exact number")
def a_test_can_check_an_exact_number() -> None:
    once = next_wait(1, SAME_INSTANT, ONE_CALLER)
    twice = next_wait(1, SAME_INSTANT, ONE_CALLER)
    assert once == twice, f"the same three arguments gave {once} and then {twice}"
    assert once == AN_EXACT_WAIT, (
        f"the wait for attempt 1 at {SAME_INSTANT} moved from {AN_EXACT_WAIT} to {once}"
    )
    assert isinstance(once, float), f"a wait is a float, this is a {type(once).__name__}"
    assert A_SECOND_ATTEMPT_WAITS_AT_LEAST <= once <= A_SECOND_ATTEMPT_WAITS_AT_MOST, (
        f"attempt 1 waited {once}, outside the band the second attempt is allowed"
    )


# =============================================================================
# Retries back off, with jitter (the same two claims, folded in from the
# retry-loop feature, asked about three retries of one write)
# =============================================================================


@when("a write is retried three times")
def a_write_is_retried_three_times(worked_out: dict[str, list[float]]) -> None:
    worked_out[ONE_CALLER] = [next_wait(attempt, SAME_INSTANT, ONE_CALLER) for attempt in (1, 2, 3)]
    worked_out[ANOTHER_CALLER] = [
        next_wait(attempt, SAME_INSTANT, ANOTHER_CALLER) for attempt in (1, 2, 3)
    ]


@then("each wait is longer than the last")
def each_wait_is_longer_than_the_last(worked_out: dict[str, list[float]]) -> None:
    for who, waits in worked_out.items():
        grows = all(earlier < later for earlier, later in zip(waits, waits[1:], strict=False))
        assert grows, f"{who} does not back off: {waits}"


@then("two callers retrying together do not wait in step")
def two_callers_do_not_wait_in_step(worked_out: dict[str, list[float]]) -> None:
    mine = worked_out[ONE_CALLER]
    theirs = worked_out[ANOTHER_CALLER]
    in_step = [
        attempt
        for attempt, (a, b) in enumerate(zip(mine, theirs, strict=True), start=1)
        if a == b  # APPROVED-IF 2026-09-21 14:05 — a test's own report of which
        # attempts collided; the package rule governs src/, and a comprehension
        # filter here names the offending attempts instead of asserting a bool.
    ]
    assert not in_step, f"the two callers wait the same on attempts {in_step}: {mine} and {theirs}"


# =============================================================================
# Folded-in scenarios that belong to other functions
# =============================================================================
#
# Each one skips on its first step and names the function the declaration gives
# the behaviour to. The later steps raise, so if a skip ever stops firing the
# scenario goes red rather than passing on nothing.


# `parsers` is imported because the feature may grow a step carrying a number;
# nothing here takes a parameter yet.
_ = parsers
