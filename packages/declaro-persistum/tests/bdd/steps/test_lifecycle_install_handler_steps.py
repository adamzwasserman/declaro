"""Bind `tests/bdd/features/lifecycle/lifecycle_install_handler.feature`, and nothing else.

WHAT THE FEATURE ASKS FOR. `lifecycle_install_handler() -> NOTHING | Fault` takes
nothing and puts one handler in front of the signals the platform sends. Four
sentences: a handler is there after the first open, there is still one after the
second, a handler the host application installed before us still runs, and the
library gets its replication in before that handler is reached.

THE FOURTH SCENARIO WAS ADDED 2026-09-22, while this was being written. The
feature said the handler chains and did not say in which order, and the order is
where the data is lost: a host handler is free to call `sys.exit`, and one that
does ends the process wherever the library had got to. Nothing else in the suite
states it.

WHY THE WHEN SAYS "OPENED" AND THIS FILE CALLS ONE FUNCTION. `orchestrator fn
session_connect` is what opens a database, and it invokes
`lifecycle_install_handler` while it does. Nobody else calls it and no caller
asks for it, so the only thing an open does about signals is this one call, and
that call is what each When makes. A step that opened a real database would be a
test of `orchestrator fn session_connect`, which is not written and is not this
card.

WHY THE WHENS ARE ONE FUNCTION. They differ in how many opens have happened and
in nothing else, so the count is a table and the body is written once.

WHY THE SIGNALS ARE REAL. `signal.signal` and `signal.getsignal` work in the main
thread of a test process, so the handler goes in for real and is read back for
real. Nothing here sends a signal: sending SIGTERM would run the library's
shutdown and then the test runner's own handler, and the process under test is
the test runner. An autouse fixture puts back every handler that was there and
empties what the module recorded, so the session ends holding what it started
with.

WHAT THIS FILE CANNOT RUN END TO END. The handler that goes in runs two steps in
order: the door, then whatever was there before. `boundary_in fn lifecycle_stop`
is the door and it is not written, so calling the installed handler would fail on
an import rather than on anything this card decides. The order is module data
rather than two lines of a body, so the fourth Then reads that order and the
third runs the chain for real against the host's own function. That is the third
sentence's claim, the host's handler still runs, proved by running it.
"""

from __future__ import annotations

import signal
from types import FrameType

import pytest
from pytest_bdd import given, scenarios, then, when

from declaro_persistum.lifecycle_install_handler import (
    IN_ORDER,
    THE_HANDLER,
    WHAT_THE_PLATFORM_SENDS,
    WHAT_WAS_THERE,
    lifecycle_install_handler,
    the_door,
    whatever_was_there,
)
from declaro_persistum.types import NOTHING

scenarios("../features/lifecycle/lifecycle_install_handler.feature")


@pytest.fixture(autouse=True)
def the_process_keeps_its_own_handlers(scenario: dict[str, object]):
    """Record what the test runner had, and give it back afterwards.

    A signal handler is process-wide state. A test that left ours in would hand
    every later test a SIGINT that runs shutdown replication, so this is not
    tidiness: it is the difference between a suite and a process that stops
    answering Ctrl-C the way pytest installed it to.
    """
    had = {number: signal.getsignal(number) for number in WHAT_THE_PLATFORM_SENDS}
    scenario["what the process had"] = had
    yield
    for number, handler in had.items():
        signal.signal(number, handler)
    WHAT_WAS_THERE.clear()


# --- the host application's own handler ------------------------------------


@given("a host application with its own handler")
def _the_host_already_installed_one(scenario: dict[str, object]) -> None:
    """A real function on both signals, recording every signal it is handed.

    It is a plain callable installed through `signal.signal`, which is what a
    host application's handler is. Nothing here stands in for it.
    """
    seen: list[int] = []

    def the_hosts_handler(number: int, _frame: FrameType | None) -> None:
        seen.append(number)

    scenario["what the host's handler saw"] = seen
    scenario["the host's handlers"] = dict.fromkeys(WHAT_THE_PLATFORM_SENDS, the_hosts_handler)
    for number in WHAT_THE_PLATFORM_SENDS:
        signal.signal(number, the_hosts_handler)


# --- the opens -------------------------------------------------------------
#
# One function over a table. The three sentences differ in how many opens have
# happened and in nothing else, so the count is the table and the body is
# written once.

HOW_MANY_OPENS: dict[str, int] = {
    "a replicating database is opened": 1,
    "a second replicating database is opened": 2,
    "the handler is installed": 1,
}


def opening_a_database(how_many: int):
    """Build the When that opens that many databases.

    An open's whole business with signals is the one call below, so the step
    makes that call and records what came back and what the platform held after
    each one. Every assertion stays in the Then.
    """

    def step(scenario: dict[str, object]) -> None:
        came_back: list[object] = []
        held: list[dict[int, object]] = []
        for _open in range(how_many):
            came_back.append(lifecycle_install_handler())
            held.append({number: signal.getsignal(number) for number in WHAT_THE_PLATFORM_SENDS})
        scenario["what came back"] = came_back
        scenario["what the platform held after each open"] = held

    return step


for sentence, opens in HOW_MANY_OPENS.items():
    when(sentence)(opening_a_database(opens))


# --- what has to be true afterwards ----------------------------------------


@then("a signal handler is installed")
def _a_handler_is_in_front_of_the_signals(scenario: dict[str, object]) -> None:
    came_back = scenario["what came back"]
    assert came_back == [NOTHING], (
        f"installing the handler came back with a refusal rather than nothing: {came_back}"
    )

    held = scenario["what the platform held after each open"][-1]
    assert set(held) == set(WHAT_THE_PLATFORM_SENDS), (
        f"the platform sends {sorted(WHAT_THE_PLATFORM_SENDS)} and the handler "
        f"only covers {sorted(held)}"
    )
    for number, handler in held.items():
        assert handler is THE_HANDLER, (
            f"signal {number} is still answered by {handler!r}, so the process "
            "would die on it with local commits the primary has never seen"
        )
        assert handler is not signal.SIG_DFL, f"signal {number} still has the platform default"
        assert handler is not signal.SIG_IGN, f"signal {number} is ignored rather than handled"


@then("one handler is installed, not two")
def _the_second_open_changed_nothing(scenario: dict[str, object]) -> None:
    after_first, after_second = scenario["what the platform held after each open"]

    assert after_first == after_second, (
        f"the second open replaced the handler the first one put in: "
        f"{after_first} then {after_second}"
    )

    # THE BUG A SECOND INSTALL CAN WRITE, and the only one worth a test here.
    # The handler chains to whatever it found. A second install that read the
    # platform again would find OUR handler and record it as what was there, so
    # every signal would run the library's shutdown, reach the chain, and run
    # the library's shutdown again. Reading the platform twice cannot see this;
    # reading what the module kept can.
    had = scenario["what the process had"]
    assert had == WHAT_WAS_THERE, (
        f"the second open recorded {WHAT_WAS_THERE} as what was there before, "
        f"and the process had {had}. A handler chained to itself runs shutdown "
        "once per install."
    )
    for number, previous in WHAT_WAS_THERE.items():
        assert previous is not THE_HANDLER, (
            f"signal {number} chains back to the library's own handler, which is "
            "a loop rather than a chain"
        )


@then("the host's handler still runs")
def _the_hosts_handler_is_reached(scenario: dict[str, object]) -> None:
    for number in WHAT_THE_PLATFORM_SENDS:
        assert signal.getsignal(number) is THE_HANDLER, (
            f"the library did not take signal {number}, so nothing replicates on it"
        )

    assert scenario["the host's handlers"] == WHAT_WAS_THERE, (
        f"the host's handler was dropped rather than kept: the module recorded {WHAT_WAS_THERE}"
    )

    # The claim itself: run the chain and watch the host's own function run.
    # The chain answers whether it found a handler of the host's to run, so a
    # signal that quietly ran nothing is a False rather than a silence.
    ran = [whatever_was_there(number, NOTHING) for number in WHAT_THE_PLATFORM_SENDS]
    assert all(ran), f"the chain found no host handler to run on some signal: {ran}"

    saw = scenario["what the host's handler saw"]
    assert saw == list(WHAT_THE_PLATFORM_SENDS), (
        f"the host's handler did not run for every signal the library took: it saw {saw}"
    )


@then("replication is asked for before the host's handler is reached")
def _the_door_comes_first(scenario: dict[str, object]) -> None:
    """The order the signal runs things in, which is module data a reader can see.

    A host handler may call `sys.exit` or `os._exit`. One that does ends the
    process where it stands, so everything the library owes has to have happened
    by the time the chain is reached, and the chain has to be last.
    """
    steps = list(IN_ORDER)

    assert steps[0] is the_door, (
        f"the door is not the first step, so a signal would reach the host's "
        f"handler with local commits still local: {steps}"
    )
    assert steps[-1] is whatever_was_there, f"the chain is not the last step: {steps}"
    assert steps.index(the_door) < steps.index(whatever_was_there), (
        f"the host's handler is reached before the library asks for replication: {steps}"
    )

    # And the handler the platform holds is the one that runs those steps, so the
    # order above is the order a real signal would take.
    for number in WHAT_THE_PLATFORM_SENDS:
        assert signal.getsignal(number) is THE_HANDLER, (
            f"signal {number} is answered by something other than the handler "
            "that runs those steps, so their order decides nothing"
        )
