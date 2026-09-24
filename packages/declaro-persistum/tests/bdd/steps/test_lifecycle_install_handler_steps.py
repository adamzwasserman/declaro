"""Bind `tests/bdd/features/lifecycle/lifecycle_install_handler.feature`.

WHAT THE FEATURE ASKS. The first open installs a handler, a second open leaves
it alone, a host's own handler still runs, and what this library owes runs
before that host handler is reached.

EVERY SCENARIO ASKS THE PLATFORM, NOT THE MODULE. What is in a signal slot is
the operating system's answer, and this file reads it with `signal.getsignal`.
Rewritten 2026-09-24 because it used to read a module dict and a module handler
object by name: both are gone, and both were the module's own bookkeeping rather
than anything a caller can see.

THE SIGNALS ARE PUT BACK AFTERWARDS. They belong to the process, not to one
scenario, and a scenario that left ours in the slot would send the next test's
shutdown through this library.
"""

from __future__ import annotations

import signal
from types import FrameType

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.lifecycle_install_handler import (
    WHAT_THE_PLATFORM_SENDS,
    lifecycle_install_handler,
)

scenarios("../features/lifecycle/lifecycle_install_handler.feature")


THE_SIGNAL_A_SCENARIO_SENDS = signal.SIGTERM


@pytest.fixture
def scenario():
    """One scenario, with every signal slot put back as it was found."""
    held = {"ran": []}
    was = {number: signal.getsignal(number) for number in WHAT_THE_PLATFORM_SENDS}
    yield held
    for number, previous in was.items():
        signal.signal(number, previous)


def _a_host_handler(ran: list[str]):
    """What a host application had in the slot before this library arrived."""

    def theirs(_number: int, _frame: FrameType | None) -> None:
        ran.append("the host's handler")

    return theirs


@given("a host application with its own handler")
def _a_host_with_its_own(scenario: dict) -> None:
    signal.signal(THE_SIGNAL_A_SCENARIO_SENDS, _a_host_handler(scenario["ran"]))


def _install(scenario: dict) -> None:
    answered = lifecycle_install_handler()
    assert answered is None, f"installing the handler was refused: {answered}"
    scenario["installed"] = [signal.getsignal(n) for n in WHAT_THE_PLATFORM_SENDS]


# TWO SENTENCES FOR ONE INSTALL. The feature says it one way where a database is
# being opened and another where the handler is the subject, and both mean the
# same call, so they are one function over the pair.
@when(parsers.re(r"^(?:a replicating database is opened|the handler is installed)$"))
def _the_handler_goes_in(scenario: dict) -> None:
    _install(scenario)


@when("a second replicating database is opened")
def _a_second_open(scenario: dict) -> None:
    """Install twice, and keep what was in the slot after each.

    THE SECOND INSTALL IS THE ONE THAT CAN GO WRONG. Reading the platform again
    finds this library's own handler, and a new handler built around that one
    would call it, so every signal would run the shutdown twice and then twice
    again.
    """
    _install(scenario)
    after_one = list(scenario["installed"])
    _install(scenario)
    scenario["after_one"] = after_one


@then("a signal handler is installed")
def _a_handler_is_installed(scenario: dict) -> None:
    """Asked of the platform: whatever is in the slot is not what was there."""
    assert all(callable(handler) for handler in scenario["installed"]), (
        f"the platform holds {scenario['installed']} for the signals that end a "
        f"process, so nothing will push the local commits on the way out"
    )
    assert signal.SIG_DFL not in scenario["installed"], (
        "a signal slot still holds the platform's default, which ends the "
        "process without pushing anything"
    )


@then("one handler is installed, not two")
def _one_handler_not_two(scenario: dict) -> None:
    """The same object is in the slot after the second install as after the first.

    A NEW ONE WOULD CHAIN TO THE OLD ONE, and the shutdown would run twice.
    """
    assert scenario["installed"] == scenario["after_one"], (
        f"the second open replaced the handler: the slots held "
        f"{scenario['after_one']} and now hold {scenario['installed']}. A new "
        f"handler built around the old one runs the shutdown twice."
    )


@then("the host's handler still runs")
def _the_hosts_handler_still_runs(scenario: dict) -> None:
    """A library that takes the slot and keeps it has changed how a program
    shuts down, and the program only asked for a database."""
    _send_the_signal(scenario)

    assert "the host's handler" in scenario["ran"], (
        f"the signal ran {scenario['ran']} and never reached the handler the "
        f"host application had installed, so this library silently changed how "
        f"that program shuts down"
    )


@then("replication is asked for before the host's handler is reached")
def _the_library_goes_first(scenario: dict) -> None:
    """A host handler is free to end the process, and one that does would take
    the local commits with it."""
    _send_the_signal(scenario)

    assert scenario["ran"].index("the door") < scenario["ran"].index("the host's handler"), (
        f"the signal ran {scenario['ran']}. A host handler that calls exit would "
        f"end the process before anything was pushed."
    )


def _send_the_signal(scenario: dict) -> None:
    """Call what the platform holds, which is what the platform would call.

    THE DOOR IS REPLACED FOR THIS, and only the door. What `lifecycle_stop` does
    is its own card's business, and running a real shutdown here would need a
    real database; what this file is about is the order the handler runs things
    in.
    """
    from declaro_persistum import lifecycle_install_handler as under_test

    was = under_test.the_door

    def the_door(_number: int, _frame: FrameType | None) -> None:
        scenario["ran"].append("the door")

    under_test.the_door = the_door  # type: ignore[assignment]
    try:
        signal.getsignal(THE_SIGNAL_A_SCENARIO_SENDS)(THE_SIGNAL_A_SCENARIO_SENDS, None)  # type: ignore[operator]
    finally:
        under_test.the_door = was  # type: ignore[assignment]
