"""Bind `tests/bdd/features/observability/log_write.feature`.

WHAT THE FEATURE ASKS FOR. Eight results about the one place this package
writes a line. Three of them are about what never reaches it: a function that
cannot do its job hands the fault back, the pure core writes nothing, and the
only failure that gets logged is one whose caller has already gone. The other
five are about what an operator finds on the line once `boundary_out fn
log_write` has written it.

THE LOG IS READ, NOT STOOD IN FOR. Every assertion here reads `caplog`, which
is pytest capturing the standard library's own records. A stand-in logger would
prove that the stand-in recorded what it was handed, which is a fact about the
stand-in. The defect this function exists for lived in the gap between what a
caller handed the writer and what the standard library received, so that gap is
exactly what may not be faked away.

AN EVENT IS FACTS. `log_write` takes `dict[str, Value]`, so a value is a
string, a number, a bool or nothing. Nothing here builds an exception, and
nothing here formats a sentence for it to write; the caller names facts and the
line carries them.
"""

from __future__ import annotations

import logging

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.budget_spent import budget_spent
from declaro_persistum.engine_of import engine_of
from declaro_persistum.log_write import LOGGER_NAME, NAMED_LEVELS, log_write
from declaro_persistum.next_wait import next_wait
from declaro_persistum.types import Value
from declaro_persistum.worth_retrying import worth_retrying

scenarios("../features/observability/log_write.feature")


# A failure, as the three strings a `Fault` is made of. The feature says a
# failure reaches the line as its own three words, so the three are named once
# here and every scenario that writes one reads them from here.
A_FAILURE: dict[str, Value] = {
    "kind": "conflict",
    "what_went_wrong": "two writers changed the same row",
    "what_to_do_instead": "read the row again and send the change once more",
}

THE_DATABASE = "libsql://orders-app.turso.io"

# A ticket whose caller stopped waiting for it. Nothing releases it, so the
# drainer has nowhere to put the outcome.
ABANDONED_TICKET = "t-9f31"

THE_WORDS = "the disk filled"

# A target no engine reads, so `fn engine_of` answers with a fault rather than
# an engine. The feature asks what a function does when it cannot do its job,
# and this is a function that cannot.
A_TARGET_NO_ENGINE_READS = "mysql://localhost/orders"

WHAT_A_FAULT_IS_MADE_OF = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


# The three events the feature writes on its own, each under the sentence that
# asks for it. Three `When` steps differing only in the dict they build would be
# one function written three times.
AN_EVENT_IS_WRITTEN: dict[str, dict[str, Value]] = {
    f'a warning is written saying "{THE_WORDS}"': {
        "level": "warning",
        "saying": THE_WORDS,
    },
    "a line is written at a level the vocabulary does not hold": {
        "level": "shout",
        "saying": THE_WORDS,
    },
    # Every name here is one the standard library keeps for itself on a record.
    # A writer that handed them over as record attributes would raise on the
    # first one; a caller naming a fact `message` is naming a fact.
    "a line is written with a fact whose name is one the logging vocabulary uses": {
        "message": "the push did not finish",
        "name": "orders",
        "levelname": "warning",
    },
}

# What each sentence adds to the failure before it is written.
WRITTEN_BESIDE: dict[str, dict[str, Value]] = {
    "that failure is written beside the name of the database it happened on": {
        "database": THE_DATABASE,
    },
    "that failure is written": {},
}

# What each sentence says has to be readable afterwards.
THE_LINE_NAMES: dict[str, tuple[Value, ...]] = {
    "the line names all three and the database": (*A_FAILURE.values(), THE_DATABASE),
    "the line names all three": tuple(A_FAILURE.values()),
}


@pytest.fixture(autouse=True)
def _every_line_is_kept(caplog: pytest.LogCaptureFixture) -> None:
    """Capture every level, so a quiet line is a line that was not written.

    Without this the root logger drops anything below warning before `caplog`
    sees it, and a scenario asserting that nothing was written would pass on a
    package that wrote at info.
    """
    caplog.set_level(logging.DEBUG)


def _lines(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """Every record this package wrote.

    pytest-bdd narrates its own fixture wiring at debug on a logger of its own,
    and capturing everything at debug catches all of it. The question every
    scenario here asks is what THIS package wrote, so that is what is read.
    """
    return [record for record in caplog.records if record.name.startswith(LOGGER_NAME)]


def _one_line(caplog: pytest.LogCaptureFixture) -> logging.LogRecord:
    """The single record this scenario wrote, or a failure saying how many."""
    written = _lines(caplog)
    assert len(written) == 1, (
        f"one line was written and the log holds {len(written)}: "
        f"{[record.getMessage() for record in written]}"
    )
    return written[0]


@when("a function cannot do its job")
def _a_function_cannot_do_its_job(scenario: dict) -> None:
    scenario["answer"] = engine_of(A_TARGET_NO_ENGINE_READS)


@then("the fault came back to its caller")
def _the_fault_came_back_to_its_caller(scenario: dict, caplog: pytest.LogCaptureFixture) -> None:
    answer = scenario["answer"]
    assert set(answer) >= WHAT_A_FAULT_IS_MADE_OF, (
        f"{A_TARGET_NO_ENGINE_READS!r} names no engine and the answer was "
        f"{answer!r}; a caller cannot tell that from an engine it could use"
    )
    assert not _lines(caplog), (
        "the fault came back AND a line was written, so the function has two "
        f"outputs and only one of them is in its signature: "
        f"{[record.getMessage() for record in _lines(caplog)]}"
    )


# NO CREW IS NEEDED HERE AND ONE USED TO BE ASKED FOR. Changed 2026-09-23: this
# step took an empty crew and asserted the ticket was not in it, which is true of
# every empty crew by construction. What the scenario is about is `log_write`
# being handed a failure with nobody left to return it to, and that is a fact
# about the event, not about where the write came from.
@given("a write whose caller abandoned its ticket")
def _a_write_whose_caller_has_gone(scenario: dict) -> None:
    scenario["abandoned"] = {"ticket": ABANDONED_TICKET, **A_FAILURE}


@when("it fails")
def _the_abandoned_write_fails(scenario: dict) -> None:
    log_write(scenario["abandoned"])


@then("the failure is logged, because there is nobody left to return it to")
def _the_abandoned_failure_reached_the_log(
    scenario: dict, caplog: pytest.LogCaptureFixture
) -> None:
    written = _one_line(caplog).getMessage()
    unsaid = sorted(
        name for name, value in scenario["abandoned"].items() if str(value) not in written
    )
    assert not unsaid, (
        f"nobody is waiting for {ABANDONED_TICKET!r}, so the line is the last "
        f"place this failure can be seen, and it does not say {unsaid}: {written!r}"
    )


# FIVE PURE FUNCTIONS THAT NEED NOTHING BUT VALUES. Changed 2026-09-23: three of
# the five took a `Database`, and the only honest way to get one is to open a
# database, which is the one thing this scenario must not do. These five answer
# from their arguments alone, so a log line from any of them is I/O nothing in
# the signature admits to.
@when("the pure core runs")
def _the_pure_core_runs(scenario: dict) -> None:
    scenario["answers"] = [
        engine_of("sqlite:///orders.db"),
        engine_of(A_TARGET_NO_ENGINE_READS),
        worth_retrying({"kind": "conflict", "what_went_wrong": "x", "what_to_do_instead": "y"}),
        budget_spent(4, 3),
        next_wait(2, 1_700_000_000.0, "a-ticket"),
    ]


@then("nothing in it wrote a line")
def _the_pure_core_wrote_no_line(scenario: dict, caplog: pytest.LogCaptureFixture) -> None:
    assert len(scenario["answers"]) == 5, (
        "no pure function ran, so an empty log says nothing about the core"
    )
    wrote = [f"{record.levelname} {record.getMessage()}" for record in _lines(caplog)]
    assert not wrote, (
        "a function in the pure core reached outside the process to write "
        f"these, which is I/O its signature does not mention: {wrote}"
    )


@when(
    parsers.re(
        r"(?P<sentence>a warning is written saying \"the disk filled\""
        r"|a line is written at a level the vocabulary does not hold"
        r"|a line is written with a fact whose name is one the logging vocabulary uses)"
    )
)
def _a_line_is_written(scenario: dict, sentence: str) -> None:
    scenario["event"] = AN_EVENT_IS_WRITTEN[sentence]
    log_write(scenario["event"])


@then('the line reads "the disk filled" and its level is warning')
def _the_line_reads_the_words_at_the_level_named(caplog: pytest.LogCaptureFixture) -> None:
    written = _one_line(caplog)
    assert THE_WORDS in written.getMessage(), (
        f"the caller said {THE_WORDS!r} and the line reads {written.getMessage()!r}"
    )
    assert written.levelname == "WARNING", (
        f"the caller named warning and the line went out at {written.levelname}, "
        "so an operator filtering on level never sees it"
    )


@then("it is refused, naming the levels there are")
def _an_undeclared_level_is_refused(scenario: dict, caplog: pytest.LogCaptureFixture) -> None:
    written = _one_line(caplog).getMessage()
    unnamed = sorted(str(word) for word in NAMED_LEVELS if str(word) not in written)
    assert not unnamed, (
        f"the caller named a level nobody declared and the line does not say "
        f"which levels there are; {unnamed} are missing from {written!r}"
    )
    assert str(scenario["event"]["level"]) in written, (
        f"the line refuses a level without saying which one was asked for: {written!r}"
    )


@then("the line reads that name and its value, like any other fact")
def _a_reserved_name_arrives_like_any_other_fact(
    scenario: dict, caplog: pytest.LogCaptureFixture
) -> None:
    written = _one_line(caplog).getMessage()
    lost = sorted(
        f"{name}={value}"
        for name, value in scenario["event"].items()
        if name not in written or str(value) not in written
    )
    assert not lost, (
        f"these facts were handed over and did not arrive on the line, so a "
        f"name the logging vocabulary uses costs the caller its fact: {lost} in {written!r}"
    )


@given("a failure carrying its kind, what went wrong and what to do instead")
def _a_failure_carrying_its_three_words(scenario: dict) -> None:
    assert set(A_FAILURE) == WHAT_A_FAULT_IS_MADE_OF, (
        f"the two scenarios below say the line names all three, and this "
        f"failure carries {sorted(A_FAILURE)}"
    )
    scenario["failure"] = dict(A_FAILURE)


@when(
    parsers.re(
        r"(?P<sentence>that failure is written beside the name of the database it happened on"
        r"|that failure is written)"
    )
)
def _that_failure_is_written(scenario: dict, sentence: str) -> None:
    log_write({**scenario["failure"], **WRITTEN_BESIDE[sentence]})


@then(
    parsers.re(r"(?P<sentence>the line names all three and the database|the line names all three)")
)
def _the_line_names_what_the_caller_handed_it(
    sentence: str, caplog: pytest.LogCaptureFixture
) -> None:
    written = _one_line(caplog).getMessage()
    missing = sorted(str(word) for word in THE_LINE_NAMES[sentence] if str(word) not in written)
    assert not missing, (
        f"an operator reading this line cannot see {missing}, which the caller "
        f"handed over: {written!r}"
    )
