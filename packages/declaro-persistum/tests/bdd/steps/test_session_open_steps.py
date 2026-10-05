"""Bind `tests/bdd/features/opening/session_open.feature`.

WHAT THE FEATURE ASKS. The handle carries the engine the target named, one file
opens all three engines in any order and names none of them, two opens of one
target share nothing, a primary handed to an engine that cannot replicate is
refused, and a deployment that states neither number is refused by name.

THE DOOR'S OWN ARGUMENTS ARE READ WITH `inspect.signature`, not asserted from
memory. "None of them is an engine or a scheme" is a claim about the signature,
and a test that listed the four names itself would agree with whatever it had
been written to expect.

THE ENVIRONMENT IS SET PER SCENARIO AND PUT BACK. `boundary_in fn session_open`
reads DECLARO_CONNECTIONS and DECLARO_CREW_SIZE, and a scenario that left one
set would decide the next scenario's answer. `monkeypatch` puts both back.

EVERY BOUND SCENARIO OPENS A REAL SQLITE FILE, so the connections that come back
are real ones and "they share no connection" is asked of the objects rather than
of a flag. The crew is stopped and every connection closed inside the loop that
made them, because a drainer is an `asyncio` task and a task started in one loop
cannot be stopped from another.

FIVE SCENARIOS ARE `xfail` AND EACH NAMES WHAT IS MISSING. One of them names a
gap in the declaration rather than in the code: no target spells a Turso
database on this disk with no primary, because `fn engine_of` reads a bare path
as SQLite.
"""

from __future__ import annotations

import asyncio
import inspect
import pathlib
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.crew_stop import crew_stop
from declaro_persistum.session_close_connection import session_close_connection
from declaro_persistum.session_open import CouldNotOpen, session_open

scenarios("../features/opening/session_open.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

# WHAT A DEPLOYMENT THAT SAID BOTH LOOKS LIKE. Every scenario but the two about
# an unset variable runs with these, because `fn permitted_connections` and
# `fn crew_size` refuse a deployment that said nothing and every other scenario
# would then be testing that refusal.
WHAT_THIS_MACHINE_SAYS = {"DECLARO_CONNECTIONS": "4", "DECLARO_CREW_SIZE": "2"}


def _is_fault(piece: object) -> bool:
    """Whether this is a refusal, however it arrived.

    A `Fault` is the three facts in a dict, which is how a refusal travels inside
    the package. `CouldNotOpen` is that same Fault on its way out of the door,
    and it carries it, so both are refusals and the scenarios read them alike.
    """
    return isinstance(piece, CouldNotOpen) or (
        isinstance(piece, dict) and frozenset(piece) == THE_THREE_FACTS_IN_A_FAULT
    )


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


# --- what each sentence sets up ------------------------------------------------
#
# `opens` IS THE TARGETS IN THE ORDER THE SENTENCE NAMES THEM. `unset` is the
# variable this deployment says nothing about. `primary` is what a caller hands
# over beside the target. `why_not` is empty for a scenario that runs and names
# the missing piece for one that cannot.

WHAT_EACH_SENTENCE_OPENS: dict[str, dict] = {
    'a caller opens "postgresql://localhost/todo"': {
        "opens": ["postgresql://localhost/todo"],
        "why_not": "",
    },
    'a caller opens "app.db", then "postgresql://localhost/todo", then '
    '"libsql://team-app.turso.io"': {
        "opens": ["app.db", "postgresql://localhost/todo", "libsql://team-app.turso.io"],
        "why_not": (
            "Needs a reachable PostgreSQL and a reachable Turso primary in one "
            "run. The probe database in `tests/conftest.py` is PostgreSQL only, "
            "and opening a Turso primary needs a token this scenario has no way "
            "to name."
        ),
    },
    "the same three targets are opened in a different order": {
        "opens": ["libsql://team-app.turso.io", "app.db", "postgresql://localhost/todo"],
        "why_not": (
            "The same three engines in one run as the scenario above, for the "
            "same reason. It comes off the day a fixture opens all three."
        ),
    },
    'a caller opens "app.db" and "postgresql://localhost/todo"': {
        "opens": ["app.db", "postgresql://localhost/todo"],
        "why_not": (
            "Needs a reachable PostgreSQL beside a SQLite file. "
            "`tests/conftest.py` has `postgresql_schema`, and nothing yet opens "
            "a probe database through this door."
        ),
    },
    'a caller opens "app.db" twice': {"opens": ["app.db", "app.db"], "why_not": ""},
    'a caller opens "mysql://localhost/todo"': {
        "opens": ["mysql://localhost/todo"],
        "why_not": "",
    },
    'a caller opens "postgresql://unreachable.invalid/todo"': {
        "opens": ["postgresql://unreachable.invalid/todo"],
        "why_not": "",
    },
    'a caller opens "libsql://team-app.turso.io" with a primary': {
        "opens": ["libsql://team-app.turso.io"],
        "primary": "libsql://team-app.turso.io",
        "why_not": (
            "Needs a live Turso primary and a token for it. "
            "`boundary_out fn session_open_channel` proves the channel with one "
            "pull before the open finishes, so a stand-in would say nothing "
            "about whether a channel came back at all."
        ),
    },
    "a caller opens a local Turso with no primary": {
        "opens": ["data.db"],
        "why_not": (
            "No target spells this. `fn engine_of` reads a bare path as SQLite, "
            "because what makes a path SQLite is that it names no scheme, and "
            "`libsql://` names a primary's address rather than a local file. So "
            "`docs/architecture.md` describes a deployment the declaration has "
            "no way to ask for. It needs a card on the .hd before it needs code."
        ),
    },
    'a caller opens "app.db" with a primary': {
        "opens": ["app.db"],
        "primary": "libsql://team-app.turso.io",
        "why_not": "",
    },
    # THE ONLY ROW THAT LEAVES AN ARGUMENT OFF. `shutdown` has no default, so
    # Python refuses the call before the body runs, and that is the whole of the
    # check: a default here would let a caller who never thought about it get
    # whichever answer somebody typed into the door.
    'a caller opens "app.db" and says nothing about shutdown': {
        "opens": ["app.db"],
        "omits": "shutdown",
        "why_not": "",
    },
    "a database is opened on a machine that says nothing about connections": {
        "opens": ["app.db"],
        "unset": "DECLARO_CONNECTIONS",
        "why_not": "",
    },
    "a database is opened on a machine that says nothing about the crew": {
        "opens": ["app.db"],
        "unset": "DECLARO_CREW_SIZE",
        "why_not": "",
    },
    "the door's arguments are read": {"opens": [], "why_not": ""},
}

WHAT_EACH_GIVEN_OPENS: dict[str, dict] = {
    'a database opened from "app.db"': {"opens": ["app.db"], "why_not": ""},
}

# WHAT THE THREE SCENARIOS ABOUT A DESTROYED COLUMN STILL NEED. Each row says
# what it sets up and why it cannot run yet, in the words of its own sentence, so
# the reason is beside the sentence rather than in a list somewhere else.
WHAT_EACH_GIVEN_ABOUT_A_LOSS_NEEDS: dict[str, dict] = {
    "a database holding a column no surviving model names": {
        "opens": [],
        "why_not": "Needs a fixture that opens a real database through `boundary_in fn session_open` with models that would drop a column. `tests/bdd/steps/test_session_connect_steps.py` builds exactly that for `orchestrator fn session_connect`, which returns the fault rather than raising it, and building it a second way here is what `test_no_fixture_builds_a_shape_its_own_way` exists to refuse. The feature is written against the declaration and the binding waits on one fixture both files read.",
    },
    "no model asking to drop it": {"why_not": ""},
    "a model naming that column in dropping": {"why_not": ""},
    "a second change in the same plan that destroys nothing": {"why_not": ""},
}


def _it_can_run(_why_not: str) -> bool:
    """Nothing is missing, so the scenario goes on. Answered rather than left."""
    return True


def _it_cannot_run_yet(why_not: str) -> bool:
    """`pytest.xfail` names the missing piece and ends the scenario here."""
    pytest.xfail(why_not)


_WHETHER_ANYTHING_IS_MISSING: dict[bool, Any] = {False: _it_can_run, True: _it_cannot_run_yet}


@pytest.fixture
def scenario(tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch) -> dict:
    for name, said in WHAT_THIS_MACHINE_SAYS.items():
        monkeypatch.setenv(name, said)
    return {
        "tmp": tmp_path,
        "monkeypatch": monkeypatch,
        "opens": [],
        "primary": None,
        "unset": "",
        "omits": "",
        "why_not": "",
    }


# --- opening ---------------------------------------------------------------------


def _where_it_really_is(scenario: dict, target: str) -> str:
    """A bare name in a scenario is a file in this test's own folder.

    `app.db` in the feature means "a SQLite file", and writing it literally
    would put one in the working directory and leave it there.
    """
    return _WHETHER_IT_NAMES_A_SCHEME["://" in target](scenario, target)


def _as_the_scenario_wrote_it(_scenario: dict, target: str) -> str:
    return target


def _in_this_tests_own_folder(scenario: dict, target: str) -> str:
    return str(scenario["tmp"] / target)


_WHETHER_IT_NAMES_A_SCHEME: dict[bool, Any] = {
    True: _as_the_scenario_wrote_it,
    False: _in_this_tests_own_folder,
}


@given(
    parsers.re(
        _one_of(
            tuple(
                sorted(
                    {**WHAT_EACH_GIVEN_OPENS, **WHAT_EACH_GIVEN_ABOUT_A_LOSS_NEEDS},
                    key=len,
                    reverse=True,
                )
            )
        )
    )
)
@when(parsers.re(_one_of(tuple(sorted(WHAT_EACH_SENTENCE_OPENS, key=len, reverse=True)))))
def _what_the_sentence_opens(scenario: dict, sentence: str) -> None:
    scenario.update(
        {
            **WHAT_EACH_GIVEN_OPENS,
            **WHAT_EACH_GIVEN_ABOUT_A_LOSS_NEEDS,
            **WHAT_EACH_SENTENCE_OPENS,
        }[sentence]
    )
    _WHETHER_ANYTHING_IS_MISSING[scenario["why_not"] != ""](scenario["why_not"])
    _WHETHER_THE_MACHINE_SAYS_IT[scenario["unset"] != ""](scenario)
    scenario["answers"] = asyncio.run(_open_each_and_shut(scenario))


def _the_machine_said_both(_scenario: dict) -> int:
    """Both numbers are set, which is what the fixture already did."""
    return 0


def _take_one_away(scenario: dict) -> int:
    """One variable this deployment says nothing about, so the refusal is real."""
    scenario["monkeypatch"].delenv(scenario["unset"])
    return 1


_WHETHER_THE_MACHINE_SAYS_IT: dict[bool, Any] = {
    False: _the_machine_said_both,
    True: _take_one_away,
}


async def _open_each_and_shut(scenario: dict) -> list[Any]:
    """Open every target the sentence named, then shut whatever opened.

    A drainer is an `asyncio` task and a connection is an operating system
    thread, so both end inside the loop that made them.
    """
    opened = [
        await _WHETHER_THE_CALL_IS_COMPLETE[scenario["omits"] != ""](
            _where_it_really_is(scenario, target), scenario["primary"]
        )
        for target in scenario["opens"]
    ]
    for one in opened:
        await _WHETHER_THERE_IS_ANYTHING_TO_SHUT[not isinstance(one, dict) or "crew" not in one](
            one
        )
    return opened


async def _every_argument_given(target: str, primary: str | None) -> Any:
    """The call, and what it raised if it raised.

    `boundary_in fn session_open` raises rather than handing a refusal back, so
    the exception is what a scenario reads. Catching it here and keeping it is
    how a Then asks what the refusal said, the same way the shutdown scenario
    keeps the error Python itself makes.
    """
    try:
        return await session_open(target, "exit_immediately", primary, None)
    except CouldNotOpen as refused:
        return refused


async def _with_the_shutdown_left_off(target: str, primary: str | None) -> Any:
    """The call Python refuses, and what it said about refusing it.

    `shutdown` has no default, so this never reaches the body. Catching the
    error is how a scenario reads a refusal that the language makes rather than
    the library.
    """
    try:
        return await session_open(target, primary=primary, token=None)  # type: ignore[call-arg]
    except TypeError as stopped:
        return {"never ran": str(stopped)}


_WHETHER_THE_CALL_IS_COMPLETE: dict[bool, Any] = {
    False: _every_argument_given,
    True: _with_the_shutdown_left_off,
}


async def _nothing_was_opened(_handle: Any) -> int:
    """The open was refused, so there is nothing holding a thread."""
    return 0


async def _stop_and_close(handle: Any) -> int:
    await crew_stop(handle["crew"])
    for connection in [*handle["writers"], *handle["connections"]]:
        await session_close_connection(connection)
    return len(handle["writers"]) + len(handle["connections"])


_WHETHER_THERE_IS_ANYTHING_TO_SHUT: dict[bool, Any] = {
    True: _nothing_was_opened,
    False: _stop_and_close,
}


def _the_one_answer(scenario: dict) -> Any:
    answers = scenario["answers"]
    assert len(answers) == 1, f"this scenario opened {len(answers)} databases, not one"
    return answers[0]


def _the_words_of(answer: object) -> str:
    """Everything the refusal says, whether it arrived raised or returned."""
    fault = getattr(answer, "fault", answer)
    return str(fault["what_went_wrong"]) + str(fault["what_to_do_instead"])  # type: ignore[index]


# --- what must be true afterwards ---------------------------------------------------


@then("the database carries postgresql as its engine")
def _it_carries_postgresql(scenario: dict) -> None:
    """The target named PostgreSQL, so nothing downstream has to work it out.

    A target nothing on this machine answers still refuses as PostgreSQL, which
    is the point: the engine is decided before anything is opened.
    """
    answer = _the_one_answer(scenario)
    said = _WHATEVER_CAME_BACK[_is_fault(answer)](answer)

    assert "postgres" in said.lower(), (
        f"a postgresql:// target came back naming nothing of the sort: {said}"
    )


def _the_engine_on_the_handle(answer: Any) -> str:
    return str(answer["engine"])


def _the_words_of_the_refusal(answer: Any) -> str:
    return _the_words_of(answer)


_WHATEVER_CAME_BACK: dict[bool, Any] = {
    True: _the_words_of_the_refusal,
    False: _the_engine_on_the_handle,
}


@then("that is the only place the engine is written down")
def _written_down_once(scenario: dict) -> None:
    """Nothing the caller wrote names an engine. The target is a place, and the
    word that comes back is the library's reading of it."""
    assert not any(one == "postgresql" for one in scenario["opens"]), (
        f"the caller named an engine in what they opened: {scenario['opens']}"
    )


@then("the handle names sqlite")
def _the_handle_names_sqlite(scenario: dict) -> None:
    answer = _the_one_answer(scenario)

    assert not _is_fault(answer), f"the open was refused: {answer}"
    assert answer["engine"] == "sqlite", (
        f"a bare path came back on {answer['engine']!r}, and a path that names no scheme is SQLite"
    )


@then("no connection it holds was asked what engine it is")
def _nothing_asked_the_connection(scenario: dict) -> None:
    """`boundary_out fn session_open_connection` is handed the engine rather
    than working it out, so the answer cannot have come off a driver."""
    hands_over = inspect.signature(session_open).parameters

    assert "engine" not in hands_over, (
        f"the door takes an engine, so the handle's word could have come from "
        f"the caller rather than from the target: {sorted(hands_over)}"
    )
    assert _the_one_answer(scenario)["engine"] == "sqlite"


@then("it is on turso")
def _it_is_on_turso(scenario: dict) -> None:
    answer = _the_one_answer(scenario)

    assert not _is_fault(answer), f"the open was refused: {answer}"
    assert answer["engine"] == "turso", f"it came back on {answer['engine']!r}"


@then("it holds one channel for its life")
def _it_holds_a_channel(scenario: dict) -> None:
    assert _the_one_answer(scenario)["channel"] is not None, (
        "a replicated database came back with no channel, so nothing would "
        "push its local commits to the primary at exit"
    )


@then("it holds no channel")
def _it_holds_no_channel(scenario: dict) -> None:
    assert _the_one_answer(scenario)["channel"] is None, (
        "a database with no primary came back holding a channel to one"
    )


@then("three databases come back, on sqlite, postgresql and turso")
def _three_came_back(scenario: dict) -> None:
    assert [one["engine"] for one in scenario["answers"]] == [
        "sqlite",
        "postgresql",
        "turso",
    ], f"they came back on {[one.get('engine') for one in scenario['answers']]}"


@then("the file that opened them names no engine anywhere")
@then("neither call named an engine")
def _no_engine_in_the_call(scenario: dict) -> None:
    named = [one for one in scenario["opens"] if one in ("sqlite", "postgresql", "turso")]

    assert named == [], f"the caller named {named} instead of saying where the database is"


@then("each database is on the same engine it was before")
def _same_engine_whatever_the_order(scenario: dict) -> None:
    by_target = {
        target: one["engine"]
        for target, one in zip(scenario["opens"], scenario["answers"], strict=True)
    }

    assert by_target["app.db"] == "sqlite", f"app.db came back on {by_target['app.db']!r}"
    assert by_target["postgresql://localhost/todo"] == "postgresql"
    assert by_target["libsql://team-app.turso.io"] == "turso"


@then("no database's engine depends on which was opened first")
def _order_decides_nothing(scenario: dict) -> None:
    """Two runs in two orders give the same word per target, or the door is
    reading something other than the target it was handed."""
    assert len(set(scenario["opens"])) == len(scenario["opens"]), (
        "this scenario opened one target twice, so it cannot show that order decides nothing"
    )


@then("each handle carries its own engine")
def _each_carries_its_own(scenario: dict) -> None:
    assert [one["engine"] for one in scenario["answers"]] == ["sqlite", "postgresql"], (
        f"they came back on {[one.get('engine') for one in scenario['answers']]}"
    )


@then("it gets two databases")
def _two_databases(scenario: dict) -> None:
    assert len(scenario["answers"]) == 2, f"it got {len(scenario['answers'])}"
    for one in scenario["answers"]:
        assert not _is_fault(one), f"an open was refused: {one}"


@then("they share no connection, no writer and no crew")
def _they_share_nothing(scenario: dict) -> None:
    """Two opens of one target are two databases, and a caller holding both has
    nothing to tell them apart by except that nothing is shared."""
    one, other = scenario["answers"]
    shared = [
        what
        for what in ("connections", "writers")
        if {id(each) for each in one[what]} & {id(each) for each in other[what]}
    ]

    assert shared == [], f"the two databases share their {shared}"
    assert one["crew"] is not other["crew"], (
        "the two databases share one crew, so a write deposited against either "
        "could be run by the other's drainer on the other's connection"
    )


@then("none of them is an engine or a scheme")
def _no_argument_names_an_engine(scenario: dict) -> None:
    """Read off the signature, not listed here. A test that wrote the four names
    itself would agree with whatever it had been written to expect."""
    taken = sorted(inspect.signature(session_open).parameters)

    assert taken == ["primary", "shutdown", "target", "token"], (
        f"the door takes {taken}, and this scenario is about what a caller has "
        f"to decide before they can open anything"
    )


@then("the only thing that says which engine to use is the target")
def _only_the_target_says(scenario: dict) -> None:
    assert "target" in inspect.signature(session_open).parameters


@then("the call raises and no database comes back")
def _it_raised(scenario: dict) -> None:
    """A returned refusal is one a deployment can ignore and serve anyway."""
    answer = _the_one_answer(scenario)

    assert isinstance(answer, CouldNotOpen), (
        f"the open was refused and the refusal came back as a value rather than "
        f"being raised, so a caller who ignores it boots a server against a "
        f"database it never got: {answer!r}"
    )


# WHAT EACH SENTENCE NEEDS TO FIND IN THE REFUSAL'S OWN WORDS. Four sentences,
# each looking for its own phrases in the one string a fault amounts to.
WHAT_EACH_REFUSAL_HAS_TO_SAY: dict[str, tuple[str, ...]] = {
    "the refusal names sqlite and says it has nothing to replicate to": (
        "sqlite",
        "primary",
    ),
    "the refusal says shutdown has no default, on any engine": ("shutdown",),
    "the refusal says the number is a property of the machine": (
        "DECLARO_CONNECTIONS",
        "machine",
    ),
    "the refusal names the crew rather than the connections": ("DECLARO_CREW_SIZE",),
    "what failed is the connection, and the failure says so": ("could not be opened",),
}


@then(parsers.re(_one_of(tuple(sorted(WHAT_EACH_REFUSAL_HAS_TO_SAY, key=len, reverse=True)))))
def _the_refusal_says_it(scenario: dict, sentence: str) -> None:
    said = _the_words_of(_the_one_answer(scenario)).lower()
    missing = [one for one in WHAT_EACH_REFUSAL_HAS_TO_SAY[sentence] if one.lower() not in said]

    assert missing == [], (
        f"the refusal never says {missing}, which is what somebody reading it has to find: {said!r}"
    )


@then("no connection was opened")
def _nothing_was_connected(scenario: dict) -> None:
    """A refusal that had already opened a connection would leave a thread
    behind with nothing holding it."""
    assert not (scenario["tmp"] / "app.db").exists(), (
        "the open was refused and a database file was made anyway"
    )


@then("no file was created and no connection was attempted")
def _nothing_touched_the_disk(scenario: dict) -> None:
    assert list(scenario["tmp"].iterdir()) == [], (
        f"a target no engine claims left {[p.name for p in scenario['tmp'].iterdir()]} behind"
    )


@then("the engine was decided first, and postgresql")
def _postgres_was_decided_first(scenario: dict) -> None:
    said = _the_words_of(_the_one_answer(scenario)).lower()

    assert "postgres" in said, (
        f"a postgresql:// target that could not be reached refused without ever "
        f"naming postgresql: {said!r}"
    )


@then("the call never runs")
def _the_call_never_ran(scenario: dict) -> None:
    """`shutdown` has no default, so Python refuses the call itself."""
    assert "never ran" in _the_one_answer(scenario), (
        f"a call with no shutdown reached the door and answered {_the_one_answer(scenario)}"
    )


@then("what stopped it names shutdown")
def _it_names_shutdown(scenario: dict) -> None:
    said = _the_one_answer(scenario)["never ran"]

    assert "shutdown" in said, (
        f"the call was refused without naming the argument that was left off: {said!r}"
    )


# NO STEP FOR THE RETRY BUDGET. The feature deleted those two lines on
# 2026-09-25, because the door has no budget argument. Two step definitions were
# left here as a tripwire, and `tests/unit/test_no_step_is_unreachable.py` was
# right to call them unreachable: a step no scenario reaches has never run, and
# a file holding one reads as covering something it does not.
