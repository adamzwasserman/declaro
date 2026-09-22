"""Every line of `schema/mechanism_for.feature`, bound to a step.

WHAT THE FUNCTION IS ASKED. `fn mechanism_for` is handed a freshness, an
engine and a capability, and answers with the operations that give a
materialized view that freshness on that engine, or with a refusal. It is not
asked what an engine supports: `fn capability_of` answers that, and its answer
arrives here as the third argument. So every scenario below builds a
`Capability` and hands it over, and none of them opens a database.

WHERE THE CAPABILITIES IN `CAPABILITIES` COME FROM. `core/capability_of.feature`
states two of the three rows in its own words: PostgreSQL has a materialized
view, and SQLite builds one from a table, its triggers and a registry row.
Turso is written here as SQLite's row, because Turso has no materialized view
either. That is this file's reading and not a line anybody wrote down, so when
`capability_of` lands its table is the one to believe.

WHAT THE ANSWER IS READ FOR, AND WHAT IS DELIBERATELY NOT READ. The scenario
"a table, its triggers and a registry row are used" would pass for free if the
assertion read `capability["built_from"]`, because this file writes those exact
words into the capability it hands over. So the Then below reads every key of
the answer's detail EXCEPT `built_from`, which leaves only words the function
chose itself.

THE FOUR REGISTRY SCENARIOS WERE DELETED ON 2026-09-21, and their steps with
them. They asked what a hand-built registry table already held -- is this name a
view, what was registered under it, list them all -- and asserted on the rows
that came back. `mechanism_for` opens no database and answers with operations,
so no assertion about its answer could have made any of them true, and they had
been carrying an `xfail` naming `reader fn schema_inspect` instead. What they
were reaching for is already stated here as a result: an engine without a
materialized view gets a table, its triggers and a registry row.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast, get_args

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.mechanism_for import mechanism_for
from declaro_persistum.types import Capability, Engine, Fault, Freshness, View

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/schema/mechanism_for.feature")

FRESHNESSES: tuple[str, ...] = get_args(Freshness)
"""The three words a caller may write, straight from the vocabulary."""

CAPABILITIES: dict[Engine, Capability] = {
    "postgresql": {"native": True, "built_from": ""},
    "sqlite": {"native": False, "built_from": "a table, its triggers and a registry row"},
    "turso": {"native": False, "built_from": "a table, its triggers and a registry row"},
}
"""What `capability_of` would say about a materialized view on each engine."""

NOTHING_TO_BUILD_FROM: Capability = {"native": False, "built_from": ""}
"""An engine told it has no materialized view and nothing to build one from."""

DECLARING: dict[str, Freshness] = {
    'a view declared "current when read"': "current when read",
    # "THE SAME DECLARATION" IS THE LINE ABOVE IT IN THE FEATURE. A scenario
    # cannot see the one before it, so the word the reader carries over is
    # written down here instead of left to memory.
    "the same declaration": "current when read",
}
"""The Gherkin phrase that declares a view, and the freshness it declares."""

PARTS: dict[str, tuple[str, tuple[str, ...]]] = {
    "a materialized view is used": ("create_view", ("materialized view",)),
    "a table, its triggers and a registry row are used": (
        "create_table",
        ("table", "trigger", "registry row"),
    ),
}
"""The Gherkin sentence -> the operation kind it means, and the words the
answer has to say. The kind is what keeps the two apart structurally: a
mechanism the engine has natively creates a view, and one built out of parts
creates a table."""

MECHANISM_WORDS: frozenset[str] = frozenset(
    {"on_commit", "trigger", "triggers", "journal", "wal", "refresh"}
)
"""Every mechanism the two scenarios name between them, as an engine spells it.
A caller writes a requirement, so no word a caller declares may be one of
these."""


def declaring_words() -> tuple[str, ...]:
    """Every word a caller writes to declare a materialized view.

    The three freshnesses and the three fields of a `View`. Read from the types
    rather than typed out, so a word added to either is checked the next run.
    """
    return FRESHNESSES + tuple(View.__annotations__)


def words_in(phrase: str) -> list[str]:
    """One phrase, split into the words it is made of.

    WHOLE WORDS, BECAUSE THE STEM IS NOT THE MECHANISM. "refreshed when asked"
    is how stale a caller can stand the view being; `REFRESH MATERIALIZED VIEW`
    is a statement PostgreSQL runs. Matching on the letters `refresh` calls the
    first one the second, which is the instrument being wrong rather than the
    vocabulary.
    """
    return phrase.replace("_", " ").split()


def is_a_fault(answer: object) -> bool:
    """True when what came back is a `Fault` rather than operations."""
    return isinstance(answer, dict) and set(answer) == set(Fault.__annotations__)


def mechanism_of(answer: object) -> list[tuple[str, str]]:
    """What the engine would end up holding, and nothing else about the answer.

    COMPARING WHOLE OPERATIONS HID A REAL DEFECT, on 2026-09-21. The detail
    carries the freshness it was picked for, so two answers built from one
    mechanism still differ in that field. A version of this function that handed
    every freshness the same mechanism passed both scenarios below until they
    were changed to read the kind and the sentence saying what the mechanism is.
    """
    return [(operation["kind"], str(operation["detail"]["is"])) for operation in answer]  # type: ignore[union-attr]


def said_by(operations: list[dict[str, Any]]) -> str:
    """Every word the answer chose for itself, joined into one string.

    `built_from` is left out on purpose: this file writes those words into the
    capability it hands over, so reading them back would test the fixture.
    """
    return " ".join(
        str(value)
        for operation in operations
        for key, value in operation["detail"].items()
        if key != "built_from"
    )


# --- what a scenario establishes -------------------------------------------


@given(parsers.re(r"a materialized view declared (?P<said>.+)"))
def a_materialized_view_declared(scenario: dict[str, Any], said: str) -> None:
    """The freshness a later When reaches for, and nothing else.

    This used to be one row of a table of four such steps. The other three read
    a registry, and they went with the scenarios that asked for them.
    """
    scenario["freshness"] = said


@given(parsers.re(r'(?P<phrase>a view declared "[^"]+"|the same declaration) on (?P<engine>\w+)'))
def a_view_declared_on_an_engine(scenario: dict[str, Any], phrase: str, engine: str) -> None:
    """One step for both declaring lines, because they differ only in words."""
    named: Engine = cast(Engine, engine)
    scenario["freshness"] = DECLARING[phrase]
    scenario["engine"] = named
    scenario["capability"] = CAPABILITIES[named]
    scenario["result"] = mechanism_for(DECLARING[phrase], named, CAPABILITIES[named])


@given("a freshness an engine cannot provide")
def a_freshness_an_engine_cannot_provide(scenario: dict[str, Any]) -> None:
    """Turso, told it has no materialized view and nothing to build one from.

    Which engine lacks what is `capability_of`'s answer, not this function's,
    so the scenario says it by handing over a capability that offers neither.
    """
    scenario["freshness"] = "always current"
    scenario["engine"] = "turso"
    scenario["capability"] = NOTHING_TO_BUILD_FROM


@given("a materialized view already in the database")
def a_materialized_view_already_in_the_database(scenario: dict[str, Any]) -> None:
    """What it was built as before anybody changed the declaration."""
    scenario["engine"] = "sqlite"
    scenario["capability"] = CAPABILITIES["sqlite"]
    scenario["freshness"] = "refreshed when asked"
    scenario["before"] = mechanism_for("refreshed when asked", "sqlite", CAPABILITIES["sqlite"])


# --- what a scenario does --------------------------------------------------


@when(parsers.re(r"a materialized view is declared (?P<freshness>.+)"))
def a_materialized_view_is_declared_as(scenario: dict[str, Any], freshness: str) -> None:
    """A scenario naming no engine is on SQLite, which is this suite's
    convention for a backend nobody named. The quotes around "eventually-ish"
    are Gherkin's way of showing a caller typed it, so they come off."""
    written = freshness.strip('"')
    scenario["freshness"] = written
    scenario["engine"] = "sqlite"
    scenario["capability"] = CAPABILITIES["sqlite"]
    scenario["result"] = mechanism_for(cast(Freshness, written), "sqlite", CAPABILITIES["sqlite"])


@when("a materialized view is declared")
def a_materialized_view_is_declared(scenario: dict[str, Any]) -> None:
    """This scenario names no freshness, so what it declares is the vocabulary
    itself. The Then reads the same words back out of the types rather than out
    of here, because the scenario above it has no When at all and has to reach
    them the same way."""
    scenario["declared"] = declaring_words()


WHERE: dict[str, Callable[[dict[str, Any]], tuple[Engine, Capability]]] = {
    "on postgresql": lambda _scenario: ("postgresql", CAPABILITIES["postgresql"]),
    "on sqlite": lambda _scenario: ("sqlite", CAPABILITIES["sqlite"]),
    "on turso": lambda _scenario: ("turso", CAPABILITIES["turso"]),
    "there": lambda scenario: (scenario["engine"], scenario["capability"]),
}
"""Where the schema is brought up to date -> the engine and the capability that
go with it. "there" is the engine the Given already named, which is why every
row is a function of the scenario rather than a pair sitting in a table."""


@when(parsers.re(r"the database is opened (?P<where>on \w+|there)"))
def the_schema_is_brought_to_the_declared_state(scenario: dict[str, Any], where: str) -> None:
    engine, capability = WHERE[where](scenario)
    scenario["engine"] = engine
    scenario["capability"] = capability
    scenario["result"] = mechanism_for(cast(Freshness, scenario["freshness"]), engine, capability)


@when("its declared freshness changes")
def its_declared_freshness_changes(scenario: dict[str, Any]) -> None:
    """From "refreshed when asked" to "always current", which is the largest
    change a caller can make: nothing maintained it, and now every write does."""
    scenario["freshness"] = "always current"
    scenario["after"] = mechanism_for("always current", scenario["engine"], scenario["capability"])


# --- what the answer has to be ---------------------------------------------


@then(
    parsers.re(
        r"(?P<sentence>a materialized view is used"
        r"|a table, its triggers and a registry row are used)"
    )
)
def the_named_mechanism_is_used(scenario: dict[str, Any], sentence: str) -> None:
    """One function for both, because they differ only in which mechanism.

    Two things are checked, and the second is the one that bites. The operation
    kind says structurally which mechanism was picked, and the words the answer
    chose say what it is made of. A function that returned one mechanism for
    every capability would pass the words and fail the kind.
    """
    kind, words = PARTS[sentence]
    operations = scenario["result"]
    assert isinstance(operations, list), f"expected operations, got {operations!r}"
    assert [operation["kind"] for operation in operations] == [kind], (
        f"expected a {kind}, got {[operation['kind'] for operation in operations]}"
    )

    said = said_by(operations)
    for word in words:
        assert word in said, f"the answer does not say {word!r}: {said}"


@then("no declaration anywhere says on_commit or trigger")
@then("the declaration names no journal mode, trigger or refresh statement")
def the_declaration_names_no_mechanism() -> None:
    """A caller writes a requirement; the library picks the mechanism.

    Both sentences make one claim about the same words, so they are one
    function over one list. Every word a caller writes to declare a
    materialized view is checked against every mechanism word the two
    scenarios name between them.
    """
    for phrase in declaring_words():
        named = MECHANISM_WORDS.intersection(words_in(phrase))
        assert not named, (
            f"a caller declaring a materialized view writes {phrase!r}, which "
            f"names the mechanism {sorted(named)}; picking a mechanism is the "
            "library's job"
        )


@then("it is accepted")
def it_is_accepted(scenario: dict[str, Any]) -> None:
    """Operations come back, and there is at least one of them. An empty list
    would be a function reporting success while setting nothing up."""
    result = scenario["result"]
    assert not is_a_fault(result), f"{scenario['freshness']!r} was refused: {result!r}"
    assert isinstance(result, list), f"expected operations, got {result!r}"
    assert result, f"{scenario['freshness']!r} was accepted and built nothing"


@then("it is refused, and the refusal lists the ones that exist")
def it_is_refused_and_lists_the_ones_that_exist(scenario: dict[str, Any]) -> None:
    """A word nobody declared comes back as a fault naming what arrived and
    every word that would have worked. Listing them is what makes the refusal
    something a caller can act on without opening the source."""
    result = scenario["result"]
    assert is_a_fault(result), f"expected a Fault, got {result!r}"
    assert result["kind"] == "refused", result

    said = " ".join([result["what_went_wrong"], result["what_to_do_instead"]])
    assert scenario["freshness"] in said, f"the refusal does not say what arrived: {said}"
    for word in FRESHNESSES:
        assert word in said, f"the refusal does not list {word!r}: {said}"


@then("reading it gives the current answer")
def reading_it_gives_the_current_answer(scenario: dict[str, Any]) -> None:
    """Every engine gets a mechanism for "always current", and it is not the
    one that waits to be asked.

    The second half is what makes this more than "something came back". If the
    function handed every freshness the same mechanism, this engine would be
    serving stale rows under a declaration that said otherwise, and the answer
    would look identical to the one for "refreshed when asked".
    """
    current = scenario["result"]
    assert not is_a_fault(current), (
        f"always current was refused on {scenario['engine']}: {current!r}"
    )
    assert isinstance(current, list) and current, f"expected operations, got {current!r}"

    stale = mechanism_for("refreshed when asked", scenario["engine"], scenario["capability"])
    assert mechanism_of(current) != mechanism_of(stale), (
        f"on {scenario['engine']} always current and refreshed when asked build "
        f"the same thing, so reading it cannot give the current answer: "
        f"{mechanism_of(current)!r}"
    )


@then("it refuses and names the engine and the freshness")
def it_refuses_and_names_the_engine_and_the_freshness(scenario: dict[str, Any]) -> None:
    """The refusal comes back in the return, and it says both halves of what
    could not be done, so a caller knows which declaration to change."""
    result = scenario["result"]
    assert is_a_fault(result), f"expected a Fault, got {result!r}"
    assert result["kind"] == "refused", result

    said = " ".join([result["what_went_wrong"], result["what_to_do_instead"]])
    assert scenario["engine"] in said, f"the refusal does not name the engine: {said}"
    assert scenario["freshness"] in said, f"the refusal does not name the freshness: {said}"
    assert result["what_to_do_instead"].strip(), "the refusal says nothing to do instead"


@then("it does not quietly provide a weaker one")
def it_does_not_quietly_provide_a_weaker_one(scenario: dict[str, Any]) -> None:
    """Nothing came back to build. Handing over operations for some other
    freshness would leave the caller with a view that is staler than the one
    they declared, and nothing would have said so."""
    assert not isinstance(scenario["result"], list), (
        f"an engine that cannot provide {scenario['freshness']!r} still handed "
        f"back operations: {scenario['result']!r}"
    )


@then("bringing the schema up to date changes what the engine holds")
def bringing_the_schema_up_to_date_changes_what_the_engine_holds(
    scenario: dict[str, Any],
) -> None:
    """A different freshness builds a different thing. If the two matched, the
    migration would run and the view would keep the freshness it had."""
    assert mechanism_of(scenario["after"]) != mechanism_of(scenario["before"]), (
        f"the freshness changed and the mechanism did not: {mechanism_of(scenario['after'])!r}"
    )


@then("it does not report success while changing nothing")
def it_does_not_report_success_while_changing_nothing(scenario: dict[str, Any]) -> None:
    """The new mechanism is operations, and there is work in them."""
    after = scenario["after"]
    assert not is_a_fault(after), f"the new freshness was refused: {after!r}"
    assert isinstance(after, list) and after, (
        f"the freshness changed and no operation came back: {after!r}"
    )
