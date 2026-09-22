"""Bind `tests/bdd/features/core/capability_of.feature`.

WHAT THE FUNCTION ANSWERS. `fn capability_of` is handed one engine and one
feature and says what that engine does about it: has it, or is built out of
something else. Three engines crossed with five features is fifteen answers, and
this file asks for all fifteen rather than for the two the prose names, because
the pair nobody filled in is the one nobody thinks to ask about.

A COMBINATION OUTSIDE THE VOCABULARY RAISES, AND THE FEATURE CALLS THAT A
FAULT. The declaration gives this function no `Fault` arm, so a refusal cannot
come back as a value: every engine has an answer about every feature, even when
that answer is that it cannot. What is left for a word nobody declared is the
`KeyError` the lookup raises, and the key is the pair, so it names the engine
and the feature together. That is the disagreement between the feature file and
the declaration, and it is resolved here in the declaration's favour.

WHY THE NATIVE CLAIMS ARE RUN AND NOT READ. A hand-written answer about three
engines drifts from those engines quietly, and asking the engine is the only
thing that catches a wrong one. So the scenario that says an engine said to have
a capability has it opens all three: SQLite through the standard library, Turso
through pyturso, and PostgreSQL through asyncpg against the database this pytest
process made for itself. Nine answers claim native and nine exercises run, and
the step fails when those two sets are not the same set, so an answer that gains
`native: True` without an exercise fails here rather than on an engine at a
deploy.

WHY POSTGRESQL IS A FAILURE AND NOT A SKIP. `tests/conftest.py` records the
measurement: with the server down the suite reported 2680 passed and 200
skipped, and 186 of those skips had been passing. Nobody counts the `s`
characters, so this calls `refuse_without_postgresql` like the rest of the
suite.

THE WITHDRAWN PRAGMA IS THE ENGINE REFUSING, NOT A STAND-IN. The last scenario
says an engine that cannot be asked what a table holds fails loudly, and it says
it without naming a mechanism. The mechanism is here: SQLite's own authorizer
denies `SQLITE_PRAGMA` and allows everything else, so the connection is real,
its contract is the driver's, and an ordinary SELECT still answers on it. This
package has twice chased a defect that lived only in a double, and a double of a
connection is exactly where that happens.

WHAT NO STEP HERE DOES. It never reads the module's own text, and it never reads
a documentation file. Every claim below is about what came back from a call or
from an engine.

THE ROUND TRIP IS DECLARED AND NOT BUILT. Creating what builds a capability,
dropping it and reading it back needs something that opens a database and says
what is there. That is `reader fn schema_inspect`, which the declaration
declares and nobody has written, so that scenario calls `pytest.xfail` naming
it. Its Then steps are written to fail loudly, so deleting the xfail turns the
scenario red rather than green.
"""

from __future__ import annotations

import asyncio
import pathlib
import sqlite3
from collections.abc import Callable
from typing import Any, get_args

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.types import Engine, Feature
from tests.conftest import POSTGRESQL_PROBE_URL, refuse_without_postgresql

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/core/capability_of.feature")


ENGINES: tuple[Engine, ...] = get_args(Engine)
FEATURES: tuple[Feature, ...] = get_args(Feature)

# An answer that is not there. Compared by identity, so no real answer can be it.
NO_ANSWER = object()

A_CAPABILITY = ("native", "built_from")


def _answer(ask: Callable[..., Any], engine: str, feature: str) -> Any:
    """What the function says about one pair, or `NO_ANSWER` where it says
    nothing."""
    try:
        return ask(engine, feature)
    except KeyError:
        return NO_ANSWER


def _every_pair(ask: Callable[..., Any]) -> dict:
    """Every engine crossed with every feature, asked one pair at a time."""
    return {
        (engine, feature): _answer(ask, engine, feature)
        for feature in FEATURES
        for engine in ENGINES
    }


@pytest.fixture
def ask():
    """The function under test, imported when a step runs rather than at import.

    A step file that cannot be imported is a collection error, which hides which
    scenario was asking for what.
    """
    from declaro_persistum.capability_of import capability_of

    return capability_of


# =============================================================================
# What an engine does about one feature
# =============================================================================


@given(parsers.re(r"^(?P<engine>postgresql|sqlite) and a materialized view$"))
def _an_engine_and_a_materialized_view(scenario: dict, engine: str) -> None:
    """One Given for both scenarios. The engine is the only difference."""
    scenario["asked"] = (engine, "materialized_view")


@given("an engine and a feature the table does not cover")
def _a_feature_the_table_does_not_cover(scenario: dict) -> None:
    """`ranges` is not one of the five words `Feature` admits, so no answer was
    ever written for it and none can be worked out."""
    scenario["asked"] = ("sqlite", "ranges")


@then("it is native")
def _it_is_native(scenario: dict, ask) -> None:
    capability = ask(*scenario["asked"])

    assert capability["native"] is True, capability
    assert capability["built_from"] == "", (
        f"the engine has it, so there is nothing else that supplies it: {capability}"
    )


@then("it is built from a table, its triggers and a registry row")
def _it_is_built_from_a_table_its_triggers_and_a_registry_row(scenario: dict, ask) -> None:
    capability = ask(*scenario["asked"])

    assert capability["native"] is False, capability
    said = capability["built_from"].lower()
    missing = [word for word in ("table", "trigger", "registry") if word not in said]
    assert missing == [], f"the answer never names {missing}: {capability['built_from']}"


@then("a fault comes back naming both")
def _a_fault_comes_back_naming_both(scenario: dict, ask) -> None:
    """The declaration gives this function no Fault arm, so what a word nobody
    declared earns is the KeyError the lookup raises. The key is the pair, so
    it carries the engine and the feature together and guesses neither."""
    engine, feature = scenario["asked"]

    with pytest.raises(KeyError) as raised:
        ask(engine, feature)

    said = str(raised.value)
    assert engine in said and feature in said, (
        f"the failure names neither the engine nor the feature that caused it: {said}"
    )


# =============================================================================
# Every engine, about every feature
# =============================================================================


@when("every engine is asked about every feature the package names")
def _every_engine_is_asked_about_every_feature(scenario: dict, ask) -> None:
    scenario["answers"] = _every_pair(ask)


@then("each pair answers, and every answer says the engine has it or names what supplies it")
def _each_pair_answers(scenario: dict) -> None:
    """Three claims about one set of answers, because a caller asking fifteen
    questions gets fifteen answers or it does not. A pair that answers nothing
    and a pair whose answer says nothing are the same silence at two widths."""
    answers = scenario["answers"]

    assert ENGINES and FEATURES, "a vocabulary read as empty, so this asserted nothing"
    assert len(answers) == len(ENGINES) * len(FEATURES), (
        f"{len(ENGINES)} engines over {len(FEATURES)} features is "
        f"{len(ENGINES) * len(FEATURES)} questions and {len(answers)} were asked"
    )
    silent = [
        f"{feature} on {engine}"
        for (engine, feature), answer in answers.items()
        if answer is NO_ANSWER
    ]
    assert silent == [], f"these pairs were never decided: {silent}"

    # EXACTLY ONE OF THE TWO. An answer claiming both would leave a reader to
    # choose, and one claiming neither answers nothing.
    wrong = [
        f"{feature} on {engine}: {answer}"
        for (engine, feature), answer in answers.items()
        if tuple(answer) != A_CAPABILITY or answer["native"] == (answer["built_from"] != "")
    ]
    assert wrong == [], (
        f"these answers neither say the engine has it nor name what supplies it: {wrong}"
    )


# =============================================================================
# Native is a claim about an engine, and only the engine can support it
# =============================================================================
#
# EACH EXERCISE RUNS THE CAPABILITY AND READS THE ANSWER BACK. Declaring a CHECK
# constraint proves the engine parses one; refusing the row it forbids proves
# the engine keeps it. The first is a claim about a parser and the second is the
# capability.


def _check_constraint_on(run: Callable[[str], Any]) -> str:
    run("CREATE TABLE cap_check (n INTEGER CHECK (n > 0))")
    try:
        run("INSERT INTO cap_check (n) VALUES (-1)")
    # BROAD ON PURPOSE: sqlite3 and pyturso do not share an exception
    # hierarchy, and what matters is that the row was refused at all.
    except Exception as refused:  # noqa: BLE001
        return f"the row it forbids was refused with {type(refused).__name__}"
    raise AssertionError("the CHECK was declared and the engine took a row it forbids")


def _partial_index_on(run: Callable[[str], Any]) -> str:
    run("CREATE TABLE cap_partial (n INTEGER, live INTEGER)")
    run("CREATE INDEX cap_partial_live ON cap_partial (n) WHERE live = 1")
    listed = list(run("PRAGMA index_list('cap_partial')"))
    partial = [row for row in listed if row[1] == "cap_partial_live" and row[4]]
    assert partial, f"the index was created and the engine does not call it partial: {listed}"
    return f"index_list calls it partial: {partial}"


THE_SQLITE_SHAPED_EXERCISES: dict[str, Callable[[Callable[[str], Any]], str]] = {
    "check_constraint": _check_constraint_on,
    "partial_index": _partial_index_on,
}


async def _postgresql_materialized_view(conn: Any) -> str:
    await conn.execute("CREATE TABLE cap_rows (n integer)")
    await conn.execute("INSERT INTO cap_rows VALUES (1)")
    await conn.execute("CREATE MATERIALIZED VIEW cap_view AS SELECT n FROM cap_rows")
    await conn.execute("REFRESH MATERIALIZED VIEW cap_view")
    held = await conn.fetchval("SELECT count(*) FROM cap_view")
    assert held == 1, f"the materialized view was made and holds {held} rows"
    return "CREATE MATERIALIZED VIEW, REFRESH and a read all ran"


async def _postgresql_check_constraint(conn: Any) -> str:
    import asyncpg

    await conn.execute("CREATE TABLE cap_check (n integer CHECK (n > 0))")
    try:
        await conn.execute("INSERT INTO cap_check VALUES (-1)")
    except asyncpg.PostgresError as refused:
        return f"the row it forbids was refused with {type(refused).__name__}"
    raise AssertionError("the CHECK was declared and PostgreSQL took a row it forbids")


async def _postgresql_partial_index(conn: Any) -> str:
    await conn.execute("CREATE TABLE cap_partial (n integer, live boolean)")
    await conn.execute("CREATE INDEX cap_partial_live ON cap_partial (n) WHERE live")
    predicate = await conn.fetchval(
        "SELECT pg_get_expr(indpred, indrelid) FROM pg_index "
        "WHERE indexrelid = 'cap_partial_live'::regclass"
    )
    assert predicate, "the index was created and PostgreSQL records no predicate on it"
    return f"pg_index records the predicate {predicate}"


async def _postgresql_enum(conn: Any) -> str:
    import asyncpg

    await conn.execute("CREATE TYPE cap_mood AS ENUM ('ok', 'bad')")
    await conn.execute("CREATE TABLE cap_enum (m cap_mood)")
    await conn.execute("INSERT INTO cap_enum VALUES ('ok')")
    try:
        await conn.execute("INSERT INTO cap_enum VALUES ('sideways')")
    except asyncpg.PostgresError as refused:
        return f"a value outside the type was refused with {type(refused).__name__}"
    raise AssertionError("the enum was declared and PostgreSQL took a value outside it")


async def _postgresql_range_type(conn: Any) -> str:
    await conn.execute("CREATE TABLE cap_range (span int4range)")
    await conn.execute("INSERT INTO cap_range VALUES ('[1,3)')")
    holds = await conn.fetchval("SELECT span @> 2 FROM cap_range")
    assert holds is True, f"the range was stored and does not answer containment: {holds}"
    return "int4range held the bounds and answered whether 2 is inside them"


THE_POSTGRESQL_EXERCISES = {
    "materialized_view": _postgresql_materialized_view,
    "check_constraint": _postgresql_check_constraint,
    "partial_index": _postgresql_partial_index,
    "enum": _postgresql_enum,
    "range_type": _postgresql_range_type,
}

_A_SCHEMA_OF_OUR_OWN = "capability_of_probe"


async def _against_postgresql() -> dict[tuple[str, str], str]:
    """Every PostgreSQL exercise, in a schema this test makes and drops."""
    import asyncpg

    try:
        conn = await asyncpg.connect(POSTGRESQL_PROBE_URL, timeout=5)
    except (OSError, asyncpg.PostgresError) as unreachable:
        refuse_without_postgresql(POSTGRESQL_PROBE_URL, unreachable)

    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_A_SCHEMA_OF_OUR_OWN}" CASCADE')
        await conn.execute(f'CREATE SCHEMA "{_A_SCHEMA_OF_OUR_OWN}"')
        await conn.execute(f'SET search_path TO "{_A_SCHEMA_OF_OUR_OWN}"')
        return {
            ("postgresql", feature): await exercise(conn)
            for feature, exercise in THE_POSTGRESQL_EXERCISES.items()
        }
    finally:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{_A_SCHEMA_OF_OUR_OWN}" CASCADE')
        await conn.close()


def _against_sqlite(path: pathlib.Path) -> dict[tuple[str, str], str]:
    conn = sqlite3.connect(path / "sqlite.db")
    try:
        return {
            ("sqlite", feature): exercise(conn.execute)
            for feature, exercise in THE_SQLITE_SHAPED_EXERCISES.items()
        }
    finally:
        conn.close()


def _against_turso(path: pathlib.Path) -> dict[tuple[str, str], str]:
    import turso

    conn = turso.connect(str(path / "turso.db"))
    try:
        return {
            ("turso", feature): exercise(conn.execute)
            for feature, exercise in THE_SQLITE_SHAPED_EXERCISES.items()
        }
    finally:
        conn.close()


@given("every answer saying an engine has a feature natively")
def _every_answer_saying_native(scenario: dict, ask) -> None:
    """Every native answer there is, so the claim is counted rather than
    sampled."""
    scenario["native"] = {
        (engine, feature)
        for engine in ENGINES
        for feature in FEATURES
        if ask(engine, feature)["native"]
    }


@when("that capability is used against that engine")
def _the_capability_is_used_against_the_engine(scenario: dict, tmp_path: pathlib.Path) -> None:
    """All three engines are opened. One that cannot be reached fails naming
    itself, because a skip nobody counts is a claim nobody supports."""
    scenario["exercised"] = {
        **_against_sqlite(tmp_path),
        **_against_turso(tmp_path),
        **asyncio.run(_against_postgresql()),
    }


@then("the engine does it")
def _the_engine_does_it(scenario: dict) -> None:
    """An answer that gains `native: True` with no exercise beside it fails
    here, which is the only place it can fail cheaply."""
    exercised = scenario["exercised"]

    assert set(exercised) == scenario["native"], (
        "these claims and these exercises are not the same set, so a claim went "
        f"unsupported or an exercise supports nothing: claimed "
        f"{sorted(scenario['native'] - set(exercised))}, exercised "
        f"{sorted(set(exercised) - scenario['native'])}"
    )
    silent = [pair for pair, evidence in exercised.items() if not evidence]
    assert silent == [], f"these exercises ran and reported nothing: {silent}"


# =============================================================================
# The round trip another function answers
# =============================================================================


@given("a feature an engine builds rather than has")
def _a_feature_an_engine_builds(scenario: dict, ask) -> None:
    """Every built answer there is, rather than the one the prose names, because
    the one nobody names is where a half-written recipe survives."""
    scenario["built"] = [
        (engine, feature)
        for engine in ENGINES
        for feature in FEATURES
        if not ask(engine, feature)["native"]
    ]
    pytest.xfail(
        "CODE NOT WRITTEN: creating what builds a capability, dropping it and "
        "reading it back needs something that opens a database and says what is "
        "there. That is `reader fn schema_inspect`, declared in "
        "docs/design/persistum.hd and not built. `fn capability_of` answers with "
        "a capability and opens nothing, so nothing it returns can be created or "
        "read back."
    )


@when("it is created on that engine and then read back")
def _it_is_created_and_read_back(scenario: dict) -> None:
    """Never reached today, and written to fail when it is."""
    scenario["read back"] = scenario["created"]


@then("it reads back as that feature rather than as the parts it is made of")
def _it_reads_back_as_the_feature(scenario: dict) -> None:
    """Never reached today, and written to fail when it is.

    An emulated materialized view is a real table. Reading it back as a table
    means the declaration says a view, the database says a table, and the next
    difference emits a drop for the object it is standing on.
    """
    assert scenario["read back"] == scenario["built"]


@then("dropping it leaves none of the parts behind")
def _dropping_it_leaves_none_of_the_parts(scenario: dict) -> None:
    """Never reached today, and written to fail when it is."""
    assert scenario["after dropping"] == []


# =============================================================================
# An engine that cannot say what it holds
# =============================================================================


@given("an engine that cannot be asked what a table holds")
def _an_engine_that_cannot_be_asked(scenario: dict, tmp_path: pathlib.Path) -> None:
    """SQLite's own authorizer refuses PRAGMA and allows everything else, so the
    connection is the driver's own and only the one capability is gone."""
    conn = sqlite3.connect(tmp_path / "withdrawn.db")
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY)")
    denied = {sqlite3.SQLITE_PRAGMA: sqlite3.SQLITE_DENY}
    conn.set_authorizer(lambda action, *_rest: denied.get(action, sqlite3.SQLITE_OK))

    assert list(conn.execute("SELECT count(*) FROM child")) == [(0,)], (
        "the whole connection was withdrawn, not its ability to describe a table"
    )
    scenario["connection"] = conn


@when("it is asked")
def _it_is_asked(scenario: dict) -> None:
    try:
        scenario["rows"] = list(scenario["connection"].execute("PRAGMA table_info('child')"))
    except sqlite3.Error as reached_the_caller:
        scenario["reached the caller"] = reached_the_caller


@then("the failure reaches the caller, and no empty answer comes back")
def _the_failure_reaches_the_caller(scenario: dict) -> None:
    reached = scenario.get("reached the caller")

    assert reached is not None, (
        f"the engine refused and the caller got rows instead of the refusal: {scenario.get('rows')}"
    )
    assert "not authorized" in str(reached), reached
    assert "rows" not in scenario, "the caller got an answer as well as an error"
