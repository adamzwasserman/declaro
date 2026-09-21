"""Bind `tests/bdd/features/core/capability_of.feature`.

WHAT THE FUNCTION ANSWERS. `fn capability_of` is handed one engine and one
feature and says what that engine does about it: has it, or is built out of
something else. Three engines crossed with five features is fifteen cells, and
this file asks for all fifteen rather than for the two the prose names, because
the cell nobody filled is the one nobody thinks to ask about.

A COMBINATION OUTSIDE THE VOCABULARY RAISES, AND THE FEATURE CALLS THAT A
FAULT. The declaration gives this function no `Fault` arm, so a refusal cannot
come back as a value: every engine has an answer about every feature, even when
that answer is that it cannot. What is left for a word nobody declared is the
`KeyError` the lookup raises, and the key is the pair, so it names the engine
and the feature together. That is the disagreement between the feature file and
the declaration, and it is resolved here in the declaration's favour.

WHY THE NATIVE CLAIMS ARE RUN AND NOT READ. `docs/architecture.md` says native
is not the same as working, and that only a test against the engine backs a
cell up. So the scenario that says so opens all three: SQLite through the
standard library, Turso through pyturso, and PostgreSQL through asyncpg against
the database this pytest process made for itself. Nine cells claim native and
nine exercises run, and the step fails when those two sets are not the same
set, so a cell that gains `native: True` without an exercise fails here rather
than on an engine at a deploy.

WHY POSTGRESQL IS A FAILURE AND NOT A SKIP. `tests/conftest.py` records the
measurement: with the server down the suite reported 2680 passed and 200
skipped, and 186 of those skips had been passing. Nobody counts the `s`
characters, so this calls `refuse_without_postgresql` like the rest of the
suite.

THE WITHDRAWN PRAGMA IS THE ENGINE REFUSING, NOT A STAND-IN. SQLite's own
authorizer denies `SQLITE_PRAGMA` and allows everything else, so the connection
is real, its contract is the driver's, and an ordinary SELECT still answers on
it. This package has twice chased a defect that lived only in a double, and a
double of a connection is exactly where that happens.

WHAT NO STEP HERE DOES. It never reads the module's own text. Every claim below
is about what came back from a call or from an engine.

ONE FUNCTION OVER A TABLE. The two scenarios that name an engine and a
materialized view are one Given keyed by the engine. The three that read the
table -- "the capability table", "any cell", "its cell" -- are one When, because
reading one cell and reading the grid are the same act at different widths. The
two that check a table are one When over whatever the Given built. And the two
that read a single PRAGMA are one When over a table whose rows carry the PRAGMA
each one names and the word its Then reads the answer back under.
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

_ARCHITECTURE = pathlib.Path(__file__).resolve().parents[3] / "docs" / "architecture.md"

# WHAT A BUILT CELL HAS TO NAME. Three, and not one: create and drop are
# different statements, and an emulated materialized view is a real table, so an
# inspector that cannot recognize it reports a table and the next difference
# proposes dropping it.
WHAT_A_BUILT_CELL_NAMES = ("created", "dropped", "recognized")

# A cell that is not there. Compared by identity, so no real cell can be it.
NO_CELL = object()

A_CAPABILITY = ("native", "built_from")


def _cell(ask: Callable[..., Any], engine: str, feature: str) -> Any:
    """What one cell says, or `NO_CELL` where nobody filled it in."""
    try:
        return ask(engine, feature)
    except KeyError:
        return NO_CELL


def _read(ask: Callable[..., Any], engines: tuple[str, ...], features: tuple[str, ...]) -> dict:
    """Every engine crossed with every feature, asked one cell at a time."""
    return {
        (engine, feature): _cell(ask, engine, feature) for feature in features for engine in engines
    }


def _complaints(table: dict) -> list[str]:
    """Everything wrong with a table: a cell nobody filled, and a built cell
    that names less than the three things a built cell has to name."""
    gaps = [
        f"{feature} has no cell for {engine}"
        for (engine, feature), cell in table.items()
        if cell is NO_CELL
    ]
    unnamed = [
        f"{feature} on {engine} never says what {word} it"
        for (engine, feature), cell in table.items()
        if cell is not NO_CELL and not cell["native"]
        for word in WHAT_A_BUILT_CELL_NAMES
        if f"{word}:" not in cell["built_from"]
    ]
    return gaps + unnamed


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
    """`ranges` is not one of the five words `Feature` admits, so no cell of
    this table was ever written for it and none can be worked out."""
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
    assert missing == [], f"the cell never names {missing}: {capability['built_from']}"


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
# What the table is, read whole
# =============================================================================


@when(parsers.re(r"^(?:the capability table|any cell|its cell) is read$"))
def _the_table_is_read(scenario: dict, ask) -> None:
    """One When for three sentences. Reading one cell and reading the grid are
    the same act at different widths, so the grid is read and the Then that
    cares about one cell picks it out."""
    scenario["table"] = _read(ask, ENGINES, FEATURES)


@then("each capability the package offers is a row")
def _each_capability_is_a_row(scenario: dict) -> None:
    rows = {feature for _engine, feature in scenario["table"]}

    assert FEATURES, "the Feature vocabulary read as empty, so this asserted nothing"
    assert rows == set(FEATURES), f"the table has rows {sorted(rows)} for {sorted(FEATURES)}"


@then("every engine the package speaks has a cell in every row")
def _every_engine_has_a_cell_in_every_row(scenario: dict) -> None:
    table = scenario["table"]

    assert len(table) == len(ENGINES) * len(FEATURES), (
        f"{len(ENGINES)} engines over {len(FEATURES)} features is "
        f"{len(ENGINES) * len(FEATURES)} cells and the table answered {len(table)}"
    )
    gaps = [
        f"{feature} has no cell for {engine}"
        for (engine, feature), cell in table.items()
        if cell is NO_CELL
    ]
    assert gaps == [], f"these cells were never decided: {gaps}"


@then("it says the engine has it, or names what supplies it")
def _it_says_one_or_names_the_other(scenario: dict) -> None:
    """Exactly one of the two, for every cell. A cell claiming both would leave
    a reader to choose, and a cell claiming neither answers nothing."""
    table = scenario["table"]

    wrong = [
        f"{feature} on {engine}: {cell}"
        for (engine, feature), cell in table.items()
        if tuple(cell) != A_CAPABILITY or cell["native"] == (cell["built_from"] != "")
    ]
    assert wrong == [], (
        f"these cells neither claim the engine has it nor name what supplies it: {wrong}"
    )


# =============================================================================
# What a built cell has to carry, and what the table is checked for
# =============================================================================


@given("a capability that is built rather than native")
def _a_capability_that_is_built(scenario: dict, ask) -> None:
    """Every built cell there is, rather than the one the prose names, because
    the one nobody names is where a half-written cell survives."""
    scenario["built"] = [
        (engine, feature)
        for engine in ENGINES
        for feature in FEATURES
        if not ask(engine, feature)["native"]
    ]


@then("it names what creates it, what removes it, and how the inspector recognizes it")
def _it_names_all_three(scenario: dict) -> None:
    built = scenario["built"]
    table = scenario["table"]

    assert built, "no cell in this table is built, so this scenario examined nothing"
    unnamed = [
        f"{feature} on {engine} never says what {word} it"
        for engine, feature in built
        for word in WHAT_A_BUILT_CELL_NAMES
        if f"{word}:" not in table[(engine, feature)]["built_from"]
    ]
    assert unnamed == [], f"these cells cover less than half a feature: {unnamed}"


@given("a cell naming what creates a capability and not how to recognize it")
def _a_cell_that_names_no_recognition(scenario: dict) -> None:
    """Written out here rather than taken from the table, because the table has
    no such cell and the check is what keeps it that way."""
    scenario["table"] = {
        ("sqlite", "materialized_view"): {
            "native": False,
            "built_from": (
                "created: a table holding the rows and the triggers that keep it "
                "current; dropped: the triggers first, then the table"
            ),
        }
    }


@given("a capability with no cell for turso")
def _a_capability_with_no_cell_for_turso(scenario: dict, ask) -> None:
    """The real table with one cell taken out, so what the check finds is the
    hole and not some second difference."""
    scenario["gap"] = ("turso", "materialized_view")
    scenario["table"] = {**_read(ask, ENGINES, FEATURES), scenario["gap"]: NO_CELL}


@when("the table is checked")
def _the_table_is_checked(scenario: dict) -> None:
    scenario["complaints"] = _complaints(scenario["table"])


@then("it fails, because the next difference would propose dropping it")
def _it_fails_over_recognition(scenario: dict) -> None:
    """An emulated materialized view is a real table. An inspector that cannot
    recognize it reports a table, the declaration says a view, and the next
    difference emits a drop for the object it is standing on."""
    complaints = scenario["complaints"]

    assert complaints, "the check passed a cell that says nothing about recognition"
    assert [one for one in complaints if "recognized" in one], (
        f"the check complained, and not about recognition: {complaints}"
    )
    # AND ABOUT NOTHING ELSE: naming recognition is the whole of the difference
    # between this cell and one the check accepts.
    named = {
        key: {
            **cell,
            "built_from": cell["built_from"] + "; recognized: the inspector asks the registry",
        }
        for key, cell in scenario["table"].items()
    }
    assert _complaints(named) == [], (
        f"the same cell still fails once it names recognition: {_complaints(named)}"
    )


@then("it fails and names the capability and the engine")
def _it_fails_and_names_both(scenario: dict) -> None:
    engine, feature = scenario["gap"]
    complaints = scenario["complaints"]

    named = [one for one in complaints if engine in one and feature in one]
    assert named, (
        f"a capability nobody decided for {engine} passed the check silently: {complaints}"
    )


@when("an engine is added")
def _an_engine_is_added(scenario: dict, ask) -> None:
    """A fourth engine, with no cell anywhere. Adding one is adding a column,
    and a column nobody filled is what this asks the check to find."""
    scenario["added"] = "duckdb"
    scenario["table"] = _read(ask, (*ENGINES, "duckdb"), FEATURES)


@then("every row needs a cell, and a row without one fails the check")
def _every_row_needs_a_cell(scenario: dict) -> None:
    complaints = _complaints(scenario["table"])

    assert complaints == [f"{feature} has no cell for duckdb" for feature in FEATURES], (
        f"adding an engine left {len(complaints)} rows complaining, for "
        f"{len(FEATURES)} rows that now have a column nobody filled: {complaints}"
    )


# =============================================================================
# What the documentation is allowed to say
# =============================================================================


@when("the documentation says what an engine supports")
def _the_documentation_says_what_an_engine_supports(scenario: dict) -> None:
    scenario["documentation"] = _ARCHITECTURE.read_text(encoding="utf-8")


@then("it says it by reading this table rather than by asserting it")
def _the_documentation_agrees_with_the_table(scenario: dict, ask) -> None:
    """The one claim `docs/architecture.md` makes about an engine and a feature
    is checked against the table every run, so the two cannot drift apart
    quietly. It cannot see a claim written in words this step does not look
    for, which is why the claim it does look for is quoted whole."""
    said = scenario["documentation"]

    claim = "PostgreSQL has a materialized view. SQLite doesn't, so we build one out of a table, its triggers, and a registry row."
    assert claim in said, (
        f"{_ARCHITECTURE.name} no longer makes the claim this step checks against "
        "the table, so nothing here is comparing anything"
    )
    assert ask("postgresql", "materialized_view")["native"] is True, (
        "the documentation says PostgreSQL has one and the table says otherwise"
    )
    built = ask("sqlite", "materialized_view")
    assert built["native"] is False, (
        "the documentation says SQLite does not have one and the table says otherwise"
    )
    missing = [word for word in ("table", "trigger", "registry") if word not in built["built_from"]]
    assert missing == [], (
        f"the documentation says a table, its triggers and a registry row, and the "
        f"cell never names {missing}"
    )


# =============================================================================
# Native is a claim a test has to support
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


@given("a cell saying a capability is native on an engine")
def _a_cell_saying_native(scenario: dict, ask) -> None:
    """Every native cell there is, so the claim is counted rather than sampled."""
    scenario["native"] = {
        (engine, feature)
        for engine in ENGINES
        for feature in FEATURES
        if ask(engine, feature)["native"]
    }


@then("a test exercises that capability against that engine")
def _a_test_exercises_it_against_that_engine(scenario: dict, tmp_path: pathlib.Path) -> None:
    """Native is a claim about an engine, and only the engine can support it.

    All three are opened. A cell that gains `native: True` with no exercise
    beside it fails here, which is the only place it can fail cheaply."""
    exercised = {
        **_against_sqlite(tmp_path),
        **_against_turso(tmp_path),
        **asyncio.run(_against_postgresql()),
    }

    assert set(exercised) == scenario["native"], (
        "these cells and these exercises are not the same set, so a claim went "
        f"unsupported or an exercise supports nothing: claimed "
        f"{sorted(scenario['native'] - set(exercised))}, exercised "
        f"{sorted(set(exercised) - scenario['native'])}"
    )
    silent = [pair for pair, evidence in exercised.items() if not evidence]
    assert silent == [], f"these exercises ran and reported nothing: {silent}"


# =============================================================================
# The PRAGMAs this package reads a live schema through
# =============================================================================
#
# THE INSPECTOR, THE DIFFER'S INPUTS AND BOTH RECONSTRUCTION PATHS REST ON THESE
# FOUR. What columns a table has, what indexes are on it, what those indexes
# cover, and what foreign keys it declares.

THE_PRAGMAS_PERSISTUM_DEPENDS_ON = (
    "table_info('child')",
    "index_list('child')",
    "index_info('child_by_parent')",
    "foreign_key_list('child')",
)

# The sentence a scenario writes -> the PRAGMA it asks for, and the word its
# Then reads the answer back under.
ONE_PRAGMA_EACH: dict[str, tuple[str, str]] = {
    "table_info is read for that table": ("table_info('child')", "columns"),
    "foreign_key_list is read for the referencing table": (
        "foreign_key_list('child')",
        "keys",
    ),
}


@given("a real SQLite database with a table, an index and a foreign key")
def _a_real_sqlite_database(scenario: dict, tmp_path: pathlib.Path) -> None:
    conn = sqlite3.connect(tmp_path / "shape.db")
    conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
    conn.execute(
        "CREATE TABLE child (id INTEGER PRIMARY KEY, "
        "parent_id INTEGER REFERENCES parent(id), name TEXT)"
    )
    conn.execute("CREATE INDEX child_by_parent ON child (parent_id)")
    scenario["connection"] = conn


@given("a connection whose PRAGMA support has been withdrawn")
def _a_connection_without_pragma(scenario: dict, tmp_path: pathlib.Path) -> None:
    """SQLite's own authorizer refuses PRAGMA and allows everything else, so the
    connection is the driver's own and only the one capability is gone."""
    conn = sqlite3.connect(tmp_path / "withdrawn.db")
    conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY)")
    denied = {sqlite3.SQLITE_PRAGMA: sqlite3.SQLITE_DENY}
    conn.set_authorizer(lambda action, *_rest: denied.get(action, sqlite3.SQLITE_OK))

    assert list(conn.execute("SELECT count(*) FROM child")) == [(0,)], (
        "the whole connection was withdrawn, not its PRAGMA support"
    )
    scenario["connection"] = conn


@when("each PRAGMA persistum depends on is asked of it")
def _each_pragma_is_asked(scenario: dict) -> None:
    scenario["answers"] = {
        pragma: list(scenario["connection"].execute(f"PRAGMA {pragma}"))
        for pragma in THE_PRAGMAS_PERSISTUM_DEPENDS_ON
    }


@when("a PRAGMA is asked of it")
def _a_pragma_is_asked(scenario: dict) -> None:
    try:
        scenario["rows"] = list(scenario["connection"].execute("PRAGMA table_info('child')"))
    except sqlite3.Error as reached_the_caller:
        scenario["reached the caller"] = reached_the_caller


@when(parsers.re(r"^(?P<sentence>(?:table_info|foreign_key_list) is read for .+)$"))
def _one_pragma_is_read(scenario: dict, sentence: str) -> None:
    """One When for the two scenarios that read a single PRAGMA. They differ in
    the PRAGMA they name and in where their Then looks for the answer, so both
    are rows and the sentence is the key: a scenario reworded without a row here
    fails by KeyError naming the sentence rather than reading somebody else's
    answer."""
    pragma, where = ONE_PRAGMA_EACH[sentence]
    scenario[where] = list(scenario["connection"].execute(f"PRAGMA {pragma}"))


@when("index_list is read and then index_info for one of its indexes")
def _index_list_and_then_index_info(scenario: dict) -> None:
    listed = list(scenario["connection"].execute("PRAGMA index_list('child')"))
    scenario["listed"] = listed
    scenario["named"] = listed[0][1]
    scenario["described"] = list(
        scenario["connection"].execute(f"PRAGMA index_info('{scenario['named']}')")
    )


@then("every one is answered by the engine")
def _every_one_is_answered(scenario: dict) -> None:
    answers = scenario["answers"]

    assert len(answers) == len(THE_PRAGMAS_PERSISTUM_DEPENDS_ON)
    unanswered = [pragma for pragma, rows in answers.items() if not rows]
    assert unanswered == [], (
        f"these PRAGMAs came back empty, so the engine cannot describe its own "
        f"shape and every reader of it is reading nothing: {unanswered}"
    )


@then("the error reaches the caller")
def _the_error_reaches_the_caller(scenario: dict) -> None:
    reached = scenario.get("reached the caller")

    assert reached is not None, (
        f"the PRAGMA was refused and the caller got rows instead of the refusal: "
        f"{scenario.get('rows')}"
    )
    assert "not authorized" in str(reached), reached
    assert "rows" not in scenario, "the caller got an answer as well as an error"


@then("it lists every column of the table")
def _it_lists_every_column(scenario: dict) -> None:
    named = [row[1] for row in scenario["columns"]]

    assert named == ["id", "parent_id", "name"], named


@then("the index named by the first is described by the second")
def _the_two_agree(scenario: dict) -> None:
    assert scenario["named"] == "child_by_parent", scenario["listed"]
    assert [row[2] for row in scenario["described"]] == ["parent_id"], scenario["described"]


@then("it names the referenced table")
def _it_names_the_referenced_table(scenario: dict) -> None:
    referenced = [row[2] for row in scenario["keys"]]

    assert referenced == ["parent"], scenario["keys"]
