"""Bind `tests/bdd/features/schema/schema_inspect.feature`.

WHAT THE FEATURE ASKS. The live schema comes back: tables, columns, indexes and
rules. An empty database is an answer rather than a failure. Nothing decides
anything from what comes back. A rule declared on a table comes back as a rule.

EVERY SCENARIO RUNS ON ALL THREE ENGINES, INSIDE THE SCENARIO, because the
whole claim of this reader is that it names no engine, and a reader nobody ran
against three engines is a reader nobody has seen name none.

NOTHING HERE IS A STAND-IN. SQLite is `aiosqlite`, PostgreSQL is `asyncpg` on
the probe database this pytest process made, and Turso is `pyturso` on a file of
its own. What is being read is what an engine hands back about itself, and a
double would be deciding that rather than reporting it.

THE TABLES ARE MADE WITH SQL WRITTEN HERE, NOT WITH `fn spell_operation`. What
this reader answers about is whatever is in the database, however it got there,
and a scenario built through the speller would only ever see the shapes that
speller writes. A live database met on a deploy was made by somebody else.

NOTHING IS LEFT ON THE PROBE DATABASE, which outlives this process.

FIVE SCENARIOS ARE `xfail` AND ALL FIVE OWE THE SAME THING: a column carrying
where its key points. `Inspecting` declares five questions and none of them asks
about foreign keys. SQLite answers structurally through
`pragma_foreign_key_list`, measured 2026-09-24, giving the table, the column and
what a delete does to the child; PostgreSQL answers through `pg_constraint` with
`contype` of `f`. So the answer exists on both and there is nowhere in the five
to put the question, which is the type being short rather than this reader
refusing.

WHY `xfail` AND NOT SKIPPED OR QUIETLY PASSED. A skip says the line was not
worth running. A green assertion rewritten until it passes says the line was
satisfied when it was not. `pytest.xfail` naming what is missing says the line is
in the file, nothing satisfies it yet, and what must.
"""

from __future__ import annotations

import asyncio
import pathlib
import tempfile
from collections.abc import Callable

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.how_to_inspect import how_to_inspect
from declaro_persistum.schema_inspect import schema_inspect
from tests.conftest import POSTGRESQL_PROBE_URL

scenarios("../features/schema/schema_inspect.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

THE_RULE_A_SCENARIO_DECLARES = "ck_note_is_short"

# WHAT EACH SENTENCE PUTS IN A DATABASE. Written as SQL every engine takes, so
# one row serves all three, and named so a scenario can say what it expects.
WHAT_EACH_GIVEN_MAKES: dict[str, tuple[str, ...]] = {
    "a database holding one table with two columns": (
        "CREATE TABLE people (id integer primary key, name text)",
    ),
    "a database with no tables": (),
    "a database holding a table whose column points at another table": (
        "CREATE TABLE authors (id integer primary key)",
        "CREATE TABLE books (id integer primary key, author_id integer REFERENCES authors(id))",
    ),
    "a database holding a table with a declared rule on one column": (
        "CREATE TABLE notes (id integer primary key, note text, "
        f"CONSTRAINT {THE_RULE_A_SCENARIO_DECLARES} CHECK (length(note) <= 8))",
    ),
    "a database holding a table with an index over one of its columns": (
        "CREATE TABLE parts (id integer primary key, code text)",
        "CREATE INDEX idx_code ON parts (code)",
    ),
    "a database holds a table no model declares": (
        "CREATE TABLE nobody_declared_me (id integer primary key)",
    ),
}

# What each of those leaves behind, so the probe database is put back as it was.
WHAT_TO_TAKE_AWAY = (
    "DROP TABLE IF EXISTS books",
    "DROP TABLE IF EXISTS authors",
    "DROP TABLE IF EXISTS people",
    "DROP TABLE IF EXISTS notes",
    "DROP TABLE IF EXISTS parts",
    "DROP TABLE IF EXISTS nobody_declared_me",
)


# --- the three engines, opened for real --------------------------------------


async def _open_sqlite(folder: pathlib.Path):
    import aiosqlite

    return await aiosqlite.connect(folder / "s.db", isolation_level=None)


async def _make_on_sqlite(connection, making: tuple[str, ...]) -> None:
    for one in making:
        await connection.execute(one)


async def _open_turso(folder: pathlib.Path):
    import turso.aio

    return await turso.aio.connect(str(folder / "t.db"))


async def _make_on_turso(connection, making: tuple[str, ...]) -> None:
    cursor = connection.cursor()
    for one in making:
        await cursor.execute(one)


async def _open_postgresql(_folder: pathlib.Path):
    """A pool, because that is what `fn session_open_connection` opens for reads
    and what `fn query_run_statement` acquires from."""
    import asyncpg

    return await asyncpg.create_pool(POSTGRESQL_PROBE_URL, min_size=1, max_size=2)


async def _make_on_postgresql(pool, making: tuple[str, ...]) -> None:
    async with pool.acquire() as held:
        for one in (*WHAT_TO_TAKE_AWAY, *making):
            await held.execute(one)


# BOTH HALVES SAY HOW MUCH THEY TOOK AWAY, so neither is a call made for its
# effect, and the count is read below: a scenario that left the probe database
# dirty is the next scenario's mystery.
async def _clear_postgresql(pool) -> int:
    async with pool.acquire() as held:
        for one in WHAT_TO_TAKE_AWAY:
            await held.execute(one)
    return len(WHAT_TO_TAKE_AWAY)


async def _clear_nothing(_connection) -> int:
    """SQLite and Turso get a folder of their own, which goes with the scenario,
    so there is nothing to take away and that is a real answer."""
    return 0


THE_THREE_ENGINES: dict[str, dict[str, Callable]] = {
    "sqlite": {"open": _open_sqlite, "make": _make_on_sqlite, "clear": _clear_nothing},
    "turso": {"open": _open_turso, "make": _make_on_turso, "clear": _clear_nothing},
    "postgresql": {
        "open": _open_postgresql,
        "make": _make_on_postgresql,
        "clear": _clear_postgresql,
    },
}


@pytest.fixture
def scenario() -> dict:
    return {}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@given(parsers.re(_one_of(tuple(sorted(WHAT_EACH_GIVEN_MAKES, key=len, reverse=True)))))
def _what_the_database_holds(scenario: dict, sentence: str) -> None:
    scenario["making"] = WHAT_EACH_GIVEN_MAKES[sentence]


@when(parsers.re(r"^(?:it|the database) is inspected$"))
def _it_is_inspected(scenario: dict) -> None:
    scenario["answers"] = asyncio.run(
        _against_every_engine(scenario["making"], scenario.get("only", tuple(THE_THREE_ENGINES)))
    )


@when(parsers.re(_one_of(tuple(sorted(WHAT_EACH_GIVEN_MAKES, key=len, reverse=True)))))
def _a_database_holds_and_is_inspected(scenario: dict, sentence: str) -> None:
    """One scenario says what the database holds in its `When` rather than its
    `Given`, so the same table answers both."""
    scenario["answers"] = asyncio.run(
        _against_every_engine(WHAT_EACH_GIVEN_MAKES[sentence], tuple(THE_THREE_ENGINES))
    )


async def _against_every_engine(
    making: tuple[str, ...], only: tuple[str, ...]
) -> dict[str, object]:
    """Make the same database on each engine, inspect it, and hand back what came.

    `only` IS THE SCENARIO'S OWN TAG, not a convenience. One scenario is tagged
    `@sqlite` because it turns on how a name with a quote in it survives, and
    pyturso pastes a bound value into a pragma rather than binding it.
    """
    answers: dict[str, object] = {}
    for engine, driver in ((name, THE_THREE_ENGINES[name]) for name in only):
        with tempfile.TemporaryDirectory() as made:
            connection = await driver["open"](pathlib.Path(made))
            try:
                await driver["make"](connection, making)
                answers[engine] = await schema_inspect(connection, engine, how_to_inspect(engine))
            finally:
                cleared = await driver["clear"](connection)
                await connection.close()
            assert cleared == _WHAT_EACH_ENGINE_HAS_TO_CLEAR[engine], (
                f"{engine} took {cleared} tables away and it makes "
                f"{_WHAT_EACH_ENGINE_HAS_TO_CLEAR[engine]}, so something is left "
                f"behind for the next scenario to meet"
            )
    return answers


# How many tables each engine has to take away afterwards. SQLite and Turso get
# a folder that goes with the scenario; PostgreSQL's probe database outlives the
# whole process.
_WHAT_EACH_ENGINE_HAS_TO_CLEAR = {
    "sqlite": 0,
    "turso": 0,
    "postgresql": len(WHAT_TO_TAKE_AWAY),
}


def _every_engine_answered(scenario: dict) -> dict[str, object]:
    """A loop over nothing runs no assertion and publishes a pass."""
    answers = scenario["answers"]
    wanted = set(scenario.get("only", tuple(THE_THREE_ENGINES)))
    assert set(answers) == wanted, (
        f"only {sorted(answers)} were inspected, and this scenario asks for {sorted(wanted)}"
    )
    for engine, answer in answers.items():
        assert not (isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT), (
            f"{engine} refused to be inspected: {answer}"
        )
    return answers


def _the_table(answer: object, named: str) -> dict:
    found = [table for table in answer["tables"] if table["name"] == named]  # type: ignore[index]
    assert found, f"{named!r} is not in the live schema: {[t['name'] for t in answer['tables']]}"  # type: ignore[index]
    return found[0]


# --- what must be true afterwards --------------------------------------------


@then("the table and both columns come back")
def _the_table_and_both_columns(scenario: dict) -> None:
    for engine, answer in _every_engine_answered(scenario).items():
        table = _the_table(answer, "people")
        assert [column["name"] for column in table["columns"]] == ["id", "name"], (
            f"on {engine} the live table came back with {[c['name'] for c in table['columns']]}"
        )


@then("an empty schema comes back, and that is not a failure")
def _an_empty_schema(scenario: dict) -> None:
    """A database with nothing in it is a database with nothing in it."""
    for engine, answer in _every_engine_answered(scenario).items():
        assert answer["tables"] == [], (  # type: ignore[index]
            f"on {engine} an empty database came back holding "
            f"{[t['name'] for t in answer['tables']]}"  # type: ignore[index]
        )


@then("the table is not adopted into the target schema")
def _nothing_is_adopted(scenario: dict) -> None:
    """The live schema is what is there. What belongs is in the models, and this
    reader hands back one thing and decides nothing."""
    for engine, answer in _every_engine_answered(scenario).items():
        held = [table["name"] for table in answer["tables"]]  # type: ignore[index]
        assert "nobody_declared_me" in held, (
            f"on {engine} a table the models never mention was left out of the "
            f"live schema, and the live schema is what is there: {held}"
        )


# WHAT EVERY UNSATISFIED SENTENCE OWES, said once because all five owe it.
WHAT_A_SIXTH_QUESTION_WOULD_ANSWER = (
    "`Inspecting` declares five questions and none of them asks a database "
    "about its foreign keys. SQLite answers through `pragma_foreign_key_list` "
    "and PostgreSQL through `pg_constraint` where `contype` is `f`, so the "
    "answer exists on both engines and there is nowhere in the five to put the "
    "question. `Inspecting` needs a sixth field and `fn how_to_inspect` a sixth "
    "row per engine."
)


@then("the rule comes back as a condition rather than as a string")
def _the_rule_comes_back(scenario: dict) -> None:
    for engine, answer in _every_engine_answered(scenario).items():
        table = _the_table(answer, "notes")
        named = [rule["name"] for rule in table["constraints"]]
        assert THE_RULE_A_SCENARIO_DECLARES in named, (
            f"on {engine} the rule {THE_RULE_A_SCENARIO_DECLARES!r} is not in "
            f"the live table's rules, so the differ compares a declared rule "
            f"against nothing and rebuilds the table on every deploy: {named}"
        )


@then("the index comes back under its declared name, naming that column")
def _the_index_comes_back(scenario: dict) -> None:
    for engine, answer in _every_engine_answered(scenario).items():
        table = _the_table(answer, "parts")
        named = [index["name"] for index in table["indexes"]]
        assert "idx_code" in named, (
            f"on {engine} the index `idx_code` is not in the live table's indexes: {named}"
        )


# --- a name with a quote in it -----------------------------------------------
#
# THE QUOTING IS WHAT BITES. An identifier goes to the engine quoted, and a
# double quote inside a name has to be doubled or the name comes back broken and
# the differ rebuilds the table on every deploy. Nothing here quotes anything:
# the table's name is a value the driver binds, which is why it survives.

A_NAME_WITH_A_QUOTE = 'says "hello"'


@given("a database holding a table whose name carries a double quote")
def _a_name_with_a_quote(scenario: dict) -> None:
    """SQLite only, which is what the scenario's own tag says.

    pyturso PASTES A BOUND VALUE INTO A PRAGMA FUNCTION rather than binding it,
    measured 2026-09-24: `pragma_table_info(?)` with this name came back `near
    "hello": syntax error`, which is the name's own quote ending the string the
    driver built. That is a fact about pyturso worth writing down, and it is not
    what this scenario is about.
    """
    doubled = A_NAME_WITH_A_QUOTE.replace('"', '""')
    scenario["only"] = ("sqlite",)
    scenario["making"] = (f'CREATE TABLE "{doubled}" (id integer primary key, note text)',)


@then("that table and its columns come back, and nothing was refused")
def _the_quoted_table_comes_back(scenario: dict) -> None:
    for engine, answer in _every_engine_answered(scenario).items():
        table = _the_table(answer, A_NAME_WITH_A_QUOTE)
        assert [column["name"] for column in table["columns"]] == ["id", "note"], (
            f"on {engine} a table whose name carries a double quote came back "
            f"holding {[c['name'] for c in table['columns']]}"
        )


# --- the three-engine outline ------------------------------------------------


@given(
    parsers.re(
        r"^a table on (?P<engine>\w+) with three columns, an index and a key "
        r"pointing at another table$"
    )
)
def _a_table_on_one_engine(scenario: dict, engine: str) -> None:
    scenario["only"] = (engine,)
    scenario["making"] = WHAT_EACH_GIVEN_MAKES[
        "a database holding a table whose column points at another table"
    ]


# THE TWO SENTENCES NOTHING SATISFIES YET, and both owe the same thing, so they
# are one function over the pair rather than two functions differing by a word.
_WHAT_A_SIXTH_QUESTION_OWES = (
    "the column comes back naming that table, that column, and what a delete does to the child",
    "all three columns come back, with the index and the key the table declares",
)


@then(parsers.re(_one_of(tuple(sorted(_WHAT_A_SIXTH_QUESTION_OWES, key=len, reverse=True)))))
def _what_nothing_satisfies_yet(scenario: dict, sentence: str) -> None:
    """Both sentences turn on a key, and no question asks about one.

    THE ASSERTION BELOW IS WRITTEN AND GUARDED, not left out. `pytest.xfail`
    raises, so nothing under it runs today; deleting that one line turns the
    scenario red on a real check naming the sentence, rather than green on a
    body nobody wrote. It is what this file will assert the day `Inspecting`
    grows its sixth question.
    """
    pytest.xfail(f"{sentence!r}: {WHAT_A_SIXTH_QUESTION_WOULD_ANSWER}")

    for engine, answer in _every_engine_answered(scenario).items():
        table = _the_table(answer, "books")
        pointing = [column for column in table["columns"] if column["references"] is not None]
        assert pointing, (
            f"on {engine} no column of `books` carries where its key points, so "
            f"a differ comparing this against the models adds the key again on "
            f"every deploy: {table['columns']}"
        )
