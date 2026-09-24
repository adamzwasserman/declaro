"""Bind `tests/bdd/features/query/query_ask.feature`.

WHAT THE FEATURE ASKS. Every grammar reaches the same door and the rows come
back. A read answers with rows and a write with how many rows it touched. A
column no model declares is refused here rather than by the engine. A question
that goes wrong comes back as a fault.

EVERY SCENARIO RUNS ON A REAL SQLITE DATABASE, with a real drainer beside it, so
a write really lands and the rows really come back.

THE FOUR GRAMMARS ARE ASKED IN THEIR OWN PHRASING. What each builder makes is
its own feature's business; what this file asks is that all four reach the door
and come back with the same rows.
"""

from __future__ import annotations

import asyncio
import pathlib

import pytest
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.crew_make import crew_make
from declaro_persistum.lifecycle_catch_up import lifecycle_catch_up
from declaro_persistum.query_ask import query_ask
from declaro_persistum.types import CatchingUp, Crew, Database, Model
from tests.conftest import a_column, a_model

scenarios("../features/query/query_ask.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

THE_CLASS = "Person"
THE_TABLE = "people"
WHO_IS_ALREADY_THERE = ("ada", "bob")
A_COLUMN_NO_MODEL_DECLARES = "nmae"


def _the_model() -> Model:
    return a_model(
        name=THE_CLASS,
        table=THE_TABLE,
        columns=[
            a_column(name="id", declared_type="integer", renamed_from=None, references=None),
            a_column(name="name", declared_type="text", renamed_from=None, references=None),
        ],
        dropping=[],
        indexes=[],
        constraints=[],
    )


# HOW EACH GRAMMAR SPELLS "EVERY PERSON". Four phrasings, one question.
HOW_EACH_GRAMMAR_ASKS: dict[str, dict] = {
    "pseudo_sql": {"model": THE_TABLE, "wanted": "all"},
    "django": {"model": THE_TABLE, "wanted": "all"},
    "prisma": {"model": THE_TABLE, "wanted": "all"},
    "sqlalchemy": {"model": THE_TABLE, "wanted": "all"},
}

WHAT_EACH_QUESTION_IS: dict[str, dict] = {
    "a caller asks for every person": {
        "asked": {"model": THE_TABLE, "wanted": "all"},
        "grammar": "pseudo_sql",
        "closed": False,
    },
    "a caller declares one new person": {
        "asked": {"model": THE_TABLE, "wanted": "add", "rows": [{"id": 3, "name": "cy"}]},
        "grammar": "pseudo_sql",
        "closed": False,
    },
    'a caller asks for the column "nmae"': {
        "asked": {"model": THE_TABLE, "wanted": "all", "select": [A_COLUMN_NO_MODEL_DECLARES]},
        "grammar": "pseudo_sql",
        "closed": False,
    },
    "a caller asks against a database whose connection has been closed": {
        "asked": {"model": THE_TABLE, "wanted": "all"},
        "grammar": "pseudo_sql",
        "closed": True,
    },
}


@pytest.fixture
def scenario(tmp_path: pathlib.Path) -> dict:
    return {"tmp": tmp_path}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@when(parsers.re(r"^a caller asks for every person in the (?P<grammar>\w+) grammar$"))
def _in_one_grammar(scenario: dict, grammar: str) -> None:
    scenario.update(
        asyncio.run(
            _ask(
                scenario,
                {"asked": HOW_EACH_GRAMMAR_ASKS[grammar], "grammar": grammar, "closed": False},
            )
        )
    )


@when(parsers.re(_one_of(tuple(sorted(WHAT_EACH_QUESTION_IS, key=len, reverse=True)))))
def _a_caller_asks(scenario: dict, sentence: str) -> None:
    scenario.update(asyncio.run(_ask(scenario, WHAT_EACH_QUESTION_IS[sentence])))


async def _a_database(folder: pathlib.Path, crew: Crew) -> tuple[Database, object]:
    import aiosqlite

    path = folder / "asked.db"
    reader = await aiosqlite.connect(path, isolation_level=None)
    writer = await aiosqlite.connect(path, isolation_level=None)
    await reader.execute(f"CREATE TABLE {THE_TABLE} (id integer primary key, name text)")
    for n, name in enumerate(WHO_IS_ALREADY_THERE, start=1):
        await reader.execute(f"INSERT INTO {THE_TABLE} VALUES (?, ?)", [n, name])
    db = Database(
        target=f"sqlite:///{path}",
        engine="sqlite",
        connections=[reader],  # type: ignore[list-item]
        writers=[writer],  # type: ignore[list-item]
        channel=None,
        crew=crew,
        shutdown="exit_immediately",
    )
    return db, writer


async def _ask(scenario: dict, question: dict) -> dict:
    crew = crew_make()
    db, writer = await _a_database(scenario["tmp"], crew)
    turning = asyncio.ensure_future(_keep_draining(db, writer))
    await _WHETHER_THE_READER_IS_CLOSED[question["closed"]](db)
    try:
        answered = await asyncio.wait_for(
            query_ask(db, _the_model(), question["grammar"], question["asked"]), 10.0
        )
    finally:
        crew["stop"].set()
        turning.cancel()
    rows = await _rows_in(db, question["closed"])
    await writer.close()
    await _WHETHER_THE_READER_IS_CLOSED[not question["closed"]](db)
    return {"answer": answered, "rows": rows}


async def _close_the_reader(db: Database) -> bool:
    await db["connections"][0].close()  # type: ignore[attr-defined]
    return False


async def _leave_it_open(_db: Database) -> bool:
    return True


_WHETHER_THE_READER_IS_CLOSED = {True: _close_the_reader, False: _leave_it_open}


async def _keep_draining(db: Database, writer: object) -> None:
    while not db["crew"]["stop"].is_set():  # type: ignore[attr-defined]
        await lifecycle_catch_up(
            CatchingUp(db=db, writer=writer, statement=None, writer_waiting=False, attempts=0)  # type: ignore[typeddict-item]
        )


async def _rows_in(db: Database, closed: bool) -> list[tuple]:
    return await _HOW_TO_READ_BACK[closed](db)


async def _read_nothing(_db: Database) -> list[tuple]:
    """A closed reader has nothing to read back, and that is a real answer."""
    return []


async def _read_the_rows(db: Database) -> list[tuple]:
    cursor = await db["writers"][0].execute(  # type: ignore[attr-defined]
        f"SELECT name FROM {THE_TABLE} ORDER BY id"
    )
    return list(await cursor.fetchall())


_HOW_TO_READ_BACK = {True: _read_nothing, False: _read_the_rows}


@then("the rows come back")
def _the_rows_come_back(scenario: dict) -> None:
    answered = scenario["answer"]

    assert isinstance(answered, list), (
        f"the caller was handed {answered!r} and asked for every person"
    )
    assert len(answered) == len(WHO_IS_ALREADY_THERE), (
        f"the caller got {len(answered)} people and the table holds {len(WHO_IS_ALREADY_THERE)}"
    )


@then("the answer is one, and the person is in the table")
def _the_write_landed(scenario: dict) -> None:
    assert scenario["answer"] == 1, (
        f"the caller was handed {scenario['answer']!r} for one declared person"
    )
    assert "cy" in [row[0] for row in scenario["rows"]], (
        f"the table holds {scenario['rows']!r} and the caller declared one more"
    )


@then("it is refused naming the model and the columns it declares")
def _refused_by_name(scenario: dict) -> None:
    """The engine would refuse this too, and its words name a table rather than
    the class the caller wrote."""
    answered = scenario["answer"]

    assert isinstance(answered, dict) and set(answered) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a column no model declares answered {answered!r}"
    )
    said = answered["what_went_wrong"] + " " + answered["what_to_do_instead"]
    assert A_COLUMN_NO_MODEL_DECLARES in said, f"the refusal does not name the column: {said!r}"
    assert "name" in said, (
        f"the refusal does not say which columns the model declares, so a caller "
        f"who mistyped one has to go and find out: {said!r}"
    )


@then("nothing reached the database")
def _nothing_reached_it(scenario: dict) -> None:
    assert [row[0] for row in scenario["rows"]] == list(WHO_IS_ALREADY_THERE), (
        f"the table holds {scenario['rows']!r} after a question that was refused"
    )


@then("a fault comes back and nothing was raised")
def _a_fault_came_back(scenario: dict) -> None:
    """A caller asked for rows. An exception is this package handing them its
    own defect instead."""
    answered = scenario["answer"]

    assert isinstance(answered, dict) and set(answered) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a question against a closed connection answered {answered!r}"
    )
