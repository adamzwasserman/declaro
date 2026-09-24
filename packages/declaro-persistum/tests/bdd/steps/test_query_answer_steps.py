"""Bind `tests/bdd/features/query/query_answer.feature`.

WHAT THE FEATURE ASKS. A read runs now and the rows come back. A write goes to
the crew and the caller gets a ticket's outcome. Rows that travel together run
as one transaction and skip the crew. Values stay out of the statement text. A
caller asks under the class's name and the rows still come back. A grammar
nobody builds for is refused by name.

EVERY SCENARIO RUNS ON A REAL SQLITE DATABASE. What this orchestrator does is
put statements to an engine, and rows in a table are the only evidence that
happened.

THE CREW IS REAL AND SO IS ITS DRAINER. A write deposited with nobody draining
would hang the scenario forever, so each write scenario runs one turn beside the
question, which is what a running database does.
"""

from __future__ import annotations

import asyncio
import pathlib

import pytest
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.crew_make import crew_make
from declaro_persistum.lifecycle_catch_up import lifecycle_catch_up
from declaro_persistum.query_answer import query_answer
from declaro_persistum.types import Answering, CatchingUp, Crew, Database, Model
from tests.conftest import a_column, a_model

scenarios("../features/query/query_answer.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

THE_CLASS = "Person"
THE_TABLE = "people"
WHO_IS_ALREADY_THERE = ("ada", "bob")


def _the_model() -> Model:
    """`class Person` over the table `people`, which are different words."""
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


# WHAT EACH SENTENCE ASKS FOR, in the library's own phrasing. `model` carries
# the class's name, because that is the only name a caller has.
WHAT_EACH_QUESTION_IS: dict[str, dict] = {
    "a caller asks for every person": {
        "asked": {"model": THE_TABLE, "wanted": "all"},
        "grammar": "pseudo_sql",
    },
    "a caller declares one new person": {
        "asked": {"model": THE_TABLE, "wanted": "add", "rows": [{"id": 3, "name": "cy"}]},
        "grammar": "pseudo_sql",
    },
    "a caller declares three people together": {
        "asked": {
            "model": THE_TABLE,
            "wanted": "add",
            "rows": [{"id": 4, "name": "dee"}, {"id": 5, "name": "eve"}, {"id": 6, "name": "fay"}],
        },
        "grammar": "pseudo_sql",
    },
    'a caller asks for people named "bob"': {
        "asked": {"model": THE_TABLE, "wanted": "all", "where": {"name": "bob"}},
        "grammar": "pseudo_sql",
    },
    "a caller asks for every person by the name of the class": {
        "asked": {"model": THE_CLASS, "wanted": "all"},
        "grammar": "pseudo_sql",
    },
    "a caller asks in a grammar nothing here can build": {
        "asked": {"model": THE_TABLE, "wanted": "all"},
        "grammar": "hieroglyphics",
    },
}


@pytest.fixture
def scenario(tmp_path: pathlib.Path) -> dict:
    return {"tmp": tmp_path}


def _one_of(sentences: tuple[str, ...]) -> str:
    return "^(?P<sentence>" + "|".join(sentences) + ")$"


@when(parsers.re(_one_of(tuple(sorted(WHAT_EACH_QUESTION_IS, key=len, reverse=True)))))
def _a_caller_asks(scenario: dict, sentence: str) -> None:
    scenario.update(asyncio.run(_ask(scenario, WHAT_EACH_QUESTION_IS[sentence])))


async def _a_database(folder: pathlib.Path, crew: Crew) -> tuple[Database, object]:
    """A real SQLite database with two people already in it."""
    import aiosqlite

    path = folder / "answers.db"
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
    """Ask the question with one drainer running beside it.

    THE DRAINER IS REAL. A write deposited with nobody draining waits forever,
    which is what a database with no crew running would do.
    """
    crew = crew_make()
    db, writer = await _a_database(scenario["tmp"], crew)
    asking = Answering(
        db=db,
        model=_the_model(),
        grammar=question["grammar"],  # type: ignore[typeddict-item]
        asked=question["asked"],
        declaration=None,  # type: ignore[typeddict-item]
        statement=None,  # type: ignore[typeddict-item]
        ticket="",
        receipt=None,  # type: ignore[typeddict-item]
        rows=[],
        answer=None,
    )
    turning = asyncio.ensure_future(_keep_draining(db, writer))
    try:
        answered = await asyncio.wait_for(query_answer(asking), 10.0)
    finally:
        crew["stop"].set()
        turning.cancel()
    rows = await _rows_in(db)
    await writer.close()
    await db["connections"][0].close()  # type: ignore[attr-defined]
    return {"answer": answered, "rows": rows, "crew": crew}


async def _keep_draining(db: Database, writer: object) -> None:
    """One drainer, taking turns until the crew is stopped."""
    while not db["crew"]["stop"].is_set():  # type: ignore[attr-defined]
        await lifecycle_catch_up(
            CatchingUp(db=db, writer=writer, statement=None, writer_waiting=False, attempts=0)  # type: ignore[typeddict-item]
        )


async def _rows_in(db: Database) -> list[tuple]:
    cursor = await db["connections"][0].execute(  # type: ignore[attr-defined]
        f"SELECT name FROM {THE_TABLE} ORDER BY id"
    )
    return list(await cursor.fetchall())


# --- what must be true afterwards --------------------------------------------


def _the_answer(scenario: dict) -> dict:
    answered = scenario["answer"]
    assert not (isinstance(answered, dict) and set(answered) == THE_THREE_FACTS_IN_A_FAULT), (
        f"the question was refused: {answered}"
    )
    return answered


@then("the statement ran against the database")
def _it_ran(scenario: dict) -> None:
    """A read that never reached the engine answers from nothing at all."""
    statement = _the_answer(scenario)["statement"]

    assert statement and THE_TABLE in statement["text"], (
        f"the statement is {statement!r}, and a read that never reached the "
        f"engine has no rows to give back"
    )


@then("the rows came back shaped as the model")
def _the_rows_came_back(scenario: dict) -> None:
    answered = _the_answer(scenario)["answer"]

    assert isinstance(answered, list) and len(answered) == len(WHO_IS_ALREADY_THERE), (
        f"the caller was handed {answered!r} and the table holds {len(WHO_IS_ALREADY_THERE)} people"
    )
    assert all("name" in row for row in answered), (
        f"the rows came back without the model's own columns on them: {answered!r}"
    )


@then("the write went to the crew")
def _it_went_to_the_crew(scenario: dict) -> None:
    """A write that ran on the reader's connection would block every read behind
    it, which is what the crew exists to stop."""
    written = [row[0] for row in scenario["rows"]]

    assert "cy" in written, (
        f"the table holds {written} and the caller declared one more person, so "
        f"the write never landed"
    )


@then("a ticket came back")
def _a_ticket_came_back(scenario: dict) -> None:
    """What the caller reads is the outcome of its own ticket, not the ticket."""
    answered = _the_answer(scenario)["answer"]

    assert answered == 1, (
        f"the caller was handed {answered!r} for one declared person, and a "
        f"write answers with how many rows it touched"
    )


@then("they run as one transaction")
def _one_transaction(scenario: dict) -> None:
    written = [row[0] for row in scenario["rows"]]

    assert {"dee", "eve", "fay"} <= set(written), (
        f"the table holds {written} and three people were declared together, so "
        f"some of the group landed and some did not"
    )


@then("nothing was deposited")
def _nothing_was_deposited(scenario: dict) -> None:
    """A group split across drainers has no rollback, so it never takes a ticket."""
    crew = scenario["crew"]

    assert crew["arrivals"] == [] and crew["writes"] == {}, (
        f"the crew holds {crew['arrivals']} after a group that has to land "
        f"together, and a group split across drainers cannot be rolled back"
    )


@then("the statement carries a placeholder")
def _it_carries_a_placeholder(scenario: dict) -> None:
    statement = _the_answer(scenario)["statement"]

    assert "?" in statement["text"], (
        f"the statement reads {statement['text']!r} and carries no placeholder"
    )
    assert "bob" not in statement["text"], (
        f"the caller's value is in the statement text: {statement['text']!r}. "
        f"A value in the text is a value nothing can escape."
    )


@then('"bob" came back beside it for the driver to bind')
def _the_value_travels_beside_it(scenario: dict) -> None:
    statement = _the_answer(scenario)["statement"]

    assert "bob" in statement["parameters"].values(), (
        f"the values beside the statement are {statement['parameters']!r} and "
        f"the caller asked for bob"
    )


@then("it is refused naming what arrived and the four spellings there are")
def _refused_by_name(scenario: dict) -> None:
    answered = scenario["answer"]

    assert isinstance(answered, dict) and set(answered) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a grammar nobody builds for answered {answered!r}"
    )
    said = answered["what_went_wrong"] + " " + answered["what_to_do_instead"]
    assert "hieroglyphics" in said, f"the refusal does not say what arrived: {said!r}"
    missing = [
        spelling
        for spelling in ("pseudo_sql", "django", "prisma", "sqlalchemy")
        if spelling not in said
    ]
    assert missing == [], (
        f"the refusal does not name {missing}, so a caller who wrote the wrong "
        f"word has to go and find out which four there are"
    )


@then("nothing reached the database")
def _nothing_reached_it(scenario: dict) -> None:
    written = [row[0] for row in scenario["rows"]]

    assert written == list(WHO_IS_ALREADY_THERE), (
        f"the table holds {written} after a question nothing could build, and it "
        f"held {list(WHO_IS_ALREADY_THERE)} before"
    )
