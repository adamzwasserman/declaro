"""Bind `tests/bdd/features/query/query_run_together.feature`.

WHAT THE FEATURE ASKS FOR. Nine results about one call that hands several
statements to a database at once: they all land, or a refusal takes every one of
them back out; the group never goes near the room and nobody gets a ticket; two
separate calls are still two separate writes; a group answers with one number;
and a group the engine will not start says so with nothing written.

THE RULE IT EXISTS FOR IS READ OFF THE ROWS, NEVER OFF THE MECHANISM. Nothing
here asks whether a rollback was called. Every scenario about a refusal reads
the table back afterwards and looks at what is in it, because a test that
watches the machinery passes while the machinery runs and the rows are still
wrong.

EVERY SCENARIO RUNS ON ALL THREE ENGINES, INSIDE THE SCENARIO. The feature says
nothing about engines, because the result is the same on each; `boundary_out fn
query_run_together` is one door with a row per driver behind it, and a row
nobody runs is a row nobody has seen work. So each scenario opens SQLite,
PostgreSQL and Turso, runs the same group on each, and asserts the same result
for each.

NOTHING HERE IS A STAND-IN. A double that got a driver contract wrong has
manufactured a defect in this package three times this week. SQLite is
`aiosqlite`, PostgreSQL is `asyncpg` on the probe database this pytest process
made, and Turso is `pyturso`, opened on a file of its own. PostgreSQL's absence
fails rather than skips, through `require_postgresql`.

NO SQL IS WRITTEN HERE. `fn compile_statement` writes every insert, update and
delete and `fn spell_operation` writes the create table, each for the engine it
is about to run on, which is also what puts the driver's own placeholders in the
text and the caller's value beside it.

THE WORK RUNS WHEN THE FIRST `Then` ASKS FOR IT. Three scenarios describe one
group across two sentences -- "a caller declares two rows exist, as a list" and
then "the second cannot be written" -- so a `When` that ran the group would run
it before the second sentence had been read. Each `When` records what was said;
the group runs once, when something first asks what happened.

WHY A TABLE THAT IS A UNIQUE KEY. A group needs one statement the engine will
refuse, and a duplicate primary key is the refusal every one of the three
spells: `UNIQUE constraint failed` on SQLite and Turso, `duplicate key value
violates unique constraint` on PostgreSQL. A row is put in the table before the
group runs, and the group tries to put the same key in again.
"""

from __future__ import annotations

import asyncio
import pathlib
import re
import tempfile
import uuid
from collections.abc import Awaitable, Callable

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.compile_statement import compile_statement
from declaro_persistum.crew_make import crew_make
from declaro_persistum.how_to_write import how_to_write
from declaro_persistum.query_run_statement import query_run_statement
from declaro_persistum.query_run_together import query_run_together
from declaro_persistum.spell_operation import spell_operation
from tests.conftest import (
    NARROWS_BY_NOTHING,
    POSTGRESQL_PROBE_URL,
    a_column,
    a_constraint,
    an_operation,
)

scenarios("../features/query/query_run_together.feature")


def _an_empty_crew() -> dict:
    """A crew holding nothing, built by the function that builds one.

    BUILT BY THE FUNCTION RATHER THAN BY WRITING THE FIELDS OUT. A crew joins a
    ticket to its write and its waiter, and a copy of today's fields would keep
    asserting that yesterday's crew is empty, and go on passing after a field a
    group could have reached moved.
    """
    return crew_make()


# --- the three engines, opened for real --------------------------------------
#
# ONE ROW PER ENGINE, THREE ENTRIES EACH: how a connection is opened, how it is
# closed, and how a statement is put to the driver with nothing catching what it
# raises. The third is the oracle for a refusal: the engine's own words are
# whatever the driver says when the same statement goes to it directly.


async def _open_sqlite(folder: pathlib.Path):
    import aiosqlite

    return await aiosqlite.connect(folder / "sqlite.db")


async def _close_sqlite(connection) -> None:
    await connection.close()


async def _run_on_sqlite(connection, text: str, values: list) -> None:
    await connection.execute(text, values)


async def _open_postgresql(_folder: pathlib.Path):
    """A pool, because that is what the door hands over.

    `boundary_out fn session_open_connection` opens asyncpg's own pool for
    PostgreSQL, and a pool has no `transaction`, so a group has to acquire a
    connection from it first. Opening a bare connection here would test a type
    production never sees, which is how every PostgreSQL statement in the
    single-statement runner went unnoticed for a day.
    """
    import asyncpg

    return await asyncpg.create_pool(POSTGRESQL_PROBE_URL, min_size=1, max_size=2)


async def _close_postgresql(connection) -> None:
    await connection.close()


async def _run_on_postgresql(connection, text: str, values: list) -> None:
    await connection.fetch(text, *values)


async def _open_turso(folder: pathlib.Path):
    """NOTHING IN pyturso IS NATIVELY SYNCHRONOUS. `turso.aio` is the driver and
    `turso.connect` is a shim over it, so everything here is awaited."""
    import turso.aio

    return await turso.aio.connect(str(folder / "turso.db"))


async def _close_turso(connection) -> None:
    await connection.close()


async def _run_on_turso(connection, text: str, values: list) -> None:
    await connection.execute(text, values)


THE_THREE_ENGINES = {
    "sqlite": {"open": _open_sqlite, "close": _close_sqlite, "run": _run_on_sqlite},
    "postgresql": {"open": _open_postgresql, "close": _close_postgresql, "run": _run_on_postgresql},
    "turso": {"open": _open_turso, "close": _close_turso, "run": _run_on_turso},
}


# --- what a scenario puts in a table -----------------------------------------

# The three rows a group declares, and the row that is already in the table when
# it runs. The key `2` is deliberately the one the second statement claims.
THREE_ROWS = [
    {"id": 1, "note": "one"},
    {"id": 2, "note": "two"},
    {"id": 3, "note": "three"},
]
ALREADY_THERE = {"id": 2, "note": "already here"}

# What scenario six changes, and what it changes the row to.
BEFORE_THE_CHANGE = {"id": 1, "note": "before"}
AFTER_THE_CHANGE = "after"

# A column the table does not have, named so the engine's refusal can be told
# apart from every other refusal in this file.
A_COLUMN_NO_MODEL_DECLARES = "nickname"


def _declaration(table: str, wanted: str, rows: list[dict] | None, condition: dict) -> dict:
    """One thing a caller wants, written out the way `Declaration` declares it."""
    return {
        "model": table,
        "wanted": wanted,
        "columns": [],
        "condition": condition,
        "order": {},
        "grouping": [],
        "having": NARROWS_BY_NOTHING,
        "paging": None,
        "rows": rows,
        "shaping": "dictionaries",
        "across": None,
    }


def _the_row_with_key(key: int) -> dict:
    """A narrowing that reaches one row by its key."""
    return {
        "each": {"id": {"operator": "eq", "value": key, "ignore_case": False}},
        "any_of": [],
        "none_of": [],
    }


def _a_table_with_a_key(table: str) -> dict:
    """A create table whose `id` is the primary key, as `Operation` declares one.

    THE KEY IS THE WHOLE POINT. Without it no statement in a group is refused
    for a reason a caller could have caused, and every refusal scenario here
    would be about a table that does not exist instead.
    """
    return an_operation(
        kind="create_table",
        table=table,
        column=None,
        columns=[
            a_column(name="id", declared_type="integer", renamed_from=None, references=None),
            a_column(name="note", declared_type="text", renamed_from=None, references=None),
        ],
        indexes=[],
        constraints=[
            a_constraint(
                name=f"{table}_pk",
                kind="primary_key",
                columns=["id"],
                rule=NARROWS_BY_NOTHING,
                value=None,
            )
        ],
        copying=[],
        values=[],
        renamed_to=None,
        view=None,
        destroys=False,
    )


def _compiled(db: dict, declaration: dict) -> dict:
    """The statement this declaration spells on this database's engine."""
    statement = compile_statement(declaration, db["engine"])
    assert "text" in statement, f"the declaration could not be compiled: {statement}"
    return statement


def _an_insert(db: dict, table: str, row: dict) -> dict:
    return _compiled(db, _declaration(table, "add", [row], NARROWS_BY_NOTHING))


def _spelled(db: dict, operation: dict) -> dict:
    """The one statement this schema change spells on this database's engine."""
    statements = spell_operation(operation, db["engine"])
    assert isinstance(statements, list), f"the change could not be spelled: {statements}"
    return statements[0]


async def _rows_in(db: dict, table: str) -> list[dict]:
    """Everything the table holds now, in key order."""
    rows = await query_run_statement(
        db["connections"][0],
        db["engine"],
        _compiled(db, _declaration(table, "all", None, NARROWS_BY_NOTHING)),
    )
    assert isinstance(rows, list), f"the table could not be read back: {rows}"
    return sorted(rows, key=lambda row: row["id"])


def _notes(rows: list[dict]) -> set[str]:
    """The `note` of every row that is in the table."""
    return {row["note"] for row in rows}


# --- opening every engine and running one group on each ----------------------


async def _against_every_engine(work: Callable[[dict, str], Awaitable[dict]]) -> dict[str, dict]:
    """Open each engine, run `work` on a table of its own, close everything.

    The table name is new every time, so nothing this file makes can meet
    anything another test left behind in the probe database.
    """
    answers: dict[str, dict] = {}
    for engine, driver in THE_THREE_ENGINES.items():
        table = "qrt_" + uuid.uuid4().hex[:12]
        with tempfile.TemporaryDirectory() as made:
            connection = await driver["open"](pathlib.Path(made))
            db = {
                "engine": engine,
                "connections": [connection],
                "writers": [],
                "crew": _an_empty_crew(),
                "shutdown": "exit_immediately",
            }
            try:
                await query_run_statement(
                    db["connections"][0], db["engine"], _spelled(db, _a_table_with_a_key(table))
                )
                answers[engine] = await work(db, table)
            finally:
                await _take_the_table_away(db, table)
                await driver["close"](connection)
    return answers


async def _take_the_table_away(db: dict, table: str) -> None:
    """PostgreSQL's probe database outlives this scenario, so a table left here
    is a table the next test meets."""
    dropping = {**_a_table_with_a_key(table), "kind": "drop_table"}
    await query_run_statement(db["connections"][0], db["engine"], _spelled(db, dropping))


async def _what_the_engine_says_about(db: dict, statement: dict) -> str:
    """The engine's own words about one statement, taken from the engine.

    RUN LAST, ON THE CONNECTION THE DATABASE HOLDS, and only ever with a
    statement the engine refuses, so nothing it does can land. Writing the
    expected message here instead would pin the test to one driver's wording and
    say nothing about what the caller is handed.
    """
    driver = THE_THREE_ENGINES[db["engine"]]
    try:
        await driver["run"](
            db["connections"][0], statement["text"], list(statement["parameters"].values())
        )
    except Exception as refused:  # noqa: BLE001 - this is the message we came for
        return str(refused)
    return ""


# --- the five groups a scenario can declare ----------------------------------


async def _a_group_of_three_that_lands(db: dict, table: str) -> dict:
    """Three rows, all of them acceptable. Four scenarios read this one group."""
    statements = [_an_insert(db, table, row) for row in THREE_ROWS]
    answer = await query_run_together(
        db["connections"][0], db["engine"], statements, how_to_write(db["engine"])
    )
    return {
        "answer": answer,
        "rows": await _rows_in(db, table),
        "crew": {**db["crew"]},
        "statements": statements,
        "db": db,
    }


async def _a_group_of_three_whose_second_is_refused(db: dict, table: str) -> dict:
    """Three rows, and the key the second claims is already taken."""
    await query_run_statement(
        db["connections"][0], db["engine"], _an_insert(db, table, ALREADY_THERE)
    )
    statements = [_an_insert(db, table, row) for row in THREE_ROWS]
    answer = await query_run_together(
        db["connections"][0], db["engine"], statements, how_to_write(db["engine"])
    )
    rows = await _rows_in(db, table)
    return {
        "answer": answer,
        "rows": rows,
        "the_engine_said": await _what_the_engine_says_about(db, statements[1]),
    }


async def _a_group_of_two_whose_second_is_refused(db: dict, table: str) -> dict:
    """The same, as a list of two, which is the smallest group there is."""
    await query_run_statement(
        db["connections"][0], db["engine"], _an_insert(db, table, ALREADY_THERE)
    )
    statements = [_an_insert(db, table, row) for row in THREE_ROWS[:2]]
    return {
        "answer": await query_run_together(
            db["connections"][0], db["engine"], statements, how_to_write(db["engine"])
        ),
        "rows": await _rows_in(db, table),
    }


async def _two_calls_whose_second_is_refused(db: dict, table: str) -> dict:
    """The contrast: the same two writes, handed over one call at a time.

    Neither call knows about the other, so the first is already committed when
    the second is refused, and it stays.
    """
    await query_run_statement(
        db["connections"][0], db["engine"], _an_insert(db, table, ALREADY_THERE)
    )
    return {
        "first": await query_run_together(
            db["connections"][0],
            db["engine"],
            [_an_insert(db, table, THREE_ROWS[0])],
            how_to_write(db["engine"]),
        ),
        "second": await query_run_together(
            db["connections"][0],
            db["engine"],
            [_an_insert(db, table, THREE_ROWS[1])],
            how_to_write(db["engine"]),
        ),
        "rows": await _rows_in(db, table),
    }


async def _a_change_and_a_removal_that_fails(db: dict, table: str) -> dict:
    """A group holding two different kinds of write, the second of them refused.

    The removal reaches a table nobody made, which every one of the three
    engines refuses, and the change before it is the thing being watched.
    """
    await query_run_statement(
        db["connections"][0], db["engine"], _an_insert(db, table, BEFORE_THE_CHANGE)
    )
    change = _compiled(
        db,
        _declaration(
            table,
            "change",
            [{"note": AFTER_THE_CHANGE}],
            _the_row_with_key(BEFORE_THE_CHANGE["id"]),
        ),
    )
    removal = _compiled(
        db,
        _declaration(
            table + "_never_made", "remove", None, _the_row_with_key(BEFORE_THE_CHANGE["id"])
        ),
    )
    return {
        "answer": await query_run_together(
            db["connections"][0], db["engine"], [change, removal], how_to_write(db["engine"])
        ),
        "rows": await _rows_in(db, table),
    }


async def _a_group_naming_a_column_no_model_declares(db: dict, table: str) -> dict:
    """The first statement names a column the table does not have.

    It is first on purpose: the two behind it are ordinary inserts that any
    engine would take, so an empty table afterwards says the group stopped
    before it wrote anything rather than that everything in it was refused.
    """
    named = {"id": 1, A_COLUMN_NO_MODEL_DECLARES: "whoever"}
    statements = [
        _compiled(db, _declaration(table, "add", [named], NARROWS_BY_NOTHING)),
        _an_insert(db, table, THREE_ROWS[1]),
        _an_insert(db, table, THREE_ROWS[2]),
    ]
    return {
        "answer": await query_run_together(
            db["connections"][0], db["engine"], statements, how_to_write(db["engine"])
        ),
        "rows": await _rows_in(db, table),
    }


# --- what each scenario said, and the group that answers it ------------------
#
# ONE ROW PER SCENARIO, KEYED BY THE SENTENCES IT SAID IN ORDER. Four of the
# nine describe the same group and read different things off it, which is why
# the left-hand side is the sentences and not the work.

WHAT_EACH_GROUP_RUNS: dict[tuple[str, ...], Callable[[dict, str], Awaitable[dict]]] = {
    ("three rows are declared together and all three are accepted",): (
        _a_group_of_three_that_lands
    ),
    ("three rows are declared together",): _a_group_of_three_that_lands,
    ("a group of three writes succeeds",): _a_group_of_three_that_lands,
    ("a caller declares a group",): _a_group_of_three_that_lands,
    ("three rows are declared together and the second violates a constraint",): (
        _a_group_of_three_whose_second_is_refused
    ),
    (
        "a caller declares two rows exist, as a list",
        "the second cannot be written",
    ): _a_group_of_two_whose_second_is_refused,
    (
        "a caller declares one row, and then another",
        "the second fails",
    ): _two_calls_whose_second_is_refused,
    (
        "a caller declares a group holding a change and a removal",
        "the removal fails",
    ): _a_change_and_a_removal_that_fails,
    (
        "a group naming a column no model declares",
        "it is declared",
    ): _a_group_naming_a_column_no_model_declares,
}

WHAT_A_CALLER_CAN_SAY = tuple(
    sorted({sentence for said in WHAT_EACH_GROUP_RUNS for sentence in said}, key=len, reverse=True)
)

# The Given's own sentence is the one no `When` says.
THE_GIVEN = "a group naming a column no model declares"
WHAT_A_WHEN_CAN_SAY = tuple(said for said in WHAT_A_CALLER_CAN_SAY if said != THE_GIVEN)


def _one_of(sentences: tuple[str, ...]) -> str:
    """A pattern matching exactly these sentences and nothing longer.

    ANCHORED, because "three rows are declared together" is the opening of
    another sentence in this same feature and pytest-bdd matches from the start.
    """
    return r"(?P<sentence>" + "|".join(re.escape(said) for said in sentences) + r")\Z"


# --- the two steps that record what was said ---------------------------------


@given(THE_GIVEN)
def _a_group_naming_a_column_no_model_declares_step(scenario: dict) -> None:
    """The one scenario whose group is described before it is declared."""
    scenario["said"] = [THE_GIVEN]


@when(parsers.re(_one_of(WHAT_A_WHEN_CAN_SAY)))
def _what_the_caller_declared(scenario: dict, require_postgresql: None, sentence: str) -> None:
    """Eleven sentences, one function. Each names a group, or names how the
    group it follows fails, and the pair is what picks the work."""
    scenario.setdefault("said", []).append(sentence)


# --- running that group, once, when something first asks what happened -------


def _run_what_was_declared(scenario: dict) -> dict[str, dict]:
    work = WHAT_EACH_GROUP_RUNS[tuple(scenario["said"])]
    scenario["outcomes"] = asyncio.run(_against_every_engine(work))
    return scenario["outcomes"]


_ALREADY_RUN: dict[bool, Callable[[dict], dict[str, dict]]] = {
    True: lambda scenario: scenario["outcomes"],
    False: _run_what_was_declared,
}


def _outcomes(scenario: dict) -> dict[str, dict]:
    """What every engine did with this scenario's group, run at most once."""
    return _ALREADY_RUN["outcomes" in scenario](scenario)


# --- what each scenario says has to be true ----------------------------------


def _all_three_are_there(engine: str, outcome: dict) -> None:
    assert outcome["rows"] == THREE_ROWS, (
        f"on {engine} the three rows declared together read back as "
        f"{outcome['rows']!r}, and a group that lands leaves all of it in the table"
    )


def _none_of_these_landed(engine: str, outcome: dict, notes: set[str]) -> None:
    """The rows the group carried are gone, and the row it did not carry is not.

    ASSERTING BOTH IS WHAT MAKES IT AN ASSERTION. A table that reads empty
    because the read went to the wrong place would satisfy the first half alone.
    """
    landed = _notes(outcome["rows"]) & notes
    assert landed == set(), (
        f"on {engine} the group was refused and {sorted(landed)} is still in "
        f"the table, so part of a group that failed survived it: {outcome['rows']!r}"
    )
    assert ALREADY_THERE["note"] in _notes(outcome["rows"]), (
        f"on {engine} the row that was in the table before the group ran is "
        f"gone as well: {outcome['rows']!r}. The rollback reached past the group"
    )


def _the_fault_names_the_second(engine: str, outcome: dict) -> None:
    fault = outcome["answer"]
    said = outcome["the_engine_said"]

    assert said != "", (
        f"{engine} accepted the second statement on its own, so this scenario "
        "is asking about a statement the engine does not refuse"
    )
    assert isinstance(fault, dict) and fault.get("kind") == "refused", (
        f"on {engine} a refused group came back as {fault!r}, and a failure "
        "travels by return as a fault of kind refused"
    )
    assert said in fault["what_went_wrong"], (
        f"on {engine} the fault says {fault['what_went_wrong']!r} and the engine "
        f"said {said!r} about the second statement. Neither the first nor the "
        "third is refused at all, so a reader who cannot see those words cannot "
        "tell which statement the group died on"
    )


def _no_ticket_was_handed_out(engine: str, outcome: dict) -> None:
    assert not outcome["crew"]["arrivals"] and not outcome["crew"]["writes"], (
        f"on {engine} the crew holds {outcome['crew']['arrivals']!r} after a "
        "group ran, so the group went through the crew, where the first write "
        "stays landed when the second fails"
    )
    assert isinstance(outcome["answer"], int), (
        f"on {engine} a group answered with {outcome['answer']!r}. A `Ticket` is "
        "a string, and a group is the one write that does not take one"
    )


def _the_first_call_is_in_the_database(engine: str, outcome: dict) -> None:
    assert outcome["first"] == 1, (
        f"on {engine} the first call answered {outcome['first']!r} instead of the one row it wrote"
    )
    assert isinstance(outcome["second"], dict), (
        f"on {engine} the second call answered {outcome['second']!r}, and this "
        "scenario is about a second call the engine refuses"
    )
    assert THREE_ROWS[0]["note"] in _notes(outcome["rows"]), (
        f"on {engine} the first call's row is gone after the second call failed: "
        f"{outcome['rows']!r}. Two calls are two writes, and the second cannot "
        "reach back into the first"
    )


def _the_change_did_not_happen_either(engine: str, outcome: dict) -> None:
    assert isinstance(outcome["answer"], dict), (
        f"on {engine} a group whose removal fails answered {outcome['answer']!r}"
    )
    assert outcome["rows"] == [BEFORE_THE_CHANGE], (
        f"on {engine} the row reads {outcome['rows']!r}. The removal beside the "
        f"change was refused, so the change to {AFTER_THE_CHANGE!r} has to be "
        "gone with it"
    )


def _the_answer_says_three_rows_changed(engine: str, outcome: dict) -> None:
    assert outcome["answer"] == len(THREE_ROWS), (
        f"on {engine} a group of {len(THREE_ROWS)} writes answered "
        f"{outcome['answer']!r} rows changed"
    )


def _it_is_one_answer(engine: str, outcome: dict) -> None:
    answer = outcome["answer"]
    assert isinstance(answer, int) and not isinstance(answer, bool), (
        f"on {engine} a group answered with a {type(answer).__name__} carrying "
        f"{answer!r}, and a group answers with one number"
    )
    assert not isinstance(answer, list | tuple | dict), (
        f"on {engine} a group of {len(THREE_ROWS)} writes answered {answer!r}, "
        "which is one answer per statement rather than one for the group"
    )


def _it_is_refused_before_any_of_its_writes_runs(engine: str, outcome: dict) -> None:
    fault = outcome["answer"]

    assert isinstance(fault, dict) and fault.get("kind") == "refused", (
        f"on {engine} a group naming {A_COLUMN_NO_MODEL_DECLARES!r} came back as {fault!r}"
    )
    assert A_COLUMN_NO_MODEL_DECLARES in fault["what_went_wrong"], (
        f"on {engine} the fault says {fault['what_went_wrong']!r} and never "
        f"names {A_COLUMN_NO_MODEL_DECLARES!r}, which is the whole of what the "
        "engine objected to"
    )
    assert outcome["rows"] == [], (
        f"on {engine} the table holds {outcome['rows']!r} after a group that "
        "could not start. The two ordinary inserts behind the refused one wrote "
        "anyway, so the group was not stopped"
    )


def _nothing_in_the_declaration_is_a_connection(engine: str, outcome: dict) -> None:
    held = [*outcome["db"]["connections"], *outcome["db"]["writers"]]

    for statement in outcome["statements"]:
        assert set(statement) == {"text", "parameters"}, (
            f"on {engine} a statement in the group carries {sorted(statement)}, "
            "and a group is declared out of text and the values that go in it"
        )
        carried = [statement["text"], *statement["parameters"].values()]
        assert not any(value is handle for value in carried for handle in held), (
            f"on {engine} a statement in the group carries the driver's own "
            f"connection: {statement!r}. A caller declares what they want "
            "written, never what it is written on"
        )


def _the_caller_never_holds_one(engine: str, outcome: dict) -> None:
    answer = outcome["answer"]
    held = [*outcome["db"]["connections"], *outcome["db"]["writers"]]

    assert isinstance(answer, int), (
        f"on {engine} the group answered with a {type(answer).__name__}, and a "
        "group answers with how many rows it touched"
    )
    assert not any(answer is handle for handle in held), (
        f"on {engine} the group handed the caller back the connection it ran on"
    )


WHAT_EACH_THEN_CHECKS: dict[str, Callable[[str, dict], None]] = {
    "all three are in the database": _all_three_are_there,
    "none of the three is in the database": lambda engine, outcome: _none_of_these_landed(
        engine, outcome, _notes(THREE_ROWS)
    ),
    "the fault names the second": _the_fault_names_the_second,
    "no ticket was handed out": _no_ticket_was_handed_out,
    "neither is in the database": lambda engine, outcome: _none_of_these_landed(
        engine, outcome, _notes(THREE_ROWS[:2])
    ),
    "the first is in the database": _the_first_call_is_in_the_database,
    "the change did not happen either": _the_change_did_not_happen_either,
    "the answer says three rows changed": _the_answer_says_three_rows_changed,
    "it is one answer, not three": _it_is_one_answer,
    "it is refused before any of its writes runs": _it_is_refused_before_any_of_its_writes_runs,
    "nothing in the group's declaration is a connection": _nothing_in_the_declaration_is_a_connection,
    "the caller never holds one": _the_caller_never_holds_one,
}


@then(parsers.re(_one_of(tuple(sorted(WHAT_EACH_THEN_CHECKS, key=len, reverse=True)))))
def _what_must_be_true(scenario: dict, sentence: str) -> None:
    """Twelve sentences, one function over a table, asked of all three engines.

    IT SAYS HOW MANY IT ASKED, because a loop over nothing passes. Every check
    here runs inside the loop below, so an empty set of outcomes would satisfy
    this step without asking anything, and the scenario would read as green on a
    batch that never ran. That is the shape this package has shipped before.
    """
    outcomes = _outcomes(scenario)

    assert len(outcomes) == len(THE_THREE_ENGINES), (
        f"{sentence!r} was asked of {sorted(outcomes)}, and this file runs every "
        f"scenario against {sorted(THE_THREE_ENGINES)}. A check nobody ran is a check "
        "that passed."
    )
    for engine, outcome in outcomes.items():
        WHAT_EACH_THEN_CHECKS[sentence](engine, outcome)
