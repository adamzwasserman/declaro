"""Bind `tests/bdd/features/query/query_ask.feature`, and nothing else.

WHAT THE FEATURE ASKS. Twenty-eight scenarios about one door. `boundary_in fn
query_ask` takes the database a caller opened, the model they are asking about,
the name of the grammar they wrote the question in, and the question itself. It
builds the record `orchestrator fn query_answer` carries, calls it, and hands
back the answer. Four of the twenty-eight are outlines over the four grammars,
so forty-three test cases run.

WHAT A CALLER CAN SEE THROUGH THIS DOOR, WHICH IS WHAT EVERY `Then` READS. The
door answers with rows, a number, true or false, a ticket, or a named absence,
or with a `Fault`. The declaration it built, the statement it compiled and the
record it filled stay behind it, in `type Answering`, which no caller holds. So
a scenario about the declaration is answered here in the two ways a caller can
answer it: four grammars asking one question come back with equal answers, and
four grammars declaring one write leave equal statements waiting in the room.
`fn compile_statement` is a function of the declaration and the engine, so four
equal statements on one engine is the declaration as far out as this door
reaches.

EVERY DATABASE HERE IS A REAL ONE, OPENED THROUGH THE OTHER DOOR. A stand-in
that got a driver contract wrong has manufactured three defects in this package
this week. `boundary_in fn session_open` opens each of the three: SQLite through
aiosqlite, PostgreSQL through asyncpg against a database each scenario makes for
itself, and Turso through pyturso on a `libsql` target. Opening is also what
makes the table, because the door migrates the models before it hands a handle
back.

EVERY SCENARIO RUNS ON ALL THREE ENGINES, INSIDE THE SCENARIO, and every `Then`
says how many it asked, because a loop over an empty set of outcomes passes
while asking nothing.

THE FOUR GRAMMARS ARE WRITTEN OUT HERE, NOT READ OFF THE PACKAGE. The words are
the caller-facing surface, and `HOW-IT-SHOULD-WORK.md` under "The data is
declared too" is where they are stated: `where` and `select` for the library's
own phrasing and for Prisma, `filter` and `values` for Django, `filter_by` and
`with_only` for SQLAlchemy. Reading them off `declaration_from_django` and its
three siblings would make the code its own oracle.

THE FEATURE SPELLS ONE GRAMMAR TWO WAYS. Its first outline says `pseudo_sql` and
every outline folded in afterwards says `pseudo-sql`, while `set grammars` in the
declaration has only the first. A table below turns either into the name the
vocabulary carries, and a spelling with no row raises naming itself.

WHAT THIS DOOR CANNOT BE ASKED TODAY, stated here rather than hidden in a skip.
Three sentences call `pytest.xfail` naming exactly what is missing, and each is
the last step of its scenario, so nothing else goes unasked:

  not a dict         `fn answer_from_rows` says an instance of the model is a
                     row carrying every column the model declares, and that
                     there is no class to build. So the rows that come back are
                     mappings, and the line asking that none of them is a dict
                     asks for a class that card refuses to build.

  a dictionary key   `type Declaration` is a record, which is a dict, and a dict
                     cannot be a key. Nothing a caller holds through this door
                     is hashable.

  the call names     `boundary_in fn query_ask` takes `grammar: Grammar`, so a
  no grammar         caller writing the call does name one. The four entry
                     points the line is reaching for -- a different name on the
                     model per grammar -- are not built, and the card says the
                     name arrives as an argument on purpose.
"""

from __future__ import annotations

import asyncio
import gc
import hashlib
import inspect
import os
import pathlib
import re
import signal
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from pydantic import BaseModel
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.lifecycle_install_handler import (
    WHAT_THE_PLATFORM_SENDS,
    WHAT_WAS_THERE,
)
from declaro_persistum.lifecycle_register import REGISTERED
from declaro_persistum.query_ask import query_ask
from declaro_persistum.session_open import session_open
from declaro_persistum.types import Grammar, Model
from tests.conftest import POSTGRESQL_PROBE_URL

scenarios("../features/query/query_ask.feature")


# =============================================================================
# the model the feature names, and the people it puts in the table
# =============================================================================

THE_TABLE_THE_FEATURE_NAMES = "people"

ALREADY_THERE = [
    {"id": 1, "name": "alice"},
    {"id": 2, "name": "bob"},
    {"id": 3, "name": "carol"},
]
THE_NAME_THE_FEATURE_ASKS_FOR = "bob"
ONE_NEW_PERSON = {"id": 4, "name": "dave"}

# The column the feature misspells, and the column no model declares.
A_COLUMN_MISSPELLED = "nmae"
A_COLUMN_NO_MODEL_DECLARES = "nickname"

# What a caller must not be able to hand any grammar.
A_STRING_OF_SQL = "SELECT * FROM people WHERE name = 'bob'"


def _a_person_model() -> type:
    """The model the feature names, declared the way a host declares one.

    DECLARED INSIDE A CALL AND NEVER AT MODULE LEVEL. `reader fn
    schema_read_declared` reads every Pydantic class alive in the process, so a
    class held by this module would be part of every other test's migration for
    the rest of the run.
    """

    class Person(BaseModel):
        __tablename__ = THE_TABLE_THE_FEATURE_NAMES
        id: int
        name: str

    return Person


# THE MODEL AS THE PACKAGE HOLDS IT. `type Model` is the shape `reader fn
# schema_read_declared` produces, and the door is handed one of those rather
# than a Pydantic class. It carries no name of its own: the feature calls it
# Person and the record calls it `people`, which is what every refusal below
# names.
THE_MODEL: Model = {
    "table": THE_TABLE_THE_FEATURE_NAMES,
    "columns": [
        {
            "name": "id",
            "declared_type": "integer",
            "renamed_from": None,
            "references": None,
            "facets": {},
        },
        {
            "name": "name",
            "declared_type": "text",
            "renamed_from": None,
            "references": None,
            "facets": {},
        },
    ],
    "dropping": [],
    "indexes": [],
    "constraints": [],
}

WHAT_THE_MODEL_DECLARES = tuple(column["name"] for column in THE_MODEL["columns"])


# =============================================================================
# the four grammars, in the words a caller writes
# =============================================================================
#
# TWO FIELDS ARE SPELLED FOUR WAYS AND THE REST ARE THE LIBRARY'S OWN.
# `HOW-IT-SHOULD-WORK.md`, "The data is declared too", writes one question in
# each of the four, and these are the words it uses. `rows` is nobody's
# phrasing, so every grammar carries it under that name.

WHAT_EACH_GRAMMAR_CALLS_IT: dict[str, dict[str, str]] = {
    "pseudo_sql": {"condition": "where", "columns": "select", "rows": "rows"},
    "django": {"condition": "filter", "columns": "values", "rows": "rows"},
    "prisma": {"condition": "where", "columns": "select", "rows": "rows"},
    "sqlalchemy": {"condition": "filter_by", "columns": "with_only", "rows": "rows"},
}

THE_FOUR_GRAMMARS = tuple(WHAT_EACH_GRAMMAR_CALLS_IT)

# EITHER SPELLING THE FEATURE USES, TURNED INTO THE ONE THE VOCABULARY CARRIES.
# No fallback: a spelling with no row raises naming itself.
THE_NAME_A_SCENARIO_SPELLS: dict[str, str] = {
    "pseudo_sql": "pseudo_sql",
    "pseudo-sql": "pseudo_sql",
    "django": "django",
    "prisma": "prisma",
    "sqlalchemy": "sqlalchemy",
}

# A name no grammar answers to, for the scenario that reaches for one.
A_GRAMMAR_NOTHING_CAN_BUILD = "mongo"

# The order the feature reads the four in, and an order that is not that one.
THE_ORDER_THE_FEATURE_READS = ("pseudo_sql", "django", "prisma", "sqlalchemy")
ANOTHER_ORDER = ("sqlalchemy", "prisma", "django", "pseudo_sql")


def _question(grammar: str, wanted: str, **parts: Any) -> dict[str, Any]:
    """One question, written in the words the named grammar gave the caller."""
    words = WHAT_EACH_GRAMMAR_CALLS_IT[grammar]
    return {
        "model": THE_TABLE_THE_FEATURE_NAMES,
        "wanted": wanted,
        **{words[field]: value for field, value in parts.items()},
    }


def _everyone_named_bob(grammar: str) -> dict[str, Any]:
    """The question every grammar scenario in this feature asks."""
    return _question(grammar, "all", condition={"name": THE_NAME_THE_FEATURE_ASKS_FOR})


# =============================================================================
# the three engines, each opened through `boundary_in fn session_open`
# =============================================================================


async def _a_postgresql_database_of_our_own(_folder: pathlib.Path, node: str) -> str:
    """One PostgreSQL database named for this test, dropped afterwards.

    Opening migrates the models, and a migration plans a drop for every table
    the models do not declare, so a shared database is one this file would
    empty. The boundary has to be the database.
    """
    named = "qask_" + hashlib.sha256(node.encode()).hexdigest()[:16]
    server, _, _ = POSTGRESQL_PROBE_URL.rpartition("/")
    await _administer([f'DROP DATABASE IF EXISTS "{named}"', f'CREATE DATABASE "{named}"'])
    return f"{server}/{named}"


async def _administer(statements: list[str]) -> None:
    """On the loop the scenario is already running, because there is one."""
    import asyncpg

    connection = await asyncpg.connect(POSTGRESQL_PROBE_URL)
    try:
        for one in statements:
            await connection.execute(one)
    finally:
        await connection.close()


async def _a_sqlite_file_in(folder: pathlib.Path, _node: str) -> str:
    return str(folder / "asked.db")


async def _a_turso_file_in(_folder: pathlib.Path, _node: str) -> str:
    """A `libsql` target is the only thing that names Turso, and pyturso reads a
    relative path, so the folder this scenario owns is the working directory."""
    return "libsql://asked.db"


WHERE_EACH_ENGINES_DATABASE_GOES: dict[str, Callable[[pathlib.Path, str], Awaitable[str]]] = {
    "sqlite": _a_sqlite_file_in,
    "postgresql": _a_postgresql_database_of_our_own,
    "turso": _a_turso_file_in,
}

THE_THREE_ENGINES = tuple(WHERE_EACH_ENGINES_DATABASE_GOES)

# WHAT THE DEPLOYMENT STATES, because `boundary_in fn session_open` reads both
# and neither has a default. One connection and one drainer is the smallest
# deployment there is, and this file never drains.
WHAT_THE_DEPLOYMENT_STATES = {"DECLARO_CONNECTIONS": "1", "DECLARO_CREW_SIZE": "1"}

# The one statement this file writes, and it is the test's own: asking an engine
# what a table holds is not something the package does for a caller. All three
# read the same words here.
WHAT_THE_TABLE_HOLDS = f"SELECT id, name FROM {THE_TABLE_THE_FEATURE_NAMES} ORDER BY id"


async def _what_the_table_holds(db: dict[str, Any]) -> list[dict[str, Any]]:
    """Every row in the table now, read straight off the driver.

    Read here rather than through another question, because a question is what
    the scenario is about and an answer that came back wrong would agree with
    itself.
    """
    from declaro_persistum.query_run_statement import query_run_statement

    rows = await query_run_statement(db, {"text": WHAT_THE_TABLE_HOLDS, "parameters": {}})
    assert isinstance(rows, list), f"the table could not be read back: {rows}"
    return [{"id": row["id"], "name": row["name"]} for row in rows]


async def _opened(target: str) -> dict[str, Any]:
    """One database, with the feature's table already migrated into it."""
    db = await session_open(target, "exit_immediately")
    assert not _is_a_fault(db), f"opening {target} had to work and it said {db}"
    return db


NARROWING_NOTHING = {"each": {}, "any_of": [], "none_of": []}


async def _filled(db: dict[str, Any]) -> None:
    """The three people every scenario in this feature starts from."""
    from declaro_persistum.compile_statement import compile_statement
    from declaro_persistum.query_run_statement import query_run_statement

    statement = compile_statement(
        {
            "model": THE_TABLE_THE_FEATURE_NAMES,
            "wanted": "add",
            "columns": [],
            "condition": NARROWING_NOTHING,
            "order": {},
            "grouping": [],
            "having": NARROWING_NOTHING,
            "paging": None,
            "rows": ALREADY_THERE,
            "across": None,
        },
        db["engine"],
    )
    assert "text" in statement, f"the three people could not be compiled: {statement}"
    answered = await query_run_statement(db, statement)
    assert not _is_a_fault(answered), f"the three people did not land: {answered}"


async def _closed(db: dict[str, Any]) -> None:
    """Everything this database opened, closed on the loop that opened it.

    aiosqlite runs one non-daemon thread per connection, so a leaked one holds
    the interpreter open after every test has finished.
    """
    for handle in [db["connection"], *db["writers"]]:
        await handle.close()


async def _kept(_target: str) -> None:
    """A file in the folder pytest made dies with the folder."""


async def _dropped(target: str) -> None:
    """A PostgreSQL database this file made, taken away again."""
    await _administer([f'DROP DATABASE IF EXISTS "{target.rpartition("/")[2]}" WITH (FORCE)'])


WHAT_BECOMES_OF_EACH_DATABASE: dict[str, Callable[[str], Awaitable[None]]] = {
    "sqlite": _kept,
    "postgresql": _dropped,
    "turso": _kept,
}


async def _against_every_engine(
    work: Callable[[dict[str, Any], str], Awaitable[dict[str, Any]]],
    grammar: str,
    folder: pathlib.Path,
    node: str,
) -> dict[str, dict[str, Any]]:
    """Open each engine, run `work` against the feature's table, close it all."""
    answers: dict[str, dict[str, Any]] = {}
    for engine in WHERE_EACH_ENGINES_DATABASE_GOES:
        mine = folder / (engine + "_" + uuid.uuid4().hex[:8])
        mine.mkdir()
        was = os.getcwd()
        os.chdir(mine)
        target = await WHERE_EACH_ENGINES_DATABASE_GOES[engine](mine, node)
        try:
            db = await _opened(target)
            try:
                await _filled(db)
                answers[engine] = await work(db, grammar)
            finally:
                await _closed(db)
        finally:
            os.chdir(was)
            await WHAT_BECOMES_OF_EACH_DATABASE[engine](target)
    return answers


# =============================================================================
# what the process is owed back
# =============================================================================


@pytest.fixture(autouse=True)
def the_process_is_left_as_it_was(monkeypatch: pytest.MonkeyPatch) -> Any:
    """Put back the environment and the three pieces of process state an open
    changes.

    `boundary_in fn session_open` reads two variables and restores neither, and
    opening a database is startup, so it takes the process's signals and writes
    itself onto the shutdown list.
    """
    for name, number in WHAT_THE_DEPLOYMENT_STATES.items():
        monkeypatch.setenv(name, number)
    held = {number: signal.getsignal(number) for number in WHAT_THE_PLATFORM_SENDS}
    was_recorded = dict(WHAT_WAS_THERE)
    was_registered = list(REGISTERED)
    gc.collect()
    try:
        yield
    finally:
        for number, handler in held.items():
            signal.signal(number, handler)
        WHAT_WAS_THERE.clear()
        WHAT_WAS_THERE.update(was_recorded)
        REGISTERED[:] = was_registered
        gc.collect()


@pytest.fixture(autouse=True)
def the_model_the_feature_names(scenario: dict[str, object]) -> Any:
    """Declared for the length of this scenario and let go afterwards."""
    scenario["model_class"] = _a_person_model()
    yield
    del scenario["model_class"]
    gc.collect()


# =============================================================================
# telling a failure from an answer
# =============================================================================

THE_FIELDS_OF_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


def _is_a_fault(answered: object) -> bool:
    return isinstance(answered, dict) and frozenset(answered) == THE_FIELDS_OF_A_FAULT


def _what_it_said(fault: dict[str, str]) -> str:
    """Both halves of a refusal, because either may carry the name it must."""
    return fault["what_went_wrong"] + " " + fault["what_to_do_instead"]


def _the_people_already_there() -> list[dict[str, Any]]:
    return [{"id": row["id"], "name": row["name"]} for row in ALREADY_THERE]


def _bob_alone() -> list[dict[str, Any]]:
    return [
        row for row in _the_people_already_there() if row["name"] == THE_NAME_THE_FEATURE_ASKS_FOR
    ]


def _room_of(db: dict[str, Any]) -> dict[str, Any]:
    """What is waiting in the room, copied, so a later deposit cannot change it."""
    return {
        "arrivals": list(db["room"]["arrivals"]),
        "writes": dict(db["room"]["writes"]),
        "receipts": dict(db["room"]["receipts"]),
    }


# =============================================================================
# asking, and declaring, through the door
# =============================================================================


async def _asked(db: dict[str, Any], grammar: Grammar, asked: dict[str, Any]) -> Any:
    """One question, put to the door exactly as a caller puts it."""
    return await query_ask(db, THE_MODEL, grammar, asked)


async def _the_statement_a_write_leaves(db: dict[str, Any], grammar: str) -> Any:
    """Declare one row in this grammar and hand back what is waiting for it.

    THIS IS THE ONLY WAY A CALLER SEES A DECLARATION. The door answers a read
    with rows and keeps everything it built, so the statement a deposited write
    leaves in the room is the one thing downstream of the declaration that a
    caller can read. Four grammars leaving one statement is what the scenarios
    about equal declarations are asking for, as far out as this door reaches.
    """
    answered = await _asked(db, grammar, _question(grammar, "add", rows=[ONE_NEW_PERSON]))
    assert isinstance(answered, str), (
        f"declaring a row in the {grammar} grammar answered {answered!r}, and a "
        "write is answered by its ticket"
    )
    return db["room"]["writes"][answered]


async def _in_every_grammar(
    db: dict[str, Any], ask: Callable[[str], dict[str, Any]]
) -> dict[str, Any]:
    """The same question, asked once under each of the four names."""
    return {grammar: await _asked(db, grammar, ask(grammar)) for grammar in THE_FOUR_GRAMMARS}


async def _the_statement_every_grammar_leaves(db: dict[str, Any]) -> dict[str, Any]:
    """The same write, declared once under each of the four names."""
    return {
        grammar: await _the_statement_a_write_leaves(db, grammar) for grammar in THE_FOUR_GRAMMARS
    }


# =============================================================================
# the work each `When` does
# =============================================================================


async def _people_named_bob_in(db: dict[str, Any], grammar: str) -> dict[str, Any]:
    """The named grammar's answer, beside all four answers and all four writes.

    The read the sentence names runs first and on its own. The three scenarios
    that read this outcome want three different things from it: the one person,
    the words the caller wrote, and whether the other three grammars built what
    this one built.
    """
    answered = await _asked(db, grammar, _everyone_named_bob(grammar))
    return {
        "answered": answered,
        "wrote": _everyone_named_bob(grammar),
        "in_every_grammar": await _in_every_grammar(db, _everyone_named_bob),
        "waiting": await _the_statement_every_grammar_leaves(db),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _every_person(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """A read of everything, in the library's own words."""
    return {
        "answered": await _asked(db, "pseudo_sql", _question("pseudo_sql", "all")),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _one_new_person(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """A write of one row, which is deposited and answered by a ticket."""
    answered = await _asked(db, "pseudo_sql", _question("pseudo_sql", "add", rows=[ONE_NEW_PERSON]))
    return {
        "answered": answered,
        "room": _room_of(db),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _the_misspelled_column(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """A question naming a column the model does not declare."""
    answered = await _asked(
        db, "pseudo_sql", _question("pseudo_sql", "all", columns=[A_COLUMN_MISSPELLED])
    )
    return {
        "answered": answered,
        "named": A_COLUMN_MISSPELLED,
        "room": _room_of(db),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _every_person_in(db: dict[str, Any], grammar: str) -> dict[str, Any]:
    """Everyone, asked in the named grammar."""
    return {
        "answered": await _asked(db, grammar, _question(grammar, "all")),
        "in_the_table": await _what_the_table_holds(db),
    }


def _narrowed_by_nickname(grammar: str) -> dict[str, Any]:
    return _question(grammar, "all", condition={A_COLUMN_NO_MODEL_DECLARES: "whoever"})


async def _narrowing_by_nickname_in(db: dict[str, Any], grammar: str) -> dict[str, Any]:
    """A narrowing by a column no model declares, written in one grammar."""
    return {
        "answered": await _asked(db, grammar, _narrowed_by_nickname(grammar)),
        "named": A_COLUMN_NO_MODEL_DECLARES,
        "room": _room_of(db),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _narrowing_by_nickname_in_each(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """The same narrowing, written in each of the four."""
    answers = await _in_every_grammar(db, _narrowed_by_nickname)
    return {
        "answered": answers[THE_FOUR_GRAMMARS[0]],
        "answers": answers,
        "named": A_COLUMN_NO_MODEL_DECLARES,
        "in_the_table": await _what_the_table_holds(db),
    }


async def _the_same_question_in_all_four(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """One question and one write, each said once in every grammar."""
    return {
        "in_every_grammar": await _in_every_grammar(db, _everyone_named_bob),
        "waiting": await _the_statement_every_grammar_leaves(db),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _a_question_that_is_not_run(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """A question written down and handed to nobody.

    WHAT A CALLER HOLDS BEFORE THEY RUN IT IS THE QUESTION THEY WROTE, which is
    an `Asked`: a record of the words their grammar gave them. Nothing is built
    until the door is called, so what is recorded here is that question, twice,
    and everything about the database that a builder holding a connection would
    have changed.
    """
    open_already = [db["connection"], *db["writers"]]
    wrote = _everyone_named_bob("pseudo_sql")
    wrote_again = _everyone_named_bob("pseudo_sql")
    return {
        "wrote": wrote,
        "wrote_again": wrote_again,
        "open_already": open_already,
        "open_now": [db["connection"], *db["writers"]],
        "room": _room_of(db),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _uses_the_grammar(db: dict[str, Any], grammar: str) -> dict[str, Any]:
    """Two questions, one after the other, in the same grammar."""
    first = await _asked(db, grammar, _everyone_named_bob(grammar))
    held = {field: db[field] for field in ("engine", "connection", "writers", "shutdown")}
    second = await _asked(db, grammar, _everyone_named_bob(grammar))
    return {
        "answered": first,
        "first": first,
        "second": second,
        "held": held,
        "after": {field: db[field] for field in held},
        "in_the_table": await _what_the_table_holds(db),
    }


async def _a_string_of_sql_to(db: dict[str, Any], grammar: str) -> dict[str, Any]:
    """A string of SQL, written where the narrowing goes."""
    return {
        "answered": await _asked(db, grammar, _question(grammar, "all", condition=A_STRING_OF_SQL)),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _a_string_of_sql_to_any_of_the_four(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """The same string of SQL, offered to each of the four in turn."""
    answers = await _in_every_grammar(
        db, lambda grammar: _question(grammar, "all", condition=A_STRING_OF_SQL)
    )
    return {
        "answered": answers[THE_FOUR_GRAMMARS[0]],
        "answers": answers,
        "in_the_table": await _what_the_table_holds(db),
    }


async def _people_in_the_django_grammar(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """One Django question, and what the process held when it was asked."""
    return {
        "answered": await _asked(db, "django", _everyone_named_bob("django")),
        "environment": dict(os.environ),
        "what_the_door_takes": tuple(inspect.signature(query_ask).parameters),
        "in_the_table": await _what_the_table_holds(db),
    }


async def _two_callers_on_one_database(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """Two callers, two grammars, one database between them."""
    one = await _asked(db, "django", _everyone_named_bob("django"))
    another = await _asked(db, "prisma", _everyone_named_bob("prisma"))
    return {
        "answered": one,
        "both": {"django": one, "prisma": another},
        "database_record": {field: db[field] for field in db},
        "in_the_table": await _what_the_table_holds(db),
    }


async def _four_questions_one_after_another(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """Four questions in the order the feature reads them, nothing between."""
    answers: dict[str, Any] = {}
    held: list[dict[str, Any]] = []
    for grammar in THE_ORDER_THE_FEATURE_READS:
        answers[grammar] = await _asked(db, grammar, _everyone_named_bob(grammar))
        held.append({field: db[field] for field in ("engine", "connection", "shutdown")})
    return {
        "answers": answers,
        "held": held,
        "in_the_table": await _what_the_table_holds(db),
    }


async def _the_same_four_in_a_different_order(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """The same four questions, asked again in another order."""
    first: dict[str, Any] = {}
    for grammar in THE_ORDER_THE_FEATURE_READS:
        first[grammar] = await _asked(db, grammar, _everyone_named_bob(grammar))
    again: dict[str, Any] = {}
    for grammar in ANOTHER_ORDER:
        again[grammar] = await _asked(db, grammar, _everyone_named_bob(grammar))
    return {"first": first, "again": again, "in_the_table": await _what_the_table_holds(db)}


async def _one_function_asks_twice(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """Django and then Prisma, inside one function, with nothing between them."""
    one = await _asked(db, "django", _everyone_named_bob("django"))
    another = await _asked(db, "prisma", _everyone_named_bob("prisma"))
    return {
        "both": {"django": one, "prisma": another},
        "in_the_table": await _what_the_table_holds(db),
    }


async def _person_asked_in_each_of_the_four(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """One model, handed to the door four times.

    The model each call was handed is kept by identity, because the scenario
    asks that there is one Person rather than one per grammar.
    """
    answers: dict[str, Any] = {}
    handed: list[int] = []
    for grammar in THE_FOUR_GRAMMARS:
        handed.append(id(THE_MODEL))
        answers[grammar] = await _asked(db, grammar, _everyone_named_bob(grammar))
    return {"answers": answers, "handed": handed, "in_the_table": await _what_the_table_holds(db)}


async def _each_entry_point_is_read(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """One question written in Django's words, offered under every name.

    THIS IS HOW A CALLER SEES THAT NOTHING GUESSES. The arguments never change;
    only the name does. Django reads `filter` and the other three do not, so the
    same arguments answer under one name and are refused under the others, which
    could not happen if the shape of the call were being worked out.
    """
    written_in_django = _everyone_named_bob("django")
    under_each: dict[str, Any] = {}
    for grammar in THE_FOUR_GRAMMARS:
        under_each[grammar] = await _asked(db, grammar, dict(written_in_django))
    return {
        "wrote": written_in_django,
        "under_each": under_each,
        "in_the_table": await _what_the_table_holds(db),
    }


def _the_words_this_grammar_does_not_read(grammar: str) -> tuple[str, ...]:
    """Every word another grammar spells its own way that this one does not read."""
    mine = set(WHAT_EACH_GRAMMAR_CALLS_IT[grammar].values())
    return tuple(
        sorted(
            {
                word
                for other in THE_FOUR_GRAMMARS
                for word in WHAT_EACH_GRAMMAR_CALLS_IT[other].values()
            }
            - mine
        )
    )


async def _the_grammar_is_entered(db: dict[str, Any], grammar: str) -> dict[str, Any]:
    """This name answers, and every word it does not read is refused under it."""
    answered = await _asked(db, grammar, _everyone_named_bob(grammar))
    foreign: dict[str, Any] = {}
    for word in _the_words_this_grammar_does_not_read(grammar):
        foreign[word] = await _asked(
            db,
            grammar,
            {
                "model": THE_TABLE_THE_FEATURE_NAMES,
                "wanted": "all",
                word: {"name": THE_NAME_THE_FEATURE_ASKS_FOR},
            },
        )
    return {"answered": answered, "foreign": foreign, "grammar": grammar}


async def _a_question_in_django_and_in_prisma(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """One write, declared in Django's words and then in Prisma's."""
    return {
        "waiting": {
            "django": await _the_statement_a_write_leaves(db, "django"),
            "prisma": await _the_statement_a_write_leaves(db, "prisma"),
        },
        "in_the_table": await _what_the_table_holds(db),
    }


async def _two_grammars_in_one_question(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """A question carrying Django's word and SQLAlchemy's, asked as Django."""
    both = {
        "model": THE_TABLE_THE_FEATURE_NAMES,
        "wanted": "all",
        WHAT_EACH_GRAMMAR_CALLS_IT["django"]["condition"]: {"name": THE_NAME_THE_FEATURE_ASKS_FOR},
        WHAT_EACH_GRAMMAR_CALLS_IT["sqlalchemy"]["condition"]: {
            "name": THE_NAME_THE_FEATURE_ASKS_FOR
        },
    }
    return {
        "answered": await _asked(db, "django", both),
        "the_other_grammars_word": WHAT_EACH_GRAMMAR_CALLS_IT["sqlalchemy"]["condition"],
        "in_the_table": await _what_the_table_holds(db),
    }


async def _a_grammar_the_model_does_not_carry(db: dict[str, Any], _grammar: str) -> dict[str, Any]:
    """A name no grammar answers to."""
    return {
        "answered": await _asked(
            db,
            A_GRAMMAR_NOTHING_CAN_BUILD,  # type: ignore[arg-type]
            _question("pseudo_sql", "all"),
        ),
        "in_the_table": await _what_the_table_holds(db),
    }


WHAT_EACH_SENTENCE_DOES: dict[str, Callable[[dict[str, Any], str], Awaitable[dict[str, Any]]]] = {
    'a caller asks for people named "" in the <grammar> grammar': _people_named_bob_in,
    "a caller asks for every person": _every_person,
    "a caller declares one new person": _one_new_person,
    'a caller asks for the column ""': _the_misspelled_column,
    "a caller asks for every person in the <grammar> grammar": _every_person_in,
    'a caller narrows by "" in the <grammar> grammar': _narrowing_by_nickname_in,
    'a caller narrows by "" in each of the four grammars': _narrowing_by_nickname_in_each,
    "the same question is asked in all four grammars": _the_same_question_in_all_four,
    "a caller writes a question and does not run it": _a_question_that_is_not_run,
    "a caller uses the <grammar> grammar": _uses_the_grammar,
    "a caller passes a string of SQL to the <grammar> grammar": _a_string_of_sql_to,
    "a caller passes a string of SQL to any of the four": _a_string_of_sql_to_any_of_the_four,
    "a caller asks for people in the <grammar> grammar": _people_in_the_django_grammar,
    (
        "one caller asks in django and another asks in prisma on that one database"
    ): _two_callers_on_one_database,
    (
        "a caller asks four questions in pseudo-sql, django, prisma and sqlalchemy"
    ): _four_questions_one_after_another,
    ("the same four questions are asked in a different order"): _the_same_four_in_a_different_order,
    "one function asks in django and then in prisma": _one_function_asks_twice,
    "Person is asked in each of the four grammars": _person_asked_in_each_of_the_four,
    "each grammar's entry point is read": _each_entry_point_is_read,
    "the <grammar> grammar is entered": _the_grammar_is_entered,
    (
        "a question built in django is compared to the same question in prisma"
    ): _a_question_in_django_and_in_prisma,
    (
        "a caller starts in django and continues in sqlalchemy on the same question"
    ): _two_grammars_in_one_question,
    (
        "a caller reaches for a grammar the model does not carry"
    ): _a_grammar_the_model_does_not_carry,
}


# =============================================================================
# reading a sentence that carries its own words
# =============================================================================

_QUOTED = re.compile(r'"[^"]*"')
_A_GRAMMAR_A_SENTENCE_NAMES = re.compile(
    r"\b(pseudo[-_]sql|django|prisma|sqlalchemy)\b(?= grammar\b)"
)


def _without_what_it_names(said: str) -> str:
    """The sentence with its values and its grammar blanked, which is the key."""
    return _A_GRAMMAR_A_SENTENCE_NAMES.sub("<grammar>", _QUOTED.sub('""', said.strip()))


WHICHEVER_IT_NAMED: dict[bool, Callable[[Any], str]] = {
    True: lambda found: THE_NAME_A_SCENARIO_SPELLS[found.group(1)],
    False: lambda _found: "",
}


def _the_grammar_in(said: str) -> str:
    """The name this sentence reached for, in the spelling the vocabulary has."""
    found = _A_GRAMMAR_A_SENTENCE_NAMES.search(said)
    return WHICHEVER_IT_NAMED[found is not None](found)


# =============================================================================
# Given: what the scenario establishes before it asks anything
# =============================================================================

WHAT_EACH_GIVEN_SAYS_IS_THERE: dict[str, tuple[str, ...]] = {
    'a model Person declaring the table "" with columns id and name': (
        THE_TABLE_THE_FEATURE_NAMES,
        *WHAT_THE_MODEL_DECLARES,
    ),
    "a database holding alice, bob and carol": tuple(row["name"] for row in ALREADY_THERE),
}


@given(parsers.re(r"^(?P<said>a (?:model|database) .+)$"))
def _what_the_scenario_established(said: str) -> None:
    """Two sentences, one function over a table.

    IT HANDS A `Then` NOTHING, AND IT IS NOT EMPTY EITHER. What the scenario
    says is there is what `_against_every_engine` puts there, and the two are
    written in two places: the sentence, and the constants at the top of this
    file. This reads the one against the other, so renaming the table, a column
    or a person in the feature fails the Given that says it rather than some
    later assertion about rows.
    """
    missing = [
        name
        for name in WHAT_EACH_GIVEN_SAYS_IS_THERE[_without_what_it_names(said)]
        if name not in said
    ]
    assert missing == [], (
        f"the scenario says {said!r} and this file sets up {missing!r}, which it "
        "does not name. The setup and the sentence have to be the same thing"
    )


# =============================================================================
# When: the caller asks
# =============================================================================


@when(parsers.re(r"^(?P<said>.+)$"))
def _what_the_caller_did(
    said: str,
    scenario: dict[str, object],
    tmp_path: pathlib.Path,
    request: pytest.FixtureRequest,
    require_postgresql: None,
) -> None:
    """Twenty-three sentences, one function over a table, on three engines each.

    A sentence with no row raises a KeyError naming itself, which is what
    happens when the feature is edited and this file is not.
    """
    work = WHAT_EACH_SENTENCE_DOES[_without_what_it_names(said)]
    scenario["outcomes"] = asyncio.run(
        _against_every_engine(work, _the_grammar_in(said), tmp_path, request.node.nodeid)
    )


def _outcomes(scenario: dict[str, object]) -> dict[str, dict[str, Any]]:
    return scenario["outcomes"]  # type: ignore[return-value]


# =============================================================================
# Then: what came back
# =============================================================================


def _the_same_declaration_was_built(engine: str, outcome: dict[str, Any]) -> None:
    """Four grammars, one write, one statement waiting for it.

    A caller never holds the declaration, so this reads the statement each
    grammar's write left in the room. `fn compile_statement` is a function of
    the declaration and the engine, and the engine is the same one for all four.
    """
    waiting = outcome["waiting"]
    assert len(waiting) == len(THE_FOUR_GRAMMARS), (
        f"on {engine} only {sorted(waiting)} were asked, and the four grammars "
        f"are {list(THE_FOUR_GRAMMARS)}"
    )
    one = waiting[THE_FOUR_GRAMMARS[0]]
    differing = {name: held for name, held in waiting.items() if held != one}
    assert differing == {}, (
        f"on {engine} the grammars did not build one declaration: "
        f"{THE_FOUR_GRAMMARS[0]} left {one!r} waiting and {differing!r} differ from it"
    )


def _rows_come_back_as_instances(engine: str, outcome: dict[str, Any]) -> None:
    answered = outcome["answered"]
    assert answered == _the_people_already_there(), (
        f"on {engine} a read of everyone answered {answered!r} and the table "
        f"holds {outcome['in_the_table']!r}"
    )
    for row in answered:
        assert tuple(sorted(row)) == tuple(sorted(WHAT_THE_MODEL_DECLARES)), (
            f"on {engine} a row came back carrying {sorted(row)} and the model "
            f"declares {sorted(WHAT_THE_MODEL_DECLARES)}. An instance of the "
            "model is a row carrying every column the model declares"
        )


def _a_ticket_comes_back(engine: str, outcome: dict[str, Any]) -> None:
    answered = outcome["answered"]
    assert isinstance(answered, str), (
        f"on {engine} declaring one new person answered {answered!r}, and a "
        "write is answered by its ticket"
    )
    assert outcome["room"]["arrivals"] == [answered], (
        f"on {engine} the room holds {outcome['room']['arrivals']!r} and the ticket is {answered!r}"
    )
    assert outcome["in_the_table"] == _the_people_already_there(), (
        f"on {engine} the table already holds {outcome['in_the_table']!r}. The "
        "caller was answered before the row reached the engine, so the row is "
        "still waiting"
    )


def _refused_naming_the_model_and_its_columns(engine: str, outcome: dict[str, Any]) -> None:
    """The refusal names the model and every column it declares.

    THE FEATURE CALLS THE MODEL Person AND THE PACKAGE CALLS IT people. `type
    Model` carries a table and no name of its own, so the name in the refusal is
    the table's.
    """
    answered = outcome["answered"]
    assert _is_a_fault(answered), (
        f"on {engine} asking for {outcome['named']!r} answered {answered!r}, and "
        "a column no model declares is refused"
    )
    said = _what_it_said(answered)
    for name in (THE_TABLE_THE_FEATURE_NAMES, outcome["named"], *WHAT_THE_MODEL_DECLARES):
        assert name in said, (
            f"on {engine} the refusal says {said!r} and never names {name!r}. A "
            "caller reading it has to be told which model refused and what it "
            "does declare"
        )


def _nothing_reached_the_database(engine: str, outcome: dict[str, Any]) -> None:
    assert outcome["in_the_table"] == _the_people_already_there(), (
        f"on {engine} the table holds {outcome['in_the_table']!r} after a "
        "question that was refused before it was compiled"
    )
    assert outcome["room"]["arrivals"] == [], (
        f"on {engine} the room holds {outcome['room']['arrivals']!r}, so the "
        "refused question was deposited rather than refused"
    )


def _one_person_and_that_person_is_bob(engine: str, outcome: dict[str, Any]) -> None:
    answered = outcome["answered"]
    assert answered == _bob_alone(), (
        f"on {engine} asking for people named {THE_NAME_THE_FEATURE_ASKS_FOR!r} "
        f"answered {answered!r} and the table holds {outcome['in_the_table']!r}"
    )


def _three_instances_of_person(engine: str, outcome: dict[str, Any]) -> None:
    answered = outcome["answered"]
    assert answered == _the_people_already_there(), (
        f"on {engine} asking for everyone answered {answered!r} and the table "
        f"holds {outcome['in_the_table']!r}"
    )
    for row in answered:
        assert tuple(sorted(row)) == tuple(sorted(WHAT_THE_MODEL_DECLARES)), (
            f"on {engine} a row came back carrying {sorted(row)} and the model "
            f"declares {sorted(WHAT_THE_MODEL_DECLARES)}"
        )


def _none_of_them_is_a_dict(engine: str, outcome: dict[str, Any]) -> None:
    """`fn answer_from_rows` says there is no class to build, so they are
    mappings, and this line asks for the class that card refuses to build."""
    pytest.xfail(
        f"on {engine} the rows came back as {[type(row).__name__ for row in outcome['answered']]}. "
        "`fn answer_from_rows` says an instance of the model is a row carrying "
        "every column the model declares and that there is no class to build, so "
        "nothing this library hands back can be other than a mapping"
    )


def _refused_in_the_callers_own_process(engine: str, outcome: dict[str, Any]) -> None:
    answered = outcome["answered"]
    assert _is_a_fault(answered), (
        f"on {engine} narrowing by {outcome['named']!r} answered {answered!r}"
    )
    assert answered["kind"] == "refused", (
        f"on {engine} the fault is of kind {answered['kind']!r}, and a column no "
        "model declares is a refusal rather than anything the engine reported"
    )
    assert outcome["in_the_table"] == _the_people_already_there(), (
        f"on {engine} the table holds {outcome['in_the_table']!r}, so the "
        "question travelled further than the caller's own process"
    )
    assert outcome["room"]["arrivals"] == [], (
        f"on {engine} the room holds {outcome['room']['arrivals']!r} after a refusal"
    )


def _the_refusal_names_person(engine: str, outcome: dict[str, Any]) -> None:
    """The feature calls the model Person; `type Model` holds it as `people`."""
    said = _what_it_said(outcome["answered"])
    assert THE_TABLE_THE_FEATURE_NAMES in said, (
        f"on {engine} the refusal says {said!r} and never names the model it "
        f"refused for, which this record holds as {THE_TABLE_THE_FEATURE_NAMES!r}"
    )


def _all_four_answers_are_equal(engine: str, outcome: dict[str, Any]) -> None:
    answers = outcome["in_every_grammar"]
    assert len(answers) == len(THE_FOUR_GRAMMARS), (
        f"on {engine} only {sorted(answers)} were asked, and the four grammars "
        f"are {list(THE_FOUR_GRAMMARS)}"
    )
    one = answers[THE_FOUR_GRAMMARS[0]]
    assert one == _bob_alone(), (
        f"on {engine} the four grammars agreed on {one!r}, and the table holds "
        f"{outcome['in_the_table']!r}. Four grammars agreeing on the wrong "
        "answer is four grammars agreeing"
    )
    differing = {name: answer for name, answer in answers.items() if answer != one}
    assert differing == {}, (
        f"on {engine} {THE_FOUR_GRAMMARS[0]} answered {one!r} and {differing!r} did not"
    )


def _the_four_declarations_are_equal(engine: str, outcome: dict[str, Any]) -> None:
    _the_same_declaration_was_built(engine, outcome)


def _a_dictionary_can_hold_one_as_a_key(engine: str, outcome: dict[str, Any]) -> None:
    """`type Declaration` is a record, a record is a dict, and a dict is not
    hashable. Nothing a caller holds through this door can be a key."""
    pytest.xfail(
        f"on {engine} the value each grammar built is a {type(outcome['waiting'][THE_FOUR_GRAMMARS[0]]).__name__}. "
        "`type Declaration` and `type Statement` are records, and Python refuses "
        "a dict as a dictionary key, so this line asks for a shape the "
        "declaration does not give either of them"
    )


def _what_they_hold_can_be_read_compared_and_stored(engine: str, outcome: dict[str, Any]) -> None:
    """What a caller holds before they run it is the question they wrote."""
    wrote = outcome["wrote"]
    assert wrote["model"] == THE_TABLE_THE_FEATURE_NAMES, (
        f"on {engine} the question reads {wrote!r}, and a caller who wants to "
        "know which model it names has to be able to read it out"
    )
    assert wrote == outcome["wrote_again"], (
        f"on {engine} the same question written twice came out as {wrote!r} and "
        f"{outcome['wrote_again']!r}, so two of them cannot be compared"
    )
    kept = {"held": wrote}
    assert kept["held"] == wrote, (
        f"on {engine} the question did not survive being stored and read back"
    )


def _building_it_opened_no_connection(engine: str, outcome: dict[str, Any]) -> None:
    """The connections the open made are the connections that are there.

    `boundary_in fn session_open` opens the read connection and the crew, and
    those are counted before the question is written, so what this reads is
    whether writing one added any.
    """
    assert outcome["open_already"] != [], (
        f"on {engine} the database had no connection open before the question "
        "was written, so a new one would not have shown up here"
    )
    assert outcome["open_now"] == outcome["open_already"], (
        f"on {engine} {outcome['open_already']!r} were open before the question "
        f"was written and {outcome['open_now']!r} after it"
    )
    assert outcome["room"]["arrivals"] == [], (
        f"on {engine} writing a question put {outcome['room']['arrivals']!r} in the room"
    )
    assert outcome["in_the_table"] == _the_people_already_there(), (
        f"on {engine} the table holds {outcome['in_the_table']!r} after a question nobody ran"
    )


def _no_part_of_it_is_a_string_of_sql(engine: str, outcome: dict[str, Any]) -> None:
    """The words read like SQL and none of them is SQL.

    A KEY IS A WORD AND A VALUE IS A VALUE, AND THEY ARE ASKED DIFFERENT
    QUESTIONS. `where` and `select` are this grammar's own words, and the
    grammar exists to read in SQL's word order, so a key is asked whether it is
    one of the words the grammar reads or a column the model declares. A value
    is asked whether it carries SQL at all, because a value is the one place a
    clause could be smuggled in as text.
    """
    sql = ("SELECT", "INSERT", "UPDATE", "DELETE", " FROM ", "WHERE", "JOIN", ";", "*")
    keys, values = _the_keys_and_values_in(outcome["wrote"])
    assert (keys, values) != ([], []), (
        f"on {engine} nothing the caller wrote was read at all, so this check asked nothing"
    )
    words = {"model", "wanted", *WHAT_EACH_GRAMMAR_CALLS_IT["pseudo_sql"].values()}
    for key in keys:
        assert key in words | set(WHAT_THE_MODEL_DECLARES), (
            f"on {engine} the caller wrote a part called {key!r}, which is "
            f"neither one of this grammar's words, {sorted(words)}, nor a column "
            f"the model declares, {sorted(WHAT_THE_MODEL_DECLARES)}"
        )
    for value in values:
        for word in sql:
            assert word not in value.upper(), (
                f"on {engine} the caller wrote the value {value!r}, which carries "
                f"{word!r}. A grammar that took a value carrying SQL would be the "
                "way back to it"
            )


def _the_keys_and_values_in(written: Any) -> tuple[list[str], list[str]]:
    """Every key and every string value anywhere in a question, kept apart."""
    by_shape: dict[str, Callable[[Any], tuple[list[str], list[str]]]] = {
        "dict": lambda held: (
            list(held) + [k for value in held.values() for k in _the_keys_and_values_in(value)[0]],
            [v for value in held.values() for v in _the_keys_and_values_in(value)[1]],
        ),
        "list": lambda held: (
            [k for value in held for k in _the_keys_and_values_in(value)[0]],
            [v for value in held for v in _the_keys_and_values_in(value)[1]],
        ),
        "str": lambda held: ([], [held]),
    }
    return by_shape.get(type(written).__name__, lambda _held: ([], []))(written)


def _nothing_holds_state_between_two_questions(engine: str, outcome: dict[str, Any]) -> None:
    assert outcome["first"] == _bob_alone(), (
        f"on {engine} the first question answered {outcome['first']!r}"
    )
    assert outcome["second"] == outcome["first"], (
        f"on {engine} the second question answered {outcome['second']!r} and the "
        f"first answered {outcome['first']!r}. Nothing happened between them, so "
        "something was carried from one to the other"
    )
    assert outcome["after"] == outcome["held"], (
        f"on {engine} the database record read {outcome['held']!r} before the "
        f"second question and {outcome['after']!r} after it"
    )


def _there_is_no_object_to_open_or_close(engine: str, outcome: dict[str, Any]) -> None:
    takes = tuple(inspect.signature(query_ask).parameters)
    assert takes == ("db", "model", "grammar", "asked"), (
        f"on {engine} the door takes {takes!r}. A caller hands it the database "
        "they already opened, the model, the name they wrote the question in, "
        "and the question, and nothing they have to open or close for a grammar"
    )
    for named in outcome["first"]:
        assert not hasattr(named, "close"), (
            f"on {engine} the answer carries {named!r}, which has something to close"
        )


def _the_model_carries_no_object_that_owns_queries(engine: str, outcome: dict[str, Any]) -> None:
    assert sorted(THE_MODEL) == ["columns", "constraints", "dropping", "indexes", "table"], (
        f"on {engine} the model record carries {sorted(THE_MODEL)}, and a field "
        "beyond what `type Model` declares is where a manager would live"
    )
    for field, held in THE_MODEL.items():
        assert not callable(held), (
            f"on {engine} the model's {field!r} is callable, so the model carries "
            "something that could own a query"
        )


def _a_column_is_named_in_one_place_only(engine: str, outcome: dict[str, Any]) -> None:
    named = [column["name"] for column in THE_MODEL["columns"]]
    assert sorted(named) == sorted(set(named)), (
        f"on {engine} the model declares {named!r}, and a column named twice is "
        "a column named in two places"
    )
    assert outcome["first"] == _bob_alone(), (
        f"on {engine} the question named {list(WHAT_THE_MODEL_DECLARES)!r} nowhere "
        f"but in the model and answered {outcome['first']!r}"
    )


def _it_is_refused(engine: str, outcome: dict[str, Any]) -> None:
    answered = outcome["answered"]
    assert _is_a_fault(answered), f"on {engine} it answered {answered!r} rather than refusing"
    assert answered["kind"] == "refused", f"on {engine} the fault is of kind {answered['kind']!r}"
    assert outcome["in_the_table"] == _the_people_already_there(), (
        f"on {engine} the table holds {outcome['in_the_table']!r} after a refusal"
    )


def _nothing_was_configured_first(engine: str, outcome: dict[str, Any]) -> None:
    named = {
        variable: value
        for variable, value in outcome["environment"].items()
        for grammar in THE_FOUR_GRAMMARS
        if grammar in variable.lower() or grammar in str(value).lower()
    }
    assert named == {}, (
        f"on {engine} the environment carries {named!r}, so a grammar can be set "
        "somewhere other than at the call"
    )
    assert outcome["answered"] == _bob_alone(), (
        f"on {engine} the question answered {outcome['answered']!r} with nothing set beforehand"
    )


def _the_call_names_no_grammar(engine: str, outcome: dict[str, Any]) -> None:
    """`boundary_in fn query_ask` takes `grammar: Grammar`, so the call does
    name one, and the entry points this line reaches for are not built."""
    pytest.xfail(
        f"on {engine} the door takes {outcome['what_the_door_takes']!r}. "
        "`boundary_in fn query_ask` carries the name of the grammar as an "
        "argument on purpose, so one door serves four spellings, and the four "
        "entry points on a model that would let the call name none are not built"
    )


def _both_are_answered(engine: str, outcome: dict[str, Any]) -> None:
    both = outcome["both"]
    assert len(both) == 2, f"on {engine} {sorted(both)} asked, and this scenario is two callers"
    for grammar, answered in both.items():
        assert answered == _bob_alone(), (
            f"on {engine} the caller writing {grammar} was answered {answered!r} "
            f"and the table holds {outcome['in_the_table']!r}"
        )


def _neither_told_the_database_which_grammar(engine: str, outcome: dict[str, Any]) -> None:
    record = outcome["database_record"]
    named = {
        field: held
        for field, held in record.items()
        for grammar in THE_FOUR_GRAMMARS
        if grammar in str(held).lower() or grammar in field.lower()
    }
    assert named == {}, (
        f"on {engine} the database record carries {named!r}, so it was told which "
        "grammar a caller was writing in"
    )


def _all_four_are_answered(engine: str, outcome: dict[str, Any]) -> None:
    answers = outcome["answers"]
    assert len(answers) == len(THE_FOUR_GRAMMARS), (
        f"on {engine} {sorted(answers)} were asked, and the four grammars are "
        f"{list(THE_FOUR_GRAMMARS)}"
    )
    for grammar, answered in answers.items():
        assert answered == _bob_alone(), (
            f"on {engine} the {grammar} question was answered {answered!r} and "
            f"the table holds {outcome['in_the_table']!r}"
        )


def _nothing_between_them_switched_anything(engine: str, outcome: dict[str, Any]) -> None:
    held = outcome["held"]
    assert len(held) == len(THE_FOUR_GRAMMARS), (
        f"on {engine} the database was read {len(held)} times and four questions were asked"
    )
    differing = [one for one in held if one != held[0]]
    assert differing == [], (
        f"on {engine} the database read {held[0]!r} at the first question and "
        f"{differing!r} later, so something between the questions changed it"
    )


def _each_answer_is_the_one_it_was_before(engine: str, outcome: dict[str, Any]) -> None:
    first = outcome["first"]
    again = outcome["again"]
    assert sorted(first) == sorted(again) == sorted(THE_FOUR_GRAMMARS), (
        f"on {engine} {sorted(first)} were asked first and {sorted(again)} after"
    )
    assert list(again) != list(first), (
        f"on {engine} both rounds asked in the order {list(first)!r}, so the "
        "order was never changed and this scenario asked nothing"
    )
    differing = {name: again[name] for name in first if again[name] != first[name]}
    assert differing == {}, (
        f"on {engine} asking in another order changed {differing!r}, which "
        f"answered {[first[name] for name in differing]!r} the first time"
    )


def _neither_call_had_to_undo_the_other(engine: str, outcome: dict[str, Any]) -> None:
    both = outcome["both"]
    assert len(both) == 2, f"on {engine} {sorted(both)} asked, and this scenario is two calls"
    for grammar, answered in both.items():
        assert answered == _bob_alone(), (
            f"on {engine} the {grammar} call answered {answered!r} with nothing "
            "run between the two to put anything back"
        )


def _it_answered_every_time(engine: str, outcome: dict[str, Any]) -> None:
    _all_four_are_answered(engine, outcome)


def _there_is_one_person(engine: str, outcome: dict[str, Any]) -> None:
    handed = outcome["handed"]
    assert len(handed) == len(THE_FOUR_GRAMMARS), (
        f"on {engine} the model was handed over {len(handed)} times and four grammars asked"
    )
    assert set(handed) == {handed[0]}, (
        f"on {engine} the four calls were handed {len(set(handed))} different "
        "models, and there is one Person"
    )


def _each_is_a_different_name(engine: str, outcome: dict[str, Any]) -> None:
    """Four names, one model.

    THE FEATURE SAYS "on the model" AND THERE IS NO SUCH SURFACE. `type Model`
    is a record of five fields and carries no entry point, and
    `boundary_in fn query_ask` takes the name beside the model instead. What is
    true, and what this asserts, is that the four names are four.
    """
    under_each = outcome["under_each"]
    assert sorted(under_each) == sorted(THE_FOUR_GRAMMARS), (
        f"on {engine} {sorted(under_each)} were read and the four grammars are "
        f"{sorted(THE_FOUR_GRAMMARS)}"
    )
    assert len(set(THE_FOUR_GRAMMARS)) == len(THE_FOUR_GRAMMARS), (
        f"on {engine} the four names are {THE_FOUR_GRAMMARS!r}, which is not four names"
    )


def _nothing_looks_at_arguments(engine: str, outcome: dict[str, Any]) -> None:
    """One set of arguments, four names, and the name is what decided."""
    under_each = outcome["under_each"]
    answered = {name for name, got in under_each.items() if got == _bob_alone()}
    refused = {name for name, got in under_each.items() if _is_a_fault(got)}
    assert answered == {"django"}, (
        f"on {engine} a question written in Django's words answered under "
        f"{sorted(answered)}. It reads {outcome['wrote']!r} whichever name it is "
        "given, so anything that answered under another name worked the shape "
        "out from the arguments"
    )
    assert refused == set(THE_FOUR_GRAMMARS) - {"django"}, (
        f"on {engine} the other three names gave {sorted(refused)} refusals, and "
        f"the whole of {sorted(set(THE_FOUR_GRAMMARS) - {'django'})} reads no word called "
        f"{WHAT_EACH_GRAMMAR_CALLS_IT['django']['condition']!r}"
    )


def _there_is_one_name_that_starts_it(engine: str, outcome: dict[str, Any]) -> None:
    assert outcome["grammar"] in THE_NAME_A_SCENARIO_SPELLS.values(), (
        f"on {engine} the scenario entered {outcome['grammar']!r}, which `set "
        f"grammars` does not carry: it has {list(THE_FOUR_GRAMMARS)}"
    )
    assert outcome["answered"] == _bob_alone(), (
        f"on {engine} entering {outcome['grammar']!r} answered {outcome['answered']!r}"
    )


def _that_name_starts_no_other_grammar(engine: str, outcome: dict[str, Any]) -> None:
    """A word this name does not read is refused under it.

    THE TWO THE NAMES CANNOT SEPARATE. `declaration_from_prisma` and
    `declaration_from_pseudo_sql` carry the same words, so no question a caller
    can write tells those two names apart from outside the door. What is left is
    what this asserts: a word belonging to a grammar this name is not reaches
    nothing under it.
    """
    foreign = outcome["foreign"]
    expected = _the_words_this_grammar_does_not_read(outcome["grammar"])
    assert sorted(foreign) == sorted(expected), (
        f"on {engine} {sorted(foreign)} were offered to {outcome['grammar']!r} "
        f"and the words it does not read are {sorted(expected)}"
    )
    assert foreign != {}, (
        f"on {engine} {outcome['grammar']!r} was offered no foreign word at all, "
        "so this check asked nothing"
    )
    for word, answered in foreign.items():
        assert _is_a_fault(answered), (
            f"on {engine} {outcome['grammar']!r} answered {answered!r} to a "
            f"question written with {word!r}, which belongs to another grammar"
        )


def _each_refusal_names_person_and_the_column(engine: str, outcome: dict[str, Any]) -> None:
    answers = outcome["answers"]
    assert len(answers) == len(THE_FOUR_GRAMMARS), (
        f"on {engine} {sorted(answers)} were asked and the four grammars are "
        f"{list(THE_FOUR_GRAMMARS)}"
    )
    for grammar, answered in answers.items():
        assert _is_a_fault(answered), (
            f"on {engine} the {grammar} narrowing by {outcome['named']!r} answered {answered!r}"
        )
        said = _what_it_said(answered)
        assert THE_TABLE_THE_FEATURE_NAMES in said and outcome["named"] in said, (
            f"on {engine} the {grammar} refusal says {said!r}, and a caller has "
            f"to be told it was {THE_TABLE_THE_FEATURE_NAMES!r} that declares no "
            f"{outcome['named']!r}"
        )


def _none_of_them_mentions_a_grammar(engine: str, outcome: dict[str, Any]) -> None:
    answers = outcome["answers"]
    assert len(answers) == len(THE_FOUR_GRAMMARS), (
        f"on {engine} {sorted(answers)} were asked and the four grammars are "
        f"{list(THE_FOUR_GRAMMARS)}"
    )
    for grammar, answered in answers.items():
        said = _what_it_said(answered).lower()
        named = [name for name in THE_NAME_A_SCENARIO_SPELLS if name in said]
        assert named == [], (
            f"on {engine} the {grammar} refusal says {said!r}, which names "
            f"{named!r}. A refusal that reads differently per grammar is four "
            "refusals"
        )


def _they_are_equal(engine: str, outcome: dict[str, Any]) -> None:
    waiting = outcome["waiting"]
    assert sorted(waiting) == ["django", "prisma"], (
        f"on {engine} {sorted(waiting)} were compared, and this scenario is django against prisma"
    )
    assert waiting["django"] == waiting["prisma"], (
        f"on {engine} django built {waiting['django']!r} and prisma built {waiting['prisma']!r}"
    )


def _neither_carries_the_name_of_its_grammar(engine: str, outcome: dict[str, Any]) -> None:
    waiting = outcome["waiting"]
    assert sorted(waiting) == ["django", "prisma"], (
        f"on {engine} {sorted(waiting)} were read, and this scenario is two of them"
    )
    for grammar, held in waiting.items():
        said = str(held).lower()
        named = [name for name in THE_NAME_A_SCENARIO_SPELLS if name in said]
        assert named == [], (
            f"on {engine} what {grammar} built reads {held!r}, which names "
            f"{named!r}, so the question remembers who wrote it"
        )


def _the_refusal_says_one_grammar(engine: str, outcome: dict[str, Any]) -> None:
    answered = outcome["answered"]
    said = _what_it_said(answered)
    assert outcome["the_other_grammars_word"] in said, (
        f"on {engine} the refusal says {said!r} and never names "
        f"{outcome['the_other_grammars_word']!r}, which is the part that belongs "
        "to the other grammar"
    )
    assert "this grammar" in said, (
        f"on {engine} the refusal says {said!r}, and a caller who wrote half a "
        "question in another grammar has to be told that a question is asked in "
        "one"
    )


def _the_refusal_lists_the_four(engine: str, outcome: dict[str, Any]) -> None:
    said = _what_it_said(outcome["answered"])
    assert A_GRAMMAR_NOTHING_CAN_BUILD in said, (
        f"on {engine} the refusal says {said!r} and never names "
        f"{A_GRAMMAR_NOTHING_CAN_BUILD!r}, which is what arrived"
    )
    missing = [name for name in THE_FOUR_GRAMMARS if name not in said]
    assert missing == [], (
        f"on {engine} the refusal says {said!r} and leaves out {missing!r}. A "
        "caller who reached for a name that does not exist has to be told the "
        "names that do"
    )


def _the_refusal_is_the_same_one(engine: str, outcome: dict[str, Any]) -> None:
    answers = outcome["answers"]
    assert len(answers) == len(THE_FOUR_GRAMMARS), (
        f"on {engine} {sorted(answers)} were offered a string of SQL and the "
        f"four grammars are {list(THE_FOUR_GRAMMARS)}"
    )
    one = answers[THE_FOUR_GRAMMARS[0]]
    differing = {name: said for name, said in answers.items() if said != one}
    assert differing == {}, (
        f"on {engine} {THE_FOUR_GRAMMARS[0]} refused with {one!r} and "
        f"{differing!r} refused differently. One of them reading differently is "
        "where a caller starts believing one grammar is the way back to SQL"
    )


WHAT_EACH_THEN_CHECKS: dict[str, Callable[[str, dict[str, Any]], None]] = {
    "the same declaration was built": _the_same_declaration_was_built,
    "rows come back as instances of the model": _rows_come_back_as_instances,
    "a ticket comes back and the caller does not wait": _a_ticket_comes_back,
    (
        "it is refused naming the model and the columns it declares"
    ): _refused_naming_the_model_and_its_columns,
    "nothing reached the database": _nothing_reached_the_database,
    "it is given one person, and that person is bob": _one_person_and_that_person_is_bob,
    "it is given three instances of Person": _three_instances_of_person,
    "none of them is a dict or a driver row": _none_of_them_is_a_dict,
    "the library refuses in the caller's own process": _refused_in_the_callers_own_process,
    "the refusal names Person": _the_refusal_names_person,
    "all four answers are equal": _all_four_answers_are_equal,
    "the four declarations they built are equal": _the_four_declarations_are_equal,
    "the four questions are equal": _the_four_declarations_are_equal,
    "a dictionary can hold one as a key": _a_dictionary_can_hold_one_as_a_key,
    (
        "what they hold can be read, compared and stored"
    ): _what_they_hold_can_be_read_compared_and_stored,
    "building it opened no connection": _building_it_opened_no_connection,
    "no part of what the caller wrote is a string of SQL": _no_part_of_it_is_a_string_of_sql,
    ("nothing holds state between two questions"): _nothing_holds_state_between_two_questions,
    "there is no object the caller must open or close": _there_is_no_object_to_open_or_close,
    (
        "the model carries no object that owns queries"
    ): _the_model_carries_no_object_that_owns_queries,
    "a column is named in one place only": _a_column_is_named_in_one_place_only,
    "it is refused": _it_is_refused,
    "nothing was configured first": _nothing_was_configured_first,
    ("the call names no grammar, only the way it is written"): _the_call_names_no_grammar,
    "both are answered": _both_are_answered,
    (
        "neither caller had to tell the database which grammar it was using"
    ): _neither_told_the_database_which_grammar,
    "all four are answered": _all_four_are_answered,
    "nothing between them switched anything": _nothing_between_them_switched_anything,
    "each answer is the one it was before": _each_answer_is_the_one_it_was_before,
    "neither call had to undo what the other did": _neither_call_had_to_undo_the_other,
    "it answered every time": _it_answered_every_time,
    "there is one Person, not one per grammar": _there_is_one_person,
    "each is a different name on the model": _each_is_a_different_name,
    ("nothing looks at arguments to decide which grammar was used"): _nothing_looks_at_arguments,
    "there is one name that starts it": _there_is_one_name_that_starts_it,
    "that name starts no other grammar": _that_name_starts_no_other_grammar,
    "each refusal names Person and the column": _each_refusal_names_person_and_the_column,
    "none of them mentions a grammar": _none_of_them_mentions_a_grammar,
    "they are equal": _they_are_equal,
    (
        "neither carries the name of the grammar that built it"
    ): _neither_carries_the_name_of_its_grammar,
    ("the refusal says a question is asked in one grammar"): _the_refusal_says_one_grammar,
    "the refusal lists the four that exist": _the_refusal_lists_the_four,
    "the refusal is the same one every grammar gives": _the_refusal_is_the_same_one,
}


@then(parsers.re(r"^(?P<said>.+)$"))
def _what_must_be_true(said: str, scenario: dict[str, object]) -> None:
    """Forty-three sentences, one function over a table, asked of all three engines.

    IT SAYS HOW MANY IT ASKED, because a loop over nothing passes. Every check
    runs inside the loop below, so an empty set of outcomes would satisfy this
    step while asking nothing.
    """
    outcomes = _outcomes(scenario)
    assert len(outcomes) == len(THE_THREE_ENGINES), (
        f"{said!r} was asked of {sorted(outcomes)}, and this file runs every "
        f"scenario against {sorted(THE_THREE_ENGINES)}. A check nobody ran is a "
        "check that passed."
    )
    for engine, outcome in outcomes.items():
        WHAT_EACH_THEN_CHECKS[_without_what_it_names(said)](engine, outcome)
