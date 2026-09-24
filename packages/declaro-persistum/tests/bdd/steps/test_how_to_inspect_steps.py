"""Bind `tests/bdd/features/core/how_to_inspect.feature`, and nothing else.

WHAT THE FEATURE ASKS. Five questions per engine: its tables, its columns, its
indexes, its rules and its views.

THE LAST TWO SCENARIOS ARE THE ONES THAT MATTER. A table's name arriving as
characters inside a question is the hole `HOW-IT-SHOULD-WORK.md:208` exists to
close: every string is either a declared name or a bound value, and nothing is
pasted in. These questions are the one place in the package where the library
writes SQL about a name a caller supplied, so they are where that rule is
easiest to break and hardest to see.

WHY NOT ASSERT THE SQL ITSELF. A scenario naming `sqlite_master` would be this
file agreeing with the table beside it, and both would be wrong together the day
an engine's catalogue moves. What is asserted is the shape: five questions, text
and values apart, and a bound name where a name is needed.
"""

from __future__ import annotations

import asyncio
import pathlib
import re
import tempfile
import typing

from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.how_to_inspect import how_to_inspect
from declaro_persistum.types import Engine, Inspecting

scenarios("../features/core/how_to_inspect.feature")

EVERY_ENGINE = typing.get_args(Engine)
THE_FIVE = frozenset(typing.get_type_hints(Inspecting))

# A question asked once per table has to bind that table's name. Which of the
# five those are is the engine's business, so this finds them by the parameter
# rather than by naming them here.
# A PLACEHOLDER, IN ANY OF THE THREE SPELLINGS THE DRIVERS READ. SQLite and
# Turso read `?` and PostgreSQL reads `$1`, which is what `fn compile_statement`
# writes too. Until 2026-09-24 this looked for `:table`, and every driver refused
# a question written that way: "Binding 1 (':table') is a named parameter, but
# you supplied a sequence".
_A_BOUND_NAME = re.compile(r"[?]|[$]\d+")


@given("any engine", target_fixture="answered")
def _one_engine() -> Inspecting:
    """SQLite, because this scenario is about the shape every engine gives."""
    return how_to_inspect("sqlite")


@then(
    "the answer carries a question about its tables, its columns, its indexes, "
    "its rules and its views"
)
def _all_five(answered: Inspecting) -> None:
    missing = sorted(THE_FIVE - set(answered))
    assert not missing, (
        f"this engine is asked nothing about {missing}, so a schema read back "
        f"from it arrives incomplete and the next migration proposes adding "
        f"whatever went unseen"
    )


@when("every engine the package names is asked", target_fixture="said")
def _every_engine_is_asked() -> dict[str, Inspecting]:
    """Walked off `Engine` itself, so an engine with no row fails here."""
    return {engine: how_to_inspect(engine) for engine in EVERY_ENGINE}


@then("each one gives all five")
def _every_engine_gives_five(said: dict[str, Inspecting]) -> None:
    assert len(said) == len(EVERY_ENGINE) >= 3, (
        f"only {sorted(said)} were asked, so an engine could be added with no "
        f"row and nothing here would say so"
    )
    short = {engine: sorted(THE_FIVE - set(answer)) for engine, answer in said.items()}
    assert not any(short.values()), f"these engines are asked nothing about {short}"


@then("more than one set of five comes back")
def _not_all_alike(said: dict[str, Inspecting]) -> None:
    """Two engines sharing a row is right. All three sharing one is not."""
    written = {
        tuple(sorted((name, q["text"]) for name, q in answer.items())) for answer in said.values()
    }
    assert len(written) > 1, (
        "all three engines are asked the same five questions, so the table says "
        "nothing a single constant would not have said"
    )


@then("each question carries its text and its values apart")
def _text_and_values_apart(said: dict[str, Inspecting]) -> None:
    wrong = [
        (engine, name)
        for engine, answer in said.items()
        for name, question in answer.items()
        if sorted(question) != ["parameters", "text"] or not question["text"]
    ]
    assert not wrong, (
        f"{wrong} are not statements, so the driver has nothing to bind and the "
        f"values would have to reach the text"
    )


@then("the questions asked per table bind the name rather than writing it in")
def _the_name_is_bound(said: dict[str, Inspecting]) -> None:
    pasted = [
        (engine, name)
        for engine, answer in said.items()
        for name, question in answer.items()
        if question["parameters"] and not _A_BOUND_NAME.search(question["text"])
    ]
    assert not pasted, (
        f"{pasted} take a value and have nowhere to bind it, so a table's name "
        f"would be pasted into the question. `HOW-IT-SHOULD-WORK.md:208`: every "
        f"string is checked against a declaration or bound as a parameter, and "
        f"there is no third case."
    )

    asked_per_table = [
        (engine, name)
        for engine, answer in said.items()
        for name, question in answer.items()
        if question["parameters"]
    ]
    assert asked_per_table, (
        "not one question on any engine takes a value, so either nothing is "
        "asked per table or a name is being written straight into the text"
    )


# --- the questions run against real databases --------------------------------
#
# ONE ROW PER ENGINE, THREE ENTRIES EACH: how a database is made with something
# in it, how a question is put to it, and how it is closed. The answers are
# read back as the names the engine gave them, which is the whole of what these
# two scenarios ask about.

WHAT_A_SCENARIO_PUTS_IN_A_DATABASE = (
    "CREATE TABLE parent (id integer primary key, tag text)",
    "CREATE TABLE child (id integer primary key, note text NOT NULL, "
    "parent_id integer REFERENCES parent(id))",
    "CREATE INDEX idx_note ON child (note)",
    # A VIEW, BECAUSE THE FIFTH QUESTION IS ABOUT VIEWS. asyncpg gives no column
    # names at all for an empty result, so a database with no view cannot say
    # what the views question answers under.
    "CREATE VIEW recent AS SELECT id FROM child",
)

THE_TABLE_THE_QUESTIONS_ARE_ABOUT = "child"

# What every engine's answer to each question has to be called. The reader takes
# five questions and a connection and names no engine, which is only possible if
# these are the names whatever answered.
WHAT_EACH_ANSWER_IS_CALLED: dict[str, set[str]] = {
    "tables": {"name"},
    "columns": {"name", "declared_type", "must_hold_something", "starts_at", "is_key"},
    "indexes": {"name", "is_unique", "spelled_out"},
    "constraints": {"name", "kind", "spelled_out"},
    "views": {"name", "spelled_out"},
}


async def _ask_sqlite(folder, questions: Inspecting, table: str) -> dict[str, tuple]:
    """One database, made once, then asked all five."""
    import aiosqlite

    connection = await aiosqlite.connect(folder / "s.db", isolation_level=None)
    try:
        for making in WHAT_A_SCENARIO_PUTS_IN_A_DATABASE:
            await connection.execute(making)

        async def ask(text: str) -> tuple:
            cursor = await connection.execute(text, [table] if "?" in text else [])
            names = [column[0] for column in cursor.description]
            return names, [dict(zip(names, row, strict=True)) for row in await cursor.fetchall()]

        return {name: await ask(questions[name]["text"]) for name in WHAT_EACH_ANSWER_IS_CALLED}
    finally:
        await connection.close()


async def _ask_turso(folder, questions: Inspecting, table: str) -> dict[str, tuple]:
    import turso.aio

    connection = await turso.aio.connect(str(folder / "t.db"))
    try:
        cursor = connection.cursor()
        for making in WHAT_A_SCENARIO_PUTS_IN_A_DATABASE:
            await cursor.execute(making)

        async def ask(text: str) -> tuple:
            await cursor.execute(text, [table] if "?" in text else [])
            names = [column[0] for column in cursor.description]
            return names, [dict(zip(names, row, strict=True)) for row in await cursor.fetchall()]

        return {name: await ask(questions[name]["text"]) for name in WHAT_EACH_ANSWER_IS_CALLED}
    finally:
        await connection.close()


async def _ask_postgresql(_folder, questions: Inspecting, table: str) -> dict[str, tuple]:
    """The probe database outlives this process, so the tables go afterwards."""
    import asyncpg

    from tests.conftest import POSTGRESQL_PROBE_URL

    connection = await asyncpg.connect(POSTGRESQL_PROBE_URL)
    try:
        for making in WHAT_A_SCENARIO_PUTS_IN_A_DATABASE:
            await connection.execute(making)

        async def ask(text: str) -> tuple:
            records = await connection.fetch(text, *([table] if "$1" in text else []))
            names = list(records[0].keys()) if records else []
            return names, [dict(record) for record in records]

        return {name: await ask(questions[name]["text"]) for name in WHAT_EACH_ANSWER_IS_CALLED}
    finally:
        for dropping in (
            "DROP VIEW IF EXISTS recent",
            "DROP TABLE IF EXISTS child",
            "DROP TABLE IF EXISTS parent",
        ):
            await connection.execute(dropping)
        await connection.close()


HOW_TO_ASK_EACH_ENGINE = {
    "sqlite": _ask_sqlite,
    "turso": _ask_turso,
    "postgresql": _ask_postgresql,
}


@when("each engine is asked its five questions against a real database", target_fixture="answered")
def _each_engine_is_asked_for_real() -> dict[str, dict[str, tuple]]:
    """Every engine, every question, against a database with something in it.

    NOTHING IS A STAND-IN. What is being asserted is the shape an engine hands
    back, and a double would be deciding that shape rather than reporting it,
    which is the exact failure this scenario exists to have caught.
    """

    async def _asked() -> dict[str, dict[str, tuple]]:
        held: dict[str, dict[str, tuple]] = {}
        for engine, ask in HOW_TO_ASK_EACH_ENGINE.items():
            questions = how_to_inspect(engine)  # type: ignore[arg-type]
            with tempfile.TemporaryDirectory() as made:
                held[engine] = await ask(
                    pathlib.Path(made), questions, THE_TABLE_THE_QUESTIONS_ARE_ABOUT
                )
        return held

    return asyncio.run(_asked())


@then("the answers come back under the same names on every engine")
def _the_same_names(answered: dict[str, dict[str, tuple]]) -> None:
    """A reader handed differently named answers has to know which engine it is
    talking to, which is the one thing `Inspecting` exists to prevent."""
    assert set(answered) == set(HOW_TO_ASK_EACH_ENGINE), (
        f"only {sorted(answered)} were asked, and a row nobody ran is a row nobody has seen answer"
    )
    for engine, asked in answered.items():
        for question, wanted in WHAT_EACH_ANSWER_IS_CALLED.items():
            names, _rows = asked[question]
            assert set(names) == wanted, (
                f"{engine} answers its {question} question under {sorted(names)} "
                f"and every engine has to answer under {sorted(wanted)}"
            )


@then(
    parsers.re(
        r"^a column that (?P<claim>must hold something|is the key) reads the same on every engine$"
    )
)
def _the_same_words(answered: dict[str, dict[str, tuple]], claim: str) -> None:
    """Names alone are not enough: SQLite said 0 and 1 where PostgreSQL said
    'YES' and 'NO', so a reader comparing them would read every PostgreSQL
    column as nullable and rebuild every table on every deploy."""
    under, of_the_column = {
        "must hold something": ("must_hold_something", "note"),
        "is the key": ("is_key", "id"),
    }[claim]

    said = {}
    for engine, asked in answered.items():
        _names, rows = asked["columns"]
        found = [row for row in rows if row["name"] == of_the_column]
        assert found, f"{engine} did not answer about the column {of_the_column!r}: {rows}"
        said[engine] = found[0][under]

    assert len(set(said.values())) == 1, (
        f"{of_the_column!r} answers {said} across the engines, and a reader "
        f"comparing those has to know which engine it is talking to"
    )
    assert set(said.values()) <= {0, 1}, (
        f"{of_the_column!r} answers {said}, and this field is 0 or 1 everywhere"
    )
