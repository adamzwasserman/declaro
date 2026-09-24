"""What a database holds right now, asked for rather than assumed.

    reader fn schema_inspect : (connection: Connection, engine: Engine, inspecting: Inspecting) -> Schema | Fault

IT ASKS SO THE TWO CAN BE COMPARED, NEVER SO IT CAN DECIDE WHAT BELONGS.
`docs/architecture.md:34`: the models say what should be there and this says
what is. Nothing here adopts a table into the target schema, and a table the
models never mention comes back in the live schema and goes no further.

IT NAMES NO ENGINE AND WRITES NO SQL. The five questions arrive already spelled
by `fn how_to_inspect`, and every one of them answers under the same names on
every engine, which is what makes one reader enough. The engine is carried to
`boundary_out fn query_run_statement` and never read here: this module has no
row per engine and no place to put one.

WHAT EVERY ENGINE ANSWERS, AND WHY THAT IS A PROPERTY OF THE QUESTIONS RATHER
THAN OF THIS FUNCTION:

    tables       name
    columns      name, declared_type, must_hold_something, starts_at, is_key
    indexes      name, is_unique, spelled_out
    constraints  name, kind, spelled_out
    views        name, spelled_out

Measured 2026-09-24, before the questions were aliased, those five came back
under five different sets of names on SQLite and PostgreSQL, and nullability
came back as 0 and 1 on one and as 'YES' and 'NO' on the other.

WHY SQLITE'S RULES ARRIVE AS TEXT AND EVERY OTHER ANSWER ARRIVES STRUCTURED.
SQLite keeps no catalogue of constraints. The only record of a CHECK or a
foreign key is the `CREATE TABLE` it was made with, so its constraints question
answers with one row carrying that whole statement under the kind
`in_the_ddl`, and this reads it. There is no other source: without that reading
the live side comes back with no rules at all, and `fn difference` then compares
every declared rule against nothing and rebuilds every table on every deploy.

WHAT IS RECOVERED FROM THAT TEXT, AND WHAT IS NOT. A named CHECK, a named
PRIMARY KEY and a named UNIQUE come back, because each is written as
`CONSTRAINT <name> <kind>` and the name is what a comparison needs. An unnamed
rule does not: SQLite keeps no name for one either, so there is nothing to
compare it by, and a rule this reader invented a name for would differ from the
declared one every time and rebuild the table on every deploy. An unnamed rule
is left out rather than guessed at, and this paragraph is the record of that.

AN EMPTY DATABASE IS AN ANSWER. A schema with no tables comes back as a schema
with no tables, and that is distinguishable from a failure because a failure is
a `Fault`.

A FAILURE COMES BACK IN THE RETURN. A question the engine refuses is a live
schema nobody read, and a migration worked out from a half-read schema drops
what it could not see.
"""

from __future__ import annotations

import re
from typing import Any, cast

from declaro_persistum.query_run_statement import query_run_statement
from declaro_persistum.types import (
    Column,
    Condition,
    Connection,
    Constraint,
    Engine,
    Fault,
    Index,
    Inspecting,
    Rows,
    Schema,
    Statement,
    Table,
    View,
)

# The narrowing that narrows by nothing, which every rule but a check carries.
NARROWS_BY_NOTHING: Condition = {"each": {}, "any_of": [], "none_of": []}

# What SQLite's constraints question says instead of a kind, because SQLite has
# no catalogue of rules and answers with the whole CREATE TABLE.
IN_THE_DDL = "in_the_ddl"

# `CONSTRAINT <name> <KIND>` in a CREATE TABLE. The name is what a comparison
# needs, and the kind is the next word.
_A_NAMED_RULE = re.compile(
    r'CONSTRAINT\s+["`\[]?(?P<name>\w+)["`\]]?\s+(?P<kind>CHECK|PRIMARY\s+KEY|UNIQUE)',
    re.IGNORECASE,
)

# SQL's words for a rule -> this package's. A kind with no row here is a rule
# this package does not carry, and it is left out rather than guessed at.
_WHAT_SQL_CALLS_IT: dict[str, str] = {
    "CHECK": "check",
    "PRIMARY KEY": "primary_key",
    "UNIQUE": "unique",
}

# PostgreSQL's one-letter kinds, from `pg_constraint.contype`.
_WHAT_POSTGRESQL_CALLS_IT: dict[str, str] = {
    "c": "check",
    "p": "primary_key",
    "u": "unique",
    "f": "check",
}


def _refused(asked: str, said: Fault) -> Fault:
    """A question the engine would not answer, on its way back as a value."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"A database could not be asked what {asked} it holds: {said['what_went_wrong']}"
        ),
        what_to_do_instead=(
            "Read what the engine said above. A migration worked out from a "
            "schema nobody could read would drop whatever it did not see, so "
            "nothing is compared until every question is answered."
        ),
    )


def _is_fault(answer: object) -> bool:
    return isinstance(answer, dict) and set(answer) == {
        "kind",
        "what_went_wrong",
        "what_to_do_instead",
    }


async def _ask(
    connection: Connection, engine: Engine, question: Statement, about: str
) -> Rows | Fault:
    """One question, with the table's name bound where the question takes one."""
    asked: Statement = {
        "text": question["text"],
        "parameters": dict.fromkeys(question["parameters"], about),
    }
    answer = await query_run_statement(connection, engine, asked)
    return cast("Rows | Fault", answer)


def _a_column(row: dict[str, Any]) -> Column:
    """One live column, out of the answer every engine gives under one set of names."""
    return Column(
        name=str(row["name"]),
        declared_type=str(row["declared_type"]),  # type: ignore[typeddict-item]
        renamed_from=None,
        references=None,
    )


def _an_index(row: dict[str, Any]) -> Index:
    return Index(name=str(row["name"]), columns=[], unique=bool(row["is_unique"]))


def _a_rule(name: str, kind: str, columns: list[str]) -> Constraint:
    return Constraint(
        name=name,
        kind=kind,  # type: ignore[typeddict-item]
        columns=columns,
        rule=NARROWS_BY_NOTHING,
        value=None,
    )


def _the_rules_the_column_answers_carry(columns: Rows) -> list[Constraint]:
    """not_null and primary_key, which every engine answers per column.

    THESE TWO DO NOT COME FROM THE CONSTRAINTS QUESTION ON ANY ENGINE. SQLite
    writes NOT NULL and PRIMARY KEY beside the column and keeps no constraint
    row for either; PostgreSQL keeps a row for the key and answers nullability
    per column. Reading both off the columns answer is the one reading that is
    the same everywhere.
    """
    return [
        _a_rule(f"{under}_{row['name']}", under, [str(row["name"])])
        for row in columns
        for under, held in (("not_null", "must_hold_something"), ("primary_key", "is_key"))
        if bool(row[held])
    ]


def _the_rules_in_the_ddl(spelled_out: str) -> list[Constraint]:
    """Every named rule in a CREATE TABLE, which is all SQLite keeps of them.

    AN UNNAMED RULE IS LEFT OUT. SQLite keeps no name for one either, so there
    is nothing to compare it by, and a name this reader invented would differ
    from the declared one every time and rebuild the table on every deploy.
    """
    return [
        _a_rule(
            found.group("name"),
            _WHAT_SQL_CALLS_IT[" ".join(found.group("kind").upper().split())],
            [],
        )
        for found in _A_NAMED_RULE.finditer(spelled_out)
    ]


def _the_rules_a_catalogue_carries(row: dict[str, Any]) -> list[Constraint]:
    """One rule out of an engine that keeps a catalogue of them.

    A KIND WITH NO ROW IS LEFT OUT RATHER THAN GUESSED AT. PostgreSQL answers
    with letters this package has no word for -- an exclusion, a trigger -- and
    a rule read as the wrong kind compares wrong every deploy.
    """
    said = str(row["kind"])
    return [
        _a_rule(str(row["name"]), _WHAT_POSTGRESQL_CALLS_IT[said], [])
        for _ in range(int(said in _WHAT_POSTGRESQL_CALLS_IT))
    ]


def _the_rules_in(constraints: Rows, columns: Rows) -> list[Constraint]:
    """Every rule this table holds, whichever way its engine keeps them."""
    return [
        *_the_rules_the_column_answers_carry(columns),
        *[
            rule
            for row in constraints
            for rule in _WHERE_THE_RULES_ARE[str(row["kind"]) == IN_THE_DDL](row)
        ],
    ]


_WHERE_THE_RULES_ARE = {
    True: lambda row: _the_rules_in_the_ddl(str(row["spelled_out"] or "")),
    False: _the_rules_a_catalogue_carries,
}


async def _one_table(
    connection: Connection, engine: Engine, inspecting: Inspecting, name: str
) -> Table | Fault:
    """One table's columns, indexes and rules, out of three questions about it."""
    asked = {
        about: await _ask(connection, engine, inspecting[about], name)  # type: ignore[literal-required]
        for about in ("columns", "indexes", "constraints")
    }
    refused = [
        _refused(about, cast(Fault, answer)) for about, answer in asked.items() if _is_fault(answer)
    ]
    return cast("Table | Fault", _THE_FIRST_REFUSAL_OR[refused != []](refused, asked, name))


def _the_first_refusal(refused: list[Fault], _asked: dict[str, Any], _name: str) -> Fault:
    return refused[0]


def _a_table_out_of_the_answers(_refused: list[Fault], asked: dict[str, Any], name: str) -> Table:
    columns = cast(Rows, asked["columns"])
    return Table(
        name=name,
        columns=[_a_column(row) for row in columns],
        indexes=[_an_index(row) for row in cast(Rows, asked["indexes"])],
        constraints=_the_rules_in(cast(Rows, asked["constraints"]), columns),
    )


_THE_FIRST_REFUSAL_OR = {True: _the_first_refusal, False: _a_table_out_of_the_answers}


def _a_view(row: dict[str, Any]) -> View:
    """One live view, carrying its name and nothing this package can compare.

    A VIEW HOLDS A DECLARATION AND AN ENGINE HOLDS A SELECT, and there is no
    reader here that turns one into the other. So the declaration comes back
    empty and `fn difference` compares views by name. Recovering a declaration
    from a SELECT is a parser nobody has written, and this comment is where it
    would be named when somebody does.
    """
    return View(
        name=str(row["name"]),
        declaration=cast("Any", None),
        freshness=None,
    )


async def schema_inspect(
    connection: Connection, engine: Engine, inspecting: Inspecting
) -> Schema | Fault:
    """Ask a database the five questions and build what it holds out of the answers.

    What comes back is the live schema: every table with its columns, indexes
    and rules, and every view by name. A database with nothing in it comes back
    as a schema with nothing in it, which is an answer. A question the engine
    refused comes back as a `Fault`, because a migration worked out from a
    half-read schema drops whatever it could not see.
    """
    tables = await _ask(connection, engine, inspecting["tables"], "")
    views = await _ask(connection, engine, inspecting["views"], "")
    refused = [
        _refused(about, cast(Fault, answer))
        for about, answer in (("tables", tables), ("views", views))
        if _is_fault(answer)
    ]
    held = [
        await _one_table(connection, engine, inspecting, str(row["name"]))
        for row in cast(Rows, tables if not _is_fault(tables) else [])
    ]
    every_refusal = [*refused, *[one for one in held if _is_fault(one)]]
    return cast(
        "Schema | Fault",
        _A_SCHEMA_OR_THE_FIRST_REFUSAL[every_refusal != []](every_refusal, held, views),
    )


def _the_first_of_them(every_refusal: list[Any], _held: list[Any], _views: object) -> Fault:
    return cast(Fault, every_refusal[0])


def _the_whole_schema(_every_refusal: list[Any], held: list[Any], views: object) -> Schema:
    return Schema(
        tables=cast("list[Table]", held),
        views=[_a_view(row) for row in cast(Rows, views)],
    )


_A_SCHEMA_OR_THE_FIRST_REFUSAL = {True: _the_first_of_them, False: _the_whole_schema}
