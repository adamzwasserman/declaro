"""Bind `tests/bdd/features/schema/spell_operation.feature`.

WHAT THE FEATURE ASKS FOR. `spell_operation` is handed one schema change and
the name of an engine, and hands back the statements that carry the change out
on that engine. One change is often several statements, so the answer is a
list, in the order the statements have to run. An engine that cannot express
the change hands back a fault instead, and the fault comes back in the return
rather than being raised.

WHAT RUNS AGAINST A REAL DATABASE, AND WHY. A statement naming a column the
table has not got is still a plausible string, so the scenarios about the enum
lookup and about the stamp open an in-memory SQLite database, run the spelled
statements on it, and read the answer out of the database. Nothing here opens a
file, a server or a network connection, and the function under test is handed
neither.

NOTHING HERE READS THE PACKAGE'S SOURCE. A claim about how a module is written
is a claim the gates over `src/` make. These steps call the function and read
what comes back.

WHAT IS NOT SATISFIED, and is failing rather than faked. Each of these is a
disagreement between this feature and `fn spell_operation` as the declaration
writes it, so each needs a decision rather than more code.

THE REBUILD SCENARIOS, thirty of them, opening `Given a database connection`.
They ask for a SQLite table rebuild that preserves the rows, the indexes, the
unique constraints, the composite primary key and the foreign keys pointing in
and out. An `Operation` carries a kind, a table name, `detail: dict<str, Value>`
and a flag. It does not carry the table it rewrites, and a value is a scalar, so
a column list cannot travel in one. A function handed none of that cannot
preserve any of it. Their steps are undefined, so each scenario says which
sentence it died on rather than reporting a green rebuild that preserved
nothing.

THE TWO QUOTE-DOUBLING ROWS. They ask for the enum value to reach the statement
TEXT with its quote doubled. An enum value is bound and travels beside the
statement, which is what `Statement.parameters` is for, so the text carries a
placeholder and the value never reaches it. Doubling a quote is what you do
when you have pasted, and this does not paste.

TAKING A VALUE OUT OF THE LOOKUP. `OperationKind` has `insert_enum_values` and
no word for removing one, so there is no operation to spell. Two scenarios ask
for it.

THE RANDOM TAIL ON A TEMPORARY NAME. Two scenarios want two names for one table
to differ, which needs a random source, and `fn spell_operation` is pure. The
rebuild drops a leftover temporary table before it makes one instead, so a
retry after a crash needs no tail to tell it apart.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.spell_operation import (
    TEMPORARY_PREFIX,
    column_definition,
    constraint_clause,
    default_clause,
    listing_the_user_tables,
    placeholder,
    same_type,
    spell_operation,
    stamping_the_schema_hash,
    temporary_name,
    type_spelled,
)
from declaro_persistum.types import Column, Engine, Operation, Statement, Value

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/schema/spell_operation.feature")

A_FAULT = {"kind", "what_went_wrong", "what_to_do_instead"}
THE_ENGINES = ("sqlite", "postgresql", "turso")

# The lookup table a declared `Literal` becomes, for the table and column the
# enum scenarios use.
THE_STATUSES = "_dp_enum_orders_status"


# --- the pieces a scenario hands over ---------------------------------------


def _column(name: str, declared_type: str, **facets: Value) -> Column:
    """One declared column, with every field a `Column` carries."""
    return {
        "name": name,
        "declared_type": declared_type,
        "renamed_from": None,
        "references": None,
        "facets": dict(facets),
    }


def _operation(kind: str, table: str, detail: dict[str, Value]) -> Operation:
    """One schema change, as the differ would hand it over."""
    return {"kind": kind, "table": table, "detail": detail, "destroys": False}  # type: ignore[typeddict-item]


def _values(statement: Statement) -> tuple[Value, ...]:
    """The values beside a statement, in the order their marks appear."""
    return tuple(statement["parameters"].values())


def _run(connection: sqlite3.Connection, statements: list[Statement]) -> None:
    """Run spelled statements on a real database, in the order they came back."""
    for statement in statements:
        connection.execute(statement["text"], _values(statement))


def _spell_and_run(
    connection: sqlite3.Connection, operation: Operation, engine: Engine
) -> list[Statement]:
    """Spell one change and carry it out, so a broken statement fails here."""
    spelled = spell_operation(operation, engine)
    assert isinstance(spelled, list), f"the change could not be spelled: {spelled}"
    _run(connection, spelled)
    return spelled


def _a_database() -> sqlite3.Connection:
    """An in-memory SQLite database with its foreign keys enforced.

    SQLite leaves foreign keys off unless they are asked for, and a lookup
    table nothing enforces accepts every value, which is the shape that passes
    a test while refusing nothing.
    """
    connection = sqlite3.connect(":memory:")
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


# --- what a scenario establishes --------------------------------------------


@given(
    parsers.re(
        r'a table "(?P<table>[^"]+)" whose status is one of "(?P<one>[^"]+)" '
        r'and "(?P<other>[^"]+)"$'
    )
)
def _a_table_whose_status_is_one_of(scenario: dict, table: str, one: str, other: str) -> None:
    """The lookup table, the table pointing at it, and the two values in it.

    Every statement here was spelled by the function under test, so a lookup
    the values never reached shows up as a scenario that cannot be set up.
    """
    connection = _a_database()
    scenario["connection"] = connection
    scenario["table"] = table

    statuses = _column("value", "text", primary_key=True)
    status = _column("status", "text")
    status["references"] = f"{THE_STATUSES}.value"

    _spell_and_run(
        connection,
        _operation(
            "create_table",
            THE_STATUSES,
            {"definition": column_definition(statuses, "sqlite")},
        ),
        "sqlite",
    )
    _spell_and_run(
        connection,
        _operation(
            "create_table",
            table,
            {
                "definition": ", ".join(
                    [
                        column_definition(_column("id", "integer", primary_key=True), "sqlite"),
                        column_definition(status, "sqlite"),
                    ]
                )
            },
        ),
        "sqlite",
    )
    for value in (one, other):
        _spell_and_run(
            connection,
            _operation("insert_enum_values", table, {"column": "status", "value": value}),
            "sqlite",
        )


@given(parsers.re(r'an order whose status is "(?P<value>[^"]+)"$'))
def _an_order_whose_status_is(scenario: dict, value: str) -> None:
    connection: sqlite3.Connection = scenario["connection"]
    connection.execute(
        f'INSERT INTO "{scenario["table"]}" ("id", "status") VALUES (?, ?)', (1, value)
    )


@given("a stamp table on a real database")
def _a_stamp_table(scenario: dict) -> None:
    connection = _a_database()
    scenario["connection"] = connection
    _spell_and_run(
        connection,
        _operation(
            "create_table",
            "_declaro_meta",
            {
                "definition": ", ".join(
                    [
                        column_definition(_column("key", "text", primary_key=True), "sqlite"),
                        column_definition(_column("value", "text"), "sqlite"),
                    ]
                )
            },
        ),
        "sqlite",
    )


@given(parsers.re(r'a table "(?P<table>[^"]+)" that the user made$'))
def _a_table_the_user_made(scenario: dict, table: str) -> None:
    _spell_and_run(
        scenario["connection"],
        _operation(
            "create_table",
            table,
            {"definition": column_definition(_column("id", "integer", primary_key=True), "sqlite")},
        ),
        "sqlite",
    )
    scenario["table"] = table


# --- what a scenario does ----------------------------------------------------

# WHICH CHANGE EACH PHRASE STANDS FOR. Two scenarios differ by the change and by
# the engine, so the words are read and the change is a row.
_A_CHANGE: dict[str, Operation] = {
    "adding a column": _operation(
        "add_column",
        "invoices",
        {"column": "settled_on", "declared_type": "date", "nullable": True},
    ),
    "dropping a column": _operation(
        "drop_column",
        "invoices",
        {
            "column": "memo",
            "keeping": "id,settled_on",
            "definition": '"id" INTEGER NOT NULL, "settled_on" TEXT',
        },
    ),
}


@when(
    parsers.re(
        r"(?P<change>adding a column|dropping a column) is spelled "
        r"for (?P<engine>[a-z]+)$"
    )
)
def _a_change_is_spelled_for(scenario: dict, change: str, engine: str) -> None:
    scenario["operation"] = _A_CHANGE[change]
    scenario["engine"] = engine
    scenario["answer"] = spell_operation(_A_CHANGE[change], engine)


@when("an operation no engine table covers is spelled")
def _an_operation_no_table_covers(scenario: dict) -> None:
    """A kind the declaration's vocabulary does not carry.

    `bulldoze_table` is not in `OperationKind`, so no engine has a row for it
    and no engine ever will until somebody declares it.
    """
    scenario["operation"] = _operation("bulldoze_table", "invoices", {})
    scenario["engine"] = "sqlite"
    scenario["answer"] = spell_operation(scenario["operation"], "sqlite")


# The word a scenario writes for a speller -> the engine it is spelling for.
_WHOSE_SPELLING: dict[str, Engine] = {
    "sqlite_type": "sqlite",
    "postgresql_type": "postgresql",
    "sqlite_default": "sqlite",
    "postgresql_default": "postgresql",
}


@when(parsers.re(r'(?P<speller>sqlite_type|postgresql_type) is given "(?P<declared>[^"]*)"$'))
def _a_type_is_spelled(scenario: dict, speller: str, declared: str) -> None:
    scenario["answer"] = type_spelled(declared, _WHOSE_SPELLING[speller])


@when(
    parsers.re(
        r"(?P<speller>sqlite_default|postgresql_default) is given "
        r'(?:the number (?P<number>-?\d+)|"(?P<text>[^"]*)")$'
    )
)
def _a_default_is_spelled(
    scenario: dict, speller: str, number: str | None, text: str | None
) -> None:
    """One step for both spellers, over the two ways a scenario writes a value."""
    written: dict[bool, Callable[[], Value]] = {
        True: lambda: int(number or "0"),
        False: lambda: text,
    }
    scenario["answer"] = default_clause(written[number is not None](), _WHOSE_SPELLING[speller])


@when(
    parsers.re(
        r'same_type compares "(?P<declared>[^"]*)" and "(?P<live>[^"]*)" '
        r'on "(?P<engine>[^"]*)"$'
    )
)
def _two_spellings_are_compared(scenario: dict, declared: str, live: str, engine: str) -> None:
    scenario["answer"] = same_type(declared, live, engine)


@when(
    parsers.re(
        r'column_definition is given a text column "(?P<name>[^"]+)" '
        r'for "(?P<engine>[^"]*)"$'
    )
)
def _a_column_is_defined(scenario: dict, name: str, engine: str) -> None:
    scenario["answer"] = column_definition(_column(name, "text"), engine)


@when(
    parsers.re(
        r'constraint_clause is given a check "(?P<name>[^"]+)" on '
        r'"(?P<column>\w+) > (?P<value>-?\d+)"$'
    )
)
def _a_constraint_is_written(scenario: dict, name: str, column: str, value: str) -> None:
    scenario["name"] = name
    scenario["answer"] = constraint_clause(
        {
            "name": name,
            "columns": [column],
            "rule": {
                "each": {column: {"operator": "gt", "value": int(value), "ignore_case": False}},
                "any_of": [],
                "none_of": [],
            },
        },
        "sqlite",
    )


@when(parsers.re(r"a boolean column is spelled for (?P<engine>[a-z]+)$"))
def _a_boolean_column_is_spelled(scenario: dict, engine: str) -> None:
    scenario["engine"] = engine
    scenario["answer"] = spell_operation(
        _operation(
            "add_column",
            "invoices",
            {"column": "settled", "declared_type": "boolean", "nullable": False},
        ),
        engine,
    )


@when("a column is spelled for an engine this package does not ship")
def _a_column_is_spelled_for_an_engine_we_do_not_ship(scenario: dict) -> None:
    scenario["answer"] = column_definition(_column("settled_on", "date"), "mysql")


_A_LOOKUP_CHANGE: dict[str, str] = {
    "added to": "insert_enum_values",
    "taken out of": "remove_enum_values",
}


@when(parsers.re(r'"(?P<value>[^"]*)" is (?P<change>added to|taken out of) the statuses$'))
def _a_status_is_changed(scenario: dict, value: str, change: str) -> None:
    """Add or remove one value, and run it when the scenario has a database.

    `remove_enum_values` is not a word `OperationKind` carries. The step asks
    for it by the name the change would have, so the fault names the gap rather
    than this file inventing a spelling nobody declared.
    """
    scenario["value"] = value
    operation = _operation(
        _A_LOOKUP_CHANGE[change],
        scenario.get("table", "orders"),
        {"column": "status", "value": value},
    )
    scenario["answer"] = spell_operation(operation, "sqlite")
    ran: dict[bool, Callable[[], None]] = {
        True: lambda: _run(scenario["connection"], scenario["answer"]),
        False: lambda: None,
    }
    ran["connection" in scenario and isinstance(scenario["answer"], list)]()


_A_MIGRATION_STATEMENT: dict[str, Callable[[Engine], Any]] = {
    "stamping the schema's hash": lambda engine: stamping_the_schema_hash(
        engine, "declaro_todo", "9f2c"
    ),
    "listing the user's tables": listing_the_user_tables,
}


@when(
    parsers.re(
        r"(?P<what>stamping the schema's hash|listing the user's tables) is spelled for "
        r"(?:(?P<engine>[a-z]+)|an engine this package does not ship)$"
    )
)
def _a_migration_statement_is_spelled(scenario: dict, what: str, engine: str | None) -> None:
    scenario["answer"] = _A_MIGRATION_STATEMENT[what](engine or "mysql")


@when(parsers.re(r'"_placeholder" is asked for "(?P<engine>[^"]*)"$'))
def _the_placeholder_is_asked_for(scenario: dict, engine: str) -> None:
    scenario["answer"] = placeholder(engine)


_HOW_MANY_NAMES: dict[str, int] = {"a": 1, "two": 2}


@when(parsers.re(r'(?P<count>a|two) temporary names? (?:is|are) made for "(?P<table>[^"]+)"$'))
def _temporary_names_are_made(scenario: dict, count: str, table: str) -> None:
    scenario["table"] = table
    scenario["names"] = [temporary_name(table) for _ in range(_HOW_MANY_NAMES[count])]
    scenario["answer"] = scenario["names"][0]


@when("the same key is stamped twice with different values")
def _the_same_key_is_stamped_twice(scenario: dict) -> None:
    connection: sqlite3.Connection = scenario["connection"]
    for value in ("9f2c", "a10d"):
        statement = stamping_the_schema_hash("sqlite", "declaro_todo", value)
        assert isinstance(statement, dict) and "text" in statement, statement
        _run(connection, [statement])
    scenario["value"] = "a10d"


@when("the user-tables query is run")
def _the_user_tables_query_is_run(scenario: dict) -> None:
    statement = listing_the_user_tables("sqlite")
    assert isinstance(statement, dict) and "text" in statement, statement
    scenario["rows"] = [
        row[0]
        for row in scenario["connection"].execute(statement["text"], _values(statement)).fetchall()
    ]


# --- what has to be true -----------------------------------------------------


@then("one statement comes back")
def _one_statement_comes_back(scenario: dict) -> None:
    answer = scenario["answer"]
    assert isinstance(answer, list), f"a spelled change comes back as a list: {answer}"
    assert len(answer) == 1, (
        f"adding a column is one ALTER TABLE on {scenario['engine']}, and "
        f"{len(answer)} statements came back: {answer}"
    )


@then("several statements come back, and they rebuild the table")
def _several_statements_rebuild_the_table(scenario: dict) -> None:
    """The four moves of a rebuild, in the order they have to run.

    SQLite cannot drop a column out of a table that carries constraints, so the
    change is a new table, the rows copied into it, the old one gone and the new
    one renamed into its place.
    """
    answer = scenario["answer"]
    assert isinstance(answer, list), f"a spelled change comes back as a list: {answer}"
    table = scenario["operation"]["table"]
    temporary = temporary_name(table)
    text = [statement["text"] for statement in answer]

    moves = [
        f'CREATE TABLE "{temporary}"',
        f'INSERT INTO "{temporary}"',
        f'DROP TABLE "{table}"',
        f'ALTER TABLE "{temporary}" RENAME TO "{table}"',
    ]
    found = [
        next((position for position, line in enumerate(text) if move in line), -1) for move in moves
    ]

    assert -1 not in found, (
        "a rebuild makes a new table, copies the rows into it, drops the old "
        f"one and renames the new one; these moves are missing: "
        f"{[move for move, at in zip(moves, found, strict=True) if at == -1]}\n{text}"
    )
    assert found == sorted(found), (
        f"the rebuild's statements came back in an order that cannot run: {text}"
    )


@then("a fault comes back naming the operation and the engine")
def _a_fault_names_the_operation_and_the_engine(scenario: dict) -> None:
    answer = scenario["answer"]
    assert set(answer) == A_FAULT, f"a refusal is a Fault, returned and never raised: {answer}"
    said = answer["what_went_wrong"] + " " + answer["what_to_do_instead"]
    missing = [
        word for word in (scenario["operation"]["kind"], scenario["engine"]) if word not in said
    ]
    assert missing == [], (
        f"the fault leaves the caller to work out what was refused: {missing} missing from {said!r}"
    )


@then(parsers.re(r'it (?:answers|gives back) "(?P<expected>[^"]*)"$'))
def _it_answers(scenario: dict, expected: str) -> None:
    """Every scenario that reads one quoted answer, whichever verb it uses."""
    assert scenario["answer"] == expected, (
        f"it answered {scenario['answer']!r} where the scenario asks for {expected!r}"
    )


@then("the statement writes it as that engine's own boolean")
def _the_statement_writes_that_engines_boolean(scenario: dict) -> None:
    answer = scenario["answer"]
    assert isinstance(answer, list), f"a spelled change comes back as a list: {answer}"
    text = answer[0]["text"]
    assert "boolean" in text, (
        f"{scenario['engine']} has a boolean of its own and the statement does not name it: {text}"
    )

    # AND IT IS THIS ENGINE'S. SQLite has no boolean and keeps one in an
    # INTEGER, so a spelling that reached both engines would be wrong on one.
    elsewhere = spell_operation(
        _operation(
            "add_column",
            "invoices",
            {"column": "settled", "declared_type": "boolean", "nullable": False},
        ),
        "sqlite",
    )
    assert isinstance(elsewhere, list) and elsewhere[0]["text"] != text, (
        f"sqlite and {scenario['engine']} got the same spelling: {text}"
    )


@then("it is refused, naming the engines it has")
@then("it refuses and names the engines it has")
def _it_is_refused_naming_the_engines(scenario: dict) -> None:
    answer = scenario["answer"]
    assert set(answer) == A_FAULT, f"a refusal is a Fault, returned and never raised: {answer}"
    said = answer["what_went_wrong"] + " " + answer["what_to_do_instead"]
    unnamed = [engine for engine in THE_ENGINES if engine not in said]
    assert unnamed == [], (
        f"the refusal does not say which engines exist, so a caller who wrote "
        f"mysql has to go and find out: {unnamed} missing from {said!r}"
    )


@then("it answers that they are the same")
def _it_answers_that_they_are_the_same(scenario: dict) -> None:
    assert scenario["answer"] is True, (
        f"a declared uuid is a TEXT column on SQLite, and it answered {scenario['answer']!r}"
    )


@then('it answers "name" TEXT')
def _it_answers_a_quoted_name_and_a_type(scenario: dict) -> None:
    assert scenario["answer"] == '"name" TEXT', (
        f"a column is its quoted name and the type this engine stores it in, "
        f"and it answered {scenario['answer']!r}"
    )


@then(parsers.re(r'it answers a CHECK clause naming "(?P<name>[^"]+)"$'))
def _it_answers_a_check_clause(scenario: dict, name: str) -> None:
    answer = scenario["answer"]
    assert isinstance(answer, str), f"a clause is text a CREATE TABLE carries: {answer}"
    assert answer.startswith(f'CONSTRAINT "{name}" CHECK ('), (
        f"a CREATE TABLE carries a constraint by name, and this reads {answer!r}"
    )


@then(parsers.re(r'a row with status "(?P<value>[^"]+)" is (?P<verdict>accepted|refused)$'))
def _a_row_with_a_status(scenario: dict, value: str, verdict: str) -> None:
    """Insert one row and let the database say.

    The lookup is a table with a foreign key pointing at it, so a value nobody
    put in the lookup is refused by the engine rather than by this library.
    """
    connection: sqlite3.Connection = scenario["connection"]
    inserting = f'INSERT INTO "{scenario["table"]}" ("id", "status") VALUES (?, ?)'
    refused: list[str] = []
    try:
        connection.execute(inserting, (7, value))
    except sqlite3.IntegrityError as complaint:
        refused.append(str(complaint))

    said = {"accepted": [], "refused": refused or ["nothing"]}[verdict]
    # What the lookup change came back as, when the scenario made one. A change
    # nobody could spell is the reason a row is still accepted.
    spelled = [scenario[key] for key in filter(scenario.__contains__, ["answer"])]
    assert refused == said, (
        f"the engine was asked to store {value!r} and answered {refused or 'by storing it'}, "
        f"where the scenario says the row is {verdict}. The change was spelled "
        f"as: {spelled}"
    )


@then("the engine refuses, because a row still points at it")
def _the_engine_refuses_because_a_row_points_at_it(scenario: dict) -> None:
    answer = scenario["answer"]
    assert isinstance(answer, list), (
        f"taking a value out of the lookup could not be spelled: {answer}"
    )
    connection: sqlite3.Connection = scenario["connection"]
    refused: list[str] = []
    try:
        _run(connection, answer)
    except sqlite3.IntegrityError as complaint:
        refused.append(str(complaint))

    assert refused != [], (
        "a row still carries the value, so the engine has to refuse the delete; "
        "it went through, which means nothing was enforcing the lookup"
    )


@then("the statement carries the value with its quote doubled")
def _the_statement_carries_the_value_with_its_quote_doubled(scenario: dict) -> None:
    answer = scenario["answer"]
    assert isinstance(answer, list), f"the change could not be spelled: {answer}"
    doubled = scenario["value"].replace("'", "''")
    carrying = [statement["text"] for statement in answer if doubled in statement["text"]]
    assert carrying != [], (
        f"no statement carries {doubled!r}: {[statement['text'] for statement in answer]}"
    )


@then("the second value is what is stored")
def _the_second_value_is_what_is_stored(scenario: dict) -> None:
    stored = (
        scenario["connection"]
        .execute('SELECT "value" FROM "_declaro_meta" WHERE "key" = ?', ("declaro_todo",))
        .fetchall()
    )
    assert [row[0] for row in stored] == [scenario["value"]], (
        f"stamping the same key twice left {stored}, and the second stamp is the one that is true"
    )


@then("there is one row, not two")
def _there_is_one_row_not_two(scenario: dict) -> None:
    ((count,),) = scenario["connection"].execute('SELECT count(*) FROM "_declaro_meta"').fetchall()
    assert count == 1, f"the stamp is written over itself, and there are {count} rows"


@then(parsers.re(r'it names "(?P<table>[^"]+)" and not the stamp table$'))
def _it_names_the_users_table_and_not_ours(scenario: dict, table: str) -> None:
    assert scenario["rows"] == [table], (
        f"the query answers with the tables a user made, and it answered {scenario['rows']}"
    )


@then("a statement comes back")
def _a_statement_comes_back(scenario: dict) -> None:
    answer = scenario["answer"]
    assert set(answer) == {"text", "parameters"}, f"a Statement carries two things: {answer}"
    assert answer["text"].strip() != "", "a statement with no text runs nowhere"


@then("it begins with the temporary prefix")
def _it_begins_with_the_temporary_prefix(scenario: dict) -> None:
    assert scenario["answer"].startswith(TEMPORARY_PREFIX), (
        f"orphan recovery looks for {TEMPORARY_PREFIX!r} and the name is {scenario['answer']!r}"
    )


@then("it carries the table's own name")
def _it_carries_the_tables_own_name(scenario: dict) -> None:
    assert scenario["table"] in scenario["answer"], (
        f"a leftover table nobody can trace back to {scenario['table']!r} is a "
        f"table nobody dares drop: {scenario['answer']!r}"
    )


@then("they are not the same name")
def _they_are_not_the_same_name(scenario: dict) -> None:
    first, second = scenario["names"]
    assert first != second, (
        "two temporary names for one table are the same, because a pure "
        "function reads no random source. The rebuild drops a leftover "
        "temporary table before it makes one, which is what a tail was for"
    )


@then("the tail is exactly as wide as the constant says")
def _the_tail_is_as_wide_as_the_constant_says(scenario: dict) -> None:
    tail = scenario["answer"].removeprefix(TEMPORARY_PREFIX).removeprefix(scenario["table"])
    assert tail != "", (
        "the name carries no tail. A random tail needs a random source, and "
        "`fn spell_operation` is pure, so the rebuild drops a leftover "
        "temporary table instead of telling two of them apart"
    )
