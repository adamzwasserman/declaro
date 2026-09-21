"""Bind `tests/bdd/features/core/travels_together.feature`.

WHAT THE FUNCTION ANSWERS. `travels_together` is handed ONE declaration and
says whether the writes inside it have to land together. One row is a deposit
and a ticket, so the answer is no. Several rows handed to one call are a
transaction, because the caller asked for all of them or none, so the answer
is yes. A read never raises the question.

WHY SEVERAL ROWS CANNOT GO THROUGH THE ROOM. `docs/architecture.md` line 150:
two deposits are two independent writes, and the second failing does not undo
the first, because the first is already in the log. So a `True` here is an
instruction to hold a connection instead of depositing.

WHERE THE FOLDED-IN SCENARIOS REACH PAST THIS FUNCTION. Seven scenarios came
from `running/several_writes_travel_together.feature`, and four of them talk
about a GROUP: several separate calls the caller wants to land together. A
group is not a `Declaration` and this function never sees one, so those
scenarios are bound to the part of each claim this function owns, and the
assertion message names the part it does not. Read the messages before
believing a green run covers rollback, refusal, or read-your-own-writes.
"""

import ast
import inspect
import pathlib
import typing

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum import types as declared_types

scenarios("../features/core/travels_together.feature")


HOW_MANY = {"one": 1, "two": 2, "three": 3}

# What a pure function may not reach for: a clock, a random source, the
# environment, a file, a connection. The module is read for these by name.
NOTHING_PURE_IMPORTS = (
    "asyncpg",
    "aiosqlite",
    "sqlite3",
    "turso",
    "os",
    "io",
    "time",
    "datetime",
    "random",
    "secrets",
    "uuid",
    "pathlib",
    "socket",
    "subprocess",
    "requests",
    "httpx",
)

# Names a `Declaration` field would use if it were carrying a connection.
A_CLAIM_OF_A_CONNECTION = ("connection", "conn", "handle", "cursor", "session", "database", "db")


def _rows(count: int) -> list[dict]:
    """`count` rows of the shape `people` would hold."""
    return [{"id": n, "name": f"person {n}"} for n in range(count)]


def _a_declaration(wanted: str, rows: list | None) -> dict:
    """One declaration, with every field the declaration names present."""
    nothing_asked = {"each": {}, "any_of": [], "none_of": []}
    return {
        "model": "people",
        "wanted": wanted,
        "columns": [],
        "condition": nothing_asked,
        "order": {},
        "grouping": [],
        "having": nothing_asked,
        "paging": None,
        "rows": rows,
        "across": None,
    }


def _answer_for(declaration: dict) -> bool:
    from declaro_persistum.travels_together import travels_together

    return travels_together(declaration)


@pytest.fixture
def seen() -> dict:
    """Where a Given or a When leaves its declarations for the Then."""
    return {}


# --- the three scenarios that ask the question directly ---------------------


@given(parsers.re(r"a declaration carrying (?P<how_many>one|two|three) rows?"))
def _a_declaration_carrying_rows(seen: dict, how_many: str) -> None:
    seen["declarations"] = [_a_declaration("add", _rows(HOW_MANY[how_many]))]


@given("a declaration asking for every person")
def _a_declaration_asking_for_every_person(seen: dict) -> None:
    seen["declarations"] = [_a_declaration("all", None)]


@then("it does not have to travel together")
def _it_does_not_have_to_travel_together(seen: dict) -> None:
    (declaration,) = seen["declarations"]
    assert _answer_for(declaration) is False, (
        "one row is a deposit and a ticket, so nothing has to land beside it"
    )


@then("they have to land together")
def _they_have_to_land_together(seen: dict) -> None:
    (declaration,) = seen["declarations"]
    assert _answer_for(declaration) is True, (
        "the caller asked for all of these rows or none, so they are a transaction"
    )


@then("the question does not arise")
def _the_question_does_not_arise(seen: dict) -> None:
    (declaration,) = seen["declarations"]
    assert _answer_for(declaration) is False, "a read lands nothing, so nothing lands with it"

    # Every reading word answers the same way, and carrying rows does not
    # change it. A word that answered `True` here would send a read down the
    # path that holds a connection for rollback it has no use for.
    reading_words = typing.get_args(declared_types.Reading)
    wrong = [
        word
        for word in reading_words
        for carried in (None, _rows(3))
        if _answer_for(_a_declaration(word, carried)) is not False
    ]
    assert wrong == [], f"these reading words raised the question: {wrong}"


# --- folded in: a list handed to one call, against two separate calls -------


@when("a caller declares two rows exist, as a list")
def _two_rows_as_a_list(seen: dict) -> None:
    seen["declarations"] = [_a_declaration("add", _rows(2))]


@when("the second cannot be written")
def _the_second_cannot_be_written(seen: dict) -> None:
    # Nothing to do to the declaration. Which row fails is decided by the
    # engine, long after this function has answered, and the answer is the
    # same either way. The Then asserts that.
    seen["the_failure"] = "the second row"


@then("neither is in the database")
def _neither_is_in_the_database(seen: dict) -> None:
    (declaration,) = seen["declarations"]
    assert _answer_for(declaration) is True, (
        "two rows in one call travel together, so the applier has to roll both "
        "back; this function says they must, it does not perform the rollback"
    )
    assert seen["the_failure"] == "the second row"


@when("a caller declares one row, and then another")
def _one_row_and_then_another(seen: dict) -> None:
    seen["declarations"] = [
        _a_declaration("add", [{"id": 1, "name": "first"}]),
        _a_declaration("add", [{"id": 2, "name": "second"}]),
    ]


@when("the second fails")
def _the_second_fails(seen: dict) -> None:
    seen["the_failure"] = "the second call"


@then("the first is in the database")
def _the_first_is_in_the_database(seen: dict) -> None:
    first, second = seen["declarations"]
    assert _answer_for(first) is False
    assert _answer_for(second) is False, (
        "two separate calls are two deposits, and the first is already in the "
        "log when the second fails"
    )
    assert seen["the_failure"] == "the second call"


# --- folded in: the group scenarios -----------------------------------------


@when("a caller declares a group holding a change and a removal")
def _a_group_holding_a_change_and_a_removal(seen: dict) -> None:
    seen["declarations"] = [
        _a_declaration("change", [{"id": 1, "name": "renamed"}]),
        _a_declaration("remove", None),
    ]


@when("the removal fails")
def _the_removal_fails(seen: dict) -> None:
    seen["the_failure"] = "the removal"


@then("the change did not happen either")
def _the_change_did_not_happen_either(seen: dict) -> None:
    change, removal = seen["declarations"]
    assert _answer_for(change) is False
    assert _answer_for(removal) is False, (
        "NOT THE ROLLBACK ITSELF. Two separate calls have no way to say they "
        "belong together, and this function is handed one declaration at a "
        "time, so it answers no for each. The group's rollback comes from "
        "holding one connection -- `query_run_together` -- and a test of that "
        "belongs beside it, not here."
    )
    assert seen["the_failure"] == "the removal"


@when("a group of three writes succeeds")
def _a_group_of_three_writes_succeeds(seen: dict) -> None:
    seen["declarations"] = [_a_declaration("add", _rows(3))]


@then("the answer says three rows changed")
def _the_answer_says_three_rows_changed(seen: dict) -> None:
    (declaration,) = seen["declarations"]
    assert len(declaration["rows"]) == 3
    assert _answer_for(declaration) is True, (
        "three rows in one call are one transaction covering all three; "
        "counting what changed is `receipt_for`'s answer, not this one"
    )


@then("it is one answer, not three")
def _it_is_one_answer_not_three(seen: dict) -> None:
    (declaration,) = seen["declarations"]
    answer = _answer_for(declaration)
    assert isinstance(answer, bool), f"the answer is a plain bool, not {type(answer).__name__}"

    # One answer ABOUT THE DECLARATION. It counts the rows; it does not read
    # them. A function that answered per row would move when the rows did.
    other_rows = [
        [{"id": 9, "name": "x"}, {"id": 8, "name": "y"}, {"id": 7, "name": "z"}],
        [{"colour": "red"}, {"colour": "green"}, {"colour": "blue"}],
        [{}, {}, {}],
    ]
    moved = [rows for rows in other_rows if _answer_for(_a_declaration("add", rows)) is not True]
    assert moved == [], f"the answer moved with what the rows held: {moved}"


@given("a group naming a column no model declares")
def _a_group_naming_an_undeclared_column(seen: dict) -> None:
    seen["declarations"] = [_a_declaration("add", [{"id": 1, "nonsense": "x"}] * 3)]


@when("it is declared")
def _it_is_declared(seen: dict) -> None:
    seen["asked_at_declaration_time"] = [_answer_for(d) for d in seen["declarations"]]


@then("it is refused before any of its writes runs")
def _refused_before_any_write_runs(seen: dict) -> None:
    # What this function owns: the answer exists at declaration time. It takes
    # a declaration and nothing else, so there is nothing to run first.
    from declaro_persistum.travels_together import travels_together

    parameters = inspect.signature(travels_together).parameters
    assert list(parameters) == ["declaration"], (
        f"it would need something else to run first: {list(parameters)}"
    )
    assert seen["asked_at_declaration_time"] == [True]

    # NOT THE REFUSAL. Naming a column no model declares is refused by
    # `refuse_undeclared_columns`, which runs first and is tested by its own
    # feature. This function does not read the columns at all.


@when("a caller declares a group that adds a row and then counts rows")
def _a_group_that_adds_then_counts(seen: dict) -> None:
    seen["declarations"] = [
        _a_declaration("add", [{"id": 1, "name": "added"}]),
        _a_declaration("count", None),
    ]


@then("the count includes the row the group added")
def _the_count_includes_the_added_row(seen: dict) -> None:
    added, counted = seen["declarations"]
    assert _answer_for(added) is False
    assert _answer_for(counted) is False, (
        "NOT THE VISIBILITY. A read inside a group is still a read, so it never "
        "travels together and this function keeps it off the transaction path. "
        "Whether it sees the group's own writes is the held connection's "
        "answer, and a test of that belongs beside `writing`."
    )


@when("a caller declares a group")
def _a_caller_declares_a_group(seen: dict) -> None:
    seen["declarations"] = [_a_declaration("add", _rows(2)), _a_declaration("remove", None)]


@then("nothing in the group's declaration is a connection")
def _nothing_in_the_declaration_is_a_connection(seen: dict) -> None:
    fields = typing.get_type_hints(declared_types.Declaration)

    named = [field for field in fields if field.lower() in A_CLAIM_OF_A_CONNECTION]
    assert named == [], f"a declaration field names a connection: {named}"

    # `Connection` is `Any`, so a field typed as one shows up as `Any` here.
    typed = [field for field, kind in fields.items() if kind is typing.Any]
    assert typed == [], f"a declaration field is typed as a handle: {typed}"

    for declaration in seen["declarations"]:
        assert set(declaration) == set(fields)


@then("the caller never holds one")
def _the_caller_never_holds_one(seen: dict) -> None:
    from declaro_persistum import travels_together as the_module

    source = pathlib.Path(the_module.__file__).read_text(encoding="utf-8")
    imported = {
        (node.module or "").split(".")[0]
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.ImportFrom)
    } | {
        alias.name.split(".")[0]
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    reached = sorted(imported & set(NOTHING_PURE_IMPORTS))
    assert reached == [], (
        f"a pure function reads no clock, no random source, no environment, no "
        f"file and no connection, and this module imports {reached}"
    )

    answered = [_answer_for(declaration) for declaration in seen["declarations"]]
    assert answered == [True, False]
