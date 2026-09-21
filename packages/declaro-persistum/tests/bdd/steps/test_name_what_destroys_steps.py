"""Bind `tests/bdd/features/core/name_what_destroys.feature`, and nothing else.

WHAT THE FUNCTION ANSWERS. `fn name_what_destroys` is handed a list of
operations and hands back a plan in which every operation carries an answer
about itself, and the plan lists the names of what the destructive ones take.
Three scenarios ask exactly that: a drop or a type change is marked, and an
addition is not.

WHAT THREE SCENARIOS ASK THAT IT CANNOT ANSWER, AND WHY THEY ARE `xfail`.
Three more were folded in from `schema-declared/losing_data_is_said_out_loud.feature`
on 2026-09-21. Two of them are about refusing a migration and about one
declared loss not carrying the next, and the declaration gives both to
`fn refuse_undeclared_loss`, which is handed the plan this builds together with
what the models declared. Marking has no fault arm, so no value this function
returns could make either scenario true. The third asks how many rows the drop
would lose, and counting rows means reading the database; a function handed a
list of operations sees no connection and no row.

An `xfail` says the line is in the file, nothing satisfies it yet, and names
what must. A skip would say the line was not worth running, and a Then rewritten
until it passes would say the line was satisfied when it was not. Every step
after an `xfail` is written anyway, and written to fail, so that deleting the
`xfail` turns the scenario red rather than green.

ONE `When` OVER A TABLE. Five scenarios say the same sentence with a different
operation in it, so there is one `When` keyed by that operation. A phrase
nobody put in the table fails by `KeyError` naming itself, rather than passing
on operations some other scenario left behind.

NOTHING HERE READS THE MODULE'S OWN TEXT, and nothing here opens a file, a clock
or a connection. Each scenario states what comes back.
"""

from __future__ import annotations

from typing import Any, get_args

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.name_what_destroys import name_what_destroys
from declaro_persistum.types import Operation, OperationKind

pytestmark = [pytest.mark.bdd, pytest.mark.precommit]

scenarios("../features/core/name_what_destroys.feature")


# =============================================================================
# What the library worked out it would do, one row per phrase the feature uses
# =============================================================================
#
# A DROP THAT SOMETHING POINTS AT IS THE SAME ROW AS A DROP. The rows that point
# at "parents" are a foreign key's business, and which order the two drops run
# in belongs to another function. What this one answers is that the parent's own
# rows go, and they go whether or not anything refers to them.

WHAT_THE_LIBRARY_WORKED_OUT: dict[str, list[Operation]] = {
    "drop a column": [
        {
            "kind": "drop_column",
            "table": "users",
            "detail": {"column": "nickname"},
            "destroys": False,
        }
    ],
    "drop a table": [{"kind": "drop_table", "table": "legacy", "detail": {}, "destroys": False}],
    "narrow a column's type": [
        {
            "kind": "alter_column",
            "table": "users",
            "detail": {"column": "age"},
            "destroys": False,
        }
    ],
    "drop a table other rows point at": [
        {"kind": "drop_table", "table": "parents", "detail": {}, "destroys": False}
    ],
    "add a column": [
        {
            "kind": "add_column",
            "table": "users",
            "detail": {"column": "email"},
            "destroys": False,
        }
    ],
}

# What each operation is called once it is in the plan's list of losses.
NAMED_IN_THE_PLAN: dict[str, str] = {
    "drop a column": "users.nickname",
    "drop a table": "legacy",
    "narrow a column's type": "users.age",
    "drop a table other rows point at": "parents",
}


@when(parsers.parse("the library works out that it would {operation}"))
def _the_library_worked_out(scenario: dict[str, Any], operation: str) -> None:
    """One step for every When of this shape. The phrase is the key."""
    scenario["phrase"] = operation
    scenario["plan"] = name_what_destroys(WHAT_THE_LIBRARY_WORKED_OUT[operation])


@then("the answer says that operation loses data")
def _the_answer_says_it_loses_data(scenario: dict[str, Any]) -> None:
    """Both halves of the answer, because a reader downstream uses both.

    `refuse_undeclared_loss` walks the operations and reads the mark on each
    one. A person reading a migration log reads the plan's list. An operation
    marked but left out of the list, or listed but unmarked, would make the two
    readers disagree about the same migration.
    """
    (operation,) = scenario["plan"]["operations"]

    assert operation["destroys"] is True, operation
    assert scenario["plan"]["destroys"] == [NAMED_IN_THE_PLAN[scenario["phrase"]]], scenario["plan"]


@then("nothing is named as losing data")
def _nothing_is_named_as_losing_data(scenario: dict[str, Any]) -> None:
    """Empty, and never absent. A plan that leaves the list out makes every
    reader of it check for the field before reading it."""
    assert scenario["plan"]["destroys"] == [], scenario["plan"]


@then("the answer does not say it loses data")
def _the_answer_does_not_say_it_loses_data(scenario: dict[str, Any]) -> None:
    """The mark on the operation itself, which is the other half of the answer
    the step above reads at the plan level."""
    (operation,) = scenario["plan"]["operations"]

    assert operation["destroys"] is False, operation


# =============================================================================
# Every kind the vocabulary declares, handed in and answered
# =============================================================================


def test_every_kind_of_operation_gets_an_answer() -> None:
    """A kind added to the vocabulary with no row here is the silent failure.

    `OperationKind` names twelve kinds. A thirteenth arriving with no row in the
    table is an operation nothing marks, which means nothing checks it either,
    and the first evidence would be rows gone after a deploy. So the vocabulary
    is the list, and every member of it is handed in.
    """
    kinds = get_args(OperationKind)
    operations: list[Operation] = [
        {"kind": kind, "table": "t", "detail": {"column": "c"}, "destroys": False} for kind in kinds
    ]

    answered = name_what_destroys(operations)["operations"]

    assert len(answered) == len(kinds)
    assert [type(one["destroys"]) for one in answered] == [bool] * len(kinds)
    assert {one["kind"] for one in answered if one["destroys"]} == {
        "drop_table",
        "drop_column",
        "alter_column",
    }


# =============================================================================
# The three this function cannot be asked
# =============================================================================


@given("a model change that would drop a column")
def _a_model_change_that_would_drop_a_column() -> None:
    pytest.xfail(
        "name_what_destroys returns a plan and has no refusal in it. Refusing a "
        "loss no model asked for is `fn refuse_undeclared_loss`, which is handed "
        "this plan and what the models declared."
    )


@when("the library is asked to bring the database up to date")
def _the_library_is_asked_to_bring_the_database_up_to_date() -> None:
    raise AssertionError("CODE NOT WRITTEN: running a migration is not marking one")


@then("it refuses and says which operation loses what")
def _it_refuses_and_says_which_operation_loses_what() -> None:
    raise AssertionError("CODE NOT WRITTEN: nothing this function returns is a refusal")


@given("two changes, one of which drops a column")
def _two_changes_one_of_which_drops_a_column() -> None:
    pytest.xfail(
        "marking never reads a permission, so it cannot tell a drop a caller "
        "named from one nobody did. `fn refuse_undeclared_loss` reads what each "
        "model declared it means to lose, one operation at a time."
    )


@when("a caller allows the drop by naming it")
def _a_caller_allows_the_drop_by_naming_it() -> None:
    raise AssertionError("CODE NOT WRITTEN: a permission reaches no argument of this function")


@then("both are applied")
def _both_are_applied() -> None:
    raise AssertionError("CODE NOT WRITTEN: applying an operation needs an engine and a connection")


@then("allowing one drop does not allow the next")
def _allowing_one_drop_does_not_allow_the_next() -> None:
    raise AssertionError("CODE NOT WRITTEN: nothing here holds a permission to carry or not carry")


@given("a column with rows in it")
def _a_column_with_rows_in_it() -> None:
    pytest.xfail(
        "how many rows carry a value is counted by asking the database, and this "
        "function is handed a list of operations and no connection. The "
        "declaration gives the count to nothing, so the feature and the "
        "declaration disagree until one of them moves."
    )


@when("the library says the drop loses data")
def _the_library_says_the_drop_loses_data() -> None:
    raise AssertionError("CODE NOT WRITTEN: this When belongs to the counting nobody owns")


@then("it says how many rows carry a value in that column")
def _it_says_how_many_rows_carry_a_value() -> None:
    raise AssertionError("CODE NOT WRITTEN: a plan carries names, and no count reaches it")
