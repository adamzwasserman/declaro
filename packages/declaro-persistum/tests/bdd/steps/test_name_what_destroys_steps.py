"""Bind `tests/bdd/features/core/name_what_destroys.feature`, and nothing else.

WHAT THE FUNCTION ANSWERS. `fn name_what_destroys` is handed a list of
operations and hands back a plan in which every operation carries an answer
about itself, and the plan lists the names of what the destructive ones take.
Three scenarios ask exactly that: a drop or a type change is marked, and an
addition is not.

THREE SCENARIOS LEFT THIS FILE ON 2026-09-22. They were folded in from
`schema-declared/losing_data_is_said_out_loud.feature` the day before and marked
`xfail`, which kept three lines in a file that could never satisfy them.

Two were about refusing a migration and about one declared loss not carrying the
next. The declaration gives both to `fn refuse_undeclared_loss`, which is handed
the plan this builds together with what the models declared, and
`features/schema/refuse_undeclared_loss.feature` already states both. Marking has
no fault arm, so no value this function returns could have made either true.

The third asked how many rows a drop would lose. Counting rows means reading the
table, no function the map names carries a count into a plan, and this one is
pure and sees no connection. It was asking for a capability nothing has.

ONE `When` OVER A TABLE. Five scenarios say the same sentence with a different
operation in it, so there is one `When` keyed by that operation. A phrase
nobody put in the table fails by `KeyError` naming itself, rather than passing
on operations some other scenario left behind.

NOTHING HERE READS THE MODULE'S OWN TEXT, and nothing here opens a file, a clock
or a connection. Each scenario states what comes back.
"""

from __future__ import annotations

from typing import Any, cast, get_args

import pytest
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.name_what_destroys import name_what_destroys
from declaro_persistum.types import Operation, OperationKind
from tests.conftest import an_operation

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


def _one(kind: str, table: str, column: str | None) -> Operation:
    """One operation, built where every step file builds one."""
    return cast(
        Operation,
        an_operation(
            kind=kind,
            table=table,
            column=column,
            columns=[],
            indexes=[],
            constraints=[],
            copying=[],
            values=[],
            renamed_to=None,
            view=None,
            destroys=False,
        ),
    )


WHAT_THE_LIBRARY_WORKED_OUT: dict[str, list[Operation]] = {
    "drop a column": [_one("drop_column", "users", "nickname")],
    "drop a table": [_one("drop_table", "legacy", None)],
    "narrow a column's type": [_one("alter_column", "users", "age")],
    "drop a table other rows point at": [_one("drop_table", "parents", None)],
    "add a column": [_one("add_column", "users", "email")],
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
    operations: list[Operation] = [_one(kind, "t", "c") for kind in kinds]

    answered = name_what_destroys(operations)["operations"]

    assert len(answered) == len(kinds)
    assert [type(one["destroys"]) for one in answered] == [bool] * len(kinds)
    assert {one["kind"] for one in answered if one["destroys"]} == {
        "drop_table",
        "drop_column",
        "alter_column",
        # A choice taken out of an enum is data lost: every row still holding
        # that value stops being one the column accepts.
        "remove_enum_values",
    }
