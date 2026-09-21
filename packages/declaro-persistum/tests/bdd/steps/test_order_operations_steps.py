"""Bind `tests/bdd/features/core/order_operations.feature`, and nothing else.

WHAT THE FUNCTION ANSWERS. `order_operations` is handed a list of schema
operations and hands back the same operations in an order their dependencies
allow, or a fault when no such order exists. `docs/architecture.md` says it in
one sentence: "a table lands before the foreign key pointing at it. That order
comes from the declared references, not from the order you happened to write
your models in, because that order is arbitrary and the dependency isn't."

WHY NO SCENARIO COMPARES AGAINST AN EXPECTED LIST. Four operations with one
dependency between them have several correct orders, so an expected list pins
one of them and fails a function that picked another correct one. Worse, it
passes a function that got the order right by accident of input order. Every
Then here reads positions out of the answer and asserts the dependency the
Given declared, which is the property the feature states.

EVERY GIVEN HANDS THE OPERATIONS OVER IN THE WRONG ORDER. A function that
returned its argument untouched would satisfy a weaker test, so the input is
always arranged so that returning it unchanged fails.

`docs/design/persistum.hd`, `fn order_operations`, declares this pure and
declares the refusal a returned `Fault` rather than a raise. The cycle scenario
reaches its assertions only because the call returned.

WHAT AN OPERATION SAYS ABOUT WHAT IT POINTS AT. `type Column` spells a foreign
key `table.column`, and it is the only place this package writes a relationship
down. An `Operation` carries the same spelling under `references` in its
`detail`, so these steps build one from the other rather than inventing a
second spelling.
"""

from __future__ import annotations

import re
from typing import get_args

from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum.order_operations import MUST_ALREADY_EXIST, order_operations
from declaro_persistum.types import Fault, Operation, OperationKind

scenarios("../features/core/order_operations.feature")


# --- the operations the scenarios talk about --------------------------------


def _an_operation(kind: OperationKind, table: str, points_at: str | None) -> Operation:
    """One operation, with every field the declaration gives an Operation.

    `points_at` is a table and column, the way a Column writes a foreign key.
    An operation that points at nothing carries no `references` at all, so
    absence is a key that is not there rather than a value to interpret.
    """
    detail: dict[str, object] = {"named": f"{kind} on {table}"}
    detail.update({key: points_at for key in ["references"] if points_at is not None})
    return {
        "kind": kind,
        "table": table,
        "detail": detail,  # type: ignore[typeddict-item]
        "destroys": kind.startswith("drop"),
    }


# A TABLE, THE INDEX ON IT, AND TWO OPERATIONS WITH NO SAY IN THE MATTER. The
# index is written first, so the answer cannot be the input.
A_TABLE_AND_ITS_INDEX: tuple[tuple[Operation, ...], tuple[tuple[int, int], ...]] = (
    (
        _an_operation("create_index", "cards", None),
        _an_operation("create_table", "boards", None),
        _an_operation("create_table", "cards", None),
        _an_operation("add_column", "cards", None),
    ),
    ((2, 0), (2, 3)),
)

# A CHILD POINTING AT A PARENT, THE CHILD WRITTEN FIRST.
A_CHILD_AND_ITS_PARENT: tuple[tuple[Operation, ...], tuple[tuple[int, int], ...]] = (
    (
        _an_operation("create_table", "cards", "boards.id"),
        _an_operation("create_table", "logs", None),
        _an_operation("create_table", "boards", None),
    ),
    ((2, 0),),
)

# THE SAME PAIR GOING AWAY. Dropping the parent while the child still points at
# it is refused by the database, so the child goes first. The parent is written
# first, so the answer cannot be the input.
A_PARENT_AND_ITS_CHILD_DROPPED: tuple[tuple[Operation, ...], tuple[tuple[int, int], ...]] = (
    (
        _an_operation("drop_table", "boards", None),
        _an_operation("drop_index", "cards", None),
        _an_operation("drop_table", "cards", "boards.id"),
    ),
    ((2, 0), (1, 2)),
)

# TWO TABLES EACH POINTING AT THE OTHER. There is no first one.
A_CIRCLE = (
    _an_operation("create_table", "cards", "boards.id"),
    _an_operation("create_table", "boards", "cards.id"),
)


# --- what a scenario established --------------------------------------------
#
# ONE STEP FUNCTION PER SHAPE, NOT ONE PER SENTENCE. The three orderable
# scenarios differ only in which operations were written down and which of them
# has to run before which, so the words are keys of this table and the regex
# below is built from them.

WHAT_WAS_WRITTEN_DOWN = {
    "a create for a table and a create for an index on it": A_TABLE_AND_ITS_INDEX,
    "two tables where one references the other": A_CHILD_AND_ITS_PARENT,
    "a drop for a table and a drop for the table pointing at it": A_PARENT_AND_ITS_CHILD_DROPPED,
}

_WRITTEN = "(?P<written>" + "|".join(re.escape(key) for key in WHAT_WAS_WRITTEN_DOWN) + ")"


@given(parsers.re(_WRITTEN))
def _what_was_written_down(scenario: dict[str, object], written: str) -> None:
    operations, must_run_before = WHAT_WAS_WRITTEN_DOWN[written]
    scenario["operations"] = list(operations)
    scenario["must_run_before"] = must_run_before


@given("a cycle between two tables")
def _a_cycle_between_two_tables(scenario: dict[str, object]) -> None:
    scenario["operations"] = list(A_CIRCLE)
    scenario["both"] = tuple(operation["table"] for operation in A_CIRCLE)


# --- the results ------------------------------------------------------------
#
# THE THREE ORDERABLE SCENARIOS END IN ONE SENTENCE EACH, ALL SAYING THE SAME
# THING ABOUT A DIFFERENT PAIR, so they are one Then over the pairs the Given
# recorded.

RAN_FIRST = (
    "the table runs first",
    "the referenced one runs first",
    "the pointing one runs first",
)

_RAN_FIRST = "(?P<said>" + "|".join(re.escape(sentence) for sentence in RAN_FIRST) + ")"


@then(parsers.re(_RAN_FIRST))
def _it_ran_first(scenario: dict[str, object], said: str) -> None:
    written: list[Operation] = scenario["operations"]  # type: ignore[assignment]
    must_run_before: tuple[tuple[int, int], ...] = scenario["must_run_before"]  # type: ignore[assignment]
    as_handed_over = list(written)

    answer = order_operations(written)

    assert isinstance(answer, list), f"{said}, and what came back is not a list: {answer}"

    # NOTHING WAS ADDED, LOST OR DUPLICATED. An ordering that drops an
    # operation would satisfy every position check below, because the
    # operation it dropped has no position to be wrong.
    assert len(answer) == len(written), (
        f"{len(written)} operations went in and {len(answer)} came back: {answer}"
    )
    assert sorted(map(id, answer)) == sorted(map(id, written)), (
        "the answer holds operations that are not the ones handed over"
    )

    # THE POSITIONS, NOT AN EXPECTED LIST. Several orders satisfy these
    # dependencies and each one is correct, so the assertion is the dependency
    # rather than one of the orders.
    place = {id(operation): position for position, operation in enumerate(answer)}
    for earlier, later in must_run_before:
        assert place[id(written[earlier])] < place[id(written[later])], (
            f"{written[earlier]['kind']} on {written[earlier]['table']} has to run "
            f"before {written[later]['kind']} on {written[later]['table']}, and the "
            f"order came back as "
            f"{[(operation['kind'], operation['table']) for operation in answer]}"
        )

    # It reads the operations and nothing else, so the same list answers the
    # same way every time and the list it was handed is left where it was.
    assert order_operations(written) == answer, "two calls on one list gave two orders"
    assert written == as_handed_over, f"ordering changed the list it was handed: {written}"

    # EVERY KIND HAS A ROW. A kind missing from the table is an operation whose
    # dependencies nobody wrote down, and the failure would be a migration that
    # runs in whatever order it was written in.
    assert frozenset(MUST_ALREADY_EXIST) == frozenset(get_args(OperationKind)), (
        "the table of what an operation needs does not cover every declared "
        f"kind: {sorted(frozenset(get_args(OperationKind)) - frozenset(MUST_ALREADY_EXIST))}"
    )


@then("a fault comes back naming both")
def _a_fault_naming_both(scenario: dict[str, object]) -> None:
    written: list[Operation] = scenario["operations"]  # type: ignore[assignment]
    both: tuple[str, ...] = scenario["both"]  # type: ignore[assignment]

    # THE FAILURE COMES BACK, IT IS NOT RAISED. A function that raised would
    # have failed this scenario on the line above the assertions.
    fault: list[Operation] | Fault = order_operations(written)

    assert isinstance(fault, dict), f"a circle has no order, and this came back: {fault}"
    assert set(fault) == {"kind", "what_went_wrong", "what_to_do_instead"}, (
        f"a Fault carries three things, and this is not one: {fault}"
    )
    assert fault["kind"] == "refused", (
        "no order exists, and running the same operations again will not make one"
    )

    said = fault["what_went_wrong"]
    assert [name for name in both if name in said] == list(both), (
        f"the fault has to name both tables in the circle, and it says: {said}"
    )
    assert fault["what_to_do_instead"].strip() != "", (
        "a fault that only says what went wrong leaves the caller nowhere to go"
    )

    # IT IS THE CIRCLE THAT IS REFUSED, NOT THE PAIR. Take one of the two
    # references away and the same two operations come back in an order.
    broken = [dict(operation) for operation in written]
    broken[1]["detail"] = {"named": broken[1]["detail"]["named"]}
    answer = order_operations(broken)  # type: ignore[arg-type]
    assert isinstance(answer, list), f"one reference is not a circle: {answer}"
    assert [operation["table"] for operation in answer] == [both[1], both[0]], (
        f"the table pointed at still has to be made first: {answer}"
    )
