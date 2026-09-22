"""The order the dependencies allow, or a refusal saying why there is none.

WHAT GOES WRONG WITHOUT IT. `docs/architecture.md`: "a table lands before the
foreign key pointing at it. That order comes from the declared references, not
from the order you happened to write your models in, because that order is
arbitrary and the dependency isn't." The order a caller wrote their models in
is arbitrary, the database's rules are not, and the two disagreeing shows up as
a statement refused halfway through a migration, with the earlier statements
already committed and the database in a state neither schema describes.

ONE RULE, NOT A RULE PER KIND. Every operation names the tables that have to
exist while it runs. Creating a table makes one exist, dropping a table ends
one, and everything else needs the tables it names to be there already. So the
two edges are the same sentence read in both directions: whatever makes a table
runs before everything needing it, and whatever ends a table runs after
everything needing it. That covers a table before the index on it, a parent
before the child that points at it, and a child dropped before its parent,
without any of the three being written down separately.

WHERE AN OPERATION SAYS WHAT IT POINTS AT. `docs/design/persistum.hd`,
`type Column`, carries a `Reference`, which names the table, the column and what
a delete does to the child. An operation carries its columns, so the tables it
points at are read straight off them. A column pointing at nothing has no
reference, which is an absence with a name rather than a value to interpret.

Until 2026-09-21 the reference was the string `table.column` in the operation's
loose `detail` bag, and this function split it apart again to get the table out.
A shape being parsed is a shape that could have been carried.

A CIRCLE IS AN ANSWER, NOT A CRASH. `fn order_operations` returns a `Fault`,
so the refusal reaches the caller as a value it can read and print. The fault
names each table left over and the tables it was waiting on, because "no order
exists" tells a reader nothing they can act on and "cards comes after boards,
boards comes after cards" tells them which reference to break.

THE SAME LIST ALWAYS ANSWERS THE SAME WAY. Nothing here reads a clock, a file
or an environment, and no set is ever iterated: the operations still free to
run are taken in the order they were written, so two runs over one list give
one order and a migration can be read before it is run.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import Fault, Operation, OperationKind

# WHERE THE TABLES IT POINTS AT ARE READ FROM. Until 2026-09-21 this was a
# `references` key invented in the operation's loose `detail` bag, holding the
# string `table.column`, which had to be split apart again to get the table out.
# An operation now carries its real columns, and a column carries a `Reference`
# with the table already its own field, so nothing is parsed here.

# What makes a table exist, and what ends it.
MAKES_A_TABLE: OperationKind = "create_table"
ENDS_A_TABLE: OperationKind = "drop_table"


def _nothing_of_its_own(_operation: Operation) -> tuple[str, ...]:
    """Creating a table is what makes it exist, so it cannot need it first."""
    return ()


def _its_own_table(operation: Operation) -> tuple[str, ...]:
    """Everything else happens to a table, which has to be there to happen to."""
    return (operation["table"],)


# EVERY DECLARED KIND HAS A ROW, AND THE TABLE IS READ WITHOUT A FALLBACK. A
# kind with no row is an operation whose dependencies nobody worked out, and a
# default would answer for it by putting it wherever it happened to be written.
# A view is named here too: nothing in a batch creates the table a view's own
# name would stand for, so the row costs nothing and a missing row would be a
# silent guess.
MUST_ALREADY_EXIST: dict[OperationKind, Callable[[Operation], tuple[str, ...]]] = {
    "create_table": _nothing_of_its_own,
    "drop_table": _its_own_table,
    "add_column": _its_own_table,
    "drop_column": _its_own_table,
    "alter_column": _its_own_table,
    "rename_column": _its_own_table,
    "rename_table": _its_own_table,
    "create_index": _its_own_table,
    "drop_index": _its_own_table,
    "create_view": _its_own_table,
    "drop_view": _its_own_table,
    "insert_enum_values": _its_own_table,
    # Taking a value out needs the lookup table there, exactly as putting
    # one in does.
    "remove_enum_values": _its_own_table,
}


def _needed_by(operation: Operation) -> tuple[str, ...]:
    """The tables that have to exist while this operation runs."""
    pointed_at = tuple(
        sorted(
            {
                column["references"]["table"]
                for column in operation["columns"]
                if column["references"] is not None
            }
        )
    )
    return MUST_ALREADY_EXIST[operation["kind"]](operation) + pointed_at


def _positions_of(operations: list[Operation], kind: OperationKind) -> dict[str, int]:
    """Table name to where the operation of this kind on it was written."""
    return {
        operation["table"]: position
        for position, operation in enumerate(operations)
        if operation["kind"] == kind
    }


def _needs(operations: list[Operation]) -> list[tuple[int, str]]:
    """Every operation beside each table it needs, in the order written."""
    return [
        (position, name)
        for position, operation in enumerate(operations)
        for name in _needed_by(operation)
    ]


def _must_run_before(operations: list[Operation]) -> dict[int, set[int]]:
    """Where each operation was written, and what has to run before it."""
    makes = _positions_of(operations, MAKES_A_TABLE)
    ends = _positions_of(operations, ENDS_A_TABLE)
    needs = _needs(operations)

    # MADE FIRST, ENDED LAST. An operation is never its own predecessor, so a
    # drop of the table it names does not wait for itself, which would read as
    # a circle of one.
    edges = [
        (makes[name], position)
        for position, name in needs
        if name in makes and makes[name] != position
    ] + [
        (position, ends[name])
        for position, name in needs
        if name in ends and ends[name] != position
    ]

    earlier: dict[int, set[int]] = {position: set() for position in range(len(operations))}
    for before, after in edges:
        earlier[after].add(before)
    return earlier


def _in_an_order_that_works(
    operations: list[Operation], placed: list[int], _waiting: dict[int, set[int]]
) -> list[Operation] | Fault:
    """Every operation found a place, so the order is the places in turn."""
    return [operations[position] for position in placed]


def _no_order_works(
    operations: list[Operation], _placed: list[int], waiting: dict[int, set[int]]
) -> list[Operation] | Fault:
    """What is left over is a circle, and the fault walks it for the reader."""
    circle = "; ".join(
        f"{operations[position]['kind']} on {operations[position]['table']} comes after "
        + ", ".join(operations[source]["table"] for source in sorted(sources))
        for position, sources in sorted(waiting.items())
    )
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"these operations depend on each other in a circle, so no order runs "
            f"them all: {circle}."
        ),
        "what_to_do_instead": (
            "Break the circle in the models: one of the two tables has to be able "
            "to exist before the other, so take one of the references off, or "
            "declare the column that points holding nothing until the table it "
            "points at is there."
        ),
    }


# Keyed by whether anything was left waiting when nothing was free to run.
_THE_ANSWER: dict[
    bool, Callable[[list[Operation], list[int], dict[int, set[int]]], list[Operation] | Fault]
] = {
    False: _in_an_order_that_works,
    True: _no_order_works,
}


def order_operations(operations: list[Operation]) -> list[Operation] | Fault:
    """Answer with the operations in an order their dependencies allow."""
    waiting = _must_run_before(operations)

    # THE FREE ONE WRITTEN EARLIEST GOES NEXT. Taking the smallest position
    # leaves operations with nothing between them in the order they arrived, so
    # the answer is a function of the list and of nothing else.
    placed: list[int] = []
    free = sorted(position for position, sources in waiting.items() if not sources)
    while free:
        position = free[0]
        placed.append(position)
        del waiting[position]
        for sources in waiting.values():
            sources.discard(position)
        free = sorted(position for position, sources in waiting.items() if not sources)

    return _THE_ANSWER[bool(waiting)](operations, placed, waiting)
