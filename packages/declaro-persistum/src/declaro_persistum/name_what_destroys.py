"""Which operations in a migration lose data, marked one by one.

WHAT THIS ANSWERS. A migration is a list of operations. Some of them take
something away: dropping a column takes what was in it, dropping a table takes
all of it, changing a column's type takes whatever no longer fits. This walks
the list and marks each operation, then collects the names of what is lost into
the plan it hands back.

WHY MARKING AND REFUSING ARE TWO FUNCTIONS. Nobody is standing at a deploy to
approve a migration, so permission to lose something is a declaration in the
code rather than an answer at a prompt. That splits the work in two: this says
what is destructive, and `fn refuse_undeclared_loss` checks each marked
operation against what a model declared it means to lose. Keeping them apart is
what lets the second one be a total check over the first one's answer.

WHY THE TABLE IS TOTAL AND HAS NO FALLBACK. `LOSES_DATA[operation["kind"]]`
raises `KeyError` on a kind nobody wrote a row for. A `.get(kind, False)` would
answer for that kind instead, and the answer would be "harmless" — which is the
one wrong answer nothing downstream can catch, because an operation nobody
marks is an operation `refuse_undeclared_loss` never looks at. A loud failure
on an unknown kind is the whole point of writing it this way.

WHY EVERY TYPE CHANGE IS MARKED, INCLUDING A WIDENING. Narrowing text to an
integer loses what no longer fits; widening an integer to text loses nothing.
An `Operation` carries the old and new types nowhere this function can read, so
it cannot tell the two apart, and it marks both. Over-marking costs one line in
a model's `dropping`. Under-marking loses rows at a deploy nobody is watching.

WHAT IT READS OUT OF AN OPERATION, and it reads nothing else. `operation["table"]`
is the table the operation is about, and the view's name for a view operation.
`operation["detail"]["column"]` is the column name, for the four kinds that name
one, so that the plan says `users.nickname` rather than `users`. A kind whose
detail is missing that key raises rather than naming the table and letting a
reader think the whole table goes.

A MARK THAT ARRIVED ALREADY SET IS REPLACED, NEVER READ. Every operation handed
back carries the answer this table gives, whatever the operation said about
itself on the way in. There is one place the question is decided, and a caller
who guessed cannot make the guess stick. `refuse_undeclared_loss` refuses a plan
in which a kind that loses nothing arrives marked, so a mark carried through
from somewhere else would stop an open over a create.
"""

from __future__ import annotations

from collections.abc import Callable
from operator import itemgetter

from declaro_persistum.types import Operation, OperationKind, Plan

# --- which kinds take something away --------------------------------------
#
# Every kind `OperationKind` declares has a row. A create takes nothing. A
# rename moves rows rather than losing them, which is the reason `renamed_from`
# exists on a column at all. An index and a view are both derived from rows that
# stay, so dropping either takes away no row anybody wrote.

LOSES_DATA: dict[OperationKind, bool] = {
    "create_table": False,
    "drop_table": True,
    "add_column": False,
    "drop_column": True,
    "alter_column": True,
    "rename_column": False,
    "rename_table": False,
    "create_index": False,
    "drop_index": False,
    "create_view": False,
    "drop_view": False,
    "insert_enum_values": False,
}


def _the_table(operation: Operation) -> str:
    """What a reader calls an operation that takes a whole table."""
    return operation["table"]


def _the_column(operation: Operation) -> str:
    """What a reader calls an operation that takes one column."""
    return f"{operation['table']}.{operation['detail']['column']}"


# --- what the plan calls each one ------------------------------------------
#
# Also total, and for the same reason: a kind added to the vocabulary without a
# row here raises instead of being named after its table, which would tell a
# reader that a whole table was going when one column was.

NAMES: dict[OperationKind, Callable[[Operation], str]] = {
    "create_table": _the_table,
    "drop_table": _the_table,
    "add_column": _the_column,
    "drop_column": _the_column,
    "alter_column": _the_column,
    "rename_column": _the_column,
    "rename_table": _the_table,
    "create_index": _the_table,
    "drop_index": _the_table,
    "create_view": _the_table,
    "drop_view": _the_table,
    "insert_enum_values": _the_table,
}


def _marked(operation: Operation) -> Operation:
    """The same operation, carrying the answer about itself.

    A new dict rather than a mutated one, so the list a caller handed in still
    says what it said.
    """
    return {
        "kind": operation["kind"],
        "table": operation["table"],
        "detail": operation["detail"],
        "destroys": LOSES_DATA[operation["kind"]],
    }


def name_what_destroys(operations: list[Operation]) -> Plan:
    """Mark every operation that loses data, and name what each one loses."""
    marked = [_marked(operation) for operation in operations]
    losing = filter(itemgetter("destroys"), marked)

    return {
        "operations": marked,
        "destroys": [NAMES[operation["kind"]](operation) for operation in losing],
    }
