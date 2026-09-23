"""Nothing is destroyed that nobody declared.

WHY ABSENCE IS NEVER PERMISSION. A migration runs at open, on a deploy, with
nobody watching it. The models are the desired state, so a column missing from
a model reads as a column to drop. It also reads as a typo, and the two are the
same text. One of them costs a rename in a pull request and the other costs the
column, so losing something is asked for by name or it does not happen.

THE ASKING IS A DECLARATION, WHICH IS WHY IT WORKS WITH NOBODY THERE. A
surviving model names a column in `dropping`. A whole table has no model left
to ask, because deleting the file is the easiest way to lose one by accident,
so `Declared.dropping_tables` and `Declared.dropping_views` are where a table
and a view are given up. All three are read at the same moment as the models
they sit beside, by the same code, on the same deploy.

WHAT ARRIVES HERE IS ALREADY MARKED. `name_what_destroys` derives which
operations lose something and says so on each one, so this reads that mark and
asks one further question: did anything declare this particular loss. One
operation nobody asked for refuses the whole plan, because a plan run in part
is a database in a state no model describes.

THE TABLE IS TOTAL, AND THE EIGHT QUIET ROWS ARE THE POINT. Four kinds of
change can lose something and there is a way to declare each. The other eight
lose nothing, so nothing declares them, and one arriving marked as destroying
means the mark is wrong. Refusing is the safe answer to a mark nobody can
explain, and reading the table with a fallback would have made that case answer
itself silently.

`Model.dropping` ALSO COVERS A NARROWING, AND THAT IS THE DECLARATION'S ONLY
WORD FOR IT. Narrowing a column's type loses what no longer fits while the
column survives, which is a different loss from dropping the column. The
declaration gives one list of column names, so naming the column in `dropping`
is how both are asked for.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import (
    NOTHING,
    Declared,
    Fault,
    Operation,
    OperationKind,
    Plan,
)

# What a refused loss says: what would have gone, and the declaration to write
# so that it may go.
Undeclared = tuple[str, str]


def _asked_for(_lost: str, _to_write: str) -> Undeclared | None:
    """A declaration named this loss, so there is nothing to report."""
    return NOTHING


def _nobody_asked(lost: str, to_write: str) -> Undeclared | None:
    """Nothing named this loss, so it comes back with the way to ask for it."""
    return (lost, to_write)


# BOTH ANSWERS TAKE BOTH SENTENCES, so a row below writes the refusal it would
# give without first working out whether it will be given. The sentence is a
# fact about the operation either way.
_PERMITTED: dict[bool, Callable[[str, str], Undeclared | None]] = {
    True: _asked_for,
    False: _nobody_asked,
}


def _columns_this_table_means_to_lose(table: str, declared: Declared) -> list[str]:
    """Every column `dropping` names on the model of this table.

    A table with no model at all contributes nothing, which is the right
    answer: a model that is gone cannot ask for anything.
    """
    return [
        column
        for model in declared["models"]
        if model["table"] == table
        for column in model["dropping"]
    ]


def _a_column_dropped(operation: Operation, declared: Declared) -> Undeclared | None:
    """Dropping a column loses what was in it."""
    table = operation["table"]
    column = str(operation["column"])
    return _PERMITTED[column in _columns_this_table_means_to_lose(table, declared)](
        f"the column {table}.{column}, and every value in it",
        f"Name {column!r} in dropping on the {table} model to lose {table}.{column}.",
    )


def _a_column_narrowed(operation: Operation, declared: Declared) -> Undeclared | None:
    """Narrowing a type loses whatever no longer fits."""
    table = operation["table"]
    column = str(operation["column"])
    return _PERMITTED[column in _columns_this_table_means_to_lose(table, declared)](
        f"whatever no longer fits in {table}.{column}",
        f"Name {column!r} in dropping on the {table} model to lose it.",
    )


def _a_table_dropped(operation: Operation, declared: Declared) -> Undeclared | None:
    """Dropping a table loses all of it, and no model is left to ask."""
    table = operation["table"]
    return _PERMITTED[table in declared["dropping_tables"]](
        f"the whole of {table}, every row and every column",
        f"Add {table!r} to dropping_tables to lose {table}. Deleting the model file "
        f"asks for nothing, because a model that is gone cannot ask.",
    )


def _a_view_dropped(operation: Operation, declared: Declared) -> Undeclared | None:
    """A view holds no rows of its own, and it is still a name somebody built on."""
    view = operation["table"]
    return _PERMITTED[view in declared["dropping_views"]](
        f"the view {view}, and anything reading through it",
        f"Add {view!r} to dropping_views to lose {view}.",
    )


def _nothing_declares_this(operation: Operation, _declared: Declared) -> Undeclared | None:
    """A change of this kind loses nothing, so it arrived marked by mistake.

    There is no declaration that allows it, because there is nothing to allow.
    Refusing costs a deploy; trusting the mark costs whatever the operation
    turns out to touch.
    """
    kind = operation["kind"]
    return _nobody_asked(
        f"whatever a {kind} on {operation['table']} was marked as destroying",
        f"A {kind} loses nothing, so no declaration allows one and this plan "
        f"cannot be permitted as it stands. What marked it as destroying is wrong.",
    )


# EVERY KIND THE DIFFER CAN EMIT, AND HOW A LOSS OF THAT KIND IS ASKED FOR.
# Twelve rows because `OperationKind` has twelve members; a thirteenth kind
# added to the vocabulary and not added here fails on the first plan that
# carries one, rather than being waved through by a default.
HOW_A_LOSS_IS_ASKED_FOR: dict[OperationKind, Callable[[Operation, Declared], Undeclared | None]] = {
    "drop_column": _a_column_dropped,
    "alter_column": _a_column_narrowed,
    "drop_table": _a_table_dropped,
    "drop_view": _a_view_dropped,
    "create_table": _nothing_declares_this,
    "add_column": _nothing_declares_this,
    "rename_column": _nothing_declares_this,
    "rename_table": _nothing_declares_this,
    "create_index": _nothing_declares_this,
    "drop_index": _nothing_declares_this,
    "create_view": _nothing_declares_this,
    "insert_enum_values": _nothing_declares_this,
}


def _let_it_run(plan: Plan, _undeclared: list[Undeclared]) -> Plan | Fault:
    """Every loss in this plan was declared, so the plan goes on as it arrived."""
    return plan


def _refuse(_plan: Plan, undeclared: list[Undeclared]) -> Plan | Fault:
    """Name everything that would have gone, and the declarations to write."""
    losses = "; ".join(lost for lost, _to_write in undeclared)
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"Bringing the database up to your models would destroy {losses}. "
            f"No model asked to lose it, and a column missing from a model is a "
            f"typo as often as a decision, so none of this plan ran."
        ),
        what_to_do_instead=" ".join(to_write for _lost, to_write in undeclared),
    )


_ANSWER: dict[bool, Callable[[Plan, list[Undeclared]], Plan | Fault]] = {
    False: _let_it_run,
    True: _refuse,
}


def refuse_undeclared_loss(plan: Plan, declared: Declared) -> Plan | Fault:
    """Refuse a plan that would destroy anything no declaration asked to lose.

    Reads the marks the plan carries and the three places a loss is declared,
    and nothing else. It opens no connection, holds no clock and knows no
    engine, so the decision is the same one every time it is asked.
    """
    undeclared = [
        said
        for operation in plan["operations"]
        if operation["destroys"]
        if (said := HOW_A_LOSS_IS_ASKED_FOR[operation["kind"]](operation, declared)) is not NOTHING
    ]
    return _ANSWER[bool(undeclared)](plan, undeclared)
