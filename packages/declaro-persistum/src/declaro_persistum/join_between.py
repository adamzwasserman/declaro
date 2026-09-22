"""The join the foreign key already declared.

`docs/design/persistum.hd` line 861:
`fn join_between : (near: Model, far: Model, condition: Condition) -> Join | Fault`

THE RELATIONSHIP IS WRITTEN DOWN ONCE, ON THE COLUMN THAT POINTS.
`persistum.hd` line 125 settled that: a foreign key sat on the model as a
mapping and again in the prose as a facet of a column, which is two places for
one fact. `Column.references` is the surviving one, so this module reads that
field and nothing else. The column's name, its type and its position say
nothing about what it is related to, and a function that consulted any of them
would answer differently the day somebody renamed a column.

WHY THE CALLER IS NOT ASKED. `HOW-IT-SHOULD-WORK.md` line 178: you declared the
relationship once, in the model, and naming it again in every query is the
duplication this library exists to remove. So `Order.of(person)` and
`where({"person": {"name": "bob"}})` both arrive here as two models and a
condition, and the join is worked out rather than repeated.

EITHER MODEL MAY BE THE ONE THAT POINTS. `Order` declares `person_id`, so
asking for the orders of a person reads the near model's columns. Asking for
the people behind an order reads the far model's, and the pair of columns is
the same pair laid out the other way round. Both directions are in the table
below, which is why a fault here means no key exists rather than the key
pointing the way this function did not look.

A FAILURE COMES BACK IN THE RETURN. Two models with no key between them is a
`Fault` naming both, not an exception and not a log line. There is no join to
work out, and the caller is the one who can say which two models were meant.

IT READS NOTHING ELSE. No clock, no random source, no environment, no file, no
connection. The two models and the condition are the whole of the input, so
this is the same function on every machine.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import Condition, Fault, Join, Model


def _keys_declared_by(pointing: Model, pointed_at: Model) -> list[tuple[str, str]]:
    """Every key `pointing` declares into `pointed_at`.

    Each pair is (the column that points, the column it points at). A column's
    `Reference` names the table and the column separately, so nothing here has
    to take a name apart to find out where it points.

    REWRITTEN 2026-09-21, WHEN A REFERENCE STOPPED BEING A STRING. It used to
    read `table.column` and split it, and it used the whole reference as a
    dictionary key. A record cannot be a key, so the old form raised
    `TypeError` on every model the declaration now describes. Nothing caught
    it, because this function's own scenarios still built references the old
    way: a test written against a type production never sees proves only that
    the test and the function agree with each other.
    """
    over_there = {column["name"] for column in pointed_at["columns"]}
    return sorted(
        (column["name"], column["references"]["column"])
        for column in pointing["columns"]
        if column["references"] is not None
        and column["references"]["table"] == pointed_at["table"]
        and column["references"]["column"] in over_there
    )


def _keys_between(near: Model, far: Model) -> list[tuple[str, str]]:
    """Every pair of columns that lines the two models up, near column first.

    The near model pointing at the far one gives the pair as it stands. The far
    model pointing back gives the same pair the other way round, so it is
    turned around here and a reader downstream never has to ask which model
    happened to hold the key.
    """
    return _keys_declared_by(near, far) + [
        (on_the_near_side, on_the_far_side)
        for on_the_far_side, on_the_near_side in _keys_declared_by(far, near)
    ]


def _the_key_the_model_declared(
    keys: list[tuple[str, str]], _near: Model, far: Model, condition: Condition
) -> Join | Fault:
    """The join, made on the key the model already declared."""
    by_column, to_column = keys[0]
    return Join(
        model=far["table"],
        by_column=by_column,
        to_column=to_column,
        condition=condition,
    )


def _no_key_joins_them(
    _keys: list[tuple[str, str]], near: Model, far: Model, _condition: Condition
) -> Join | Fault:
    """Two models with no key between them. There is nothing to work out."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"No key joins {near['table']} and {far['table']}. Neither model declares a "
            f"column that references the other, and a foreign key is the only place this "
            f"library records that two models are related."
        ),
        what_to_do_instead=(
            f"Declare the relationship on the column that points: give {near['table']} or "
            f"{far['table']} a column whose `references` names a column of the other. If "
            f"the two are related through a third model, ask for that model as well."
        ),
    )


# KEYED BY WHETHER A KEY WAS FOUND, so both answers are written down once and a
# reader can see which is which without following a branch.
_ANSWER: dict[bool, Callable[[list[tuple[str, str]], Model, Model, Condition], Join | Fault]] = {
    True: _the_key_the_model_declared,
    False: _no_key_joins_them,
}


def join_between(near: Model, far: Model, condition: Condition) -> Join | Fault:
    """Work out the join between two models from the key one of them declares.

    `condition` is what the caller wrote about the far model. It is carried on
    to the join unread: checking the names in it against the far model is
    `refuse_undeclared_columns`'s answer, and turning it into text is
    `compile_statement`'s.
    """
    keys = _keys_between(near, far)
    return _ANSWER[bool(keys)](keys, near, far, condition)
