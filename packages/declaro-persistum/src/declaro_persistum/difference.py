"""Which operations turn the live schema into the target.

`docs/design/persistum.hd` declares it as `fn difference`, taking the schema
the database holds and the schema the models want, and handing back a list of
operations.

WHAT IT IS. You change a model, and the library reads the database, compares it
to your models and works out what to do. This is the comparison, and it is set
theory over two schemas: what is only in the target is created, what is only in
the live database is dropped, what differs is altered.

WHY THERE IS NO REFUSAL IN THE RETURN. Two schemas can always be compared. What
cannot always be done is applying the result, and that is somebody else's card:
`fn order_operations` refuses a set of operations no order satisfies, and
`fn refuse_undeclared_loss` refuses a migration that would destroy something no
model asked to lose. An empty list is an answer here, not a failure, and
nothing in the return can be read as one.

NO ENGINE APPEARS ANYWHERE IN IT. An operation says what must change;
`fn spell_operation` is what turns one into the statements a particular engine
runs. That split is what lets a migration be worked out and read without a
database, and it is why this module writes no SQL and names no engine.

A COLUMN MISSING FROM A MODEL IS NOT PERMISSION TO DROP IT, and this function
is not where that is settled. It is handed two schemas and neither carries the
permission: `Model.dropping` is how losing data is asked for, and
`fn refuse_undeclared_loss` is what reads it. So a column or table on the live
side only comes back as a drop that says `destroys`, and the migration stops
later unless a model named it. Deciding here would need the models, which this
function is not given.

WHAT IT DOES NOT COMPARE, said out loud rather than left to be discovered.
`Table` carries indexes and constraints and this reads neither as a comparison,
so a changed index emits no operation of its own. `Column` carries facets and a
reference and this compares only the declared type, so a column that keeps its
type and changes its floor emits nothing either. Those are still gaps, and they
are recorded here so neither is mistaken for a comparison that ran and found
nothing.

WHAT IT NOW HANDS OVER, WHICH IT COULD NOT BEFORE 2026-09-21. `Operation` grew
`columns`, `indexes`, `constraints`, `copying` and `values`, so a `create_table`
no longer arrives at `fn spell_operation` as a bare name. Every operation this
function builds fills the fields its kind names and leaves the rest empty:

  `create_table` carries the target table's columns, indexes and constraints.
  `add_column` carries the one column being added.
  `alter_column` and `drop_column` carry the target table's whole shape
  afterwards, because SQLite and Turso carry either out by building a new table
  and copying into it, and the new table needs every column, index and
  constraint the old one is meant to end with.
  `copying` is which columns that copy carries across, which is the columns the
  live table and the target table both have. Only this function can say it,
  because only this function holds both schemas.

A COLUMN THAT DISAPPEARS IS NOT IN `copying`, which is how the rebuild loses it
without being told to. A column being altered is, because the engine converts
what is already stored.

A RENAME IS NOT WORKED OUT HERE EITHER. A column gone and another appeared is
either a rename or a drop and an add, and the difference is your data.
`fn name_what_is_ambiguous` reads the answer off `renamed_from` on the column
that replaced the old one, over the operations this function produced. So a
drop and an add come back, and that pair is exactly what that card is given.
"""

from __future__ import annotations

from declaro_persistum.types import (
    Column,
    Constraint,
    Index,
    Operation,
    OperationKind,
    Schema,
    Table,
    Value,
    View,
)

# WHETHER CARRYING AN OPERATION OUT DESTROYS SOMETHING, ONE ROW PER KIND. The
# table is total over `OperationKind`, so a kind added to the vocabulary and
# not added here is a KeyError naming itself rather than a silent `False`.
#
# `alter_column` IS TRUE BECAUSE NARROWING A TYPE LOSES WHAT NO LONGER FITS,
# and nothing here can tell narrowing from widening: a declared type is a
# string, and two strings have no order. Saying it destroys is the safe
# direction, because the worst it costs is a model having to say so, while the
# other reading costs the column.
DESTROYS: dict[OperationKind, bool] = {
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


def _operation(
    kind: OperationKind,
    name: str,
    column: str | None,
    columns: list[Column],
    indexes: list[Index],
    constraints: list[Constraint],
    copying: list[str],
    values: list[Value],
    renamed_to: str | None,
    view: View | None,
) -> Operation:
    """One operation, carrying everything its kind names and whether it destroys.

    `Operation` calls that name `table`, and a view operation puts the view's
    name there, because a view is the object the operation acts on and the type
    has one field for it.

    EVERY CALLER WRITES EVERY FIELD OUT, and most of them write `[]` or the named
    absence. An argument with a default would let a caller forget one and be
    answered with an empty list that reads exactly like a table with no columns,
    which is the shape this package already refuses in `src/`.

    THE FOUR THAT WERE A BAG UNTIL 2026-09-23 are `column`, `renamed_to` and the
    view. They were keys in a dict called `detail`, which let this function emit
    `create_view` operations carrying no view at all: nothing checked, and every
    one of them was refused downstream for a key nobody had put in.
    """
    return Operation(
        kind=kind,
        table=name,
        column=column,
        columns=columns,
        indexes=indexes,
        constraints=constraints,
        copying=copying,
        values=values,
        renamed_to=renamed_to,
        view=view,
        destroys=DESTROYS[kind],
    )


def _by_name[Named: Table | Column | View](items: list[Named]) -> dict[str, Named]:
    """Tables, columns or views under their own names, so the comparison below
    is three set operations rather than three searches."""
    return {item["name"]: item for item in items}


def _columns_of(live: Table, target: Table) -> list[Operation]:
    """What turns one table's live columns into its declared ones.

    A DROP AND AN ALTER CARRY THE WHOLE TABLE AFTERWARDS, because SQLite and
    Turso carry both out by building a new table and copying into it. An add
    carries only the column added, because every engine adds one in place.
    """
    name = target["name"]
    held = _by_name(live["columns"])
    wanted = _by_name(target["columns"])
    # The columns both sides have, in the order the target declares them, which
    # is the order the copy reads and writes them in.
    carried_across = [column for column in wanted if column in held]

    added = [
        _operation(
            "add_column",
            name,
            column,
            [wanted[column]],
            [],
            target["constraints"],
            [],
            [],
            None,
            None,
        )
        for column in wanted
        if column not in held
    ]
    dropped = [
        _operation(
            "drop_column",
            name,
            column,
            target["columns"],
            target["indexes"],
            target["constraints"],
            carried_across,
            [],
            None,
            None,
        )
        for column in held
        if column not in wanted
    ]
    altered = [
        _operation(
            "alter_column",
            name,
            column,
            target["columns"],
            target["indexes"],
            target["constraints"],
            carried_across,
            [],
            None,
            None,
        )
        for column in wanted
        if column in held and held[column]["declared_type"] != wanted[column]["declared_type"]
    ]
    return added + dropped + altered


def _views_of(live: Schema, target: Schema) -> list[Operation]:
    """What turns the live views into the declared ones.

    A VIEW WHOSE DECLARATION CHANGED COMES BACK AS A DROP AND A CREATE. Some
    engines can replace one in a single statement and some cannot, and which is
    which is an engine's answer, not this one's. A view holds no rows, so
    neither half destroys anything.
    """
    held = _by_name(live["views"])
    wanted = _by_name(target["views"])

    created = [
        _operation("create_view", name, None, [], [], [], [], [], None, wanted[name])
        for name in wanted
        if name not in held
    ]
    dropped = [
        _operation("drop_view", name, None, [], [], [], [], [], None, held[name])
        for name in held
        if name not in wanted
    ]
    replaced = [
        operation
        for name in wanted
        if name in held and held[name] != wanted[name]
        for operation in (
            _operation("drop_view", name, None, [], [], [], [], [], None, held[name]),
            _operation("create_view", name, None, [], [], [], [], [], None, wanted[name]),
        )
    ]
    return created + dropped + replaced


def difference(live: Schema, target: Schema) -> list[Operation]:
    """Work out which operations turn the live schema into the target.

    Reads the two schemas and nothing else. It opens no connection, reads no
    clock and touches no file, so the same pair of schemas gives the same
    answer every time and a migration can be worked out before anything runs.

    THE LIST IS IN THE ORDER IT WAS WORKED OUT, WHICH IS NOT A RUNNING ORDER.
    `fn order_operations` puts them in an order their dependencies allow.
    """
    held = _by_name(live["tables"])
    wanted = _by_name(target["tables"])

    created = [
        _operation(
            "create_table",
            name,
            None,
            wanted[name]["columns"],
            wanted[name]["indexes"],
            wanted[name]["constraints"],
            [],
            [],
            None,
            None,
        )
        for name in wanted
        if name not in held
    ]
    dropped = [
        _operation("drop_table", name, None, [], [], [], [], [], None, None)
        for name in held
        if name not in wanted
    ]
    columns = [
        operation
        for name in wanted
        if name in held
        for operation in _columns_of(held[name], wanted[name])
    ]
    return created + dropped + columns + _views_of(live, target)
