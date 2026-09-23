"""Which fields one kind of schema change fills.

    fn what_an_operation_carries : (kind: OperationKind) -> Carrying

AN OPERATION CARRIED A BAG OF ANYTHING UNTIL 2026-09-23, so an `alter_column`
holding a list of enum values was expressible and meant nothing. Every field it
may fill is now named, and this says which of them each kind uses, so an
operation can be checked against its own kind.

ADDING A KIND OF CHANGE IS ONE MORE LINE HERE. Not a new field, not a new
function: the thirteen rows below are the whole of what the differ can emit, and
a fourteenth is a row.

NO FALLBACK ON THE READ. `CARRIES[kind]`, never `.get`. A kind nobody wrote a
row for fails at the lookup rather than being filed under a row written for a
different one.
"""

from __future__ import annotations

from declaro_persistum.types import Carrying, OperationKind

_NOTHING_BUT_ITS_NAME: Carrying = {
    "columns": False,
    "indexes": False,
    "constraints": False,
    "copying": False,
    "values": False,
    "renamed_to": False,
    "view": False,
}


def _filling(**named: bool) -> Carrying:
    """One row, saying which fields it fills and taking the rest as false.

    Written this way round so a row states only what it carries. The seven
    names are the whole space, so nothing is defaulted that a reader could not
    work out from the row above.
    """
    return {**_NOTHING_BUT_ITS_NAME, **named}  # type: ignore[typeddict-item]


# THIRTEEN ROWS, ONE PER KIND OF CHANGE. A rebuild carries `copying` because
# SQLite alters a column by making a new table and copying into it, and only the
# differ knows which columns come across.
CARRIES: dict[OperationKind, Carrying] = {
    "create_table": _filling(columns=True, constraints=True, indexes=True),
    "drop_table": _NOTHING_BUT_ITS_NAME,
    "add_column": _filling(columns=True),
    "drop_column": _filling(columns=True),
    "alter_column": _filling(columns=True, constraints=True, indexes=True, copying=True),
    "rename_column": _filling(columns=True),
    "rename_table": _filling(renamed_to=True),
    "create_index": _filling(indexes=True),
    "drop_index": _filling(indexes=True),
    "create_view": _filling(view=True),
    "drop_view": _NOTHING_BUT_ITS_NAME,
    "insert_enum_values": _filling(values=True),
    "remove_enum_values": _filling(values=True),
}


def what_an_operation_carries(kind: OperationKind) -> Carrying:
    """Say which fields this kind of change fills."""
    return CARRIES[kind]
