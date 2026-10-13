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
    # RENAMING NEEDS THE NEW NAME, and this row denied it until 2026-10-07. It
    # claimed `columns` and denied `renamed_to`, so a change built from the row
    # alone was refused by `fn spell_operation` for want of the field the row
    # said it did not need.
    #
    # AND IT NEEDS THE COLUMNS AFTER ALL, SINCE 2026-10-12, because a rename on a
    # database with a primary is not a rename. Measured 2026-10-10:
    # `ALTER TABLE ... RENAME COLUMN` runs on the local copy and reaches no
    # primary, so a replicated rename is spelled as an add, a copy and a drop,
    # and the add has to write the arriving column's declared type.
    "rename_column": _filling(renamed_to=True, columns=True),
    "rename_table": _filling(renamed_to=True, columns=True, constraints=True),
    "create_index": _filling(indexes=True),
    "drop_index": _filling(indexes=True),
    "create_view": _filling(view=True),
    # DROPPING A VIEW NEEDS THE VIEW AND NOT ONLY ITS NAME, because whether it
    # was materialized decides the statement: PostgreSQL drops a materialized
    # view with different words. This row said the name was enough until
    # 2026-10-07, and all three engines refused a change built from it.
    "drop_view": _filling(view=True),
    "insert_enum_values": _filling(values=True),
    "remove_enum_values": _filling(values=True),
}


def what_an_operation_carries(kind: OperationKind) -> Carrying:
    """Say which fields this kind of change fills."""
    return CARRIES[kind]
