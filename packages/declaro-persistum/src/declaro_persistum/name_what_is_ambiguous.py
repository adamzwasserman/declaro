"""A column gone and another appeared, and only the models know which it was.

    fn name_what_is_ambiguous : (operations: list<Operation>, declared: Declared) -> list<Operation> | Fault

Comparing the models against a live database sees that `email` is gone and
`email_address` has appeared. Two readings fit: somebody renamed the column and
the rows moved with it, or somebody dropped one column and added another and
the rows went. The comparison cannot tell them apart, and the difference is
your data.

SO THE MODELS SAY WHICH, AND THIS IS WHERE THEY ARE READ. The column that
replaced the old one names what it replaced, and a drop somebody means to make
is named in that model's `dropping`. Either declaration settles the pair. When
neither is written, this stops the migration and the refusal says which two
columns it is about and which line to write.

NOTHING HERE GUESSES, AND NOTHING HERE SCORES. This package once carried seven
patterns that scored a gone name against an appeared one, of which six could
never be reached because a containment rule returned first with a lower score.
A table renamed and reshaped scored under the threshold, no question was
raised, the drop ran, and the rows went. A drop beside an add is ambiguous by
construction, and a score is a guess wearing a number.

WHICH WAY ROUND A RENAME IS WRITTEN DOWN, because two functions reading this
pair in opposite directions would rename a column to itself and lose the other
without anything saying so. A model is not symmetric here: `renamed_from` sits
on the NEW column and its value is the OLD name. The operation reads the same
way round. `operation["column"]` holds the old name, which is the name the live
database still has and the value `renamed_from` carries. `operation["renamed_to"]`
holds the new name, which is the declaring column's own `name`.

A RENAME MOVES A COLUMN INSIDE ONE TABLE. So a gone column is only ever paired
with a column that appeared on the same table. Pairing across tables would
refuse two changes that have nothing to do with each other, and would spell a
rename no engine could carry out.

THE REFUSAL COMES BACK IN THE RETURN. It is not raised and it is not logged.
Nobody is watching a deploy, so a caller gets a value they can read, carrying
the two column names and the declaration that settles them.

Pure: two arguments in, one value out. No clock, no random source, no
environment, no file and no connection. The operations are read in the order
they arrive and handed back in that order, so the same input always gives the
same answer. It knows no engine: it is handed operations and models, and
neither carries an engine's words.

NO TABLE HERE TAKES A FALLBACK. Every lookup below is into a table built with a
row for every key it will be asked about, so no key can be answered for by a
default nobody wrote.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import NOTHING, Declared, Fault, Operation, OperationKind

# What a comparison emits when a column is gone, when one has appeared, and
# what the two become when the models say they are one column. They are typed
# as the vocabulary rather than as text, so a spelling that is not one of the
# twelve operations fails where it is written.
GONE: OperationKind = "drop_column"
APPEARED: OperationKind = "add_column"
RENAMED: OperationKind = "rename_column"

# A column operation carries its column's name under `column`, which is what
# `fn name_what_destroys` and `fn refuse_undeclared_loss` read it by. A rename
# is about the column that is already there, so `column` is the name it had and
# `to` is the name it takes. Agreed across the five functions being written
# against `Operation` on 2026-09-21; `to` is carried by a rename and by nothing
# else here.
COLUMN = "column"
TO = "to"

# What becomes of each operation handed in. Every operation gets one of these
# three, so the table below is asked nothing it has no row for.
STAYS = "stays as it is"
BECOMES_A_RENAME = "becomes the rename"
FOLDED_IN = "folded into the rename"


def _each(operations: list[Operation], kind: OperationKind) -> list[tuple[int, str, str]]:
    """Where it sits, its table and its column, for every operation of one kind."""
    return [
        (at, operation["table"], str(operation["column"]))
        for at, operation in enumerate(operations)
        if operation["kind"] == kind
    ]


def _replacements(declared: Declared) -> dict[tuple[str, str], str]:
    """(table, the column it replaced) -> the column that replaced it.

    This is the whole of what a model says about a rename: the new column names
    the old one, once, in the place every other declaration is written.
    """
    return {
        (model["table"], str(column["renamed_from"])): column["name"]
        for model in declared["models"]
        for column in model["columns"]
        if column["renamed_from"] is not NOTHING
    }


def _asked_to_lose(declared: Declared) -> dict[tuple[str, str], str]:
    """(table, column) -> the table, for every column a model means to lose.

    A column absent from a model is not permission to drop it, because a typo
    is also a column absent from a model. Naming it in `dropping` is the
    permission, and a column named there is a column the author has already
    said they mean to lose, which is one of the two readings written down.
    """
    return {
        (model["table"], column): model["table"]
        for model in declared["models"]
        for column in model["dropping"]
    }


def _a_rename(operation: Operation, appeared: str) -> Operation:
    """The one operation that replaces a gone column and the one that appeared.

    IT CARRIES WHAT THE DROP CARRIED, so the rebuild the rename turns into has
    the table's columns, indexes and rules to build from. Until 2026-09-23 this
    wrote four keys and left five fields off the record it claimed to return,
    and nothing noticed, because the four it wrote were the four its own tests
    read back.
    """
    return Operation(
        kind=RENAMED,
        table=operation["table"],
        column=operation["column"],
        columns=operation["columns"],
        indexes=operation["indexes"],
        constraints=operation["constraints"],
        copying=operation["copying"],
        values=operation["values"],
        renamed_to=appeared,
        view=operation["view"],
        destroys=False,
    )


# WHAT BECOMES OF ONE OPERATION, keyed by the three answers and nothing else. A
# settled drop becomes the rename and keeps the drop's place in the list, and
# the add that goes with it comes back as nothing.
_WHAT_BECOMES_OF_IT: dict[str, Callable[[Operation, str], list[Operation]]] = {
    STAYS: lambda operation, _appeared: [operation],
    BECOMES_A_RENAME: lambda operation, appeared: [_a_rename(operation, appeared)],
    FOLDED_IN: lambda _operation, _appeared: [],
}


def _with_the_renames_made(
    operations: list[Operation],
    fate: dict[int, str],
    appeared_as: dict[int, str],
    _ambiguous: list[tuple[str, str, str]],
) -> list[Operation] | Fault:
    """Every operation, with each settled pair standing as one rename."""
    return [
        built
        for at, operation in enumerate(operations)
        for built in _WHAT_BECOMES_OF_IT[fate[at]](operation, appeared_as[at])
    ]


def _refuse(
    _operations: list[Operation],
    _fate: dict[int, str],
    _appeared_as: dict[int, str],
    ambiguous: list[tuple[str, str, str]],
) -> list[Operation] | Fault:
    """Name every pair nothing settles, and the line that settles the first."""
    pairs = "; ".join(
        f'"{was}" is gone from {table} and "{now}" has appeared on it'
        for table, was, now in ambiguous
    )
    table, was, now = ambiguous[0]
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"{pairs}. Either {was} was renamed to {now}, which keeps its rows, or "
            f"{was} was dropped and {now} added, which loses them. Your models say "
            f"neither, and there is no way to tell the two apart from outside."
        ),
        "what_to_do_instead": (
            f'Say which in the model. Write renamed_from="{was}" on {now} if {now} '
            f"replaced it, or name {was} in {table}'s dropping if you mean to lose "
            f"what was in it."
        ),
    }


_ANSWER: dict[
    bool,
    Callable[
        [list[Operation], dict[int, str], dict[int, str], list[tuple[str, str, str]]],
        list[Operation] | Fault,
    ],
] = {
    False: _with_the_renames_made,
    True: _refuse,
}


def name_what_is_ambiguous(
    operations: list[Operation], declared: Declared
) -> list[Operation] | Fault:
    """Turn each declared rename into one operation, and refuse what nothing declares.

    A gone column is settled when the models say another column replaced it, or
    when the model that lost it named it in `dropping`. A gone column nothing
    settles, sitting beside a column that appeared on the same table, is the
    pair this refuses.
    """
    gone = _each(operations, GONE)
    appeared = _each(operations, APPEARED)
    replaced_by = _replacements(declared)
    losing = _asked_to_lose(declared)
    arrived = {(table, column): at for at, table, column in appeared}

    # A rename is declared only when the column it names as its predecessor is
    # a column that is actually gone AND the replacement is actually here. A
    # model naming a column nobody dropped describes a rename already carried
    # out, and re-running it would rename a column that is no longer there.
    renames = {
        (table, column): replaced_by[(table, column)]
        for _at, table, column in gone
        if (table, column) in replaced_by and (table, replaced_by[(table, column)]) in arrived
    }
    taken = {(table, appeared_as): was for (table, was), appeared_as in renames.items()}

    unsettled_gone = [
        (table, column)
        for _at, table, column in gone
        if (table, column) not in renames and (table, column) not in losing
    ]
    unclaimed = [(table, column) for _at, table, column in appeared if (table, column) not in taken]

    # EVERY PAIR THAT COULD BE READ TWO WAYS, in the order the operations
    # arrived. A gone column is paired only with a column that appeared on its
    # own table, because that is the only place a rename can have moved it.
    ambiguous = [
        (table, was, now) for table, was in unsettled_gone for far, now in unclaimed if far == table
    ]

    fate = dict.fromkeys(range(len(operations)), STAYS)
    fate.update({at: BECOMES_A_RENAME for at, table, column in gone if (table, column) in renames})
    fate.update({at: FOLDED_IN for at, table, column in appeared if (table, column) in taken})

    appeared_as = dict.fromkeys(range(len(operations)), "")
    appeared_as.update(
        {at: renames[(table, column)] for at, table, column in gone if (table, column) in renames}
    )

    return _ANSWER[bool(ambiguous)](operations, fate, appeared_as, ambiguous)
