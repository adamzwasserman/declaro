"""How one engine writes one schema change.

    fn how_to_spell : (kind: OperationKind, engine: Engine) -> Spelling | Fault

`docs/architecture.md:56` and `:62`: `SQL_GENERATORS` is one of the seven tables
and it decides how an operation is written. Reading it here is what lets
`fn spell_operation` take what the table said and name no engine.

TWO THINGS DIFFER AND THEY ARE THE TWO FIELDS. The statements a change takes,
and whether the engine gets there by rebuilding the table. SQLite alters a
column by making a new table and copying into it, so one declared change is four
statements there and one everywhere else. `Operation.copying` carries which
columns come across, and only the differ knows, because only the differ holds
both the live schema and the target.

THE STATEMENTS ARE SHAPES AND NOT FINISHED TEXT. A row says the order of the
statements a change takes; the names and the values are filled by the speller,
which is where a declared name is checked and a value is bound.
`HOW-IT-SHOULD-WORK.md:208`: every string is one of those two and nothing is
pasted in.

A PAIR NOBODY WROTE DOWN IS A FAULT, NOT A GUESS. Thirteen kinds and three
engines is thirty-nine rows, and a fourteenth kind has to be written down before
it renders rather than falling through to whatever was nearest.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import Engine, Fault, OperationKind, Spelling


def _one(statement: str) -> Spelling:
    """A change one statement carries out, on an engine that alters in place."""
    return {"statements": [statement], "rebuilds": False}


# WHAT EVERY ENGINE WRITES, before the rebuilds any of them need. All three
# speak the same SQL for these, so the table is one row per kind and one
# override per engine rather than thirty-nine rows nobody could read.
_THE_SAME_EVERYWHERE: dict[OperationKind, Spelling] = {
    "create_table": _one("CREATE TABLE"),
    "drop_table": _one("DROP TABLE"),
    "add_column": _one("ALTER TABLE ADD COLUMN"),
    "drop_column": _one("ALTER TABLE DROP COLUMN"),
    "alter_column": _one("ALTER TABLE ALTER COLUMN"),
    "rename_column": _one("ALTER TABLE RENAME COLUMN"),
    "rename_table": _one("ALTER TABLE RENAME TO"),
    "create_index": _one("CREATE INDEX"),
    "drop_index": _one("DROP INDEX"),
    "create_view": _one("CREATE VIEW"),
    "drop_view": _one("DROP VIEW"),
    "insert_enum_values": _one("INSERT INTO"),
    "remove_enum_values": _one("DELETE FROM"),
}

# A REBUILD IS FOUR STATEMENTS IN ONE ORDER. Make the new table, copy what
# survives into it, drop the old one, and put the new one under the old name.
# Doing them in any other order loses the data.
_A_REBUILD: Spelling = {
    "statements": [
        "CREATE TABLE",
        "INSERT INTO SELECT FROM",
        "DROP TABLE",
        "ALTER TABLE RENAME TO",
    ],
    "rebuilds": True,
}

# WHAT THIS ENGINE WRITES INSTEAD. SQLite has no ALTER COLUMN and no way to drop
# a constraint, so both go through a rebuild. Turso aims at SQLite compatibility
# and is written down separately, because the day it gains ALTER COLUMN one row
# changes and nothing else does.
_WHAT_THIS_ENGINE_WRITES_INSTEAD: dict[Engine, dict[OperationKind, Spelling]] = {
    "sqlite": {"alter_column": _A_REBUILD},
    "turso": {"alter_column": _A_REBUILD},
    "postgresql": {},
}


def _the_row(kind: OperationKind, engine: Engine) -> Spelling | Fault:
    """The shared row, or this engine's own where it has one."""
    return _WHAT_THIS_ENGINE_WRITES_INSTEAD[engine].get(kind, _THE_SAME_EVERYWHERE[kind])


def _no_row_for_that(kind: OperationKind, engine: Engine) -> Spelling | Fault:
    """A change nobody wrote down, named rather than guessed at."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"There is no rule for writing {kind!r} on {engine!r}. This library "
            f"emits {len(_THE_SAME_EVERYWHERE)} kinds of schema change and that "
            f"is not one of them."
        ),
        what_to_do_instead=(
            f"Ask for one of {sorted(_THE_SAME_EVERYWHERE)}, or add a row for "
            f"{kind!r} saying what each engine writes for it."
        ),
    )


# Whether the generators table holds a row for this change. Both answers are
# written out, so a fourteenth kind is refused by a row rather than spelled as
# whatever the lookup happened to fall through to.
_WHETHER_IT_IS_WRITTEN_DOWN: dict[bool, Callable[[OperationKind, Engine], Spelling | Fault]] = {
    True: _the_row,
    False: _no_row_for_that,
}


def how_to_spell(kind: OperationKind, engine: Engine) -> Spelling | Fault:
    """Say how this engine writes this schema change."""
    return _WHETHER_IT_IS_WRITTEN_DOWN[kind in _THE_SAME_EVERYWHERE](kind, engine)
