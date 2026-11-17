"""How one engine writes one schema change.

    fn how_to_spell : (kind: OperationKind, engine: Engine, replicating: bool) -> Spelling | Fault

`docs/architecture.md:56` and `:62`: `SQL_GENERATORS` is one of the seven tables
and it decides how an operation is written. Reading it here is what lets
`fn spell_operation` take what the table said and name no engine.

THREE THINGS DIFFER AND THEY ARE THE THREE FIELDS. The statements a change
takes, whether the engine gets there by rebuilding the table, and whether each
statement has to be committed on its own. SQLite alters a column by making a new
table and copying into it, so one declared change is four statements there and
one everywhere else. `Operation.copying` carries which columns come across, and
only the differ knows, because only the differ holds both the live schema and
the target.

THE STATEMENTS ARE SHAPES AND NOT FINISHED TEXT. A row says the order of the
statements a change takes; the names and the values are filled by the speller,
which is where a declared name is checked and a value is bound.
`HOW-IT-SHOULD-WORK.md:208`: every string is one of those two and nothing is
pasted in.

A PAIR NOBODY WROTE DOWN IS A FAULT, NOT A GUESS. Thirteen kinds and three
engines is thirty-nine rows, and a fourteenth kind has to be written down before
it renders rather than falling through to whatever was nearest.

WHETHER THERE IS A PRIMARY IS PART OF THE QUESTION, SINCE 2026-10-12, because a
statement that runs on the local copy is not a statement that reaches the
primary. Measured 2026-10-10 against a real Turso primary, one change per table,
read back on a replica that had never seen it: a push carries create table, add
column, drop column and ordinary rows, and carries neither
`ALTER TABLE ... RENAME COLUMN` nor `ALTER TABLE ... RENAME TO`. Each of those
runs locally, reports success and sends nothing, so the copy and the primary
part company and nothing says a word.

AND THE REBUILD IS WHY THAT REACHES FURTHER THAN THE TWO RENAMES. A rebuild
ends with `ALTER TABLE ... RENAME TO`, and SQLite and Turso rebuild for an alter
and for a drop, so the statement that cannot leave the machine sits underneath
two changes that never mention a rename. Measured 2026-10-12 against the same
primary: the push after a rebuild failed with
`SQL string could not be parsed: near RP, "None"`, the primary was left holding
`_declaro_tmp_<table>` and had lost the real table, and the next push on that
channel failed with `no such table`. The local copy was correct throughout.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import Engine, Fault, OperationKind, Spelling


def _one(statement: str) -> Spelling:
    """A change one statement carries out, on an engine that alters in place."""
    return {"statements": [statement], "rebuilds": False, "commits_between": False}


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
    "commits_between": False,
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


# --- and what it writes when the database has a primary ----------------------
#
# A RENAME BECOMES AN ADD, A COPY AND A DROP, each committed on its own. All
# three of those replicate. Measured 2026-10-10: the same three in one
# transaction failed the push and left the literal string 'None' on the primary,
# so the commit between them is the measurement and not a preference.
_A_COLUMN_MOVED_BY_COPYING: Spelling = {
    "statements": [
        "ALTER TABLE ADD COLUMN",
        "UPDATE SET",
        "ALTER TABLE DROP COLUMN",
    ],
    "rebuilds": False,
    "commits_between": True,
}

# A TABLE MOVES THE SAME WAY, and it is a rebuild without the rename at the end:
# make the table under its new name, copy the rows across, drop the old one.
_A_TABLE_MOVED_BY_COPYING: Spelling = {
    "statements": [
        "CREATE TABLE",
        "INSERT INTO SELECT FROM",
        "DROP TABLE",
    ],
    "rebuilds": True,
    "commits_between": True,
}

# A DROP REPLICATES AND A REBUILD DOES NOT, so a replicated Turso writes the
# plain statement every other engine writes rather than the rebuild it would
# write locally.
_A_COLUMN_DROPPED_OUTRIGHT: Spelling = _THE_SAME_EVERYWHERE["drop_column"]

# AND A COLUMN THAT CANNOT BE ALTERED IN PLACE CANNOT BE ALTERED AT ALL HERE.
# Turso has no ALTER COLUMN, its rebuild cannot leave the machine, and there is
# no third spelling, so this is named rather than half carried out.
_CANNOT_REACH_THE_PRIMARY: OperationKind = "alter_column"

_WHAT_A_REPLICA_WRITES_INSTEAD: dict[Engine, dict[OperationKind, Spelling]] = {
    # Neither of these replicates, so neither is ever asked with a primary. Each
    # keeps a row, because a read with no row is a read with a fallback.
    "sqlite": _WHAT_THIS_ENGINE_WRITES_INSTEAD["sqlite"],
    "postgresql": _WHAT_THIS_ENGINE_WRITES_INSTEAD["postgresql"],
    "turso": {
        "rename_column": _A_COLUMN_MOVED_BY_COPYING,
        "rename_table": _A_TABLE_MOVED_BY_COPYING,
        "drop_column": _A_COLUMN_DROPPED_OUTRIGHT,
    },
}

# WHICH SET OF OVERRIDES THIS DATABASE READS, keyed on whether it has a primary.
_WHAT_IT_WRITES: dict[bool, dict[Engine, dict[OperationKind, Spelling]]] = {
    False: _WHAT_THIS_ENGINE_WRITES_INSTEAD,
    True: _WHAT_A_REPLICA_WRITES_INSTEAD,
}

# WHICH KINDS A REPLICA HAS NO SPELLING FOR AT ALL, per engine. Written as a
# table so the answer is a row rather than a check, and so an engine that gains
# a spelling for one loses it from a row rather than from a condition.
_WHAT_A_REPLICA_CANNOT_WRITE: dict[Engine, frozenset[OperationKind]] = {
    "sqlite": frozenset(),
    "postgresql": frozenset(),
    "turso": frozenset({_CANNOT_REACH_THE_PRIMARY}),
}

_NOTHING_IS_OUT_OF_REACH: dict[Engine, frozenset[OperationKind]] = {
    "sqlite": frozenset(),
    "postgresql": frozenset(),
    "turso": frozenset(),
}

_WHAT_IS_OUT_OF_REACH: dict[bool, dict[Engine, frozenset[OperationKind]]] = {
    False: _NOTHING_IS_OUT_OF_REACH,
    True: _WHAT_A_REPLICA_CANNOT_WRITE,
}


# WHETHER THIS ENGINE WRITES THIS CHANGE ITS OWN WAY. Two rows: the engine has
# its own spelling for this kind, or every engine writes it the same. Asking the
# engine's table for a default reads as one answer with a spare, and it is two
# answers; it also looks up the shared row on every call, including the calls
# that had the engine's own.
_WHETHER_THIS_ENGINE_HAS_ITS_OWN: dict[bool, Callable[[OperationKind, Engine, bool], Spelling]] = {
    True: lambda kind, engine, replicating: _WHAT_IT_WRITES[replicating][engine][kind],
    False: lambda kind, _engine, _replicating: _THE_SAME_EVERYWHERE[kind],
}


def _the_row(kind: OperationKind, engine: Engine, replicating: bool) -> Spelling | Fault:
    """The shared row, or this engine's own where it has one."""
    return _WHETHER_THIS_ENGINE_HAS_ITS_OWN[kind in _WHAT_IT_WRITES[replicating][engine]](
        kind, engine, replicating
    )


def _it_cannot_reach_the_primary(
    kind: OperationKind, engine: Engine, _replicating: bool
) -> Spelling | Fault:
    """A change this engine can only make locally, named rather than half made.

    `fn how_to_migrate` is what an operator reads afterwards; this is what stops
    them needing to. A rebuild on a replicated Turso ran, reported success and
    took the table off the primary, which is the one outcome worth refusing for.
    """
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"A {kind} on {engine} cannot reach the primary. {engine} has no "
            f"ALTER COLUMN, so it alters a column by rebuilding the table, and "
            f"the rebuild ends by renaming the new table into place. Measured "
            f"2026-10-12: a rename runs on the local copy, reports success and "
            f"sends nothing, so the push after a rebuild failed, the primary was "
            f"left holding the temporary table and had lost the real one, and "
            f"every later push on that channel failed too."
        ),
        "what_to_do_instead": (
            "Declare the change as a new column and a dropping line rather than "
            "as a change to the column that is there. A column added, filled and "
            "dropped all reach the primary; a column altered in place does not."
        ),
    }


def _no_row_for_that(kind: OperationKind, engine: Engine, _replicating: bool) -> Spelling | Fault:
    """A change nobody wrote down, named rather than guessed at."""
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"There is no rule for writing {kind!r} on {engine!r}. This library "
            f"emits {len(_THE_SAME_EVERYWHERE)} kinds of schema change and that "
            f"is not one of them."
        ),
        "what_to_do_instead": (
            f"Ask for one of {sorted(_THE_SAME_EVERYWHERE)}, or add a row for "
            f"{kind!r} saying what each engine writes for it."
        ),
    }


# Whether the generators table holds a row for this change. Both answers are
# written out, so a fourteenth kind is refused by a row rather than spelled as
# whatever the lookup happened to fall through to.
def _no_such_engine(_kind: OperationKind, engine: Engine, _replicating: bool) -> Spelling | Fault:
    """An engine this package does not ship, named rather than subscripted.

    The engine reaches here as a value, and a value can be anything a caller
    typed. Reading the table by subscript would raise where the question was
    not asked, so the row is read after this and not before.
    """
    return {
        "kind": "refused",
        "what_went_wrong": (
            f"You asked about {engine!r}, and this package ships "
            f"{', '.join(sorted(_WHAT_THIS_ENGINE_WRITES_INSTEAD))}."
        ),
        "what_to_do_instead": (
            f"Ask about one of {sorted(_WHAT_THIS_ENGINE_WRITES_INSTEAD)}, or add a row for "
            f"{engine!r} to every one of the tables an engine has a row in."
        ),
    }


# Whether this engine can carry this change to where it has to go. Asked after
# the pair is known to be written down, because a kind nobody declared has no
# reach to ask about.
_WHETHER_IT_CAN_GET_THERE: dict[bool, Callable[[OperationKind, Engine, bool], Spelling | Fault]] = {
    True: _it_cannot_reach_the_primary,
    False: _the_row,
}


def _written_down(kind: OperationKind, engine: Engine, replicating: bool) -> Spelling | Fault:
    """The pair has a rule, so the only question left is whether it can land."""
    return _WHETHER_IT_CAN_GET_THERE[kind in _WHAT_IS_OUT_OF_REACH[replicating][engine]](
        kind, engine, replicating
    )


_WHETHER_IT_IS_WRITTEN_DOWN: dict[
    tuple[bool, bool], Callable[[OperationKind, Engine, bool], Spelling | Fault]
] = {
    (True, True): _written_down,
    (True, False): _no_such_engine,
    (False, True): _no_row_for_that,
    (False, False): _no_such_engine,
}


def how_to_spell(kind: OperationKind, engine: Engine, replicating: bool) -> Spelling | Fault:
    """Say how this engine writes this schema change, here, on this database.

    `replicating` says whether the database has a primary, which
    `fn session_connect` knows from whether it is holding a channel. It is part
    of the question because a statement that runs on the local copy is not a
    statement that reaches the primary.
    """
    return _WHETHER_IT_IS_WRITTEN_DOWN[
        (kind in _THE_SAME_EVERYWHERE, engine in _WHAT_THIS_ENGINE_WRITES_INSTEAD)
    ](kind, engine, replicating)
