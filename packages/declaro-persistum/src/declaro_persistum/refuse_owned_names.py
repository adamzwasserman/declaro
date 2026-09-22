"""A name the library or an engine owns is refused where the models are read.

THE LIBRARY KEEPS ITS OWN TABLES AND COLUMNS BESIDE YOURS, and a collision is
silent until it is fatal. A model that declares one of those names does not
fail at the point it is written. It fails later, in a migration that drops
something, or in a query that reads the library's rows and hands them back as
yours. So the collision is caught at the one moment both lists are in hand: the
moment the declared models are read.

THE REFUSAL COMES BACK, IT IS NOT RAISED AND IT IS NOT LOGGED. `fn
refuse_owned_names` declares the answer as an absence or a fault. Absence means
nothing was wrong. A fault names the model, the name, the prefix and who owns
it, and says what to write instead, because a caller who is only told "that
name is reserved" has to come and read this file to find out which name.

THE OWNED NAMES ARE A TABLE, taken from `refuse_owned_names.feature` and from
the card `fn refuse_owned_names` in `docs/design/persistum.hd`, which name the
same five: `_declaro_meta`, `_dp_` and `_declaro_tmp_` are the library's own,
and two more belong to the engines. Written as rows rather than as a chain of
comparisons, so the list can be read, printed and added to in one place.

THEY ARE IDENTIFIERS, NOT STATEMENTS. Nothing here is rendered, nothing here
runs, and no engine is named: two of the prefixes are named after an engine but
neither is that engine's word for itself, and this function is never handed an
`engine` to decide anything with.

CASE IS NOT PART OF THE QUESTION, AND THE FEATURE DOES NOT SAY SO. It writes
every name in lower case and never asks what happens to a shouted one. Folding
is the safer reading: this package has already watched a database fold a stored
table name, decide the declared one was missing, and drop and recreate it on
every deploy. A name that is owned in lower case and free in upper case would
be that failure again, so the comparison folds and the prefixes are written
folded.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import NOTHING, Declared, Fault

# prefix -> who owns every name that starts with it.
OWNED_NAMES: dict[str, str] = {
    "_declaro_meta": "the library",
    "_dp_": "the library",
    "_declaro_tmp_": "the library",
    "sqlite_": "the engine",
    "turso_": "the engine",
    # MEASURED ON A LIVE REPLICA, 2026-09-22. Filling one leaves six tables
    # behind and this prefix is the one the others miss:
    # `__turso_internal_seq___turso_internal_autoincrement_turso_cdc`. Without
    # it the migration reads the engine's own bookkeeping as tables a model
    # forgot, plans to drop them, and `fn refuse_undeclared_loss` stops every
    # Turso open.
    "__turso": "the engine",
}


# BOTH HANDLERS TAKE THE SAME ONE ARGUMENT, which is how this package writes a
# dispatch pair: the table hands every row the same value, and a row that
# needed a different shape could not sit in it.
def _nothing_is_wrong(_claims: list[tuple[str, str, str]]) -> None | Fault:
    """No declared name starts with a prefix anybody else owns."""
    return NOTHING


def _refuse(claims: list[tuple[str, str, str]]) -> None | Fault:
    """Name every collision, say who owns the prefix, and say what to do."""
    named = "; ".join(
        f"the model {table} declares {name}, and every name starting {prefix} "
        f"belongs to {OWNED_NAMES[prefix]}"
        for table, name, prefix in claims
    )
    prefixes = ", ".join(sorted({prefix for _table, _name, prefix in claims}))
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"{named}. Two things of one name collide, and the collision "
            f"surfaces later as a migration that destroys something or a query "
            f"that reads the wrong rows."
        ),
        what_to_do_instead=(
            f"Rename each of them to a name that does not start {prefixes}. "
            f"A name that merely holds those letters further along is yours."
        ),
    )


_ANSWER: dict[bool, Callable[[list[tuple[str, str, str]]], None | Fault]] = {
    False: _nothing_is_wrong,
    True: _refuse,
}


def refuse_owned_names(declared: Declared) -> None | Fault:
    """Refuse a model claiming a name the library or an engine owns.

    Reads the table above and the models it was handed, and nothing else. It
    opens no file, holds no connection and asks nothing of the clock, so the
    decision can be tested without a database.
    """
    claims = [
        (model["table"], name, prefix)
        for model in declared["models"]
        for name in [model["table"], *(column["name"] for column in model["columns"])]
        for prefix in OWNED_NAMES
        if name.casefold().startswith(prefix)
    ]
    return _ANSWER[bool(claims)](claims)
