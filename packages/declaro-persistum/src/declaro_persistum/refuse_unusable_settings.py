"""A setting this engine cannot use is refused, not ignored.

One door accepts the union of what three engines need. That is what makes
`open` take a target and nothing else, and it is also what lets a caller hand
PostgreSQL a busy timeout, or SQLite a primary. Dropping the setting on the
floor would be a silent default wearing a different hat: the caller asked for
something, nothing happened, and nothing said so. So the setting comes back as
a refusal naming the engine and the setting.

Which settings belong to which engine is a table. Reading it the other way
round, `busy_timeout_s` is a local file waiting its turn at a lock, which is
SQLite and Turso; `primary` is the address a replica pulls from, which is Turso
alone. PostgreSQL holds neither, because it waits on a server rather than on a
file and it replicates without being told to by this library.

The table carries only the two setting names the specification names. Adding a
setting means adding it here, beside the engines that use it, which is the one
place to look and the one place to change.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import Engine, Fault, Value

USABLE_SETTINGS: dict[Engine, frozenset[str]] = {
    "sqlite": frozenset({"busy_timeout_s"}),
    "postgresql": frozenset(),
    "turso": frozenset({"busy_timeout_s", "primary"}),
}


# BOTH HANDLERS TAKE ALL THREE, AND EACH READS WHAT IT NEEDS. A leading
# underscore marks the arguments a handler does not read, which is how this
# package has always written a dispatch pair: the table hands every row the
# same three values, and a row that used a different shape could not sit in it.
def _keep(
    _engine: Engine, settings: dict[str, Value], _unusable: list[str]
) -> dict[str, Value] | Fault:
    """Every setting is one this engine uses, so they all come back."""
    return settings


def _refuse(
    engine: Engine, _settings: dict[str, Value], unusable: list[str]
) -> dict[str, Value] | Fault:
    """Name the engine, name every setting it cannot use, and say what to
    write instead."""
    names = ", ".join(unusable)
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"You opened a {engine} database and set {names}. "
            f"{engine} has no use for {names}, so setting it would change nothing."
        ),
        what_to_do_instead=f"Write the call again without {names}.",
    )


_ANSWER: dict[bool, Callable[[Engine, dict[str, Value], list[str]], dict[str, Value] | Fault]] = {
    False: _keep,
    True: _refuse,
}


def refuse_unusable_settings(
    engine: Engine, settings: dict[str, Value]
) -> dict[str, Value] | Fault:
    """Refuse a setting this engine cannot use; hand back the rest unchanged.

    Reads the table above and nothing else. It opens no file, holds no
    connection and asks nothing of the clock, so the decision can be tested
    without a database.
    """
    usable = USABLE_SETTINGS[engine]
    unusable = [name for name in settings if name not in usable]
    return _ANSWER[bool(unusable)](engine, settings, unusable)
