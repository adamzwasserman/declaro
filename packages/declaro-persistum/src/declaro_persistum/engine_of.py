"""`engine_of`: which engine a target names.

A string in, one of three words out, or a refusal naming the three. It opens
nothing, sends nothing and asks nobody, so a test can call it in a loop and on
a target pointing at a host that does not exist.

WHY IT IS NOT PART OF THE DOOR. Opening a database does I/O and deciding which
engine a target names does not. Kept in one function, every test of the
decision would need a database, and the decision is exactly the part worth
testing cheaply.

IT NEVER READS A CONNECTION, and `docs/architecture.md` refuses that in its own
words: a driver connection does not say which engine it belongs to, and the
closest you can get is where its class happens to live. That makes the SQL this
package generates depend on somebody else's file layout, so the driver
reorganizes its own package and the placeholders change with nothing here
having moved.

THE SCHEME IS THE WHOLE DECISION, AND THE FILE EXTENSION IS NOT PART OF IT.
`notes.sqlite3` and `notes.txt` both answer sqlite, because what makes a bare
path SQLite is that it names no scheme, not that it ends in `.db`. A function
that read the extension would also have to say what `data` and `backup.tar` are,
and it would have to touch the disk to find out whether either exists.

WHY THERE IS NOT A SINGLE BRANCH HERE. Every decision is a lookup in a table
keyed by a value, so a scheme nobody has thought of yet is a row and not an
edit, and no arm of any decision can go untaken and unseen. The one cost is on
the last line: `.get(scheme, _refused(scheme))` builds a refusal on every call
and throws it away on a hit. That is the price of not branching, and it is one
small dict.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import Engine, Fault

# WHAT IN A TARGET NAMES EACH ENGINE, straight from `set engines` in
# `docs/design/persistum.hd`. The empty key is a bare path: `"app.db"` carries
# no `://`, and only SQLite is a bare path.
_ENGINE_BY_SCHEME: dict[str, Engine] = {
    "": "sqlite",
    "sqlite": "sqlite",
    "postgresql": "postgresql",
    "postgres": "postgresql",
    "libsql": "turso",
}

# A SCHEME THAT SAYS HOW A TARGET TRAVELS AND NOT WHAT IS AT THE OTHER END.
# `https://team-app.turso.io` looks like Turso to a reader who already knows it
# is Turso, and looks like a web address to everyone else. Guessing one of the
# three here would be right often enough to be trusted and wrong silently.
_SAYS_NOTHING_ABOUT_AN_ENGINE = frozenset({"http", "https", "ws", "wss", "ftp", "file"})

# THE SCHEMES A CALLER MAY WRITE, spelled out once and quoted by all three
# refusals, so what_to_do_instead says what to write rather than repeating what
# went wrong.
_WRITE_ONE_OF_THESE = (
    "Write a target one of the three engines reads: a bare path such as app.db, "
    "or sqlite:///app.db, for sqlite; postgresql://host/name or postgres://host/name "
    "for postgresql; libsql://name.turso.io for turso."
)


def _nothing_was_named(_target: str) -> Fault:
    """An empty string names no engine and no database."""
    return {
        "kind": "refused",
        "what_went_wrong": "A target is required and this one is empty, so there is "
        "nothing in it to name an engine: not a scheme, not a path.",
        "what_to_do_instead": _WRITE_ONE_OF_THESE,
    }


def _no_engine_reads_that_scheme(scheme: str) -> Fault:
    """A scheme spelled plainly, and none of the three answers to it."""
    return {
        "kind": "refused",
        "what_went_wrong": f"No engine here reads a {scheme} target. This package "
        "opens sqlite, postgresql and turso, and nothing else.",
        "what_to_do_instead": _WRITE_ONE_OF_THESE,
    }


def _not_enough_to_name_an_engine(scheme: str) -> Fault:
    """A scheme that says how the target travels, not what is at the far end."""
    return {
        "kind": "refused",
        "what_went_wrong": f"A {scheme} target is ambiguous: an {scheme} URL is not "
        f"enough to name an engine, because {scheme} says how the target travels and "
        "not what is at the other end. Reading it as turso, or as anything else, "
        "would be a guess.",
        "what_to_do_instead": _WRITE_ONE_OF_THESE,
    }


# WHICH REFUSAL A SCHEME EARNS. Two refusals rather than one, because "nobody
# supports mysql" and "https could be any of them" send a caller to different
# fixes, and one message covering both would send them to neither.
_REFUSAL: dict[bool, Callable[[str], Fault]] = {
    True: _not_enough_to_name_an_engine,
    False: _no_engine_reads_that_scheme,
}


def _refused(scheme: str) -> Fault:
    """The refusal this scheme earns."""
    return _REFUSAL[scheme in _SAYS_NOTHING_ABOUT_AN_ENGINE](scheme)


# HOW A SCHEME IS READ OUT OF A TARGET. `str.partition` hands back the separator
# it found, which is `""` when there was none, so the two readings are a table
# keyed by that separator rather than a test of whether it is there.
_SCHEME_WHEN: dict[str, Callable[[str], str]] = {
    "": lambda _head: "",
    "://": lambda head: head.strip().lower(),
}


def _what_the_scheme_names(target: str) -> Engine | Fault:
    """The engine a target's scheme names, or the refusal it earns."""
    head, separator, _rest = target.partition("://")
    scheme = _SCHEME_WHEN[separator](head)
    return _ENGINE_BY_SCHEME.get(scheme, _refused(scheme))


# WHETHER THERE IS A TARGET AT ALL. An empty target and a bare path both read as
# the empty scheme, and they are not the same answer, so emptiness is settled
# first and by its own table.
_READ_THE_TARGET: dict[bool, Callable[[str], Engine | Fault]] = {
    True: _nothing_was_named,
    False: _what_the_scheme_names,
}


def engine_of(target: str) -> Engine | Fault:
    """Name the engine a target names, or refuse and say which three exist.

    It opens nothing, sends nothing, reads no clock and reads no file. Given the
    same string it gives the same word, on a machine with no database on it and
    for a host that does not resolve.
    """
    return _READ_THE_TARGET[target.strip() == ""](target)
