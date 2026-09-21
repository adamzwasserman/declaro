"""A materialized view has to be kept up to date, and the engine decides how.

WHAT A CALLER SAYS AND WHAT THIS ANSWERS. A caller says how fresh the view has
to be, in three words that are requirements: always current, current when read,
refreshed when asked. None of them is a mechanism. PostgreSQL has a real
materialized view and SQLite does not, and bridging that is this library's job
rather than the caller's, which is what this function is. It hands back the
operations that set the mechanism up.

IT IS TOLD WHAT THE ENGINE CAN DO; IT DOES NOT ASK. `capability_of` answers
whether an engine has a materialized view natively and what builds one where it
does not, and that answer arrives here as the third argument. So nothing below
reads a table of engines. The engine's name is used in two places only: in the
refusal, so a caller knows where the declaration failed, and in the detail, so
whoever applies the operations can see which engine they were picked for.

A REFUSAL COMES BACK IN THE RETURN. An engine told it has no materialized view
and nothing to build one from cannot meet any freshness, and saying so is the
whole answer. Quietly handing back a weaker mechanism would leave the caller
with a staler view than they declared and nothing would have said so. The
refusal is returned, never raised and never logged.

THREE TABLES, ALL TOTAL, NONE READ WITH A FALLBACK. `OFFERS` turns the two
booleans a capability amounts to into one of three words. `ANSWER` has a row
for each of those three. `MECHANISM` has a row for each of the six pairs of a
word that offers something and a freshness. Every lookup is a subscript, so a
key nobody wrote a row for is a `KeyError` at the lookup rather than a mechanism
written for some other combination. A freshness nobody declared is caught before
any of them, by `_DECLARED`, and comes back as a fault listing the three.

WHAT THIS FUNCTION IS NOT GIVEN, AND SO CANNOT SAY. The view is not an
argument, so no operation here can name the table it acts on. `table` is the
empty string, and whoever holds the `View` fills it in. That is a gap in the
declaration rather than a choice made here, and it is written down so nobody
reads an empty name as a default.

WHAT THE OPERATION VOCABULARY IS SHORT OF. `OperationKind` names twelve schema
changes and none of them is a trigger or a row inserted into the library's
registry, which are two of the three parts a built materialized view is made
of. So a built mechanism comes back as the one operation the vocabulary can
name, `create_table`, carrying the triggers and the registry row in its detail.
Splitting them into operations of their own needs two more words in the
vocabulary, and those are not ours to add here.

Pure: no clock, no random, no environment, no file, no connection.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import Capability, Engine, Fault, Freshness, Operation

# THE VIEW IS NOT AN ARGUMENT, SO NOTHING HERE KNOWS ITS NAME. Named rather than
# written as a bare "" at each site, so a reader meets the reason once.
NO_TABLE_NAMED = ""

# What a capability amounts to, as one word. Native wins where both are set,
# because an engine that has the thing does not need one built. Total over both
# booleans, so no capability can arrive that this does not have a word for.
OFFERS: dict[tuple[bool, bool], str] = {
    (True, True): "native",
    (True, False): "native",
    (False, True): "built",
    (False, False): "nothing",
}

# One row per pair of a capability that offers something and a freshness. Each
# says which operation kind carries the mechanism, and what the mechanism is in
# the words this function chose. Six rows, and all six differ: a caller who
# changes the freshness changes what the engine holds, which is the whole point
# of declaring it.
MECHANISM: dict[tuple[str, Freshness], dict[str, str]] = {
    ("native", "always current"): {
        "kind": "create_view",
        "is": (
            "a materialized view the engine holds itself, brought up to date "
            "inside every write to what it reads"
        ),
    },
    ("native", "current when read"): {
        "kind": "create_view",
        "is": (
            "a materialized view the engine holds itself, brought up to date before each read of it"
        ),
    },
    ("native", "refreshed when asked"): {
        "kind": "create_view",
        "is": (
            "a materialized view the engine holds itself, brought up to date "
            "only when a caller asks for a refresh"
        ),
    },
    ("built", "always current"): {
        "kind": "create_table",
        "is": (
            "a table holding the rows, triggers that update it inside every "
            "write to what it reads, and a registry row naming it a view"
        ),
    },
    ("built", "current when read"): {
        "kind": "create_table",
        "is": (
            "a table holding the rows, triggers that mark it stale, a rebuild "
            "before each read of it, and a registry row naming it a view"
        ),
    },
    ("built", "refreshed when asked"): {
        "kind": "create_table",
        "is": (
            "a table holding the rows, a rebuild that runs only when a caller "
            "asks, and a registry row naming it a view"
        ),
    },
}

# The three words a caller may write. Read off the table above rather than typed
# out again, so the two cannot drift apart.
_DECLARED: tuple[str, ...] = tuple(dict.fromkeys(freshness for _offers, freshness in MECHANISM))


def _operations(freshness: Freshness, engine: Engine, capability: Capability) -> list[Operation]:
    """The operations that set the mechanism up on an engine that has one."""
    offers = OFFERS[(capability["native"], bool(capability["built_from"].strip()))]
    mechanism = MECHANISM[(offers, freshness)]
    return [
        {
            "kind": mechanism["kind"],  # type: ignore[typeddict-item]
            "table": NO_TABLE_NAMED,
            "detail": {
                "is": mechanism["is"],
                "for_freshness": freshness,
                "on_engine": engine,
                # WHAT THE CAPABILITY SAID, CARRIED THROUGH UNCHANGED. It is
                # provenance for whoever applies this, and it is never what the
                # mechanism above was picked from.
                "built_from": capability["built_from"],
            },
            "destroys": False,
        }
    ]


def _refuse_engine(freshness: Freshness, engine: Engine, _capability: Capability) -> Fault:
    """Name the engine, name the freshness, and offer nothing weaker."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"You declared a materialized view {freshness} on {engine}. "
            f"{engine} has no materialized view of its own and nothing to build "
            f"one from, so nothing here can keep a view {freshness}."
        ),
        what_to_do_instead=(
            f"Declare the view on an engine that can hold one, or drop the "
            f"materialized view and query the tables directly. Nothing staler "
            f"than {freshness} is being set up in its place."
        ),
    )


def _refuse_freshness(freshness: Freshness, _engine: Engine, _capability: Capability) -> Fault:
    """Name the word that arrived, and list the three that exist."""
    exist = ", ".join(_DECLARED)
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"You declared a materialized view {freshness!r}, which is not one "
            f"of the freshnesses this library knows. The ones that exist are: "
            f"{exist}."
        ),
        what_to_do_instead=f"Write the declaration again with one of: {exist}.",
    )


# WHAT A CAPABILITY'S WORD MEANS HERE. Two of the three build something and the
# third refuses, and all three take the same arguments, so a row that used a
# different shape could not sit in this table.
ANSWER: dict[str, Callable[[Freshness, Engine, Capability], list[Operation] | Fault]] = {
    "native": _operations,
    "built": _operations,
    "nothing": _refuse_engine,
}

# A WORD NOBODY DECLARED IS ANSWERED BEFORE ANY TABLE IS READ. Indexed by
# whether the freshness is one of the three, which is the one question that has
# to come first: every table below is keyed by a declared freshness.
_ON_FRESHNESS: dict[bool, Callable[[Freshness, Engine, Capability], list[Operation] | Fault]] = {
    True: lambda freshness, engine, capability: ANSWER[
        OFFERS[(capability["native"], bool(capability["built_from"].strip()))]
    ](freshness, engine, capability),
    False: _refuse_freshness,
}


def mechanism_for(
    freshness: Freshness, engine: Engine, capability: Capability
) -> list[Operation] | Fault:
    """Pick the mechanism that gives a materialized view the freshness asked for.

    Reads the tables above and nothing else. It opens no file, holds no
    connection and asks nothing of the clock, so the decision can be tested
    without a database.
    """
    return _ON_FRESHNESS[freshness in _DECLARED](freshness, engine, capability)
