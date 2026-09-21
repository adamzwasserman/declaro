"""How many drainers this database runs.

`docs/architecture.md:154`: a replicated database holds one connection by
construction, so N drainers there are N coroutines queued behind one lock. The
deployment asks for a number and that one rule overrides it, so a replicated
database runs one whatever it asked for, and a local database runs what it
asked for.

AN UNSET VARIABLE IS A CASE, NOT A DEFAULT. A local database with nothing asked
for has no number this function is entitled to invent: the right crew is a
property of the machine the deployment runs on, and four, or one, or the count
of its cores would each be a number nobody chose. So the call refuses, saying
what was not set. `crew_size` returns a plain `int` and the declaration gives it
no `Fault`, so the refusal is raised rather than returned. Its sibling
`permitted_connections` returns `int | Fault` and answers the same case the
other way; they are not the same function and do not answer alike.

A REPLICATED DATABASE WITH NOTHING ASKED FOR STILL RUNS ONE. The override does
not consult the deployment at all, so there is nothing missing to refuse over.

WHY A TABLE AND NOT A BRANCH. The rule is four rows of a truth table: whether
the database is replicated, and whether the deployment said anything. Written as
`if` it becomes three decisions made at run time; written as a dict it is the
rule itself, and a reader can see all four answers at once.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import NOTHING


def _one(_asked: int | None) -> int:
    """A replicated database holds one connection, so it runs one drainer.

    The underscore says this answer does not consult the deployment: one is what
    the engine holds, whatever was asked for.
    """
    return 1


def _what_the_deployment_asked(asked: int | None) -> int:
    """A local database runs the number it was given. `int` narrows the union;
    this entry of the table is only ever reached with a number in hand."""
    return int(asked)  # type: ignore[arg-type]


def _nothing_was_asked(_asked: int | None) -> int:
    raise ValueError(
        "no crew size was set, so this local database has no number of drainers "
        "to run. The right crew is a property of the machine, so set it in the "
        "deployment rather than letting this pick a number for you."
    )


# (replicated, nothing was asked) -> the answer
_THE_RULE: dict[tuple[bool, bool], Callable[[int | None], int]] = {
    (True, True): _one,
    (True, False): _one,
    (False, True): _nothing_was_asked,
    (False, False): _what_the_deployment_asked,
}


def crew_size(asked: int | None, replicated: bool) -> int:
    """Settle how many drainers this database runs.

    `asked` is what the deployment set, or `NOTHING` if it set nothing.
    """
    return _THE_RULE[(replicated, asked is NOTHING)](asked)
