"""How one engine writes one of the fifteen comparisons.

    fn how_to_compare : (operator: Operator, engine: Engine) -> Comparing | Fault

`docs/architecture.md:56` and `:66`: `OPERATORS` is one of the seven tables, and
it decides how each of the fifteen conditions renders. Reading it here is what
lets the renderer hold no operator of its own.

FOUR THINGS DIFFER AND THEY ARE THE FOUR FIELDS. The symbol, how many values go
beside it, the pattern a value is wrapped in where the engine matches part of
the text, and what the engine says instead when case does not matter.

WHY CASE IS A COLUMN RATHER THAN A SECOND OPERATOR.
`HOW-IT-SHOULD-WORK.md:143`: every text operator takes an `ignore_case`
companion, and the same `like` matched a name on one engine and nothing on
another, silently. Making the engines agree is the library's job. PostgreSQL
spells it `ILIKE`; SQLite and Turso fold case in `LIKE` already and need no
second word.

A PAIR NOBODY WROTE DOWN IS A FAULT, NOT A GUESS. Fifteen operators and three
engines is forty-five rows, and the sixteenth operator has to be written down
before it renders rather than falling through to whatever was nearest.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import Comparing, Engine, Fault, Operator

# HOW EVERY ENGINE WRITES EACH COMPARISON, before the one column any of them
# disagree about. All three speak the same SQL for these, which is why the table
# is shaped as one row per operator and one override per engine rather than as
# forty-five separate rows nobody could read.
_THE_SAME_EVERYWHERE: dict[Operator, Comparing] = {
    "eq": {"symbol": "=", "shape": "one", "wraps": "", "ignoring_case": ""},
    "ne": {"symbol": "!=", "shape": "one", "wraps": "", "ignoring_case": ""},
    "lt": {"symbol": "<", "shape": "one", "wraps": "", "ignoring_case": ""},
    "lte": {"symbol": "<=", "shape": "one", "wraps": "", "ignoring_case": ""},
    "gt": {"symbol": ">", "shape": "one", "wraps": "", "ignoring_case": ""},
    "gte": {"symbol": ">=", "shape": "one", "wraps": "", "ignoring_case": ""},
    "in": {"symbol": "IN", "shape": "list", "wraps": "", "ignoring_case": ""},
    "not_in": {"symbol": "NOT IN", "shape": "list", "wraps": "", "ignoring_case": ""},
    "between": {"symbol": "BETWEEN", "shape": "range", "wraps": "", "ignoring_case": ""},
    "like": {"symbol": "LIKE", "shape": "one", "wraps": "{}", "ignoring_case": "LIKE"},
    "contains": {"symbol": "LIKE", "shape": "pattern", "wraps": "%{}%", "ignoring_case": "LIKE"},
    "startswith": {"symbol": "LIKE", "shape": "pattern", "wraps": "{}%", "ignoring_case": "LIKE"},
    "endswith": {"symbol": "LIKE", "shape": "pattern", "wraps": "%{}", "ignoring_case": "LIKE"},
    "is_null": {"symbol": "IS NULL", "shape": "none", "wraps": "", "ignoring_case": ""},
    "is_not_null": {"symbol": "IS NOT NULL", "shape": "none", "wraps": "", "ignoring_case": ""},
}

# THE ONE COLUMN AN ENGINE DISAGREES ABOUT. PostgreSQL is case-sensitive in
# `LIKE` and has `ILIKE` for the other reading. SQLite and Turso fold ASCII case
# in `LIKE` already, so their answer is the same word and `HOW-IT-SHOULD-WORK.md`
# line 143's promise is kept without a second operator.
_WHAT_THIS_ENGINE_SAYS_INSTEAD: dict[Engine, dict[Operator, str]] = {
    "postgresql": dict.fromkeys(("like", "contains", "startswith", "endswith"), "ILIKE"),
    "sqlite": {},
    "turso": {},
}


def _the_row(operator: Operator, engine: Engine) -> Comparing | Fault:
    """The shared row, with this engine's own word where it has one."""
    return {
        **_THE_SAME_EVERYWHERE[operator],
        **{
            "ignoring_case": _WHAT_THIS_ENGINE_SAYS_INSTEAD[engine].get(
                operator, _THE_SAME_EVERYWHERE[operator]["ignoring_case"]
            )
        },
    }


def _no_row_for_that(operator: Operator, engine: Engine) -> Comparing | Fault:
    """An operator nobody wrote down, named rather than guessed at."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"There is no rule for writing {operator!r} on {engine!r}. This "
            f"library renders {len(_THE_SAME_EVERYWHERE)} comparisons and that "
            f"is not one of them."
        ),
        what_to_do_instead=(
            f"Narrow with one of {sorted(_THE_SAME_EVERYWHERE)}, or add a row "
            f"for {operator!r} so every engine renders it the same way."
        ),
    )


# Whether the operators table holds a row for this comparison. Both answers are
# written out, so a sixteenth operator is refused by a row rather than rendered
# as whatever the lookup happened to fall through to.
_WHETHER_IT_IS_WRITTEN_DOWN: dict[bool, Callable[[Operator, Engine], Comparing | Fault]] = {
    True: _the_row,
    False: _no_row_for_that,
}


def how_to_compare(operator: Operator, engine: Engine) -> Comparing | Fault:
    """Say how this engine writes this comparison."""
    return _WHETHER_IT_IS_WRITTEN_DOWN[operator in _THE_SAME_EVERYWHERE](operator, engine)
