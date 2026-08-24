"""One function that renders SQL, over a table of statement templates.

WHAT THIS REPLACES. Every operation had its own generator function, and there
were two full sets of them, one for PostgreSQL and one shared by SQLite and
Turso. Thirty-two functions to write sixteen statements twice. Each function
did two jobs at once: work out its optional pieces, then paste them into text.
Mixing those meant neither half could become data, so the SQL was scattered
through code and nothing could compare one engine's spelling with another's.

The two jobs are separate here. A row carries the text and the names the text
fills from, on one line, so a template and its parameter list cannot drift
apart. `render` looks the row up and fills it. That is the whole renderer.

WHY THE NAMES ARE ON THE SAME LINE AS THE TEXT. `Operation.details` was typed
`dict[str, Any]`, so nothing recorded what any operation carried. One part of
the package filled those bags and three parts read them, on the honour system.
A row's names ARE that record, and they are checked on every render:
a template asking for a name the row does not declare fails here, at the table,
rather than as a KeyError inside a migration.

WHAT IS NOT A TEMPLATE. Three things resist, and each is a supervisor that
composes several renders rather than a special case bolted onto one:

  alter_column      one statement per property that changed
  create_view       a materialized view is emulated, which is several
  drop_view         the same, in reverse

They live in `COMPOSERS`, and they call `render` like anyone else.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypedDict

from declaro_persistum.boundary import helper
from declaro_persistum.types import NOTHING, Dialect, Nothing, Operation

# A reader takes one named value out of the operation it is given. It is the
# only thing that knows where in `details` a value lives, which is why the
# templates carry no navigation at all.
Reader = Callable[[Operation], Any]


class Rendering(TypedDict):
    """One statement: its text, the names it fills from, and how many.

    A NAME COMES FROM ONE OF TWO PLACES, and the row says which. `reads` are
    taken out of the operation, each by the reader beside it. `given` are the
    ones a supervisor works out and passes in, because there is nowhere in the
    operation to read them from: which of PostgreSQL's `alter_column`
    statements runs depends on what changed, so `rendering` decides. Both are
    declared and `render` requires every one of them.

    `repeat` names a value that is a list. The template is rendered once per
    item, with that name bound to the item rather than the list. It exists for
    `insert_enum_values`, which writes one INSERT per declared value, and it is
    a field rather than a second table because the alternative is a separate
    mechanism for one operation.
    """

    template: str
    reads: dict[str, Reader]
    given: frozenset[str]
    repeat: str | Nothing


# =============================================================================
# The readers
# =============================================================================
#
# Six of them, and they are the entire vocabulary a row can use to reach into
# an operation. A seventh would need a reason; a bespoke one per operation is
# what this module exists to remove.


@helper
def the_table() -> Reader:
    """The table the operation names."""
    return lambda operation: operation["table"]


@helper
def detail(key: str) -> Reader:
    """A value the operation must carry. Absent is a KeyError, not a blank."""
    return lambda operation: operation["details"][key]


@helper
def within(outer: str, key: str, absent: Any) -> Reader:
    """A value nested inside one of the operation's details.

    `absent` is required and has no default. An index has no WHERE clause more
    often than it has one, and what a missing clause renders as differs per
    field: an empty string for a fragment, an empty list for columns.
    """
    return lambda operation: operation["details"][outer].get(key, absent)


@helper
def flag(outer: str, key: str, text: str) -> Reader:
    """A keyword that is present or is nothing at all, like UNIQUE."""
    return lambda operation: text * bool(operation["details"][outer].get(key))


@helper
def clause(outer: str, key: str, keyword: str) -> Reader:
    """An optional clause, written with its keyword, or nothing at all.

    `f" WHERE {x}"` when x is there and `""` when it is not, which was written
    out by hand beside every optional clause in every generator.
    """
    return lambda operation: "".join(
        f"{keyword}{value}"
        for value in [operation["details"][outer].get(key)]
        if value
    )


@helper
def built_by(
    build: Callable[[Operation, Dialect], Any],
) -> Callable[[Dialect], Reader]:
    """A value that has to be assembled, like a CREATE TABLE's body.

    The assembly is a function because its length depends on the input: one
    clause per column, and the column count is not known here. It is still
    only one of the names the template fills from.
    """
    return lambda dialect: lambda operation: build(operation, dialect)


# =============================================================================
# Rendering
# =============================================================================


@helper
def _refuse_undeclared_names(
    op_type: str, declared: set[str], given: dict[str, Any]
) -> dict[str, Any]:
    """A name the row does not declare, refused where the row is.

    This is the check that the old shape could not perform. A caller that
    passed `from_col` where the template wanted `from_column` produced SQL
    with a literal `{from_column}` in it, or a KeyError from `str.format`
    naming neither the operation nor the engine.
    """
    raise KeyError(
        f"{op_type} takes {sorted(declared)}. "
        f"It was given {sorted(given)}: "
        f"{sorted(set(given) - declared)} unknown, "
        f"{sorted(declared - set(given))} missing."
    )


@helper
def _the_checked_names(
    _op_type: str, _declared: set[str], given: dict[str, Any]
) -> dict[str, Any]:
    """The names given are exactly the ones declared, so they are the answer.

    BOTH ENTRIES RETURN THE VALUES. This used to return `None` and be called
    for its refusal alone, which reads as a function nobody finished: an empty
    body hands `None` back to a caller that cannot tell "not written" from
    "nothing to do".
    """
    return given


_WHEN_NAMES_MATCH: dict[
    bool, Callable[[str, set[str], dict[str, Any]], dict[str, Any]]
] = {
    True: _the_checked_names,
    False: _refuse_undeclared_names,
}


@helper
def render(op_type: str, rendering: Rendering, **named: Any) -> list[str]:
    """Fill one row's template with the values given, by name.

    THE SIGNATURE IS `**named` DELIBERATELY. A dict of details is a bag that
    travels through the package without anyone having to say what is in it,
    which is how the same bag came to be read three different ways. Named
    arguments are checked against the row's own list at the point of use.
    """
    declared = set(rendering["reads"]) | rendering["given"]
    checked = _WHEN_NAMES_MATCH[set(named) == declared](op_type, declared, named)

    return _REPEATED[rendering["repeat"] is not NOTHING](rendering, checked)


@helper
def _rendered_once(rendering: Rendering, named: dict[str, Any]) -> list[str]:
    """One statement, which is what fifteen of the eighteen rows are."""
    return [rendering["template"].format(**named)]


@helper
def _rendered_per_item(rendering: Rendering, named: dict[str, Any]) -> list[str]:
    """One statement per item of the repeated name.

    A LIST, NEVER A JOINED STRING. These used to be joined with `;` and split
    apart again by the appliers, and a semicolon inside a declared value cut a
    statement in half. Nothing between here and the engine has to know what a
    semicolon means.
    """
    over = str(rendering["repeat"])
    return [
        rendering["template"].format(**{**named, over: item})
        for item in named[over]
    ]


_REPEATED: dict[bool, Callable[[Rendering, dict[str, Any]], list[str]]] = {
    False: _rendered_once,
    True: _rendered_per_item,
}


@helper
def filled(rendering: Rendering, operation: Operation) -> dict[str, Any]:
    """Read every name a row declares out of the operation it describes.

    This is the supervisor's half: the row says what it needs, the fillers say
    where each one lives, and the operation is read once. Nothing here knows
    what any particular operation means.

    The row's `given` names are not read here. A supervisor works those out
    and passes them to `render` itself.
    """
    return {name: read(operation) for name, read in rendering["reads"].items()}


@helper
def rendered(op_type: str, rendering: Rendering, operation: Operation) -> list[str]:
    """One operation, read and filled and rendered."""
    return render(op_type, rendering, **filled(rendering, operation))
