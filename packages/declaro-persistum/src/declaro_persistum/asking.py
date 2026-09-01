"""The vocabulary. One word, one meaning, spelled by every grammar.

WHAT THIS IS. HOW-IT-SHOULD-WORK.md names the things a caller can ask for.
This module is those things as signatures. Each grammar -- pseudo-SQL, Django,
Prisma, SQLAlchemy -- is a way of phrasing these, so a word means the same in
all four and there is one place that decides what it means.

NOTHING HERE IS BUILT. Every function raises, deliberately and by name. A body
that returned an empty list or a zero would make its feature green while the
behavior did not exist, which is the exact failure the feature files are there
to prevent. A red test is the honest state of unbuilt work.

Each function has exactly one feature file under `tests/bdd/features/asking/`,
naming it, describing what it means. The bijection is the point: a function
with no scenario is code nothing describes, and a scenario with no function
describes code that does not exist.

WORDS THAT SHARE A SHAPE SHARE A FUNCTION, and what separates them is a row in
a table. Four questions about rows do the same three things -- build a question
from a model and a narrowing, run it, shape what comes back -- and differ by
what the question asks for and which key of the answer it fills. Written as
four functions, those are four paths that have to agree; written as one
function over four rows, there is nothing to disagree about. Where a word is
alone in its shape it stays a plain function, because a table with one row
tabulates nothing.

THE COST IS REAL AND IT IS PAID KNOWINGLY. A reader grepping for `def count`
finds a table row rather than a signature, and mypy checks the generated
signature once rather than per word. The row carries the docstring, so
`help(asking.count)` still answers, and the return type is checked for all of
them because every word returns the same declared `Answer`.

EVERY WORD GIVES BACK AN `Answer`, six keys, of which a word fills one. The
table says which. The other five are `NOTHING`, so a caller cannot walk past an
absent row or a failure by mistake, and `failure` is a key rather than an
exception because whether anyone hears about a failure is the orchestrator's
decision and not this module's.

NONE OF THEM CARRIES `@boundary` YET, and that is deliberate. Each one will do
I/O when it is built, but today each one raises, so the marker would declare an
edge that is not there. `tests/unit/test_a_boundary_marker_names_a_real_edge.py`
caught exactly that: 23 markers on functions with no I/O in them. The marker
goes on when the I/O does.

THE SHAPE EVERY ONE OF THEM TAKES. A `Database` the caller declared, a model
carrying `@table`, and columns the model declares. No SQL text, no connection,
no engine name -- which is the test in the document, applied to this module
before it is written rather than after.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, NoReturn, TypedDict

from declaro_persistum.database import Database
from declaro_persistum.types import Answer, AnswerKey, Direction

__all__ = [
    "add",
    "all",
    "average",
    "change",
    "change_all",
    "count",
    "count_distinct",
    "distinct",
    "exists",
    "first",
    "grouped_by",
    "having",
    "largest",
    "of",
    "one",
    "ordered_by",
    "page",
    "remove",
    "remove_all",
    "replace",
    "smallest",
    "sum",
    "values",
]


class _Word(TypedDict):
    """What one word in a shared shape carries of its own.

    `means` is its docstring, and `fills` is the key of the `Answer` it
    answers into. Those two are the whole difference between the words in a
    table, which is why they are the whole row.
    """

    means: str
    fills: AnswerKey


def _unbuilt(name: str, *given: Any) -> NoReturn:
    """Refuse by name, so a caller learns what is missing rather than a shape.

    NOT A STUB THAT PASSES. Returning `[]` or `0` here would turn every
    feature green and assert that unbuilt behavior works.

    IT TAKES THE CALL'S OWN ARGUMENTS, and not to suppress a lint rule. A
    declared signature nothing reads is a signature nobody has checked against
    a caller; passing them here means the parameter list is exercised the
    moment anyone calls it, and the refusal can say what it was handed.
    """
    raise NotImplementedError(
        f"{name} is declared in HOW-IT-SHOULD-WORK.md and is not built. "
        f"It was called with {len(given)} arguments. "
        f"Its feature file is tests/bdd/features/asking/{name}.feature."
    )


def _spelled(asked: Callable[..., Any], word: str, says: _Word) -> Callable[..., Any]:
    """Give a generated function the name and the meaning its row carries.

    Without this a caller sees `asked` in a traceback and `None` from
    `help()`, which would make the table cost a reader more than it saves.
    """
    asked.__name__ = word
    asked.__qualname__ = word
    asked.__doc__ = says["means"]
    return asked


# --------------------------------------------------------------------- rows

# FOUR QUESTIONS ABOUT ROWS. Each builds a question from the model and the
# narrowing, runs it, and shapes what comes back. What separates them is what
# the question asks the engine for and which key it answers into.
_ABOUT_ROWS: dict[str, _Word] = {
    "all": {
        "means": "Every row of this model the narrowing names, as instances of it.",
        "fills": "rows",
    },
    "one": {
        "means": "Exactly one row, or a named absence when there is none.",
        "fills": "row",
    },
    "exists": {
        "means": "Whether any matching row exists. Reads no rows to answer.",
        "fills": "truth",
    },
    "count": {
        "means": "How many rows match. Returns the number, not a row carrying it.",
        "fills": "number",
    },
}


def _about_rows(word: str) -> Callable[..., Any]:
    """One question about the rows a narrowing names."""

    async def asked(db: Database, model: type, **narrowed: Any) -> Answer:
        _unbuilt(word, db, model, narrowed)

    return _spelled(asked, word, _ABOUT_ROWS[word])


all, one, exists, count = (_about_rows(word) for word in _ABOUT_ROWS)


# TWO QUESTIONS THAT NEED AN ORDER, and it is required on both. "First" with no
# order is whichever row the engine happened to reach, which is not an answer a
# caller can rely on.
#
# AN ORDER IS A MAPPING, `{"name": "ascending"}`, and the keys break ties left
# to right. It was `["-name"]` until the audit noticed this function and
# `_over_columns` were one shape, which they were: `list[str]` twice. They are
# not one thing, and the reason they read alike is that `-name` is a column
# name carrying a directive rather than a column name. Declaring the direction
# separates them for the reason they are actually separate.
_IN_A_STATED_ORDER: dict[str, _Word] = {
    "first": {
        "means": "The first row in a stated order, or a named absence.",
        "fills": "row",
    },
    "ordered_by": {
        "means": "Every matching row, in the order named. Keys break ties left to right.",
        "fills": "rows",
    },
}


def _in_a_stated_order(word: str) -> Callable[..., Any]:
    """One question whose answer depends on an order the caller states."""

    async def asked(
        db: Database, model: type, order: dict[str, Direction], **narrowed: Any
    ) -> Answer:
        _unbuilt(word, db, model, order, narrowed)

    return _spelled(asked, word, _IN_A_STATED_ORDER[word])


first, ordered_by = (_in_a_stated_order(word) for word in _IN_A_STATED_ORDER)


async def page(db: Database, model: type, size: int, number: int, **narrowed: Any) -> Answer:
    """One page of rows, counting pages from one."""
    _unbuilt("page", db, model, size, number, narrowed)


# ----------------------------------------------------------- numbers over rows

# FIVE QUESTIONS ABOUT ONE COLUMN, one shape, and the words are the only
# difference. `query/table.py` already made this call for the aggregate
# factories -- `sum_, avg_, min_, max_ = (_aggregate(word) for word in
# _AGGREGATES)` -- and this follows it.
_OVER_A_COLUMN: dict[str, _Word] = {
    "sum": {
        "means": "The total of one column over the matching rows.",
        "fills": "number",
    },
    "average": {
        "means": "The mean of one column over the matching rows.",
        "fills": "number",
    },
    "smallest": {
        "means": "The lowest value of one column over the matching rows.",
        "fills": "number",
    },
    "largest": {
        "means": "The highest value of one column over the matching rows.",
        "fills": "number",
    },
    "count_distinct": {
        "means": "How many different values one column holds over the matching rows.",
        "fills": "number",
    },
}


def _over_a_column(word: str) -> Callable[..., Any]:
    """One question about one column, named for the word a caller writes."""

    async def asked(db: Database, model: type, column: str, **narrowed: Any) -> Answer:
        _unbuilt(word, db, model, column, narrowed)

    return _spelled(asked, word, _OVER_A_COLUMN[word])


sum, average, smallest, largest, count_distinct = (_over_a_column(word) for word in _OVER_A_COLUMN)


# TWO QUESTIONS ABOUT SEVERAL COLUMNS, the same way.
_OVER_COLUMNS: dict[str, _Word] = {
    "values": {
        "means": "Only the named columns, as mappings rather than model instances.",
        "fills": "mappings",
    },
    "distinct": {
        "means": "The named columns with duplicate combinations removed.",
        "fills": "mappings",
    },
}


def _over_columns(word: str) -> Callable[..., Any]:
    """One question about a list of columns."""

    async def asked(db: Database, model: type, columns: list[str], **narrowed: Any) -> Answer:
        _unbuilt(word, db, model, columns, narrowed)

    return _spelled(asked, word, _OVER_COLUMNS[word])


values, distinct = (_over_columns(word) for word in _OVER_COLUMNS)


# ------------------------------------------------------------------ grouping


async def grouped_by(
    db: Database, model: type, columns: list[str], measures: dict[str, Any], **narrowed: Any
) -> Answer:
    """One row per group, carrying the group's columns and its measures."""
    _unbuilt("grouped_by", db, model, columns, measures, narrowed)


async def having(
    db: Database,
    model: type,
    columns: list[str],
    measures: dict[str, Any],
    kept: dict[str, Any],
    **narrowed: Any,
) -> Answer:
    """Groups narrowed by their measures. `where` narrows rows; this narrows groups."""
    _unbuilt("having", db, model, columns, measures, kept, narrowed)


# ------------------------------------------------------------------- writing

# THREE DECLARATIONS ABOUT ONE ROW. Each says something the caller wants true
# of a row, and each reports how many rows it touched. `declared` is the row,
# or for `add` a list of them, and the word says what is being declared about
# it. Zero is an answer a caller acts on rather than an error.
_ABOUT_ONE_ROW: dict[str, _Word] = {
    "add": {
        "means": "Declare that a row, or a list of rows, exists. A list lands together.",
        "fills": "number",
    },
    "remove": {
        "means": "Declare that one row no longer exists.",
        "fills": "number",
    },
    "replace": {
        "means": "Declare that this row exists with these values, whatever was there.",
        "fills": "number",
    },
}


def _about_one_row(word: str) -> Callable[..., Any]:
    """One declaration about a row the caller hands over."""

    async def asked(db: Database, declared: Any) -> Answer:
        _unbuilt(word, db, declared)

    return _spelled(asked, word, _ABOUT_ONE_ROW[word])


add, remove, replace = (_about_one_row(word) for word in _ABOUT_ONE_ROW)


async def change(db: Database, instance: Any, **new_values: Any) -> Answer:
    """Declare new values for one row this database already holds."""
    _unbuilt("change", db, instance, new_values)


async def change_all(
    db: Database, model: type, narrowed: dict[str, Any], **new_values: Any
) -> Answer:
    """Declare new values for every matching row. Reports how many changed."""
    _unbuilt("change_all", db, model, narrowed, new_values)


async def remove_all(db: Database, model: type, narrowed: dict[str, Any]) -> Answer:
    """Declare that every matching row no longer exists."""
    _unbuilt("remove_all", db, model, narrowed)


# -------------------------------------------------------------- across models


async def of(db: Database, model: type, related: Any, **narrowed: Any) -> Answer:
    """The rows of `model` that belong to `related`, by a declared reference.

    The foreign key already says the two are related, so the caller names the
    two things and the library works out the join.
    """
    _unbuilt("of", db, model, related, narrowed)


# EVERY WORD AND THE KEY IT FILLS, in one place, so a test can read it and the
# builder can too. The tabulated words carry it in their rows; the seven that
# are alone in their shape carry it here, because a word's answer key is part
# of what it means whether or not it shares a function with anything.
FILLS: dict[str, AnswerKey] = {
    **{word: says["fills"] for word, says in _ABOUT_ROWS.items()},
    **{word: says["fills"] for word, says in _IN_A_STATED_ORDER.items()},
    **{word: says["fills"] for word, says in _OVER_A_COLUMN.items()},
    **{word: says["fills"] for word, says in _OVER_COLUMNS.items()},
    **{word: says["fills"] for word, says in _ABOUT_ONE_ROW.items()},
    "page": "rows",
    "grouped_by": "mappings",
    "having": "mappings",
    "change": "number",
    "change_all": "number",
    "remove_all": "number",
    "of": "rows",
}
