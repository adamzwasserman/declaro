"""The vocabulary. One word, one meaning, spelled by every dialect.

WHAT THIS IS. HOW-IT-SHOULD-WORK.md names the things a caller can ask for.
This module is those things as signatures. Each dialect -- pseudo-SQL, Django,
Prisma, SQLAlchemy -- is a grammar over these, so a word means the same in all
four and there is one place that decides what it means.

NOTHING HERE IS BUILT. Every function raises, deliberately and by name. A body
that returned an empty list or a zero would make its feature green while the
behaviour did not exist, which is the exact failure the feature files are there
to prevent. A red test is the honest state of unbuilt work.

Each function has exactly one feature file under `tests/bdd/features/asking/`,
naming it, describing what it means. The bijection is the point: a function
with no scenario is code nothing describes, and a scenario with no function
describes code that does not exist.

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
from typing import Any, NoReturn

from declaro_persistum.database import Database

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


def _unbuilt(name: str, *given: Any) -> NoReturn:
    """Refuse by name, so a caller learns what is missing rather than a shape.

    NOT A STUB THAT PASSES. Returning `[]` or `0` here would turn every
    feature green and assert that unbuilt behaviour works.

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


# --------------------------------------------------------------------- rows


async def all(db: Database, model: type, **narrowed: Any) -> list[Any]:
    """Every row of this model the narrowing names, as instances of it."""
    _unbuilt("all", db, model, narrowed)


async def one(db: Database, model: type, **narrowed: Any) -> Any:
    """Exactly one row, or a named absence when there is none."""
    _unbuilt("one", db, model, narrowed)


async def first(db: Database, model: type, order: list[str], **narrowed: Any) -> Any:
    """The first row in a stated order, or a named absence.

    `order` is required. "First" with no order is whichever row the engine
    happened to reach, which is not an answer a caller can rely on.
    """
    _unbuilt("first", db, model, order, narrowed)


async def page(db: Database, model: type, size: int, number: int, **narrowed: Any) -> list[Any]:
    """One page of rows, counting pages from one."""
    _unbuilt("page", db, model, size, number, narrowed)


async def ordered_by(db: Database, model: type, order: list[str], **narrowed: Any) -> list[Any]:
    """Every matching row, in the order named. A leading minus reverses."""
    _unbuilt("ordered_by", db, model, order, narrowed)


async def exists(db: Database, model: type, **narrowed: Any) -> bool:
    """Whether any matching row exists. Reads no rows to answer."""
    _unbuilt("exists", db, model, narrowed)


# ----------------------------------------------------------- numbers over rows


async def count(db: Database, model: type, **narrowed: Any) -> int:
    """How many rows match. Returns the number, not a row carrying it."""
    _unbuilt("count", db, model, narrowed)


# FIVE QUESTIONS ABOUT ONE COLUMN, one shape, and the words are the only
# difference. `query/table.py` already made this call for the aggregate
# factories -- `sum_, avg_, min_, max_ = (_aggregate(word) for word in
# _AGGREGATES)` -- and this follows it. The cost is real: a reader grepping for
# `def sum` finds a table row instead of a signature. The row carries the
# docstring, so `help(asking.sum)` still answers.
_OVER_A_COLUMN: dict[str, str] = {
    "sum": "The total of one column over the matching rows.",
    "average": "The mean of one column over the matching rows.",
    "smallest": "The lowest value of one column over the matching rows.",
    "largest": "The highest value of one column over the matching rows.",
    "count_distinct": "How many different values one column holds over the matching rows.",
}


def _over_a_column(word: str) -> Callable[..., Any]:
    """One question about one column, named for the word a caller writes."""

    async def asked(db: Database, model: type, column: str, **narrowed: Any) -> Any:
        _unbuilt(word, db, model, column, narrowed)

    asked.__name__ = word
    asked.__qualname__ = word
    asked.__doc__ = _OVER_A_COLUMN[word]
    return asked


sum, average, smallest, largest, count_distinct = (_over_a_column(word) for word in _OVER_A_COLUMN)


# TWO QUESTIONS ABOUT SEVERAL COLUMNS, the same way.
_OVER_COLUMNS: dict[str, str] = {
    "values": "Only the named columns, as mappings rather than model instances.",
    "distinct": "The named columns with duplicate combinations removed.",
}


def _over_columns(word: str) -> Callable[..., Any]:
    """One question about a list of columns."""

    async def asked(
        db: Database, model: type, columns: list[str], **narrowed: Any
    ) -> list[dict[str, Any]]:
        _unbuilt(word, db, model, columns, narrowed)

    asked.__name__ = word
    asked.__qualname__ = word
    asked.__doc__ = _OVER_COLUMNS[word]
    return asked


values, distinct = (_over_columns(word) for word in _OVER_COLUMNS)


# ------------------------------------------------------------------ grouping


async def grouped_by(
    db: Database, model: type, columns: list[str], measures: dict[str, Any], **narrowed: Any
) -> list[dict[str, Any]]:
    """One row per group, carrying the group's columns and its measures."""
    _unbuilt("grouped_by", db, model, columns, measures, narrowed)


async def having(
    db: Database,
    model: type,
    columns: list[str],
    measures: dict[str, Any],
    kept: dict[str, Any],
    **narrowed: Any,
) -> list[dict[str, Any]]:
    """Groups narrowed by their measures. `where` narrows rows; this narrows groups."""
    _unbuilt("having", db, model, columns, measures, kept, narrowed)


# ------------------------------------------------------------------- writing


async def add(db: Database, declared: Any) -> int:
    """Declare that a row, or a list of rows, exists. A list lands together."""
    _unbuilt("add", db, declared)


async def change(db: Database, instance: Any, **new_values: Any) -> int:
    """Declare new values for one row this database already holds."""
    _unbuilt("change", db, instance, new_values)


async def change_all(db: Database, model: type, narrowed: dict[str, Any], **new_values: Any) -> int:
    """Declare new values for every matching row. Reports how many changed."""
    _unbuilt("change_all", db, model, narrowed, new_values)


async def remove(db: Database, instance: Any) -> int:
    """Declare that one row no longer exists."""
    _unbuilt("remove", db, instance)


async def remove_all(db: Database, model: type, narrowed: dict[str, Any]) -> int:
    """Declare that every matching row no longer exists."""
    _unbuilt("remove_all", db, model, narrowed)


async def replace(db: Database, instance: Any) -> int:
    """Declare that this row exists with these values, whatever was there."""
    _unbuilt("replace", db, instance)


# -------------------------------------------------------------- across models


async def of(db: Database, model: type, related: Any, **narrowed: Any) -> list[Any]:
    """The rows of `model` that belong to `related`, by a declared reference.

    The foreign key already says the two are related, so the caller names the
    two things and the library works out the join.
    """
    _unbuilt("of", db, model, related, narrowed)
