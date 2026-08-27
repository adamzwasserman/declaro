"""A teardown that fails must reach the caller, not a log.

FOUR FUNCTIONS PUT `PRAGMA foreign_keys` BACK, and all four used to catch
their own failure, write an error to a log, and return a count. Every one of
the four callers discarded that count. So a connection left with foreign key
enforcement OFF was announced on a channel no caller can read and reported
through a value nobody looked at.

The reason none of them could simply raise is real: each runs in an `except`
or a `finally`, where raising replaces the failure already travelling. A
rebuild that failed on a bad column would be reported as "could not re-enable
foreign keys", which is a report about the wrong thing.

SO THE FAILURE IS RETURNED, and the caller puts it on the exception going out.
These tests hold that shape from both ends: the teardown hands back what
stopped it, and the exception the caller raises carries it.

WHY A NOTE AND NOT A WRAPPED EXCEPTION. `add_note` leaves the original
exception type intact, so `except MigrationError` still catches, and the text
prints in the traceback without any logging configuration being right.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pytest

from declaro_persistum.abstractions.pragma_compat import enforcement_restored
from declaro_persistum.exceptions import also_failed
from declaro_persistum.types import NOTHING, is_nothing

pytestmark = pytest.mark.precommit


class _RefusesThePragma:
    """A connection whose PRAGMA fails, which is the case that was silent.

    NOT A MOCK OF THE DATABASE. Nothing here stands in for a query result; the
    one call under test is the one that raises, and a real SQLite connection
    cannot be made to refuse `PRAGMA foreign_keys = ON` on demand.
    """

    def __init__(self, refuse_with: Exception) -> None:
        self._refuse_with = refuse_with

    async def execute(self, _sql: str, *_args: Any) -> Any:
        raise self._refuse_with


async def test_a_refused_pragma_comes_back_as_a_value() -> None:
    """The teardown hands its failure to the caller instead of a log."""
    refused = sqlite3.OperationalError("database is locked")

    came_back = await enforcement_restored(_RefusesThePragma(refused))

    assert came_back is refused


async def test_a_pragma_that_works_comes_back_as_nothing() -> None:
    """Success is NOTHING, so the caller can tell the two apart."""
    connection = await _a_real_sqlite_connection()

    came_back = await enforcement_restored(connection)

    assert is_nothing(came_back), came_back
    await connection.close()


async def test_the_failure_rides_out_on_the_exception_already_travelling() -> None:
    """The rebuild's own failure still leads, and the teardown rides along."""
    rebuild_failed = ValueError("no such column: nickname")

    also_failed(
        rebuild_failed,
        "putting foreign key enforcement back",
        sqlite3.OperationalError("database is locked"),
    )

    assert rebuild_failed.__notes__ == [
        "putting foreign key enforcement back ALSO failed: "
        "OperationalError: database is locked. That work was NOT done."
    ]


async def test_the_original_failure_is_never_replaced() -> None:
    """The type and message the caller catches on are untouched.

    A wrapped exception would change what `except MigrationError` catches, and
    a raised teardown failure would replace the message entirely. This is the
    property that made logging look like the only option.
    """
    rebuild_failed = ValueError("no such column: nickname")

    also_failed(rebuild_failed, "x", sqlite3.OperationalError("y"))

    assert type(rebuild_failed) is ValueError
    assert str(rebuild_failed) == "no such column: nickname"


async def test_a_teardown_that_worked_adds_nothing() -> None:
    """No note at all, rather than a note saying everything was fine."""
    rebuild_failed = ValueError("no such column: nickname")

    also_failed(rebuild_failed, "putting enforcement back", NOTHING)

    assert not hasattr(rebuild_failed, "__notes__")


async def _a_real_sqlite_connection() -> Any:
    import aiosqlite

    return await aiosqlite.connect(":memory:")
