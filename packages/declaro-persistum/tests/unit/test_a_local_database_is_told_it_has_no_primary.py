"""Asking a local-only database to replicate is refused, and nothing said so.

`replicate` used to return True for a database with no primary, and its own
docstring said what that meant: "true by vacuity rather than by success". A
caller cannot tell that apart from a replication that worked, so it was
changed to raise. Nothing tested the raise.

MEASURED 2026-08-19: no test asserts it, and the three inert callables the
local openers install for this case are reported dark by coverage. That is
correct rather than missing. `replicate` refuses before it reaches them, and
they exist so the `Database` record holds a function rather than a None that
every caller would have to check. A function that is never called is cheaper
than a None that is.

WHAT THIS PINS is the pair: the refusal at the boundary, and the fact that the
inert callables are still there and still safe if something ever does reach
them. Removing either half leaves the other looking correct on its own.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum import close, open_sqlite
from declaro_persistum.database import (
    is_replicated,
    replicate,
    replicate_if_idle,
)

pytestmark = [pytest.mark.precommit, pytest.mark.sqlite]


async def _local() -> Any:
    return await open_sqlite(
        os.path.join(tempfile.mkdtemp(), "l.db"),
        shutdown="exit_immediately",
        busy_timeout_s=5.0,
    )


async def test_a_local_database_reports_no_primary() -> None:
    """The predicate the rest of the library branches on."""
    database = await _local()
    try:
        assert is_replicated(database) is False
    finally:
        await close(database)


async def test_asking_it_to_replicate_raises() -> None:
    database = await _local()
    try:
        with pytest.raises(ValueError):
            await replicate(database)
    finally:
        await close(database)


async def test_the_refusal_names_the_database_and_what_to_do() -> None:
    """A refusal a caller cannot act on is a crash with better manners."""
    database = await _local()
    try:
        with pytest.raises(ValueError) as caught:
            await replicate(database)

        message = str(caught.value)
        assert database["path"] in message, message
        assert "primary" in message, message
    finally:
        await close(database)


async def test_the_opportunistic_pass_declines_instead_of_raising() -> None:
    """The two are asked different questions and answer differently.

    `replicate` is a caller stating a requirement, so a database that cannot
    meet it must say so. `replicate_if_idle` runs on a timer and asks whether
    a pass is worth doing, and "no" is an ordinary answer there.
    """
    database = await _local()
    try:
        assert await replicate_if_idle(database) is False
    finally:
        await close(database)


async def test_the_inert_callables_are_present_and_safe() -> None:
    """The other half. They are unreachable, not absent.

    A missing entry would make every caller check before calling, which is
    the shape the record was built to avoid.

    THEY REPORT HOW MANY PASSES THEY RAN, and for a database with no primary
    that is none. It used to be `None`, which made the implementation a
    function with an empty body: a scanner could not tell it from code
    nobody had written, and a reader could not tell it from a line somebody
    had deleted. Zero says the same thing and says it on purpose.
    """
    database = await _local()
    try:
        assert callable(database["replicate_once"])
        assert callable(database["refresh_once"])

        assert await database["replicate_once"](database) is True
        assert await database["refresh_once"](database) == 0, (
            "a database with no primary pulled something"
        )
    finally:
        await close(database)
