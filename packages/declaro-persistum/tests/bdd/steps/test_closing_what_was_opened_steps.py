"""Steps for turso/closing_what_was_opened.feature.

THE COUNT IS THE CLAIM. A release that closed something and one that had
nothing to close must not look the same, so every scenario reads the number
back rather than checking that nothing raised.
"""

from __future__ import annotations

import asyncio
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, scenarios, then, when

from declaro_persistum.database import no_passes

scenarios("../features/turso/closing_what_was_opened.feature")


class _ARealFile:
    """A real aiosqlite connection to a real file, closed for real.

    Not a stand-in: `.close()` on it is the driver's close, and `is_shut` asks
    the connection rather than a flag this test set.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        self.conn: Any = None

    async def open(self) -> Any:
        import aiosqlite

        self.conn = await aiosqlite.connect(self.path)
        return self.conn

    async def is_shut(self) -> bool:
        try:
            await self.conn.execute("SELECT 1")
        except Exception:  # noqa: BLE001 - a closed connection refuses, which is the answer
            return True
        return False


class Ctx(TypedDict, total=False):
    file: Any
    closed: int


@pytest.fixture
def ctx() -> Ctx:
    return {"file": None, "closed": -1}


@given("a connection that is open")
def _given_open(ctx, tmp_path):
    ctx["file"] = _ARealFile(str(tmp_path / "t.db"))
    asyncio.run(ctx["file"].open())


@given("a replicated database holding a connection")
def _given_replica(ctx, tmp_path):
    ctx["file"] = _ARealFile(str(tmp_path / "t.db"))
    asyncio.run(ctx["file"].open())


@given("a database with no primary")
def _given_local(ctx):
    ctx["file"] = None


@when("it is closed")
def _when_closed(ctx):
    from declaro_persistum.turso_database import _close_connection

    ctx["closed"] = asyncio.run(_close_connection(ctx["file"].conn))


@when("it is released")
def _when_released(ctx):
    from declaro_persistum.turso_database import _close_connection

    async def _go() -> int:
        if ctx["file"] is None:
            # A local file carries `no_passes` as its release, which is the
            # answer rather than an absence.
            return await no_passes({})  # type: ignore[arg-type]
        return await _close_connection(ctx["file"].conn)

    ctx["closed"] = asyncio.run(_go())


@then("one connection was closed")
def _then_one(ctx):
    assert ctx["closed"] == 1, ctx["closed"]


@then("no connection was closed")
def _then_none(ctx):
    assert ctx["closed"] == 0, (
        f"a database with nothing to release reported {ctx['closed']}, which a "
        f"caller cannot tell from a real close"
    )


@then("the connection is shut")
def _then_shut(ctx):
    assert asyncio.run(ctx["file"].is_shut()), (
        "the count said one closed and the connection still answers queries"
    )
