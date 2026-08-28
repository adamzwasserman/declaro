"""Steps for replication/provisioning_a_copy.feature: the real pyturso driver.

WHAT THIS REPLACES. One scenario replaced `turso.aio.sync` wholesale with a
stand-in and then asserted the stand-in had been handed the path. The stand-in
accepted any path a caller passed.

The real driver does not, and the very first run against it failed on the path
shape the old scenario used: a file two folders deep, with neither folder
there. The stand-in had been hiding that since it was written.
"""

from __future__ import annotations

import asyncio
import os
import pathlib
import tempfile
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, scenarios, then, when

pytestmark = pytest.mark.network

scenarios("../features/replication/provisioning_a_copy.feature")


class Ctx(TypedDict, total=False):
    path: pathlib.Path
    before: list[str]
    after: list[str]


@pytest.fixture
def ctx() -> Ctx:
    return {"path": pathlib.Path(), "before": [], "after": []}


def _the_primary() -> tuple[str, str]:
    url = os.environ.get("TEST_TURSO_URL", "")
    token = os.environ.get("TEST_TURSO_AUTH_TOKEN", "")
    assert url and token, "TEST_TURSO_URL and TEST_TURSO_AUTH_TOKEN must be set"
    return url, token


async def _tables_on_the_primary() -> list[str]:
    """What the primary holds, read through a throwaway copy of its own."""
    import turso.aio.sync

    url, token = _the_primary()
    scratch = pathlib.Path(tempfile.mkdtemp()) / "peek.db"
    conn = await turso.aio.sync.connect(
        str(scratch), remote_url=url, auth_token=token
    )
    try:
        await conn.pull()
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        return sorted(row[0] for row in await cursor.fetchall())
    finally:
        await conn.close()


@given("a path in a folder that exists, with no copy at it yet")
def _given_folder_exists(ctx, tmp_path):
    ctx["path"] = tmp_path / "app.db"
    assert not ctx["path"].exists()


@given("a path whose folder does not exist")
def _given_no_folder(ctx, tmp_path):
    ctx["path"] = tmp_path / "tenants" / "acme" / "app.db"
    assert not ctx["path"].parent.exists()


@when("a copy is asked for at that path")
def _when_copy_asked(ctx):
    from declaro_persistum.turso_database import replicate_path

    url, token = _the_primary()

    async def _go() -> None:
        ctx["before"] = await _tables_on_the_primary()
        await replicate_path(str(ctx["path"]), primary=url, token=token)
        ctx["after"] = await _tables_on_the_primary()

    asyncio.run(_go())


@when("a replica is opened at that path")
def _when_replica_opened(ctx):
    from declaro_persistum import close, open_turso

    url, token = _the_primary()

    async def _go() -> None:
        db = await open_turso(
            str(ctx["path"]), primary=url, token=token, shutdown="exit_immediately"
        )
        await close(db)

    asyncio.run(_go())


@then("a copy is there")
def _then_copy_there(ctx):
    assert ctx["path"].exists(), (
        f"nothing was made at {ctx['path']}; a caller asked for a copy and "
        f"got no file"
    )


@then("the primary is unchanged")
def _then_primary_unchanged(ctx):
    assert ctx["before"] == ctx["after"], (
        f"asking for a local copy changed the primary: "
        f"{set(ctx['after']) ^ set(ctx['before'])}"
    )
