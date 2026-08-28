"""The connection Protocols must describe the drivers, not my memory of them.

MEASURED 2026-08-21. 158 parameters in `src/` are annotated `Any`, and 119 of
them are a database handle: 75% of every type escape in the package is one
missing declaration. `Any` declares an unbounded value space, and Umbra
derives its facets from the declared type, so each of those contributes input
regions no test can ever close. bulk_loader.py alone has 29 of them, 6
conditionals, and 141 of the package's 1383 unexercised input regions.

THERE ARE TWO SHAPES, NOT ONE, and reading the call sites rather than guessing
is what showed it. Every method actually invoked on a connection in this
package:

    execute 92, commit 17, fetch 13, close 8, rollback 7, fetchrow 3,
    fetchval 3, transaction 3, executemany 1, copy_records_to_table 1

asyncpg answers `fetch`, `fetchrow`, `fetchval` and `transaction`, and has no
cursor and no `commit`. aiosqlite and pyturso answer `execute` returning a
cursor, plus `commit` and `rollback`. One Protocol covering both would be so
loose it would say nothing.

WHY THIS FILE ASSERTS AGAINST LIVE CONNECTIONS. A Protocol I wrote from memory
would type the package against a driver that does not exist, and the mistake
would surface as a runtime AttributeError somewhere else entirely. This
package has already been bitten twice by a stub whose contract did not match
the library it stood in for. So each test here opens the real driver and asks
it.

Turso and PostgreSQL skip when unreachable, naming what they tried. SQLite
always runs.
"""

import os
import pathlib
import tempfile

import pytest

from declaro_persistum.types import (
    DbapiConnection,
    DbapiCursor,
    FetchingConnection,
)
from tests.conftest import probe_url, refuse_without_postgresql


@pytest.mark.sqlite
async def test_aiosqlite_is_a_dbapi_connection() -> None:
    """aiosqlite: execute returns the cursor directly."""
    import aiosqlite

    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        assert isinstance(conn, DbapiConnection)

        cursor = await conn.execute("SELECT 1")

        assert isinstance(cursor, DbapiCursor)
        assert await cursor.fetchone() == (1,)
    finally:
        await conn.close()
        os.unlink(path)


@pytest.mark.turso
async def test_pyturso_is_a_dbapi_connection() -> None:
    """pyturso: the same shape, and this is the one worth checking.

    It is a Rust rewrite rather than a SQLite binding, so "SQLite-compatible"
    is a claim to test rather than an assumption to build on.
    """
    import turso.aio

    conn = await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "t.db"))
    try:
        assert isinstance(conn, DbapiConnection)

        cursor = await conn.execute("SELECT 1")

        assert isinstance(cursor, DbapiCursor)
        assert await cursor.fetchall() == [(1,)]
    finally:
        await conn.close()


@pytest.mark.postgresql
async def test_asyncpg_is_a_fetching_connection() -> None:
    """asyncpg: no cursor and no commit, so it is a different Protocol."""
    import asyncpg

    url = os.environ.get(
        "TEST_POSTGRESQL_URL",
        probe_url(),
    )
    try:
        conn = await asyncpg.connect(url)
    except (OSError, asyncpg.PostgresError) as exc:
        refuse_without_postgresql(url, exc)

    try:
        assert isinstance(conn, FetchingConnection)

        assert await conn.fetchval("SELECT 1") == 1
        assert [tuple(r) for r in await conn.fetch("SELECT 1 AS n")] == [(1,)]
    finally:
        await conn.close()


@pytest.mark.postgresql
async def test_asyncpg_is_not_a_dbapi_connection() -> None:
    """The two Protocols must actually separate the drivers.

    Without this, a Protocol loose enough to accept everything would pass
    every test above while telling a reader nothing, which is what `Any` was
    already doing.
    """
    import asyncpg

    url = os.environ.get(
        "TEST_POSTGRESQL_URL",
        probe_url(),
    )
    try:
        conn = await asyncpg.connect(url)
    except (OSError, asyncpg.PostgresError) as exc:
        refuse_without_postgresql(url, exc)

    try:
        assert not isinstance(conn, DbapiConnection), (
            "asyncpg answered the DBAPI Protocol, so the two do not separate "
            "the drivers and neither says anything"
        )
    finally:
        await conn.close()


@pytest.mark.sqlite
async def test_aiosqlite_is_not_a_fetching_connection() -> None:
    """And the separation holds in the other direction."""
    import aiosqlite

    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        assert not isinstance(conn, FetchingConnection)
    finally:
        await conn.close()
        os.unlink(path)


def test_no_dialect_module_still_takes_an_untyped_connection() -> None:
    """The ratchet. A dialect module knows which driver it holds.

    `inspector/postgresql.py` is only ever handed an asyncpg connection and
    `inspector/sqlite.py` only ever an aiosqlite one, so `Any` there was never
    describing uncertainty. It was the declaration nobody wrote.
    """
    import ast

    root = pathlib.Path(__file__).resolve().parents[2] / "src" / "declaro_persistum"
    typed = (
        "inspector/postgresql.py",
        "inspector/sqlite.py",
        "inspector/turso.py",
        "applier/sqlite.py",
        "applier/turso.py",
    )

    offenders: list[str] = []
    for relative in typed:
        module = root / relative
        for node in ast.walk(ast.parse(module.read_text())):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            arguments = node.args
            for parameter in arguments.posonlyargs + arguments.args + arguments.kwonlyargs:
                annotated = parameter.annotation
                if (
                    "conn" in parameter.arg
                    and annotated is not None
                    and ast.unparse(annotated).startswith("Any")
                ):
                    offenders.append(f"{relative}:{node.name}({parameter.arg})")

    assert not offenders, (
        f"{len(offenders)} connections still declared `Any` in a module that "
        f"knows its driver: {offenders}"
    )
