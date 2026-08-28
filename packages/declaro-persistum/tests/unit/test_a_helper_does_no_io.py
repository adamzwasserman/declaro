"""A function marked `@helper` says it does no I/O, so it must not do any.

MEASURED 2026-08-28. Twenty functions carried `@helper` -- "Data in, data out.
No I/O, no clock, no hidden state" -- while running a database query:

    _get_columns            connection.fetch(...)
    _get_indexes            connection.fetch(...)
    table_exists            connection.fetchval(...)
    postgres_read_rows      conn.fetch(...)
    _fetch_asyncpg          connection.fetch(...)
    ... and fifteen more

Nothing had noticed, because the Slop Audit's reader knew `execute` and
`commit` and not asyncpg's read verbs. It learned them on 2026-08-28 and
reported all twenty at once.

WHY A MARKER THAT LIES IS WORSE THAN NO MARKER. The whole point of writing the
boundary down is that a reader does not have to work out for themselves which
functions may open a socket. A `@helper` that opens one teaches the opposite,
and `honest-check` reads these markers to decide what a function is allowed to
do -- so the lie also buys permissions the function should not have.

THE VERBS ARE THE AUDIT'S OWN. Read verbs pair with a connection-shaped
receiver, because `close` on a string buffer is not I/O. Filesystem names are
plain, because they change a disk whatever the variable is called.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

pytestmark = pytest.mark.precommit

SRC = pathlib.Path(__file__).resolve().parents[2] / "src" / "declaro_persistum"

# On a connection-shaped receiver. `close` on a buffer is not I/O.
_ON_A_CONNECTION = frozenset(
    {
        "execute",
        "executemany",
        "executescript",
        "callproc",
        "fetch",
        "fetchrow",
        "fetchval",
        "fetchmany",
        "fetchall",
        "fetchone",
        "commit",
        "rollback",
        "close",
        "push",
        "pull",
    }
)

# Whatever the variable is called, these change a disk.
_ON_ANYTHING = frozenset(
    {
        "mkdir",
        "rmdir",
        "touch",
        "unlink",
        "rename",
        "chmod",
        "symlink",
    }
)

_CONNECTION_SHAPED = ("conn", "connection", "cursor", "cur", "held")


def _reaches_the_world(node: ast.AST) -> list[str]:
    """Every call in this function that opens a socket or changes a disk."""
    return sorted(
        {
            call.func.attr
            for call in ast.walk(node)
            if isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and (
                call.func.attr in _ON_ANYTHING
                or (
                    call.func.attr in _ON_A_CONNECTION
                    and isinstance(call.func.value, ast.Name)
                    and any(s in call.func.value.id.lower() for s in _CONNECTION_SHAPED)
                )
            )
        }
    )


def _helpers_that_do_io(path: pathlib.Path) -> list[str]:
    """Every `@helper` in one file that reaches the world, as name: verbs."""
    return [
        f"{path.name}:{node.lineno} {node.name} calls {', '.join(reaches)}"
        for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, ast.AsyncFunctionDef | ast.FunctionDef)
        and any(isinstance(d, ast.Name) and d.id == "helper" for d in node.decorator_list)
        for reaches in [_reaches_the_world(node)]
        if reaches
    ]


def test_no_helper_reaches_the_world() -> None:
    offenders = [
        offender for path in sorted(SRC.rglob("*.py")) for offender in _helpers_that_do_io(path)
    ]

    assert offenders == [], (
        f"these carry `@helper`, which says they do no I/O, and they open a "
        f"socket or change a disk. Mark them `@boundary`, or move the call to "
        f"the caller at the edge: {offenders}"
    )


def test_the_scanner_finds_one_when_there_is_one(tmp_path) -> None:
    """The gate's own horizon. A scan that finds nothing must be able to find."""
    planted = tmp_path / "planted.py"
    planted.write_text(
        "from declaro_persistum.boundary import helper\n\n\n"
        "@helper\n"
        "async def _reads(conn):\n"
        "    return await conn.fetch('SELECT 1')\n"
    )

    found = _helpers_that_do_io(planted)

    assert len(found) == 1, f"the scanner cannot see a helper doing I/O: {found}"
    assert "fetch" in found[0]
