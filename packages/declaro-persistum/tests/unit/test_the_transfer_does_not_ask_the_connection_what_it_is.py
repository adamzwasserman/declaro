"""The transfer sniffed its connection's type instead of using the loader.

MEASURED 2026-08-18 and left alone at the time. `transfer.py` asks
`hasattr(conn, "fetch")` at three sites to decide between asyncpg and the
DB-API drivers:

    _ensure_progress_table   one takes `()` and commits, one does not
    _get_table_status        `$1` and fetchrow, against `?` and a cursor
    _upsert_progress         `$1..$6` positionally, against `?` as a tuple

and at two more, `hasattr(conn, "commit")`, which the first version of this
file did not know about: it named three and the scan found five.

That is a dispatch on type, in a package whose stated shape is dispatch
tables, and the answer was already in the room: `bulk_transfer` resolves a
`BulkLoader` per engine and passes it into the first of the three, which
ignored it.

IT IS NOT A DEFECT, WHICH IS WHY IT SAT. The sniff gives the right answer on
all three engines: asyncpg has `fetch`, aiosqlite and pyturso do not. What it
costs is that the correlation is asserted nowhere. A driver that grew a
`fetch` method, or an asyncpg wrapper that hid one, would send a connection
down the wrong branch, and every one of those branches writes to the table
that decides whether a resumed transfer skips or repeats work.

WHAT THIS FILE ASSERTS is the shape, not the behaviour. The three behavioural
files beside it already cover resume, progress and both transfer directions,
and they pass either way. This one fails while the sniff exists.
"""

from __future__ import annotations

import ast
import inspect
import pathlib

import pytest

from declaro_persistum import transfer
from declaro_persistum.bulk_loader import BULK_LOADERS, BulkLoader

pytestmark = [pytest.mark.precommit]

SOURCE = pathlib.Path(inspect.getfile(transfer)).read_text()


def test_no_function_asks_a_connection_what_it_is() -> None:
    """`hasattr(conn, ...)` is a dispatch on type wearing a duck-typing coat."""
    sniffs = [
        f"line {node.lineno}: {ast.unparse(node)}"
        for node in ast.walk(ast.parse(SOURCE))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "hasattr"
    ]

    assert sniffs == [], (
        f"{len(sniffs)} connection-type checks remain: {sniffs}. The engine "
        f"is known at the top of `bulk_transfer` and the loader is already "
        f"passed in."
    )


def test_every_loader_carries_the_progress_operations() -> None:
    """The floor. Removing the sniff without providing the operations would
    leave the module unable to do the work at all."""
    required = ("commit", "ensure_progress", "read_progress", "write_progress")

    for engine, loader in BULK_LOADERS.items():
        for operation in required:
            assert operation in loader, f"{engine} loader has no {operation!r}"
            assert callable(loader[operation]), f"{engine}.{operation}"


def test_the_two_families_use_different_functions() -> None:
    """Three engines, two parameter styles, and they are not interchangeable.

    A table pointing both families at one function would pass the test above
    and send `$1` to SQLite.
    """
    for operation in ("commit", "ensure_progress", "read_progress", "write_progress"):
        postgres = BULK_LOADERS["postgresql"][operation]
        sqlite = BULK_LOADERS["sqlite"][operation]

        assert postgres is not sqlite, operation
        assert BULK_LOADERS["sqlite"][operation] is BULK_LOADERS["turso"][operation]


def test_the_type_declares_them() -> None:
    """A TypedDict that does not declare a key cannot be checked for it."""
    for operation in ("commit", "ensure_progress", "read_progress", "write_progress"):
        assert operation in BulkLoader.__annotations__, operation
