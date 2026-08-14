"""The bulk loaders must move rows, and something must run them.

`create_bulk_loader` raised NameError for postgresql, sqlite and turso — every
dialect it claimed to support — from 7a5c9ac until today. The de-classing sweep
removed `PostgreSQLBulkLoader` and `GenericBulkLoader` and kept the factory that
returns them.

NOTHING CAUGHT IT BECAUSE NOTHING RAN IT. `transfer.py` is the only caller, and
its tests cover `_build_table_fk_graph` and `_toposort_tables` — the two pure
helpers — and nothing that touches a database. A module can be entirely
inoperative and still sit inside a green suite when no test executes it.

So the gate in `test_every_name_src_calls_exists` is only half the fix. It
proves the names resolve. This proves the rows move, against a real SQLite file
rather than a mock: a mock would have been just as happy with the broken
version, because a mock asserts what I believed the loader does rather than what
it does.

PostgreSQL's loader needs a server, so it is checked structurally here and
exercised by `bulk_transfer`'s own PostgreSQL tests. That gap is stated rather
than hidden — a structural check is not a behavioural one, and calling it one
would repeat the mistake this file exists to close.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.precommit

OPERATIONS = {
    "read_rows",
    "load_rows",
    "count_rows",
    "delete_rows",
    "disable_fk_checks",
    "enable_fk_checks",
}


def test_every_supported_dialect_has_every_operation() -> None:
    from declaro_persistum.bulk_loader import BULK_LOADERS

    assert set(BULK_LOADERS) == {"postgresql", "sqlite", "turso"}, (
        f"BULK_LOADERS covers {sorted(BULK_LOADERS)}; transfer.py looks up "
        f"whatever dialect it was handed"
    )
    for dialect, loader in BULK_LOADERS.items():
        assert set(loader) == OPERATIONS, (
            f"{dialect} is missing {sorted(OPERATIONS - set(loader))}"
        )
        for name, fn in loader.items():
            assert callable(fn), f"{dialect}[{name!r}] is not callable"


@pytest.mark.asyncio
async def test_the_sqlite_loader_round_trips_rows(tmp_path) -> None:
    """Count, load, read back, delete. A real file, no mock."""
    import aiosqlite

    from declaro_persistum.bulk_loader import BULK_LOADERS

    loader = BULK_LOADERS["sqlite"]
    path = str(tmp_path / "bulk.db")

    async with aiosqlite.connect(path) as conn:
        await conn.execute("CREATE TABLE t (id INTEGER, name TEXT)")
        await conn.commit()

        assert await loader["count_rows"](conn, "t") == 0

        rows = [(1, "one"), (2, "two"), (3, "three")]
        assert await loader["load_rows"](conn, "t", ["id", "name"], rows) == 3
        await conn.commit()

        assert await loader["count_rows"](conn, "t") == 3
        assert await loader["read_rows"](conn, "t", ["id", "name"]) == rows

        page = await loader["read_rows"](
            conn, "t", ["id", "name"], offset=1, limit=1
        )
        assert page == [(2, "two")], f"offset/limit paged wrong: {page}"

        assert await loader["delete_rows"](conn, "t") == 3
        await conn.commit()
        assert await loader["count_rows"](conn, "t") == 0


@pytest.mark.asyncio
async def test_loading_nothing_writes_nothing(tmp_path) -> None:
    """An empty batch is a real case: the last page of an exact multiple."""
    import aiosqlite

    from declaro_persistum.bulk_loader import BULK_LOADERS

    loader = BULK_LOADERS["sqlite"]
    async with aiosqlite.connect(str(tmp_path / "bulk.db")) as conn:
        await conn.execute("CREATE TABLE t (id INTEGER)")
        await conn.commit()
        assert await loader["load_rows"](conn, "t", ["id"], []) == 0
        assert await loader["count_rows"](conn, "t") == 0


@pytest.mark.asyncio
async def test_the_sqlite_and_turso_loaders_are_the_same_one() -> None:
    """Both speak DB-API. Two copies would be two places to fix a bug."""
    from declaro_persistum.bulk_loader import BULK_LOADERS

    assert BULK_LOADERS["sqlite"] == BULK_LOADERS["turso"]
