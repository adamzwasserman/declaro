"""A defect in this package must not be reported as the database refusing.

WRITTEN BEFORE THE CODE, against a measured misattribution.

`bulk_transfer` copies one table at a time and catches `Exception` per table,
so a failure is recorded against that table and the loop carries on. That is
right for a table the engine refuses: one bad table should not stop the other
thirty-nine.

It is wrong for a bug of ours, because ours fails on EVERY table. Measured
2026-08-17 by making `_transfer_table` raise a TypeError:

    Failed to transfer items:  TypeError: _transfer_table() got an unexpected...
    Failed to transfer logs:   TypeError: ...
    Failed to transfer orders: TypeError: ...
    Failed to transfer users:  TypeError: ...
    success=False  tables_failed=4

One defect, four failures, and with forty tables it is forty. The report names
four tables and none of them is at fault. An operator reads "4 tables failed",
opens the database, and finds nothing wrong with any of them.

THE TYPE IS THE SIGNAL, NOT THE MESSAGE. `AttributeError`, `IndexError`,
`KeyError`, `NameError` and `TypeError` are how Python reports that code called
something wrongly. A driver refusing a write raises its own type. Sorting on
the type is decidable; sorting on the message text is what made `is_contention`
retry a `KeyError('busy')`.
"""

from __future__ import annotations

import pathlib

import pytest

# `_transfer_table` and the loader table moved to `transfer_steps` when
# transfer.py was split for length. `bulk_transfer` binds them at import,
# so a substitution on `transfer` is read by nothing and the test passes
# while proving nothing. It names the module that owns them.
from declaro_persistum import bulk_transfer, close, open_sqlite
from declaro_persistum.database import writing
from declaro_persistum.exceptions import A_BUG_IN_THIS_PACKAGE
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]

TABLES = ("users", "orders", "items", "logs")


def _models(directory: pathlib.Path) -> pathlib.Path:
    path = directory / "models.py"
    path.write_text(
        "from pydantic import BaseModel\n"
        "from declaro_persistum import table, field\n"
        + "".join(
            f'@table("{t}")\nclass M_{t}(BaseModel):\n    id: str = field(primary_key=True)\n\n'
            for t in TABLES
        )
    )
    return path


async def _pair(directory: pathlib.Path):
    source = await open_sqlite(
        str(directory / "s.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    target = await open_sqlite(
        str(directory / "t.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    for db in (source, target):
        async with writing(db) as conn:
            for table in TABLES:
                await conn.execute(f'CREATE TABLE "{table}" (id TEXT PRIMARY KEY)')
            await conn.commit()
    async with writing(source) as conn:
        for table in TABLES:
            await conn.execute(f'INSERT INTO "{table}" VALUES (?)', ("a",))
        await conn.commit()
    return source, target


def test_the_tuple_names_the_types_python_uses_for_a_wrong_call() -> None:
    """One definition, so a handler cannot decide for itself what ours means."""
    assert (
        AttributeError,
        IndexError,
        KeyError,
        NameError,
        TypeError,
    ) == A_BUG_IN_THIS_PACKAGE


async def test_a_clean_transfer_still_reports_success(tmp_path) -> None:
    """The baseline. Without it, a handler that raised on everything would pass."""
    source, target = await _pair(tmp_path)
    try:
        result = await bulk_transfer(
            source,
            target,
            str(_models(tmp_path)),
            tables=NOTHING,
            on_progress=NOTHING,
            batch_size=NOTHING,
            expand_enums=NOTHING,
            resume=NOTHING,
        )
        assert result["success"] is True, result
        assert result["tables_transferred"] == len(TABLES)
        assert result["tables_failed"] == 0
    finally:
        await close(source)
        await close(target)
