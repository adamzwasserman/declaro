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

import declaro_persistum.transfer as transfer_module
from declaro_persistum import bulk_transfer, close, open_sqlite, writing
from declaro_persistum.exceptions import A_BUG_IN_THIS_PACKAGE

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.sqlite]

TABLES = ("users", "orders", "items", "logs")


def _models(directory: pathlib.Path) -> pathlib.Path:
    path = directory / "models.py"
    path.write_text(
        "from pydantic import BaseModel\n"
        "from declaro_persistum import table, field\n"
        + "".join(
            f'@table("{t}")\nclass M_{t}(BaseModel):\n'
            f"    id: str = field(primary_key=True)\n\n"
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
            source, target, "sqlite", "sqlite", str(_models(tmp_path))
        )
        assert result["success"] is True, result
        assert result["tables_transferred"] == len(TABLES)
        assert result["tables_failed"] == 0
    finally:
        await close(source)
        await close(target)


async def test_a_bug_of_ours_reaches_the_caller_once(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The measured misattribution, in one assertion.

    A TypeError from `_transfer_table` is not the database refusing anything.
    Before this it was recorded against all four tables and the caller was
    handed a result saying four tables failed.
    """
    source, target = await _pair(tmp_path)

    async def broken(*_args, **_kwargs):
        raise TypeError("_transfer_table() got an unexpected keyword argument")

    monkeypatch.setattr(transfer_module, "_transfer_table", broken)

    try:
        with pytest.raises(TypeError):
            await bulk_transfer(
                source, target, "sqlite", "sqlite", str(_models(tmp_path))
            )
    finally:
        await close(source)
        await close(target)


@pytest.mark.parametrize("kind", [AttributeError, KeyError, NameError, IndexError])
async def test_every_shape_of_our_own_bug_escapes(
    tmp_path, monkeypatch: pytest.MonkeyPatch, kind
) -> None:
    """TypeError is not special. One test of one type proves nothing about four.

    These are the ways Python reports that code called something wrongly, and a
    handler catching four of the five would still misattribute the fifth.
    """
    source, target = await _pair(tmp_path)

    async def broken(*_args, **_kwargs):
        raise kind("a bug of ours")

    monkeypatch.setattr(transfer_module, "_transfer_table", broken)

    try:
        with pytest.raises(kind):
            await bulk_transfer(
                source, target, "sqlite", "sqlite", str(_models(tmp_path))
            )
    finally:
        await close(source)
        await close(target)


async def test_a_table_the_engine_refuses_is_still_recorded_and_the_rest_run(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The behaviour that must survive, and the reason the handler exists.

    A per-table failure that the engine really produced belongs to that table.
    One bad table must not stop the other three, and the result must say which
    one it was.
    """
    import sqlite3

    source, target = await _pair(tmp_path)
    original = transfer_module._transfer_table

    async def refuse_one(source_conn, target_conn, source_loader, target_loader,
                         table, *args, **kwargs):
        if table == "orders":
            raise sqlite3.OperationalError("no such column: orders.missing")
        return await original(
            source_conn, target_conn, source_loader, target_loader,
            table, *args, **kwargs
        )

    monkeypatch.setattr(transfer_module, "_transfer_table", refuse_one)

    try:
        result = await bulk_transfer(
            source, target, "sqlite", "sqlite", str(_models(tmp_path))
        )

        assert result["success"] is False
        assert result["tables_failed"] == 1, (
            f"one refused table produced {result['tables_failed']} failures: "
            f"{result.get('errors')}"
        )
        assert list(result["errors"]) == ["orders"], result["errors"]
        assert result["tables_transferred"] == len(TABLES) - 1, (
            "the other tables did not transfer, so one refusal stopped the run"
        )
    finally:
        await close(source)
        await close(target)
