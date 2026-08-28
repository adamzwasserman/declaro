"""Steps for transfer/whose_fault_is_a_failed_table.feature.

WHAT THIS REPLACES. Six tests made a copy fail by replacing `_transfer_table`
on the module, which stops the real function running. They had no other
handle: the run record carries the two connections, the two loaders and the
batch size, but not the step that does the copying, so the only way to reach
it was the module attribute.

It carries the copy step now. Handing one in is calling the code with its
input, the same way a Database carries the callables it needs.
"""

from __future__ import annotations

import asyncio
import pathlib
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.bulk_loader import BULK_LOADERS
from declaro_persistum.transfer import _ensure_progress_table
from declaro_persistum.transfer_steps import (
    _record_a_failed_table,
    _stream_the_table,
)

scenarios("../features/transfer/whose_fault_is_a_failed_table.feature")

_OUR_BUGS: dict[str, type[Exception]] = {
    "TypeError": TypeError,
    "AttributeError": AttributeError,
    "KeyError": KeyError,
    "NameError": NameError,
    "IndexError": IndexError,
}


class _EngineRefused(Exception):
    """What a driver raises when it will not take a table.

    NOT A STAND-IN FOR A DRIVER. It is an exception type that is not one of
    ours, which is the only property the code under test reads: the sorting
    is on the type, never on the message.
    """


class Ctx(TypedDict, total=False):
    run: Any
    outcome: Any
    raised: Exception | None


@pytest.fixture
def ctx() -> Ctx:
    return {"run": None, "outcome": None, "raised": None}


def _a_run(copy: Any, target: Any = None) -> Any:
    """A run record carrying the copy step this scenario wants.

    `target` is a real connection when the scenario reaches code that writes
    progress, and None when it does not. Recording a failed table writes a row
    to the progress table, so those scenarios open a real SQLite file.
    """
    return {
        "source_conn": None,
        "target_conn": target,
        "source_loader": BULK_LOADERS["sqlite"],
        "target_loader": BULK_LOADERS["sqlite"],
        "batch_size": 100,
        "on_progress": None,
        "resume": False,
        "copy_one_table": copy,
    }


async def _a_real_target(path: pathlib.Path) -> Any:
    """A real SQLite file with the progress table already in it."""
    import aiosqlite

    conn = await aiosqlite.connect(str(path / "target.db"))
    await _ensure_progress_table(conn, BULK_LOADERS["sqlite"])
    return conn


@given(parsers.parse("a run whose copy step raises the bug {ours}"))
def _given_our_bug(ctx, ours):
    async def _copy(*_args: Any, **_kwargs: Any) -> Any:
        raise _OUR_BUGS[ours]("a bug of ours")

    ctx["run"] = _a_run(_copy)


@given("a run whose copy step raises a refusal from the engine")
def _given_refusal(ctx):
    async def _copy(*_args: Any, **_kwargs: Any) -> Any:
        raise _EngineRefused("no such column: nickname")

    ctx["run"] = _a_run(_copy)


@given(parsers.parse("a run whose copy step moves {rows:d} rows"))
def _given_success(ctx, rows):
    async def _copy(*_args: Any, **_kwargs: Any) -> Any:
        return {"rows_transferred": rows, "total_rows": rows}

    ctx["run"] = _a_run(_copy)


@when("one table is copied")
def _when_copied(ctx, tmp_path):
    async def _go() -> None:
        target = await _a_real_target(tmp_path)
        run = {**ctx["run"], "target_conn": target}
        try:
            ctx["outcome"] = await _stream_the_table(run, "orders", ["id"], None)
            ctx["raised"] = None
        except Exception as raised:  # noqa: BLE001 - which type escapes is the answer
            ctx["outcome"] = None
            ctx["raised"] = raised
        finally:
            await target.close()

    asyncio.run(_go())


@when(parsers.parse('_record_a_failed_table is given a refusal for "{table}"'))
def _when_recorded(ctx, table, tmp_path):
    async def _go() -> None:
        target = await _a_real_target(tmp_path)
        try:
            ctx["outcome"] = await _record_a_failed_table(
                _a_run(None, target), table, _EngineRefused("nope")
            )
        finally:
            await target.close()

    asyncio.run(_go())


@then(parsers.parse("{ours} reaches the caller"))
def _then_reaches(ctx, ours):
    assert ctx["raised"] is not None, (
        f"a bug of ours was swallowed and reported as a table failure: "
        f"{ctx['outcome']}"
    )
    assert type(ctx["raised"]) is _OUR_BUGS[ours], type(ctx["raised"])


@then("no table is blamed")
def _then_nobody_blamed(ctx):
    assert ctx["outcome"] is None, ctx["outcome"]


@then("the table is recorded as failed")
def _then_failed(ctx):
    assert ctx["raised"] is None, ctx["raised"]
    assert ctx["outcome"]["outcome"] == "failed", ctx["outcome"]


@then("the table is recorded as transferred")
def _then_transferred(ctx):
    assert ctx["raised"] is None, ctx["raised"]
    assert ctx["outcome"]["outcome"] == "transferred", ctx["outcome"]


@then("the failure names the engine's own error")
def _then_names_error(ctx):
    assert "_EngineRefused" in ctx["outcome"]["error"], ctx["outcome"]
    assert "no such column" in ctx["outcome"]["error"], ctx["outcome"]


@then(parsers.parse("it reports {rows:d} rows"))
def _then_rows(ctx, rows):
    assert ctx["outcome"]["rows"] == rows, ctx["outcome"]
