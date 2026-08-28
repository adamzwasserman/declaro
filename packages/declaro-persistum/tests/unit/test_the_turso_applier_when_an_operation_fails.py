"""The Turso applier's whole failure path had never run.

MEASURED 2026-08-19 with `coverage json` over the whole suite. Unrun in
`applier/turso.py`:

    the `except` an operation's failure lands in
    the rollback, and the branch where the rollback itself fails
    re-enabling foreign keys after a failed reconstruction, and that failing
    the MigrationError a failed reconstruction raises
    recording a skipped operation
    the summary that names how many were skipped
    the dry-run return

Every test of this applier had handed it operations that work. That leaves the
half of the module written for the case where they do not, which is the half
that decides whether a failed migration is reported as one.

WHY A SKIP IS NOT A FAILURE HERE, AND WHY IT IS NOT A SUCCESS EITHER. This
applier skips an operation the engine refuses and carries on, so one
unsupported change does not block a batch. `migration_succeeded` then reports
success only when nothing was skipped: a caller reading `success` writes the
schema stamp, and a stamp written over a partial migration is never
re-proposed.

RECONSTRUCTION IS THE EXCEPTION AND RAISES. A half-finished table rebuild can
leave an orphaned `_declaro_tmp` table holding the only copy of the rows, so
that failure must not be swallowed into a list.

THE FAILURES ARE REAL. An operation is made to fail by asking the engine for
something it refuses -- a second table of the same name, a constraint SQLite
and Turso cannot add -- rather than by patching anything.
"""

from __future__ import annotations

import os
import tempfile
from typing import Any

import pytest

from declaro_persistum.applier import apply
from declaro_persistum.applier.turso import get_dialect, get_transaction_mode
from declaro_persistum.exceptions import MigrationError

pytestmark = [pytest.mark.precommit, pytest.mark.turso]

CREATE_WIDGETS: dict[str, Any] = {
    "op": "create_table",
    "table": "widgets",
    "details": {
        "columns": {"id": {"type": "integer", "primary_key": True}},
        "primary_key": None,
        "indexes": {},
        "constraints": {},
    },
}

CREATE_GADGETS: dict[str, Any] = {
    **CREATE_WIDGETS,
    "table": "gadgets",
}

# SQLite and Turso have no ADD CONSTRAINT, and the renderer says so by name.
ADD_CONSTRAINT: dict[str, Any] = {
    "op": "add_constraint",
    "table": "widgets",
    "details": {
        "constraint": "ck_id",
        "definition": {"type": "check", "expression": "id > 0"},
    },
}


async def _connection() -> Any:
    import turso.aio

    return await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "f.db"))


async def _apply(connection: Any, operations: list[dict], **kwargs: Any) -> Any:
    return await apply(
        connection,
        operations,
        list(range(len(operations))),
        "turso",
        dry_run=kwargs.get("dry_run", False),
    )


async def test_a_refused_operation_is_not_a_successful_migration() -> None:
    """A caller reading `success` writes the schema stamp."""
    connection = await _connection()
    try:
        result = await _apply(connection, [ADD_CONSTRAINT])

        assert result["success"] is False, result
    finally:
        await connection.close()


async def test_the_refusal_is_named_in_the_result() -> None:
    """A count of skips tells an operator nothing about which change is gone."""
    connection = await _connection()
    try:
        result = await _apply(connection, [ADD_CONSTRAINT])

        assert "ck_id" in result["error"], result["error"]
    finally:
        await connection.close()


async def test_one_refusal_does_not_block_the_rest_of_the_batch() -> None:
    """The reason the skip exists at all."""
    connection = await _connection()
    try:
        result = await _apply(connection, [CREATE_WIDGETS, ADD_CONSTRAINT])

        cursor = connection.cursor()
        await cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='widgets'")
        assert await cursor.fetchall(), "the operation before the refusal did not run"
        assert result["success"] is False, result
    finally:
        await connection.close()


async def test_a_batch_with_no_refusals_succeeds() -> None:
    """The control. An applier that always reported failure would pass above."""
    connection = await _connection()
    try:
        result = await _apply(connection, [CREATE_WIDGETS, CREATE_GADGETS])

        assert result["success"] is True, result
        assert result.get("error") is None, result
    finally:
        await connection.close()


async def test_a_failed_reconstruction_raises_rather_than_skipping() -> None:
    """A half-finished rebuild can hold the only copy of the rows.

    The operation names a column the table does not have, so the rebuild
    cannot be planned and fails. That must not become a line in a list.
    """
    connection = await _connection()
    try:
        await _apply(connection, [CREATE_WIDGETS])

        with pytest.raises(MigrationError) as caught:
            await _apply(
                connection,
                [
                    {
                        "op": "alter_column",
                        "table": "widgets",
                        "details": {
                            "column": "no_such_column",
                            "changes": {"type": {"from": "text", "to": "integer"}},
                        },
                    }
                ],
            )

        assert "widgets" in str(caught.value), caught.value
    finally:
        await connection.close()


async def test_a_dry_run_returns_the_sql_and_touches_nothing() -> None:
    connection = await _connection()
    try:
        result = await _apply(connection, [CREATE_WIDGETS], dry_run=True)

        cursor = connection.cursor()
        await cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='widgets'")
        assert not await cursor.fetchall(), "a dry run created the table"
        assert result, result
    finally:
        await connection.close()


def test_the_module_names_its_dialect_and_transaction_mode() -> None:
    """Two constants the dispatch tables read, and nothing had read them."""
    assert get_dialect() == "turso"
    assert get_transaction_mode() == "per_operation"
