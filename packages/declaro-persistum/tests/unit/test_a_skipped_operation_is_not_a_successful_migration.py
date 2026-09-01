"""A migration that skipped operations did not succeed.

The Turso applier skips an operation it cannot apply, records it, and carries
on. Then:

    success = len(executed) > 0 or len(skipped) == 0

MEASURED 2026-08-17 by evaluating that expression:

    executed=3 skipped=0  -> success=True
    executed=3 skipped=2  -> success=True   <- two operations did not happen
    executed=1 skipped=5  -> success=True   <- five did not happen
    executed=0 skipped=2  -> success=False

So a migration reports success as long as ONE operation worked, whatever else
was skipped. A caller reading `success` writes the schema stamp and moves on,
and the database is missing the columns those operations would have added.

That is the exact failure the skip-if-clean stamp then makes permanent: the
next boot compares hashes, sees a match, and never re-proposes the operations
that never ran.

A MIGRATION IS ALL OR IT IS NOT DONE. `success` must mean every operation
landed. `skipped` is still reported, so a caller that wants to tolerate skips
can look at it and decide; what it must not do is have that decision made for
it by an expression it cannot see.

SEPARATELY, OUR OWN BUGS ARE NOT SKIPS. A `KeyError` from a malformed
operation was reported as "Table reconstruction failed for child. Check for
orphaned _declaro_tmp tables." No reconstruction had started, so there are no
orphaned tables to check for, and the operator is sent hunting.
"""

from __future__ import annotations

import pathlib
import tempfile

import pytest

from declaro_persistum import close, open_turso
from declaro_persistum.applier import apply as apply_operations
from declaro_persistum.database import writing
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.asyncio, pytest.mark.precommit, pytest.mark.turso]


async def _seeded():
    directory = pathlib.Path(tempfile.mkdtemp())
    db = await open_turso(
        str(directory / "t.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING
    )
    async with writing(db) as conn:
        await conn.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, note TEXT)")
        await conn.execute("INSERT INTO child VALUES (1, 'x')")
        await conn.commit()
    return db


def test_the_success_rule_requires_every_operation() -> None:
    """The expression itself, before any database is involved.

    Written as a table because the defect is in the rule, not in any one run,
    and a table says which combinations were considered.
    """
    from declaro_persistum.applier.turso import migration_succeeded

    assert migration_succeeded(skipped=0) is True
    assert migration_succeeded(skipped=0) is True, (
        "a migration with nothing to do succeeded; there was nothing to fail"
    )
    assert migration_succeeded(skipped=2) is False, (
        "three operations landed and two did not, and this reported success. "
        "The caller stamps the schema as current and the two never run again."
    )
    assert migration_succeeded(skipped=5) is False
    assert migration_succeeded(skipped=2) is False


async def test_a_clean_migration_reports_success() -> None:
    """The baseline, against a real Turso database."""
    from declaro_persistum.differ import diff

    db = await _seeded()
    try:
        actual = {
            "child": {
                "columns": {
                    "id": {"type": "INTEGER", "primary_key": True},
                    "note": {"type": "TEXT", "nullable": True},
                }
            }
        }
        target = {
            "child": {
                "columns": {
                    "id": {"type": "INTEGER", "primary_key": True},
                    "note": {"type": "TEXT", "nullable": False},
                }
            }
        }
        ops = [
            o
            for o in diff(actual, target, engine="turso", decisions={})["operations"]
            if o["op"] == "alter_column"
        ]
        async with writing(db) as conn:
            result = await apply_operations(
                conn, ops, list(range(len(ops))), "turso", dry_run=False
            )
        assert result["success"] is True, result.get("error")
    finally:
        await close(db)


async def test_a_malformed_operation_does_not_blame_orphaned_tables() -> None:
    """Our own bug, reported as a reconstruction that never started.

    "Check for orphaned _declaro_tmp tables" sends an operator looking for
    tables that cannot exist, because nothing was created before the KeyError.
    """
    db = await _seeded()
    try:
        malformed = [
            {
                "op": "alter_column",
                "table": "child",
                "details": {"column": "note", "nullable": False},
            }
        ]
        async with writing(db) as conn:
            with pytest.raises(KeyError):
                await apply_operations(conn, malformed, [0], "turso", dry_run=False)
    finally:
        await close(db)
