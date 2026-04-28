"""
Regression tests: apply_migrations_async must dialect-dispatch the orphaned
tmp-table recovery pre-flight.

Bug fixed in 0.1.2:
    In 0.1.0 and 0.1.1, ``apply_migrations_async`` called
    ``_recover_orphaned_tmp_tables(pool)`` unconditionally as a pre-flight.
    That function queries ``sqlite_master`` directly, which does not exist
    on Postgres, so every Postgres-backed app crashed at startup with
    ``asyncpg.exceptions.UndefinedTableError: relation "sqlite_master"
    does not exist`` before any actual migration logic ran.

    Fix: gate by dialect — only call recovery for ``sqlite`` and ``turso``.
"""

from pathlib import Path

import pytest

from declaro_persistum import migrations as m


class _Abort(Exception):
    """Sentinel — used to stop apply_migrations_async right after the gate."""


class _AbortPool:
    """Pool whose first acquire() raises Abort.

    Lets us run apply_migrations_async far enough to exercise the dialect
    gate, then short-circuit before any real DB work.
    """

    def acquire(self) -> "_AbortAcquireCM":
        return _AbortAcquireCM()


class _AbortAcquireCM:
    async def __aenter__(self) -> None:
        raise _Abort("past the gate")

    async def __aexit__(self, *exc: object) -> None:
        return None


@pytest.fixture
def schema_file(tmp_path: Path) -> Path:
    """A minimal schema file that exists (so the existence-check guard passes)."""
    p = tmp_path / "models.py"
    p.write_text("# empty schema for dispatch test\n")
    return p


@pytest.mark.asyncio
async def test_postgresql_dialect_skips_sqlite_recovery(
    schema_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: dialect='postgresql' must NOT call _recover_orphaned_tmp_tables."""
    recovery_calls: list[object] = []

    async def stub_recovery(pool: object) -> int:
        recovery_calls.append(pool)
        return 0

    monkeypatch.setattr(m, "_recover_orphaned_tmp_tables", stub_recovery)

    with pytest.raises(_Abort):
        await m.apply_migrations_async(_AbortPool(), "postgresql", schema_file)

    assert recovery_calls == [], (
        "Recovery scan must be skipped for postgresql — it queries sqlite_master "
        "which does not exist on Postgres"
    )


@pytest.mark.asyncio
async def test_sqlite_dialect_runs_recovery(
    schema_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Counterpart: dialect='sqlite' must call _recover_orphaned_tmp_tables exactly once."""
    recovery_calls: list[object] = []

    async def stub_recovery(pool: object) -> int:
        recovery_calls.append(pool)
        return 0

    monkeypatch.setattr(m, "_recover_orphaned_tmp_tables", stub_recovery)

    with pytest.raises(_Abort):
        await m.apply_migrations_async(_AbortPool(), "sqlite", schema_file)

    assert len(recovery_calls) == 1, (
        "Recovery scan must run for sqlite to repair orphaned _declaro_tmp_* "
        "tables left behind by failed reconstruction"
    )


@pytest.mark.asyncio
async def test_turso_dialect_runs_recovery(
    schema_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """dialect='turso' must call _recover_orphaned_tmp_tables (same temp-table scheme as sqlite)."""
    recovery_calls: list[object] = []

    async def stub_recovery(pool: object) -> int:
        recovery_calls.append(pool)
        return 0

    monkeypatch.setattr(m, "_recover_orphaned_tmp_tables", stub_recovery)

    with pytest.raises(_Abort):
        await m.apply_migrations_async(_AbortPool(), "turso", schema_file)

    assert len(recovery_calls) == 1, "Recovery scan must run for turso"
