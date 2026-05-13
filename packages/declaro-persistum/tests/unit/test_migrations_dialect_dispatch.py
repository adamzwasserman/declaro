"""
Regression tests: orphaned-tmp-table recovery dispatches by dialect.

Bug fixed in 0.1.2:
    In 0.1.0 and 0.1.1, ``apply_migrations_async`` called
    ``_recover_orphaned_tmp_tables(pool)`` unconditionally as a pre-flight.
    That function queries ``sqlite_master`` directly, which does not exist
    on Postgres, so every Postgres-backed app crashed at startup with
    ``asyncpg.exceptions.UndefinedTableError: relation "sqlite_master"
    does not exist`` before any actual migration logic ran.

Honest-test refactor in 0.1.6:
    The dispatch decision was extracted from ``apply_migrations_async``
    into a pure helper, ``_dialect_needs_orphan_recovery(dialect)``. These
    tests now assert that pure function directly — no monkeypatching of
    internal symbols, no fake pools, no sentinel exceptions used to
    short-circuit the function being tested. The previous test shape was
    a smell pointing at this missing extraction.
"""

from declaro_persistum.migrations import _dialect_needs_orphan_recovery


def test_postgresql_does_not_need_orphan_recovery() -> None:
    """Postgres has no sqlite_master and no _declaro_tmp_* reconstruction —
    the recovery scan must NOT run."""
    assert _dialect_needs_orphan_recovery("postgresql") is False


def test_sqlite_needs_orphan_recovery() -> None:
    """SQLite reconstruction may leave orphaned _declaro_tmp_<table>
    tables behind on partial failure; the recovery scan repairs them."""
    assert _dialect_needs_orphan_recovery("sqlite") is True


def test_turso_needs_orphan_recovery() -> None:
    """Turso uses the same temp-table scheme as SQLite for reconstruction."""
    assert _dialect_needs_orphan_recovery("turso") is True


def test_unknown_dialect_defaults_to_no_recovery() -> None:
    """Unknown dialects are not run through the SQLite-specific scan —
    fail-closed against future dialects that may not support it."""
    assert _dialect_needs_orphan_recovery("mysql") is False
    assert _dialect_needs_orphan_recovery("") is False
    assert _dialect_needs_orphan_recovery("PostgreSQL") is False  # case-sensitive
