"""
Regression tests: schema hash mixes in declaro version for skip-if-clean.

Bug-class fixed in 0.1.4:
    The skip-if-clean optimization stores a hash of the schema file after a
    successful migration. If a buggy version of declaro produced an
    incorrect schema, the stored hash represented "the result of running
    that buggy version against this file." Upgrading to a fixed version
    left the stored hash matching the unchanged file, so the runner
    skipped re-introspection and the corrupted schema persisted silently
    until the user edited their model file or passed force=True.

    Fix: include `declaro_persistum.__version__` in the hash input.
    Any version bump invalidates the cache, forcing one re-introspection
    pass on first startup after upgrade. This ensures fixes ship to
    consumers automatically.
"""

from pathlib import Path

import pytest

from declaro_persistum import migrations as m


@pytest.fixture
def schema_file(tmp_path: Path) -> Path:
    p = tmp_path / "models.py"
    p.write_text("# stable schema content for hash tests\n")
    return p


def test_hash_is_stable_across_calls_at_same_version(schema_file: Path) -> None:
    """Two calls at the same version on the same file must produce identical hashes."""
    h1 = m._compute_schema_hash(schema_file)
    h2 = m._compute_schema_hash(schema_file)
    assert h1 == h2


def test_hash_changes_when_declaro_version_changes(
    schema_file: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: bumping declaro_persistum.__version__ must invalidate the hash.

    Without this guarantee, a buggy version's "clean" hash would survive an
    upgrade and silently hide the fix from any consumer whose schema file
    didn't change.
    """
    import declaro_persistum

    monkeypatch.setattr(declaro_persistum, "__version__", "9.9.9-test-a")
    h_a = m._compute_schema_hash(schema_file)

    monkeypatch.setattr(declaro_persistum, "__version__", "9.9.9-test-b")
    h_b = m._compute_schema_hash(schema_file)

    assert h_a != h_b, (
        "Schema hash must differ when declaro_persistum.__version__ differs. "
        "Otherwise upgrades cannot invalidate stale clean-hashes from buggy versions."
    )


def test_hash_changes_when_file_changes_at_same_version(schema_file: Path) -> None:
    """Sanity: file content changes still invalidate the hash."""
    h_before = m._compute_schema_hash(schema_file)

    schema_file.write_text("# different content\n")
    h_after = m._compute_schema_hash(schema_file)

    assert h_before != h_after


def test_delimiter_prevents_file_version_collision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The hash uses a NUL delimiter between file content and version,
    so a file ending with a literal version string can't collide with an
    empty file at that version.
    """
    import declaro_persistum

    file_a = tmp_path / "a.py"
    file_a.write_bytes(b"")

    file_b = tmp_path / "b.py"
    file_b.write_bytes(b"X.Y.Z")

    monkeypatch.setattr(declaro_persistum, "__version__", "X.Y.Z")
    h_a = m._compute_schema_hash(file_a)
    h_b = m._compute_schema_hash(file_b)

    # Without a delimiter, hashing concat("", "X.Y.Z") and concat("X.Y.Z", "")
    # would produce the same bytes. With the NUL delimiter, they differ.
    assert h_a != h_b
