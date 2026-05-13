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

    Fix: include the declaro version in the hash input. Any version bump
    invalidates the cache.

Honest-test refactor in 0.1.6:
    ``_compute_schema_hash`` now takes ``version`` as a parameter rather
    than reading ``declaro_persistum.__version__`` from a module-level
    constant. The tests below are pure-function assertions —
    ``assert _compute_schema_hash(file, "X") != _compute_schema_hash(file, "Y")``
    — with no monkeypatching of module globals.
"""

from pathlib import Path

import pytest

from declaro_persistum.migrations import _compute_schema_hash


@pytest.fixture
def schema_file(tmp_path: Path) -> Path:
    p = tmp_path / "models.py"
    p.write_text("# stable schema content for hash tests\n")
    return p


def test_hash_is_stable_across_calls_at_same_version(schema_file: Path) -> None:
    """Two calls at the same version on the same file must produce identical hashes."""
    h1 = _compute_schema_hash(schema_file, "1.0.0")
    h2 = _compute_schema_hash(schema_file, "1.0.0")
    assert h1 == h2


def test_hash_changes_when_version_changes(schema_file: Path) -> None:
    """Regression: bumping declaro version invalidates the hash.

    Without this guarantee, a buggy version's stored "clean" hash would
    survive an upgrade and silently hide the fix from any consumer whose
    schema file did not change.
    """
    h_a = _compute_schema_hash(schema_file, "9.9.9-test-a")
    h_b = _compute_schema_hash(schema_file, "9.9.9-test-b")
    assert h_a != h_b, (
        "Schema hash must differ when declaro version differs. "
        "Otherwise upgrades cannot invalidate stale clean-hashes from buggy versions."
    )


def test_hash_changes_when_file_changes_at_same_version(schema_file: Path) -> None:
    """Sanity: file content changes still invalidate the hash."""
    h_before = _compute_schema_hash(schema_file, "1.0.0")

    schema_file.write_text("# different content\n")
    h_after = _compute_schema_hash(schema_file, "1.0.0")

    assert h_before != h_after


def test_delimiter_prevents_file_version_collision(tmp_path: Path) -> None:
    """The hash uses a NUL delimiter between file content and version,
    so a file ending with a literal version string cannot collide with an
    empty file at that version.
    """
    file_a = tmp_path / "a.py"
    file_a.write_bytes(b"")

    file_b = tmp_path / "b.py"
    file_b.write_bytes(b"X.Y.Z")

    h_a = _compute_schema_hash(file_a, "X.Y.Z")
    h_b = _compute_schema_hash(file_b, "X.Y.Z")

    # Without a delimiter, hashing concat("", "X.Y.Z") and concat("X.Y.Z", "")
    # would produce the same bytes. With the NUL delimiter, they differ.
    assert h_a != h_b
