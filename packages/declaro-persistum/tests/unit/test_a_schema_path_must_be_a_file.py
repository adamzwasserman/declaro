"""`apply_migrations_async` takes a models FILE, and says so only for one of
the two ways a path can be wrong.

MEASURED 2026-08-17. The guard is `if not schema_path.exists()`, so:

    a missing path      logged, and returned as {"success": False, "error": ...}
    a DIRECTORY         passes the guard, then three frames later
                        IsADirectoryError: [Errno 21] Is a directory

A directory exists, so `exists()` is the wrong question. The contract is "a
Python module containing Pydantic models", which the docstring states and the
CLI repeats with `metavar="FILE"`.

WHY A DIRECTORY IS THE LIKELY MISTAKE RATHER THAN AN EXOTIC ONE. This package
has both shapes: `load_models_from_module` takes a file and
`load_schema_from_models` takes a directory, and `docs/usage.md` shows a
`models/` directory holding `users.py` and `orders.py`. Reaching for the
directory is the natural error, and it is the one that crashes instead of
answering.
"""

from __future__ import annotations

import pathlib

import pytest

from declaro_persistum.migrations import _compute_schema_hash

pytestmark = pytest.mark.precommit


def test_a_models_file_hashes(tmp_path: pathlib.Path) -> None:
    """The contract, so the refusals below mean something."""
    models = tmp_path / "models.py"
    models.write_text("x = 1\n")

    assert len(_compute_schema_hash(models, "0.3.1")) == 64


def test_the_version_changes_the_hash(tmp_path: pathlib.Path) -> None:
    """A version bump must invalidate the skip-if-clean stamp.

    That is the whole reason the version is mixed in: a loader fix has to
    reach deployments whose schema file has not changed.
    """
    models = tmp_path / "models.py"
    models.write_text("x = 1\n")

    assert _compute_schema_hash(models, "0.3.1") != _compute_schema_hash(
        models, "0.3.2"
    )


def test_the_content_changes_the_hash(tmp_path: pathlib.Path) -> None:
    models = tmp_path / "models.py"
    models.write_text("x = 1\n")
    first = _compute_schema_hash(models, "0.3.1")
    models.write_text("x = 2\n")

    assert _compute_schema_hash(models, "0.3.1") != first


def test_a_directory_is_refused_by_name(tmp_path: pathlib.Path) -> None:
    """The measured crash, turned into an answer.

    `IsADirectoryError` three frames down names `read_bytes`, not the argument
    the caller got wrong, and it arrives from a function the caller never
    called.
    """
    models = tmp_path / "models"
    models.mkdir()
    (models / "users.py").write_text("x = 1\n")

    with pytest.raises(ValueError, match="directory"):
        _compute_schema_hash(models, "0.3.1")


def test_the_refusal_names_the_function_that_takes_a_directory(
    tmp_path: pathlib.Path,
) -> None:
    """A refusal that does not say what to do instead is half an answer.

    Both shapes exist in this package, so the caller is one function away from
    being right and the message should say which.
    """
    models = tmp_path / "models"
    models.mkdir()

    with pytest.raises(ValueError) as caught:
        _compute_schema_hash(models, "0.3.1")

    assert "load_schema_from_models" in str(caught.value), str(caught.value)


def test_a_missing_path_is_still_refused(tmp_path: pathlib.Path) -> None:
    """The case the old guard did catch must keep being caught."""
    with pytest.raises((ValueError, FileNotFoundError, OSError)):
        _compute_schema_hash(tmp_path / "nothing.py", "0.3.1")
