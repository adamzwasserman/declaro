"""`declaro apply` writes DDL to a live database without validating it.

MEASURED 2026-08-19. `validate_schema` is called from `cmd_validate` and
nowhere else in the CLI. `cmd_apply` and `cmd_migrate_remote` both reach
`apply_operations` without it.

`apply_migrations_async` was fixed earlier today to ask the validator before
migrating, and the CLI does not go through that function: it diffs and applies
directly. So the command an operator actually runs against production still
did not consult a single rule.

WHAT IT COSTS. A foreign key pointing at a table nobody declared is caught by
comparing the schema against itself, and SQLite accepts the DDL without
complaint because it does not check a reference at creation time. The command
prints its operations, applies them, exits 0, and the column points at
nothing.

WHY THE SUBPROCESS. `declaro apply` is a process with an exit code, and the
exit code is what a deploy script reads. Calling `cmd_apply` in-process tests
the function; running the module tests the command.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = [pytest.mark.precommit, pytest.mark.sqlite]

CLEAN = """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
name = { type = "text" }
"""

# An unknown type is a WARNING, not an error: it may be a custom type or an
# enum. A gate stopping on warnings would refuse this, and the in-process
# version of this fix had exactly that gap, invisible until a warning-only
# schema was added.
WARNS = """
[shapes]
[shapes.columns]
id = { type = "integer", primary_key = true }
area = { type = "geography" }
"""

# `authors` is declared nowhere. The validator catches it; SQLite does not.
DANGLING = """
[posts]
[posts.columns]
id = { type = "integer", primary_key = true }
author_id = { type = "integer", references = "authors.id" }
"""


def _apply(tmp_path: Path, toml: str) -> subprocess.CompletedProcess[str]:
    models = tmp_path / "schema"
    models.mkdir(exist_ok=True)
    (models / "tables.toml").write_text(toml)

    return subprocess.run(
        [
            sys.executable,
            "-m",
            "declaro_persistum.cli.main",
            # FOUR SLASHES for an absolute path. Three is the documented
            # relative form and opens a file under the working directory.
            "-c",
            f"sqlite:///{tmp_path / 'probe.db'}",
            "-s",
            str(models),
            "apply",
            # `--unattended`, not `--yes`. The first version guessed and the
            # parser refused the argument, which the clean-schema control
            # caught: a non-zero exit for the wrong reason looks exactly like
            # a refusal working.
            "--unattended",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )


def _tables(tmp_path: Path) -> list[str]:
    import sqlite3

    connection = sqlite3.connect(tmp_path / "probe.db")
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name NOT LIKE '_declaro%' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    connection.close()
    return sorted(r[0] for r in rows)


def test_a_clean_schema_still_applies(tmp_path: Path) -> None:
    """The control. A gate that refused everything would pass the rest."""
    result = _apply(tmp_path, CLEAN)

    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert _tables(tmp_path) == ["users"]


def test_an_invalid_schema_exits_non_zero(tmp_path: Path) -> None:
    """The exit code is what a deploy script reads."""
    result = _apply(tmp_path, DANGLING)

    assert result.returncode != 0, f"{result.stdout}\n{result.stderr}"


def test_an_invalid_schema_creates_nothing(tmp_path: Path) -> None:
    """A refusal that had already written the DDL is not a refusal."""
    _apply(tmp_path, DANGLING)

    assert _tables(tmp_path) == [], _tables(tmp_path)


def test_the_operator_is_told_which_reference_is_dangling(tmp_path: Path) -> None:
    """An operator reading a deploy log needs the table name, not a count."""
    result = _apply(tmp_path, DANGLING)

    output = result.stdout + result.stderr
    assert "authors" in output, output


def test_a_schema_the_validator_only_warns_about_still_applies(
    tmp_path: Path,
) -> None:
    """The control on errors-versus-warnings, which nothing else here measures.

    Widening the gate to stop on warnings passes every other test in this
    file, and the same mutation survived the in-process version of this check
    until a warning-only schema was written for it.
    """
    from declaro_persistum.loader import load_schema
    from declaro_persistum.validator import validate_schema

    result = _apply(tmp_path, WARNS)

    report = validate_schema(load_schema(str(tmp_path / "schema")))
    assert report["warnings"] and not report["errors"], report
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert _tables(tmp_path) == ["shapes"], _tables(tmp_path)
