"""`declaro diff` had never executed a line, and it is the command users run.

MEASURED 2026-08-20, and only after measuring the CLI properly. `cmd_diff` is
36 statements and not one of them had run.

I MISSED IT ALL SESSION BY EXCLUDING THE FILE FROM EVERY SCAN. `cli/commands.py`
reads 32 percent under ordinary coverage, and I explained that away each time
as "the CLI is tested through subprocesses, so coverage cannot see it," without
ever checking. Turning on subprocess coverage takes it to 53.7 percent: part of
the explanation was true and the conclusion drawn from it was not. Forty-six
percent of the file is untested, and three whole commands are dark.

WHAT `diff` DOES THAT NOTHING ELSE DOES. It is the read-only command, the one
an operator runs before deciding whether to apply anything, and it decides
three things on its own:

    drift against snapshot.toml   ->  exit 2, and nothing else runs
    ambiguities, unattended       ->  raise, rather than guess
    ambiguities, interactive      ->  ask, then re-diff with the answers

Each of those is a refusal, and a refusal that does not happen is worse than
one that happens wrongly: the operator reads "no differences" and applies.

EXIT CODES ARE THE INTERFACE. A deploy script reads the number, not the text,
so each is asserted as a number.
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

ADDS_A_COLUMN = """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
name = { type = "text" }
note = { type = "text" }
"""

# `name` gone, `title` added, same type: a rename or a drop-and-add, and the
# differ declines to pick.
AMBIGUOUS = """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
title = { type = "text" }
"""


def _schema(tmp_path: Path, toml: str) -> Path:
    models = tmp_path / "schema"
    models.mkdir(exist_ok=True)
    (models / "tables.toml").write_text(toml)
    return models


def _run(tmp_path: Path, toml: str, *args: str) -> subprocess.CompletedProcess[str]:
    models = _schema(tmp_path, toml)
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "declaro_persistum.cli.main",
            # FOUR SLASHES for an absolute path; three is the documented
            # relative form and opens a file under the working directory.
            "-c",
            f"sqlite:///{tmp_path / 'probe.db'}",
            "-s",
            str(models),
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=120,
        stdin=subprocess.DEVNULL,
    )


def _apply(tmp_path: Path, toml: str) -> subprocess.CompletedProcess[str]:
    return _run(tmp_path, toml, "apply", "--unattended")


def test_a_database_matching_its_schema_reports_no_differences(
    tmp_path: Path,
) -> None:
    """Exit 0, and the operator is told there is nothing to do."""
    _apply(tmp_path, CLEAN)

    result = _run(tmp_path, CLEAN, "diff")

    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"


def test_a_pending_change_is_shown(tmp_path: Path) -> None:
    """The whole point of the command: say what would happen."""
    _apply(tmp_path, CLEAN)

    result = _run(tmp_path, ADDS_A_COLUMN, "diff")

    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    output = result.stdout + result.stderr
    assert "note" in output, output


def test_diff_changes_nothing(tmp_path: Path) -> None:
    """The floor, and the reason the command exists separately from apply.

    An operator runs this to decide. If it applied anything, the decision
    would already have been made for them.
    """
    import sqlite3

    _apply(tmp_path, CLEAN)

    _run(tmp_path, ADDS_A_COLUMN, "diff")

    connection = sqlite3.connect(tmp_path / "probe.db")
    columns = [
        row[1] for row in connection.execute('PRAGMA table_info("users")').fetchall()
    ]
    connection.close()
    assert columns == ["id", "name"], columns


def test_an_ambiguous_change_exits_non_zero_without_a_terminal(
    tmp_path: Path,
) -> None:
    """No terminal means unattended, and unattended must not guess.

    Renaming `name` to `title` and dropping `name` while adding `title`
    produce the same two schemas. One keeps the column's rows and the other
    destroys them.
    """
    _apply(tmp_path, CLEAN)

    result = _run(tmp_path, AMBIGUOUS, "diff")

    assert result.returncode != 0, f"{result.stdout}\n{result.stderr}"


def test_the_ambiguity_is_named_in_the_output(tmp_path: Path) -> None:
    """An operator needs the two column names to answer it."""
    _apply(tmp_path, CLEAN)

    result = _run(tmp_path, AMBIGUOUS, "diff")

    output = result.stdout + result.stderr
    assert "name" in output and "title" in output, output


def test_a_clean_diff_still_works_with_verbose(tmp_path: Path) -> None:
    """`-v` takes three extra branches through the function."""
    _apply(tmp_path, CLEAN)

    result = _run(tmp_path, CLEAN, "-v", "diff")

    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert "schema" in (result.stdout + result.stderr).lower()
