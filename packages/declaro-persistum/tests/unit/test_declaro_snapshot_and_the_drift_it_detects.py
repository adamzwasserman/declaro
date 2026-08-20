"""`declaro snapshot` writes the file `declaro diff` refuses on, and neither ran.

MEASURED 2026-08-20. `cmd_snapshot` is 24 statements and none had executed.
`_check_drift` and the exit-2 path in `cmd_diff` that consumes its output had
not either.

WHAT DRIFT MEANS AND WHY IT STOPS EVERYTHING. A snapshot records what the
database looked like when the schema was last applied. If the database has
changed since -- somebody added a column by hand, or a second deploy ran from
a different branch -- then the diff about to be computed is against a
database nobody's schema describes, and applying it would overwrite whatever
that was.

So `declaro diff` exits 2 and computes nothing. Not 1, which is the ambiguity
code, and not 0. A deploy script reads the number.

`--force` is the operator saying they know. It has to work, or the only way
past a drift is to delete the snapshot, which loses the record entirely.

THE PAIR IS TESTED AS A PAIR because neither half means anything alone. A
snapshot nothing reads is a file; a drift check with no snapshot to compare
against never fires.

`apply` WRITES THE SNAPSHOT ITSELF, at line 230, and I did not know that when
I wrote this file. Two tests assumed a fresh database had none: one expected
`snapshot` to write the first, and got "a snapshot already exists ... pass
--force"; the other expected no drift check to run, and got exit 2. Both were
my premise, not a defect. The command exists for re-recording after a
deliberate manual change, which is why it refuses to overwrite without
`--force` or a terminal.
"""

from __future__ import annotations

import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

pytestmark = [pytest.mark.precommit, pytest.mark.sqlite]

SCHEMA = """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
name = { type = "text" }
"""


def _run(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    models = tmp_path / "schema"
    models.mkdir(exist_ok=True)
    (models / "tables.toml").write_text(SCHEMA)
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "declaro_persistum.cli.main",
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


def _applied(tmp_path: Path) -> None:
    result = _run(tmp_path, "apply", "--unattended")
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"


def _drifted(tmp_path: Path) -> None:
    """A column added behind the package's back, which is what drift is."""
    connection = sqlite3.connect(tmp_path / "probe.db")
    connection.execute('ALTER TABLE "users" ADD COLUMN "added_by_hand" TEXT')
    connection.commit()
    connection.close()


def test_apply_writes_the_snapshot_itself(tmp_path: Path) -> None:
    """So the first `snapshot` an operator runs is always a re-record."""
    _applied(tmp_path)

    assert (tmp_path / "schema" / "snapshot.toml").exists()


def test_snapshot_refuses_to_overwrite_without_a_terminal(tmp_path: Path) -> None:
    """Overwriting is what it does, so it asks, and there is nobody to ask.

    The snapshot is the only record of what the database looked like when the
    schema was last applied. Replacing it silently in a script would make
    every drift check afterwards compare against whatever the drift was.
    """
    _applied(tmp_path)

    result = _run(tmp_path, "snapshot")

    assert result.returncode != 0, f"{result.stdout}\n{result.stderr}"
    assert "--force" in (result.stdout + result.stderr)


def test_snapshot_rewrites_the_file_when_forced(tmp_path: Path) -> None:
    _applied(tmp_path)
    _drifted(tmp_path)

    result = _run(tmp_path, "snapshot", "--force")

    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
    assert "added_by_hand" in (tmp_path / "schema" / "snapshot.toml").read_text()


def test_the_snapshot_records_the_tables_that_are_there(tmp_path: Path) -> None:
    """The floor. An empty file exists and records nothing."""
    _applied(tmp_path)

    written = (tmp_path / "schema" / "snapshot.toml").read_text()
    assert "users" in written, written


def test_a_diff_against_an_unchanged_database_is_clean(tmp_path: Path) -> None:
    """The control. A drift check that fired always would pass the rest."""
    _applied(tmp_path)

    result = _run(tmp_path, "diff")

    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"


def test_drift_exits_two(tmp_path: Path) -> None:
    """Two, not one and not zero. The number is the interface."""
    _applied(tmp_path)
    _drifted(tmp_path)

    result = _run(tmp_path, "diff")

    assert result.returncode == 2, (
        f"exit {result.returncode}\n{result.stdout}\n{result.stderr}"
    )


def test_the_drift_names_what_changed(tmp_path: Path) -> None:
    """An operator cannot act on "the database has drifted"."""
    _applied(tmp_path)
    _drifted(tmp_path)

    result = _run(tmp_path, "diff")

    output = result.stdout + result.stderr
    assert "added_by_hand" in output, output


def test_force_gets_past_the_drift(tmp_path: Path) -> None:
    """The operator saying they know.

    Without it the only way past a drift is deleting the snapshot, which
    loses the record the check depends on.
    """
    _applied(tmp_path)
    _drifted(tmp_path)

    result = _run(tmp_path, "diff", "--force")

    assert result.returncode == 0, (
        f"exit {result.returncode}\n{result.stdout}\n{result.stderr}"
    )


def test_no_snapshot_means_no_drift_check(tmp_path: Path) -> None:
    """Absence is a real answer: nothing has been recorded to compare against.

    A database whose snapshot has been deleted must not be blocked by a check
    that has nothing to check. `apply` writes one, so the file is removed here
    rather than simply not created.
    """
    _applied(tmp_path)
    (tmp_path / "schema" / "snapshot.toml").unlink()
    _drifted(tmp_path)

    result = _run(tmp_path, "diff")

    assert result.returncode == 0, (
        f"exit {result.returncode}\n{result.stdout}\n{result.stderr}"
    )
