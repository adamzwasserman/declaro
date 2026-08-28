"""`--interactive` asks the operator, takes the answer, and loses it.

MEASURED 2026-08-20, saving exactly the decision
`_resolve_ambiguities_interactive` builds when an operator chooses "rename":

    {"type": "rename", "table": "users", "from_column": "name",
     "to_column": "title", "column": None, "decided_at": "..."}

    TypeError: Object of type 'NoneType' is not TOML serializable

TOML HAS NO NULL. A key with no value is not written; it is omitted, and
`load_decisions` already reads an absent key back as None. The writer sets
every key on every decision and fills the ones that do not apply with None,
which is the one thing the format cannot carry.

WHAT IT COSTS. The operator is asked whether a column was renamed or dropped
-- the question that decides whether that column's rows survive -- answers it,
and the process dies while recording the answer. Nothing is saved, so the next
run asks again, and again. The command is unusable for the case it exists for.

BOTH HALVES WERE TESTED AND THE SEAM WAS NOT. The prompt builds a decision and
`save_decisions` writes a dict; each works on its own, and no test had ever
passed one to the other. That is recorded as bd issue declaro-p42.

TESTED THROUGH A PSEUDO-TERMINAL, because the prompt only runs when stdin and
stdout are both a tty: the CLI reads `sys.stdin.isatty()` to decide between
asking and refusing. Piping stdin makes it take the unattended branch and the
prompt never runs, which is how this survived a subprocess test suite.
"""

from __future__ import annotations

import os
import pty
import subprocess
import sys
from pathlib import Path

import pytest

from declaro_persistum.loader import load_decisions, save_decisions

pytestmark = [pytest.mark.precommit, pytest.mark.sqlite]

BEFORE = """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
name = { type = "text" }
"""

AMBIGUOUS = """
[users]
[users.columns]
id = { type = "integer", primary_key = true }
title = { type = "text" }
"""

# The exact shape the prompt builds for each answer, copied from
# `_resolve_ambiguities_interactive` rather than invented.
RENAME = {
    "users_name": {
        "type": "rename",
        "table": "users",
        "from_column": "name",
        "to_column": "title",
        "column": None,
        "decided_at": "2026-08-20T00:00:00+00:00",
    }
}
DROP = {
    "users_name": {
        "type": "drop",
        "table": "users",
        "column": "name",
        "from_column": None,
        "to_column": None,
        "decided_at": "2026-08-20T00:00:00+00:00",
    }
}


@pytest.mark.parametrize("decisions", [RENAME, DROP], ids=["rename", "drop"])
def test_the_decision_can_be_written(tmp_path: Path, decisions: dict) -> None:
    """The seam. Each half worked and nothing passed one to the other."""
    save_decisions(str(tmp_path), decisions)


@pytest.mark.parametrize("decisions", [RENAME, DROP], ids=["rename", "drop"])
def test_the_decision_reads_back_as_it_was_answered(tmp_path: Path, decisions: dict) -> None:
    """The floor. Writing without error is not recording the answer.

    The differ acts on `type`, `from_column` and `to_column`, and a rename
    read back as a drop destroys the column's rows.
    """
    save_decisions(str(tmp_path), decisions)

    loaded = load_decisions(str(tmp_path))

    key = "users_name"
    assert loaded[key]["type"] == decisions[key]["type"]
    assert loaded[key]["from_column"] == decisions[key]["from_column"]
    assert loaded[key]["to_column"] == decisions[key]["to_column"]
    assert loaded[key]["column"] == decisions[key]["column"]


def _run_on_a_terminal(tmp_path: Path, toml: str, answer: str) -> tuple[int, str]:
    """Run `declaro diff` with a real tty on both ends, and answer the prompt.

    A pipe makes `isatty()` false and the CLI takes the unattended branch, so
    the prompt this exists to test never runs.
    """
    models = tmp_path / "schema"
    models.mkdir(exist_ok=True)
    (models / "tables.toml").write_text(toml)

    parent, child = pty.openpty()
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "declaro_persistum.cli.main",
            "-c",
            f"sqlite:///{tmp_path / 'probe.db'}",
            "-s",
            str(models),
            "diff",
            "--interactive",
        ],
        stdin=child,
        stdout=child,
        stderr=child,
    )
    os.close(child)
    os.write(parent, answer.encode())

    output = b""
    try:
        while True:
            chunk = os.read(parent, 4096)
            if not chunk:
                break
            output += chunk
    except OSError:
        pass
    finally:
        os.close(parent)
    return process.wait(timeout=120), output.decode(errors="replace")


def _applied(tmp_path: Path, toml: str) -> None:
    models = tmp_path / "schema"
    models.mkdir(exist_ok=True)
    (models / "tables.toml").write_text(toml)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "declaro_persistum.cli.main",
            "-c",
            f"sqlite:///{tmp_path / 'probe.db'}",
            "-s",
            str(models),
            "apply",
            "--unattended",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"


def test_answering_the_prompt_does_not_crash(tmp_path: Path) -> None:
    """End to end, with the operator answering "rename"."""
    _applied(tmp_path, BEFORE)

    code, output = _run_on_a_terminal(tmp_path, AMBIGUOUS, "1\n")

    assert "not TOML serializable" not in output, output
    assert code == 0, f"exit {code}\n{output}"


def test_the_answer_is_on_disk_afterwards(tmp_path: Path) -> None:
    """The whole point. An answer not written down is asked again next run."""
    _applied(tmp_path, BEFORE)

    _run_on_a_terminal(tmp_path, AMBIGUOUS, "1\n")

    pending = tmp_path / "schema" / "migrations" / "pending.toml"
    assert pending.exists(), "the answer was not recorded"
    assert "rename" in pending.read_text(), pending.read_text()


def test_the_recorded_answer_is_read_on_the_next_run(tmp_path: Path) -> None:
    """And it has to stop the question being asked again.

    `cmd_diff` loads decisions before diffing, so a recorded answer means the
    second run is not ambiguous at all.
    """
    _applied(tmp_path, BEFORE)
    _run_on_a_terminal(tmp_path, AMBIGUOUS, "1\n")

    loaded = load_decisions(str(tmp_path / "schema"))

    assert loaded, "nothing was recorded"
    assert next(iter(loaded.values()))["type"] == "rename", loaded


def test_a_value_that_is_falsy_but_present_is_kept(tmp_path: Path) -> None:
    """`is not None` rather than truthiness, and the two are not the same.

    Dropping every falsy value writes the same file for this decision and
    passes every test above, because none of them carries a falsy value that
    means anything. An empty string is a value the operator gave; None is a
    key that does not apply. Only the second is absence.
    """
    decisions = {
        "users_name": {
            "type": "rename",
            "table": "users",
            "from_column": "",
            "to_column": "title",
            "column": None,
            "decided_at": "2026-08-20T00:00:00+00:00",
        }
    }

    save_decisions(str(tmp_path), decisions)

    loaded = load_decisions(str(tmp_path))
    assert loaded["users_name"]["from_column"] == "", loaded
    assert loaded["users_name"]["column"] is None, loaded
