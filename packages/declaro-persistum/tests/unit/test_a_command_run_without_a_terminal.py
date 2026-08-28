"""Two defects found by running the CLI the way automation runs it.

`declaro generate` was fixed earlier today after being measured broken. Four
of the five commands had still never been run end to end, so I ran them.

MEASURED 2026-08-17, against a real SQLite database and the schema this
package ships.

FIRST: `declaro snapshot` dies whenever stdin is not a terminal.

    $ declaro -c sqlite:////tmp/p.db -s schema/ snapshot < /dev/null
    Error: EOF when reading a line
    exit=1

`cmd_snapshot` calls `input()` unconditionally when a snapshot already exists
and `--force` was not passed. In a shell that is a prompt; in CI, in cron, in a
pipe or in a container it is an EOFError. Taking a snapshot after a deploy is
exactly the automated case, and `cmd_diff` and `cmd_apply` both already derive
`unattended = not sys.stdin.isatty()` -- this command was the one that did not.

SECOND: `declaro validate` warns about the schema this package ships.

    ⚠ Schema has 1 warning(s):
      - Table 'todos', column 'id': Primary key column should be NOT NULL

The check reads `col_def.get("nullable", True)`, so a primary key that does not
repeat `nullable: False` is read as nullable and warned about. A PRIMARY KEY is
NOT NULL on every engine this package supports, by definition rather than by
declaration, so the warning asks the author to say a thing twice and fires on
the package's own example.

THAT DEFAULT IS THE SAME BUG TWICE IN ONE DAY. `_compute_column_alterations`
had it too, and a PostgreSQL migration emitted `nullable: {"from": false, "to":
true}` on an unchanged schema forever, an operation PostgreSQL refuses. Two
instances of one mistake in two modules is the architecture talking: the
question "does this column accept NULL" was being re-derived by whoever needed
it. It is `is_nullable` in `types.py` now, asked by both.

An EXPLICIT `nullable: True` on a primary key is still warned about, because
that is a contradiction the author wrote down rather than one this code
invented.
"""

from __future__ import annotations

import pathlib
import subprocess
import sys

import pytest

pytestmark = pytest.mark.precommit

SHIPPED = (
    pathlib.Path(__file__).resolve().parents[2]
    / "examples"
    / "todo_app_native"
    / "schema"
    / "todos.toml"
)


def _run(args: list[str], cwd: pathlib.Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "-m", "declaro_persistum.cli.main", *args],
        capture_output=True,
        text=True,
        timeout=120,
        stdin=subprocess.DEVNULL,  # no terminal, as in CI
        cwd=str(cwd),
    )


@pytest.fixture
def project(tmp_path: pathlib.Path) -> pathlib.Path:
    schema = tmp_path / "schema"
    schema.mkdir()
    (schema / "todos.toml").write_text(SHIPPED.read_text())
    return tmp_path


def _url(project: pathlib.Path) -> str:
    return f"sqlite:///{project / 'p.db'}"


# ---------------------------------------------------------------------------
# snapshot, without a terminal
# ---------------------------------------------------------------------------


def test_snapshot_works_when_there_is_nothing_to_overwrite(project) -> None:
    """The floor: the first snapshot asks nothing, so it must already work."""
    result = _run(
        ["-c", _url(project), "-s", str(project / "schema"), "apply", "--force"],
        project,
    )
    assert result.returncode == 0, result.stderr[-1500:]

    snapshot = project / "schema" / "snapshot.toml"
    assert snapshot.exists(), "apply did not write a snapshot to overwrite"


def test_snapshot_does_not_die_on_a_closed_stdin(project) -> None:
    """The measured defect. `input()` on a pipe is an EOFError, not a prompt."""
    _run(
        ["-c", _url(project), "-s", str(project / "schema"), "apply", "--force"],
        project,
    )

    result = _run(["-c", _url(project), "-s", str(project / "schema"), "snapshot"], project)

    assert "EOF when reading a line" not in result.stderr, (
        f"the command tried to prompt with no terminal attached:\n{result.stderr[-800:]}"
    )


def test_snapshot_says_how_to_proceed_when_it_will_not_ask(project) -> None:
    """Refusing is fine. Refusing without naming the flag is not."""
    _run(
        ["-c", _url(project), "-s", str(project / "schema"), "apply", "--force"],
        project,
    )

    result = _run(["-c", _url(project), "-s", str(project / "schema"), "snapshot"], project)

    assert result.returncode != 0, "it overwrote a snapshot without being asked and without --force"
    assert "--force" in result.stderr or "--force" in result.stdout, (
        f"the reader is refused and not told what to pass:\n{result.stderr[-800:]}"
    )


def test_snapshot_with_force_overwrites_without_asking(project) -> None:
    """The escape hatch the refusal names has to work."""
    _run(
        ["-c", _url(project), "-s", str(project / "schema"), "apply", "--force"],
        project,
    )
    snapshot = project / "schema" / "snapshot.toml"
    snapshot.write_text("# clobbered\n")

    result = _run(
        ["-c", _url(project), "-s", str(project / "schema"), "snapshot", "--force"],
        project,
    )

    assert result.returncode == 0, result.stderr[-1500:]
    assert "clobbered" not in snapshot.read_text()


# ---------------------------------------------------------------------------
# validate, against the schema this package ships
# ---------------------------------------------------------------------------


def test_the_shipped_schema_validates_without_warnings(project) -> None:
    """The package's own example must not trip its own validator."""
    result = _run(["-s", str(project / "schema"), "validate"], project)

    assert result.returncode == 0, result.stderr[-1500:]
    assert "warning" not in result.stdout.lower(), (
        f"the shipped example schema produces a warning:\n{result.stdout}"
    )


def test_a_primary_key_needs_no_second_declaration() -> None:
    """The rule, at the level it is decided rather than through the CLI."""
    from declaro_persistum.validator import validate_schema

    schema = {"t": {"columns": {"id": {"type": "text", "primary_key": True}}}}

    # Reached by name. This returned `tuple[list[str], list[str]]` and I got
    # the order wrong twice writing this very test, which is why it does not
    # any more.
    result = validate_schema(schema)

    assert not result["warnings"], result["warnings"]
    assert not result["errors"], result["errors"]


def test_a_primary_key_declared_nullable_is_still_a_contradiction() -> None:
    """The floor. Silencing the check entirely would pass the test above.

    An explicit `nullable: True` on a primary key is something the author
    wrote down, and it cannot be honoured on any engine.
    """
    from declaro_persistum.validator import validate_schema

    schema = {"t": {"columns": {"id": {"type": "text", "primary_key": True, "nullable": True}}}}

    assert validate_schema(schema)["warnings"], "a nullable primary key was accepted in silence"


def test_the_question_is_asked_in_one_place() -> None:
    """Both callers ask `is_nullable`; neither re-derives it.

    The `.get("nullable", True)` default produced this defect twice in one day,
    in the differ and in the validator, and a third caller would produce it
    again.
    """
    import inspect

    from declaro_persistum import validator
    from declaro_persistum.differ import core
    from declaro_persistum.types import is_nullable

    assert is_nullable({"primary_key": True}) is False
    assert is_nullable({"nullable": True}) is True
    assert is_nullable({}) is True

    # PARSED, NOT GREPPED. The first version of this searched the source text
    # and matched the comment in validator.py that RECORDS the defect, so the
    # gate reported the fix as the thing it was looking for.
    import ast

    for module in (validator, core):
        tree = ast.parse(inspect.getsource(module))
        offenders = [
            node.lineno
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and getattr(node.func, "attr", None) == "get"
            and len(node.args) == 2
            and isinstance(node.args[0], ast.Constant)
            and node.args[0].value == "nullable"
            and isinstance(node.args[1], ast.Constant)
            and node.args[1].value is True
        ]
        assert not offenders, (
            f"{module.__name__} decides nullability for itself at lines "
            f"{offenders}, which is where this defect came from twice"
        )
