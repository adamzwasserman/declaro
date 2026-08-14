"""Every name the package references must exist.

TWO CLASS-DELETION SWEEPS EACH LEFT A CALL SITE BEHIND, and both survived.

    7a5c9ac  delete every class            removed PostgreSQLBulkLoader and
                                           GenericBulkLoader, kept the factory
                                           that returns them
    bf5f7d0  introspection is functions    removed _normalize_view_query, kept
                                           the two lines that call it

Every supported dialect through `create_bulk_loader` raised NameError, and so
did PostgreSQL view introspection. Nothing failed, because nothing ran them: the
bulk path has no test, and neither does the view path on a real PostgreSQL.

WHY THE `# type: ignore` MADE IT WORSE RATHER THAN CAUSING IT. The two factory
lines carried `# type: ignore[return-value]`, which does not suppress
`name-defined` — mypy reported the missing names the whole time. What was
missing is anything that READS mypy. An escape on the exact line that is broken
is also a sign saying "this line was hard to typecheck", and it was the last
sign anyone left.

SO THE GATE IS THE FIX, NOT THE THREE REPAIRS. A deletion sweep is a normal and
good thing to do; leaving the call site is the failure, and only a machine
notices reliably. This asserts the narrowest useful property — every referenced
name resolves — rather than a clean bill of health, so it can hold at zero from
today without first fixing every other complaint mypy has.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.precommit

SRC = Path(__file__).resolve().parents[2] / "src" / "declaro_persistum"


def _mypy(*args: str) -> str:
    if shutil.which("mypy") is None:
        pytest.skip("mypy is not installed")
    return subprocess.run(
        ["mypy", "--no-error-summary", *args],
        capture_output=True,
        text=True,
        cwd=SRC.parents[1],
    ).stdout


def test_the_gate_can_see_the_source_at_all() -> None:
    """A gate that reads nothing passes forever."""
    assert SRC.is_dir(), f"{SRC} is not a directory"
    assert len(list(SRC.rglob("*.py"))) > 20, "found almost no source to check"


def test_no_module_calls_a_name_that_does_not_exist() -> None:
    undefined = [
        line
        for line in _mypy(str(SRC)).splitlines()
        if "[name-defined]" in line
    ]
    assert not undefined, (
        "these reference names that do not exist, so the line raises NameError "
        "the moment it runs:\n  " + "\n  ".join(undefined)
    )


def test_no_module_exports_a_name_that_does_not_exist() -> None:
    """`__all__` is a promise, and mypy does not read it.

    `applier/turso.py` listed "TursoApplier" long after the class was deleted,
    so `from declaro_persistum.applier.turso import *` raised AttributeError
    while the test above stayed green. Same failure as the four before it -- a
    definition removed, a reference left -- in the one place the gate that was
    built for exactly that could not see.

    Ruff's F822 reads `__all__` and mypy's name-defined does not, so this needs
    the second instrument rather than a wider setting on the first.
    """
    if shutil.which("ruff") is None:
        pytest.skip("ruff is not installed")
    out = subprocess.run(
        ["ruff", "check", "--output-format=concise", "--select", "F822", str(SRC)],
        capture_output=True,
        text=True,
        cwd=SRC.parents[1],
    ).stdout
    phantom = [line for line in out.splitlines() if " F822 " in line]
    assert not phantom, (
        "these are promised by __all__ and do not exist, so a star import "
        "raises AttributeError:\n  " + "\n  ".join(phantom)
    )


def test_the_package_root_exports_everything_it_imports() -> None:
    """`__init__.py` pulls a name in for one reason: to hand it to callers.

    It named `open_sqlite`, `open_postgresql`, `open_turso` and `start_crew` in
    `__all__` while leaving out `reading`, `writing`, `close`, `flush` and
    `Database` — so the declared contract let you open a database and start a
    crew, and not read or write the thing you opened. All nine were importable;
    only the promise was wrong, in the opposite direction to the TursoApplier
    above.

    F401 in this one file is exactly that question: a name imported here and
    neither used nor exported is a name that arrived for nobody.
    """
    if shutil.which("ruff") is None:
        pytest.skip("ruff is not installed")
    init = SRC / "__init__.py"
    out = subprocess.run(
        ["ruff", "check", "--output-format=concise", "--select", "F401", str(init)],
        capture_output=True,
        text=True,
        cwd=SRC.parents[1],
    ).stdout
    stranded = [line for line in out.splitlines() if " F401 " in line]
    assert not stranded, (
        "imported into the package root and neither used nor in __all__:\n  "
        + "\n  ".join(stranded)
    )
