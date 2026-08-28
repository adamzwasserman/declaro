"""A supported backend that is not running must fail, not skip.

WHY THIS IS A GATE AND NOT A STYLE NOTE. Measured 2026-08-25 with PostgreSQL
stopped: the suite reported 2680 passed and 200 skipped, and 186 of those
skips were tests that had been passing a minute earlier. Nobody counts the `s`
characters. A run on a machine without PostgreSQL said the package was tested,
while the whole PostgreSQL applier, its inspector, its type mapping and the
alter-column path only that engine has went unexercised.

That has already produced a wrong claim here: a session reported seven times
that PostgreSQL was unavailable on this machine, because every test that would
have said otherwise had quietly skipped.

WHAT MAY STILL SKIP. Turso Cloud wants credentials that are not on every
machine, and `ruff`, `mypy` and `pyturso` are optional installs. Those are
absences of a tool or a secret rather than of a backend the package claims to
support. `test_require_fixtures_actually_require` also skips deliberately, and
says why in its own docstring: it asks whether the fixture is over-eager, and
it can say nothing at all without a server.
"""

from __future__ import annotations

import pathlib
import re

import pytest

pytestmark = pytest.mark.precommit

ROOT = pathlib.Path(__file__).resolve().parents[1]

# A skip whose reason names PostgreSQL. The two spellings the suite used
# before this gate went in, plus anything that mentions the engine at all.
_POSTGRESQL_SKIP = re.compile(
    r"pytest\.skip\(\s*f?\"[^\"]*(?:PostgreSQL|postgres|no server at)[^\"]*\"",
    re.IGNORECASE,
)

# It asks whether `require_postgresql` rejects a working environment, so it
# has nothing to say when there is no environment to work in.
#
# AND THIS FILE, because a scanner that reads its own error message and its
# own sample reports itself. That is not a false positive to tolerate: it is
# an instrument measuring its own reflection, and it would have hidden a real
# offender behind two of its own lines.
_ALLOWED = {
    "test_require_fixtures_actually_require.py",
    pathlib.Path(__file__).name,
}


def _offenders() -> list[str]:
    return [
        f"{path.relative_to(ROOT.parent)}:{line}"
        for path in sorted(ROOT.rglob("*.py"))
        if path.name not in _ALLOWED
        for line, text in enumerate(path.read_text().splitlines(), start=1)
        if _POSTGRESQL_SKIP.search(text)
    ]


def test_no_test_skips_because_postgresql_is_not_running() -> None:
    """Use `refuse_without_postgresql`, which says what to do about it."""
    offenders = _offenders()

    assert not offenders, (
        f"these skip when PostgreSQL is not reachable, so the suite reports a "
        f"pass while a supported backend goes untested: {offenders}. Call "
        f"`refuse_without_postgresql(url, exc)` from tests/conftest.py instead."
    )


def test_the_gate_still_finds_what_it_is_looking_for() -> None:
    """A scanner that matches nothing is indistinguishable from a clean tree.

    Asserted against a sample rather than against the tree, so this keeps
    saying something after the last real offender is gone.
    """
    sample = """
        try:
            connection = await asyncpg.connect(url)
        except OSError as exc:
            pytest.skip(f"no PostgreSQL at {url}: {exc}")
    """
    found = [line for line in sample.splitlines() if _POSTGRESQL_SKIP.search(line)]

    assert len(found) == 1, f"the scanner no longer finds a skip it should: {found}"
