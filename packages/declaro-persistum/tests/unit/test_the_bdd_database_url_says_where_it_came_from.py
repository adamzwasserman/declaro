"""Where the BDD scenarios point, and how they answer when nobody said.

WRITTEN BEFORE THE CODE. `get_sqlite_url` and `get_postgresql_url` were
deleted on 2026-08-16 for this shape:

    return os.environ.get("TEST_POSTGRESQL_URL", DEFAULT)

which cannot tell three cases apart, and two of them are not the same thing:

    unset                a developer machine, so the local default is right
    set to a real url    an operator pointing the suite somewhere
    set to ""            an operator who meant to set it and did not

The third silently became the first. `TEST_POSTGRESQL_URL=` in a CI file, or a
shell variable that expanded to nothing, ran the whole suite against localhost
and reported it as a pass against whatever the operator thought they had
configured.

ABSENCE IS A NAMED CASE, NOT A DEFAULT (Rule 14). `resolve_url` returns the
documented default when the variable is genuinely unset and raises when it is
set to nothing, so the two are distinguishable at the one place that knows.

THE SQLITE PATH IS DELIBERATELY NOT CONFIGURABLE, and that is a fix rather
than an omission. `get_turso_url` already carried the reason in its own
docstring: `TEST_TURSO_URL` holds a REMOTE `libsql://` url and was handed to
`turso.connect()`, which takes a local filesystem path, so the engine tried to
open a file named "libsql://..." and failed with `IoError: open: NotFound`.
The SQLite twin has the same shape and the same trap.
"""

from __future__ import annotations

import pytest

from tests.bdd.factories.connection_factory import (
    postgresql_url,
    resolve_url,
    sqlite_path,
    turso_path,
)

pytestmark = pytest.mark.precommit


# ---------------------------------------------------------------------------
# The three cases, told apart
# ---------------------------------------------------------------------------


def test_an_unset_variable_gives_the_documented_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("EXAMPLE_URL", raising=False)

    assert resolve_url("EXAMPLE_URL", default="postgresql://local/db") == (
        "postgresql://local/db"
    )


def test_a_set_variable_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXAMPLE_URL", "postgresql://elsewhere/db")

    assert resolve_url("EXAMPLE_URL", default="postgresql://local/db") == (
        "postgresql://elsewhere/db"
    )


def test_a_variable_set_to_empty_raises_and_names_itself(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The case the old `.get(name, default)` could not see."""
    monkeypatch.setenv("EXAMPLE_URL", "")

    with pytest.raises(ValueError, match="EXAMPLE_URL is set to an empty"):
        resolve_url("EXAMPLE_URL", default="postgresql://local/db")


def test_a_variable_set_to_whitespace_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """`TEST_PG_URL=" "` is the same mistake with a space in it."""
    monkeypatch.setenv("EXAMPLE_URL", "   ")

    with pytest.raises(ValueError, match="EXAMPLE_URL is set to an empty"):
        resolve_url("EXAMPLE_URL", default="postgresql://local/db")


def test_the_default_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    """No default for the default. A caller says what absence means."""
    with pytest.raises(TypeError):
        resolve_url("EXAMPLE_URL")  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# What the scenarios actually call
# ---------------------------------------------------------------------------


def test_postgresql_url_reads_the_variable_the_rest_of_the_suite_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`TEST_POSTGRESQL_URL`, the same name `tests/conftest.py` uses.

    Two names for one setting would let `require_postgresql` probe one server
    and the scenarios run against another, and the suite would report a pass.
    """
    monkeypatch.setenv("TEST_POSTGRESQL_URL", "postgresql://somewhere/else")

    assert postgresql_url() == "postgresql://somewhere/else"


def test_postgresql_url_defaults_to_the_shared_constant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.conftest import DEFAULT_POSTGRESQL_URL

    monkeypatch.delenv("TEST_POSTGRESQL_URL", raising=False)

    assert postgresql_url() == DEFAULT_POSTGRESQL_URL


def test_postgresql_url_raises_when_the_variable_is_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TEST_POSTGRESQL_URL", "")

    with pytest.raises(ValueError, match="TEST_POSTGRESQL_URL"):
        postgresql_url()


# ---------------------------------------------------------------------------
# The local engines, which take a path and not a url
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("variable", ["TEST_SQLITE_URL", "TEST_TURSO_URL"])
def test_a_local_path_ignores_the_environment(
    monkeypatch: pytest.MonkeyPatch, variable: str
) -> None:
    """A remote url handed to a local engine is the `IoError` this prevents.

    `turso.connect()` and `aiosqlite.connect()` take a filesystem path. Given
    `libsql://host/db` they try to open a file with that name, and the failure
    reads as a missing database rather than a misconfigured one.
    """
    monkeypatch.setenv(variable, "libsql://somewhere/remote")

    assert not sqlite_path().startswith("libsql://")
    assert not turso_path().startswith("libsql://")


def test_the_two_local_engines_do_not_share_one_file() -> None:
    """One file for both would make each engine's scenarios depend on the other."""
    assert sqlite_path() != turso_path()


def test_a_local_path_is_stable_within_a_run() -> None:
    """Steps in one scenario open separate connections to the same database.

    A path that changed per call would give each step its own empty file, and
    a Given that created a table would be invisible to the When that queried
    it.
    """
    assert sqlite_path() == sqlite_path()
    assert turso_path() == turso_path()
