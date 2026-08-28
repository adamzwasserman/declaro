"""The test suite must never reach a database somebody else owns.

WHY THIS IS A GATE. A migration engine's tests are destructive by
construction. They introspect whatever database they are pointed at, diff it
against a small declared schema, and apply the result -- which means a
drop_table for every table the declaration does not mention. That is the
engine working correctly. It is catastrophic aimed at a real database.

MEASURED 2026-08-25. This package's `.env` sets `TEST_POSTGRESQL_URL` to
`declarotodo`, which holds real data, and `probe_url()` preferred it over the
scratch database it builds. Two separate paths then destroyed it on every full
run:

    the session fixture ran DROP DATABASE "declarotodo" and remade it empty

    every migrating test diffed it down to whatever schema that test declared

`declarotodo` was found already at zero tables. A canary table planted in it
was gone after one run, by the second path, after the first had been closed.

WHAT THE FIX IS. A caller may say which SERVER to use. The database name is
built from this process id and never from anything a caller passed, so there
is no database the suite reaches that it did not make.
"""

from __future__ import annotations

import os
import re

import pytest

from tests.conftest import DEFAULT_POSTGRESQL_URL, probe_url

pytestmark = pytest.mark.precommit

# `declaro_probe_<pid>` and nothing else.
_OURS = re.compile(r"^declaro_probe_\d+$")


def _database_of(url: str) -> str:
    return url.rpartition("/")[2]


def test_the_probe_database_is_one_this_process_named() -> None:
    """Whatever the environment says."""
    name = _database_of(probe_url())

    assert _OURS.match(name), (
        f"the suite is pointed at {name!r}, which it did not create. Every "
        f"migrating test diffs the database it reaches down to its own "
        f"declared schema, so this is a database the suite will empty."
    )


def test_an_environment_url_moves_the_server_and_not_the_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The caller chooses the server. The suite chooses the database."""
    monkeypatch.setenv("TEST_POSTGRESQL_URL", "postgresql://someone@elsewhere:5432/production")

    chosen = probe_url()

    assert chosen.startswith("postgresql://someone@elsewhere:5432/"), (
        f"a caller asked for another server and did not get it: {chosen}"
    )
    assert _OURS.match(_database_of(chosen)), (
        f"a caller named a database and the suite took it: {chosen}"
    )


def test_the_default_names_no_real_database() -> None:
    """`declarotodo` was the default here, and it holds data.

    `postgres` is the maintenance database every server has. Nothing in this
    suite writes to it; it is somewhere to connect in order to ask about
    others.
    """
    assert _database_of(DEFAULT_POSTGRESQL_URL) == "postgres", (
        f"the default reaches {_database_of(DEFAULT_POSTGRESQL_URL)!r}, which "
        f"is a database somebody may be keeping something in"
    )


def test_the_gate_still_recognises_a_database_that_is_not_ours() -> None:
    """A pattern that matches everything would pass on any name at all."""
    assert not _OURS.match("declarotodo")
    assert not _OURS.match("declaro_probe_")
    assert _OURS.match(f"declaro_probe_{os.getpid()}")
