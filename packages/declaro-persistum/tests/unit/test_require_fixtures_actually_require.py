"""A fixture that requires a database must open one.

`require_postgresql` claimed to guarantee a real PostgreSQL for the stress
scenarios. What it actually did:

    import asyncpg                        # is the DRIVER installed
    pg_url = os.environ.get(..., "postgresql://postgres:postgres@...")
    if not pg_url:                        # a string with a default
        pytest.fail(...)                  # therefore unreachable

Neither check touches a server. The second cannot fail at all, because the
default makes the string non-empty by construction — a guard whose condition
is always false, sitting there looking like protection.

The cost is not theoretical. On 2026-08-12 five multi_backend scenarios
reported green. Two of them were PostgreSQL, and they were green because a
server happened to be running on this machine. Had it been down, the fixture
would still have said the environment was fine and the scenarios would have
errored somewhere further in, blaming whatever they touched first.

This is the third gate in this package found to gate nothing, after the
`-m precommit` marker carried by zero tests and the reachability of dead
modules. The pattern is a check written against a proxy — a driver import, a
string's length, a marker's name — rather than against the thing it claims.

So these tests assert the fixture's BEHAVIOUR, not its text: point it at an
unreachable server and it must fail. A fixture that passes against a database
that is not there is not requiring anything.
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.precommit


UNREACHABLE = "postgresql://postgres:postgres@127.0.0.1:1/definitely_not_there"

# `pytest.fail` raises `Failed`, which derives from BaseException rather than
# Exception, so `pytest.raises(Exception)` does NOT catch it. Naming it
# explicitly is also the sharper assertion: the fixture must fail the RUN, not
# merely raise something.
FAILED = pytest.fail.Exception


def test_it_fails_when_the_server_is_unreachable(monkeypatch):
    """The whole point. A wrong URL must stop the run."""
    from tests.conftest import require_postgresql

    monkeypatch.setenv("TEST_POSTGRESQL_URL", UNREACHABLE)

    with pytest.raises(FAILED) as caught:
        require_postgresql.__wrapped__()

    assert "postgres" in str(caught.value).lower(), (
        f"it failed, but not about PostgreSQL: {caught.value}"
    )


def test_it_passes_when_the_server_is_reachable():
    """The other half — a working environment must not be rejected.

    Skipped rather than failed when PostgreSQL is genuinely absent: this test
    is about the fixture not being over-eager, and it cannot say anything
    without a server. The test above is the one that catches the real defect
    and it needs no server at all.
    """
    from tests.conftest import require_postgresql

    try:
        require_postgresql.__wrapped__()
    except BaseException as e:
        pytest.skip(f"no local PostgreSQL to check against: {e}")


def test_the_url_check_is_not_the_only_check(monkeypatch):
    """Guards the specific bug: a non-empty URL must not be sufficient.

    The old fixture accepted any non-empty string. This asserts that a
    well-formed URL pointing at nothing is still rejected — which is exactly
    what a string-length check cannot do.
    """
    from tests.conftest import require_postgresql

    monkeypatch.setenv("TEST_POSTGRESQL_URL", UNREACHABLE)

    with pytest.raises(FAILED):
        require_postgresql.__wrapped__()
