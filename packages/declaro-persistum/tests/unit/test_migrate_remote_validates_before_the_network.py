"""`declaro migrate-remote` reached a cloud database before checking the schema.

MEASURED 2026-08-19. `cmd_migrate_remote` loads its models, expands enums,
optionally strips foreign keys, and then opens a connection to Turso cloud and
applies DDL, without calling `validate_schema` once. It was the last of the
three paths that migrate, after `apply_migrations_async` and `cmd_apply` were
each given the check earlier today.

THE ORDER IS THE POINT HERE, more than on the other two. This command writes
to a production cloud database over the network. A schema the validator would
refuse should be refused on the developer's machine, before an auth token is
used and before anything is created remotely. The check goes above the
connection, so a refusal costs no round trip and touches nothing.

That is also what makes it testable without a cloud database: the URL below is
not reachable and must never be reached. A test that got a connection error
instead of a refusal would be telling us the check is in the wrong place.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = [pytest.mark.precommit]

CLEAN = """
from pydantic import BaseModel
from declaro_persistum import table, field


@table("users")
class User(BaseModel):
    id: int = field(primary_key=True)
    name: str
"""

DANGLING = """
from pydantic import BaseModel
from declaro_persistum import table, field


@table("posts")
class Post(BaseModel):
    id: int = field(primary_key=True)
    author_id: int = field(references="authors.id")
"""

# A URL that resolves to nothing. Reaching it is the failure this test is
# looking for, not the failure it is asserting.
UNREACHABLE = "libsql://declaro-nonexistent-probe.turso.io"


def _migrate_remote(tmp_path: Path, body: str) -> subprocess.CompletedProcess[str]:
    models = tmp_path / "models.py"
    models.write_text(body)

    return subprocess.run(
        [
            sys.executable,
            "-m",
            "declaro_persistum.cli.main",
            "migrate-remote",
            "--remote",
            UNREACHABLE,
            "--token",
            "not-a-real-token",
            "--schema",
            str(models),
            "--init",
            "--dry-run",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )


def test_an_invalid_schema_is_refused(tmp_path: Path) -> None:
    result = _migrate_remote(tmp_path, DANGLING)

    assert result.returncode != 0, f"{result.stdout}\n{result.stderr}"


def test_the_refusal_names_the_dangling_reference(tmp_path: Path) -> None:
    """An operator needs the table name, not a count."""
    result = _migrate_remote(tmp_path, DANGLING)

    output = result.stdout + result.stderr
    assert "authors" in output, output


def test_it_is_refused_without_reaching_the_network(tmp_path: Path) -> None:
    """The order, which is the whole reason to put the check here.

    The URL is unreachable. If the refusal came after the connection attempt,
    the output would carry a network failure rather than the validator's
    words, and an auth token would already have been used.
    """
    result = _migrate_remote(tmp_path, DANGLING)

    # THE WORDS THE ENGINE ACTUALLY USES. The first version of this list held
    # "connect", "network", "resolve", "timed out" and "unauthorized", and the
    # failure it was written to catch says "sync engine operation failed ...
    # status=404, body=Host not found". None of those five appear in it, so
    # the test passed while the command was reaching the network on every run.
    output = (result.stdout + result.stderr).lower()
    for reaching in (
        "sync engine",
        "host not found",
        "404",
        "connect",
        "network",
        "timed out",
        "unauthorized",
    ):
        assert reaching not in output, (
            f"the schema was refused only after reaching the network: {output}"
        )


def test_a_clean_schema_gets_past_the_check(tmp_path: Path) -> None:
    """The control. A gate that refused everything would pass the rest.

    This one is expected to fail at the network, because the URL is
    unreachable and a clean schema has no reason to stop earlier. What it must
    not do is fail with the validator's words.
    """
    result = _migrate_remote(tmp_path, CLEAN)

    output = result.stdout + result.stderr
    assert "not valid" not in output.lower(), output
