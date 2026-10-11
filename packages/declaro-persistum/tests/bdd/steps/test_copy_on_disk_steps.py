"""Bind `tests/bdd/features/opening/copy_on_disk.feature`, and nothing else.

WHAT THE FEATURE ASKS. A file that is there answers yes. A path with nothing at
it, a path inside a folder nobody made, and a path the process may not read all
answer no rather than refusing. Asking makes nothing. And two asks either side of
a delete give two different answers.

EVERY BOUND SCENARIO ASKS A REAL DISK. What this reader does is look, and a
stand-in for the disk would be a test of the stand-in. Each scenario works inside
pytest's own temporary folder, so the files it makes and deletes are its own.

THE FOLDER THAT IS LOCKED IS UNLOCKED AGAIN AT THE END, by the fixture rather
than by the scenario. A folder left at mode 000 defeats the cleanup that removes
the temporary tree, and the failure lands on whichever test runs next.

NOTHING HERE OPENS A DATABASE. Two scenarios used to, and they went to
`session/session_connect.feature` on 2026-10-10 where the order lives. This card
answers yes or no; what an open does with the answer belongs to the open.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
from collections.abc import Iterator
from typing import Any

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.copy_on_disk import copy_on_disk

scenarios("../features/opening/copy_on_disk.feature")


# WHAT A READER THAT ONLY LOOKS MUST NEVER CALL. Each of these makes something,
# opens something, or writes something, and this reader is asked a question
# before a caller has decided to do any of them.
WHAT_MAKES_OR_OPENS_SOMETHING = frozenset(
    {"mkdir", "touch", "open", "connect", "write_text", "write_bytes", "makedirs", "rename"}
)

A_FILE_THAT_IS_THERE = "already.db"


@pytest.fixture
def scenario(tmp_path: pathlib.Path) -> Iterator[dict]:
    held: dict[str, Any] = {
        "tmp": tmp_path,
        "path": "",
        # Every folder this scenario took the permissions off, unlocked at the
        # end so the temporary tree can be removed.
        "locked": [],
        # What the open did with the answer, for the two scenarios that ask.
        # Empty here, so pulling the guard off one of them fails on the
        # assertion and names what is missing rather than on a key nobody filled.
        "pulls": [],
        "waited_on_the_primary": True,
        "where_the_schema_was_read": -1,
    }
    yield held
    for folder in held["locked"]:
        folder.chmod(0o700)


# --- what each sentence sets up ----------------------------------------------


@given("a database file already on this machine")
def _a_file_that_is_there(scenario: dict) -> None:
    made = scenario["tmp"] / A_FILE_THAT_IS_THERE
    made.write_bytes(b"")
    scenario["path"] = str(made)


@given(parsers.parse('a database file already on this machine, written as a "{scheme}" target'))
def _a_file_written_as_a_target(scenario: dict, scheme: str) -> None:
    """The same file, written the way a deployment writes it.

    Four slashes, because the path is absolute and one slash belongs to the
    `://`. That is what the two doors read, and it is what a caller hands over.
    """
    made = scenario["tmp"] / A_FILE_THAT_IS_THERE
    made.write_bytes(b"")
    scenario["path"] = f"{scheme}:///{made}"


@given("a target naming a postgresql server")
def _a_target_naming_a_server(scenario: dict) -> None:
    scenario["path"] = "postgresql://localhost/declarotodo"


@given("a path where no file has been made")
def _a_path_with_nothing_at_it(scenario: dict) -> None:
    scenario["path"] = str(scenario["tmp"] / "nothing.db")


@given("a path inside a folder nobody has made")
def _a_path_in_a_folder_nobody_made(scenario: dict) -> None:
    """The folder is one a deployment would have had to make, and nobody did."""
    scenario["path"] = str(scenario["tmp"] / "no-such-folder" / "app.db")


@given("a path the process has no permission to read")
def _a_path_it_may_not_read(scenario: dict) -> None:
    """A real file inside a folder this process cannot look into.

    The permission goes on the folder rather than on the file, because reading
    whether a file is there is a question about the folder: a file at mode 000 is
    still a file somebody can see.
    """
    folder = scenario["tmp"] / "locked"
    folder.mkdir()
    (folder / "app.db").write_bytes(b"")
    folder.chmod(0o000)
    scenario["locked"].append(folder)
    scenario["path"] = str(folder / "app.db")


# --- asking ------------------------------------------------------------------


@when(parsers.re(r"^copy_on_disk is asked about (?:its path|it)$"))
def _it_is_asked(scenario: dict) -> None:
    scenario["before"] = sorted(path.name for path in scenario["tmp"].iterdir())
    scenario["answer"] = copy_on_disk(scenario["path"])
    scenario["after"] = sorted(path.name for path in scenario["tmp"].iterdir())


@when("copy_on_disk is asked twice and the file is deleted between the asks")
def _it_is_asked_twice(scenario: dict) -> None:
    """The disk changes under it, and the second answer is the new state."""
    first = copy_on_disk(scenario["path"])
    pathlib.Path(scenario["path"]).unlink()
    scenario["answers"] = (first, copy_on_disk(scenario["path"]))


# --- what must be true afterwards --------------------------------------------


# ONE SENTENCE IN TWO WORDS, AND THE WORD IS THE ANSWER. Each row carries what
# that answer means and what a caller does wrong when it is the other one.
WHAT_EACH_ANSWER_COSTS: dict[str, tuple[bool, str]] = {
    "yes": (True, "an open fills a copy that is already here"),
    "no": (False, "an open reads an empty database as the live schema"),
}


@then(parsers.re(r"^it answers (?P<word>yes|no)$"))
def _it_answered(scenario: dict, word: str) -> None:
    wanted, cost = WHAT_EACH_ANSWER_COSTS[word]

    assert scenario["answer"] is wanted, (
        f"the answer about {scenario['path']} is {word}, and the reader said "
        f"{scenario['answer']!r}, so {cost}"
    )


@then("nothing was refused")
def _nothing_was_refused(scenario: dict) -> None:
    """A missing folder is the ordinary case on a machine that has just started."""
    assert isinstance(scenario["answer"], bool), (
        f"the reader answered {scenario['answer']!r} rather than yes or no, and a "
        f"caller asking whether a file is there has nothing to do with a fault"
    )


@then("a copy it cannot read is a copy it cannot use")
def _a_copy_it_cannot_read(scenario: dict) -> None:
    assert scenario["answer"] is False, (
        f"the reader answered {scenario['answer']!r} about a path inside a folder "
        f"this process may not look into. A process running as root reads it "
        f"regardless, and these scenarios are written to run as a user"
    )


@then("no file and no folder were made")
def _nothing_was_made(scenario: dict) -> None:
    assert scenario["after"] == scenario["before"], (
        f"the folder held {scenario['before']} before the ask and "
        f"{scenario['after']} after it, so asking made something"
    )


@then("asking is not the same as opening")
def _asking_is_not_opening(scenario: dict) -> None:
    """Read off the reader's own source, because a scenario watching one folder
    cannot see a call that makes something somewhere else."""
    assert scenario["answer"] is False, f"the reader answered {scenario['answer']!r}"
    called = {
        getattr(node.func, "attr", "") or getattr(node.func, "id", "")
        for node in ast.walk(ast.parse(inspect.getsource(copy_on_disk)))
        if isinstance(node, ast.Call)
    }
    makes = sorted(called & WHAT_MAKES_OR_OPENS_SOMETHING)

    assert makes == [], (
        f"the reader calls {makes}, and a question about the disk that makes a "
        f"folder or opens a connection answers itself yes the next time it is asked"
    )


@then("it answers yes and then no")
def _two_asks_two_answers(scenario: dict) -> None:
    assert scenario["answers"] == (True, False), (
        f"the two asks either side of a delete answered {scenario['answers']}, so "
        f"something is remembering the first answer and a caller is told about a "
        f"file that is gone"
    )
