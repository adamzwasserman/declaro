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

TWO SCENARIOS ARE `xfail` AND BOTH NAME THE SAME MISSING DECISION. They ask what
a caller's open does with the answer, and `orchestrator fn session_connect` does
not ask it yet: `boundary_out fn session_open_channel` proves the channel with a
pull of its own, so every replicated open waits on the primary once before
anything here could decide whether to wait again. Which of the two pulls goes is
the author's decision, not this file's.
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


# TWO SENTENCES THAT CANNOT RUN YET, AND EACH SAYS WHY. They ask what an open
# does with this answer, and the open does not ask yet.
_WHAT_AN_OPEN_WOULD_HAVE_TO_DECIDE = (
    "Needs `orchestrator fn session_connect` to ask this reader before it fills "
    "a copy. It does not: `boundary_out fn session_open_channel` proves the "
    "channel with one pull, which runs on every replicated open before anything "
    "could decide whether a copy needs filling, so a warm copy waits on the "
    "primary whatever this reader answers."
)

WHAT_EACH_REPLICATED_SENTENCE_SETS: dict[str, str] = {
    "a replicated database whose copy is already on this machine": (
        _WHAT_AN_OPEN_WOULD_HAVE_TO_DECIDE
    ),
    "a replicated database with no copy on this machine": _WHAT_AN_OPEN_WOULD_HAVE_TO_DECIDE,
}


@given(
    parsers.re(
        "^(?P<sentence>"
        + "|".join(sorted(WHAT_EACH_REPLICATED_SENTENCE_SETS, key=len, reverse=True))
        + ")$"
    )
)
def _a_replicated_database(sentence: str) -> None:
    pytest.xfail(WHAT_EACH_REPLICATED_SENTENCE_SETS[sentence])


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


@when("a caller opens it")
def _a_caller_opens_it(scenario: dict) -> None:
    """Reached only if somebody takes the guard off one of the two scenarios
    above, and then the Thens read the empty record and say what is missing."""
    scenario["asked"] = True


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


@then("the copy was not pulled at open")
def _the_copy_was_not_pulled(scenario: dict) -> None:
    assert scenario["pulls"] == [], (
        f"the copy was already here and the open pulled {scenario['pulls']}, "
        f"which costs a round trip to the primary for a copy that is current"
    )


@then("the open did not wait on the primary")
def _the_open_did_not_wait(scenario: dict) -> None:
    assert scenario["waited_on_the_primary"] is False, (
        "the open waited on the primary for a copy that was already here, and a "
        "deployment measures that wait at about two seconds"
    )


@then("the copy was pulled once")
def _the_copy_was_pulled_once(scenario: dict) -> None:
    assert len(scenario["pulls"]) == 1, (
        f"a cold copy holds no rows until it has pulled, and the open pulled "
        f"{len(scenario['pulls'])} times"
    )


@then("the live schema was read after that pull, not before")
def _the_schema_was_read_after_the_pull(scenario: dict) -> None:
    """Reading it first reads an empty database and plans a migration that
    creates every table the primary already has."""
    assert scenario["where_the_schema_was_read"] > len(scenario["pulls"]) - 1, (
        f"the live schema was read at step "
        f"{scenario['where_the_schema_was_read']} and the copy was filled by "
        f"{scenario['pulls']}, so the migration was planned against an empty copy"
    )
