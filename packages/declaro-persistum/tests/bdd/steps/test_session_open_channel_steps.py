"""Bind `tests/bdd/features/session/session_open_channel.feature`.

WHAT THE FEATURE ASKS. A copy is made where the path says, with the folder if
there is not one. The open makes the main file and leaves the driver's three
sidecars for the first push. A path this process cannot write is a fault rather
than a handle.

NOTHING HERE TOUCHES THE NETWORK, AND THAT IS THE CARD RATHER THAN A CHOICE OF
TEST. Measured 2026-10-10: the driver builds its sync half on the first push or
pull, so this open reaches no primary. The primary and the token are strings it
carries and does not use, so these scenarios hand it strings and need no
credentials. The two scenarios that did need a primary moved to
`turso/channel_push.feature` the same day.

THE FOLDER THAT IS LOCKED IS UNLOCKED AGAIN AT THE END, by the fixture rather
than by the scenario. A folder left at mode 000 defeats the cleanup that removes
the temporary tree, and the failure lands on whichever test runs next.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import os
import pathlib
from collections.abc import Iterator
from typing import Any

import pytest
from pytest_bdd import given, scenarios, then, when

from declaro_persistum import session_open_channel as the_door
from declaro_persistum.session_open_channel import session_open_channel

scenarios("../features/session/session_open_channel.feature")


THE_THREE_FACTS_IN_A_FAULT = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})

# THE DRIVER'S THREE SIDECARS, which arrive with the first push and not before.
WHAT_THE_FIRST_PUSH_MAKES = ("-changes", "-info", "-wal")

# WHAT A ROUND TRIP IS CALLED ON THE DRIVER'S OWN OBJECT. A door that names one
# of these has gone to the primary, whatever it then does with the answer.
WHAT_REACHES_THE_PRIMARY = frozenset({"push", "pull", "checkpoint"})


def _the_staging_primary() -> tuple[str, str]:
    """The live primary this suite replicates from, or a failure naming what is
    missing.

    THE SCENARIOS MAKE NO ROUND TRIP AND STILL NAME A REAL PRIMARY, because that
    is what turns the sidecar scenario into a check: a pull put back in this door
    would land, bring the primary down, and make the three files that scenario
    says are absent. Against a primary that does not exist it would fail instead
    and leave the disk looking the same.
    """
    url = os.environ.get("TEST_TURSO_URL", "")
    token = os.environ.get("TEST_TURSO_AUTH_TOKEN", "")
    missing = [
        name
        for name, value in (("TEST_TURSO_URL", url), ("TEST_TURSO_AUTH_TOKEN", token))
        if not value
    ]
    assert not missing, (
        f"{', '.join(missing)} is not set in .env. These scenarios make no round "
        f"trip, and they name the real primary so that a pull put back in this "
        f"door would show up as three files on the disk."
    )
    return url, token


@pytest.fixture
def scenario(tmp_path: pathlib.Path) -> Iterator[dict]:
    held: dict[str, Any] = {"tmp": tmp_path, "locked": []}
    yield held
    for folder in held["locked"]:
        folder.chmod(0o700)


# --- what each scenario establishes ------------------------------------------


@given("a path in a folder that exists, with no copy at it yet")
def _a_path_in_a_folder_that_exists(scenario: dict) -> None:
    scenario["path"] = str(scenario["tmp"] / "replica.db")
    assert not pathlib.Path(scenario["path"]).exists()


@given("a path whose folder does not exist")
def _a_path_whose_folder_does_not_exist(scenario: dict) -> None:
    scenario["folder"] = scenario["tmp"] / "not" / "made" / "yet"
    scenario["path"] = str(scenario["folder"] / "replica.db")
    assert not scenario["folder"].exists()


@given("a path needing a folder inside one this process may not write")
def _a_path_needing_a_folder_it_cannot_make(scenario: dict) -> None:
    """A folder at mode 000 with the database a level below it.

    The database goes a level down because the open makes the folder and writes
    nothing else. A path directly inside the locked folder hands back a channel,
    since the driver writes nothing until the first push.
    """
    folder = scenario["tmp"] / "locked"
    folder.mkdir()
    folder.chmod(0o000)
    scenario["locked"].append(folder)
    scenario["path"] = str(folder / "made-here" / "replica.db")


@when("a channel is opened at that path")
def _a_channel_is_opened(scenario: dict) -> None:
    url, token = _the_staging_primary()
    scenario["answer"] = asyncio.run(session_open_channel(scenario["path"], url, token))


# --- what must be true afterwards --------------------------------------------


@then("a copy is there")
def _a_copy_is_there(scenario: dict) -> None:
    """Read off the disk, because a channel coming back is not evidence of one."""
    assert pathlib.Path(scenario["path"]).exists(), (
        f"the channel came back and there is no copy at {scenario['path']}, so "
        f"nothing was replicated into"
    )


@then("the folder was made")
def _the_folder_was_made(scenario: dict) -> None:
    """The driver takes a path and will not make the directory around it, and a
    fresh machine has neither."""
    assert scenario["folder"].exists(), (
        f"{scenario['folder']} was not made, so a deployment on a fresh disk is "
        f"refused for a folder it asked this library to put a database in"
    )


@then("the main file is there and the three the driver needs are not")
def _the_main_file_and_nothing_else(scenario: dict) -> None:
    """A path holding a main file alone is a copy nothing can read yet, which is
    what anybody asking whether a copy is here has to know."""
    main = pathlib.Path(scenario["path"])
    early = [one for one in WHAT_THE_FIRST_PUSH_MAKES if main.with_name(main.name + one).exists()]

    assert main.is_file(), f"the open made no file at {main}"
    assert early == [], (
        f"the open made {early} as well, so the first push is not what completes "
        f"a copy and asking whether a copy is here can stop at the main file"
    )


@then("a fault comes back naming the path")
def _a_fault_names_the_path(scenario: dict) -> None:
    answer = scenario["answer"]

    assert isinstance(answer, dict) and set(answer) == THE_THREE_FACTS_IN_A_FAULT, (
        f"a path this process cannot write answered {answer!r}, and a caller "
        f"holding that writes to a database that cannot be made"
    )
    assert scenario["path"] in answer["what_went_wrong"], (
        f"the fault does not name the path it could not open: {answer['what_went_wrong']!r}"
    )


@then("no channel is handed back")
def _no_channel_is_handed_back(scenario: dict) -> None:
    assert not hasattr(scenario["answer"], "cursor"), (
        "a connection came back beside the fault, and a caller holding one "
        "writes to it believing it replicates"
    )


@then("the open made no round trip")
def _the_open_made_no_round_trip(scenario: dict) -> None:
    """Read off the door's own source, because a trip that was made and failed
    leaves a disk that looks exactly like one that was never made."""
    assert not isinstance(scenario["answer"], dict), (
        f"the channel did not open: {scenario['answer']}"
    )
    called = {
        getattr(node.func, "attr", "") or getattr(node.func, "id", "")
        for node in ast.walk(ast.parse(inspect.getsource(the_door)))
        if isinstance(node, ast.Call)
    }
    reaches = sorted(called & WHAT_REACHES_THE_PRIMARY)

    assert reaches == [], (
        f"this door calls {reaches} on the driver, so it spends a round trip to "
        f"prove a channel that the open's own push proves anyway"
    )
