"""A connection that reads is never a connection that syncs.

WHAT THIS PREVENTS, MEASURED ON THE OLD PERSISTUM BY ANOTHER PROJECT ON
2026-10-02. Twenty identical reads against a cloud-synced Turso replica mostly
finished in under a millisecond, and four of them took 542, 642, 923 and 1019ms.
Per-phase timing put the whole of one 730ms request inside a single read: 587ms
in the read, 0 to 70ms in everything around it. The pool was warm and nothing had
been written between the reads. Some reads were waiting on a round trip to the
primary while most served the local file.

WHY THE REWRITE CANNOT DO THAT. `fn session_open_connection` opens Turso with
`turso.aio.connect(path)`, a path and nothing else, so that object has no remote
url and no token, no sync half, and nothing to wait for. The synced object is a
second connection that `boundary_in fn session_open_channel` builds with
`turso.aio.sync.connect(path, primary, auth_token=)`, and only `fn channel_pull`
and `fn channel_push` ever touch it. The old version gave one object both jobs.

WHY A TEST AND NOT A COMMENT. The driver pulls on its own schedule once a
threshold trips: `sync.connect` takes `pull_bytes_threshold` and
`push_operations_threshold`. So a reader handed the primary starts blocking on
the cloud at times nobody picked, intermittently, under load, and every test here
would still pass. Handing the primary to the read door to save an open is the
obvious shortcut, and this is what refuses it.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

pytestmark = pytest.mark.precommit

_SRC = pathlib.Path(__file__).resolve().parents[2] / "src" / "declaro_persistum"
_THE_READ_DOOR = _SRC / "session_open_connection.py"
_THE_SYNC_DOOR = _SRC / "session_open_channel.py"

# WHAT A CALL TO THE DRIVER IS SPELLED AS, by the dotted name being called. The
# first opens a local file. The second opens a file that talks to a primary.
LOCAL = "turso.aio.connect"
SYNCED = "turso.aio.sync.connect"

# WHAT NAMING A PRIMARY LOOKS LIKE in a call to the driver, whichever way it is
# passed. A second positional argument is the remote url; these are the keywords.
WHAT_CARRIES_A_PRIMARY = frozenset({"remote_url", "auth_token", "primary", "token"})


def _dotted(node: ast.expr) -> str:
    """The dotted name of whatever is being called, as source spells it."""
    return ast.unparse(node)


def _every_call_to(said: ast.Module, spelled: str) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(said)
        if isinstance(node, ast.Call) and _dotted(node.func) == spelled
    ]


def _how_a_call_names_a_primary(call: ast.Call) -> list[str]:
    """Every way this one call hands the driver a primary, or an empty list."""
    return [
        *["a second positional argument"] * int(len(call.args) > 1),
        *[word.arg for word in call.keywords if word.arg in WHAT_CARRIES_A_PRIMARY],
    ]


def _read(path: pathlib.Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"))


# BOTH DOORS, READ ONCE, WHEN THIS FILE LOADS. Collecting this file is the edge.
WHAT_THE_READ_DOOR_SAYS = _read(_THE_READ_DOOR)
WHAT_THE_SYNC_DOOR_SAYS = _read(_THE_SYNC_DOOR)


def test_the_read_door_opens_turso_by_path_alone() -> None:
    """A read connection that knows a primary is a read that can wait on it."""
    calls = _every_call_to(WHAT_THE_READ_DOOR_SAYS, LOCAL)

    assert calls, (
        f"{_THE_READ_DOOR.name} makes no call to {LOCAL}, so either the door was "
        f"rewritten or this gate is reading the wrong file. Either way nothing "
        f"below is checking anything."
    )
    naming = {call.lineno: _how_a_call_names_a_primary(call) for call in calls}
    guilty = {line: how for line, how in naming.items() if how}

    assert not guilty, (
        "the door that opens a connection for reading hands the driver a primary:\n"
        + "\n".join(
            f"    {_THE_READ_DOOR.name}:{line} passes {', '.join(how)}"
            for line, how in sorted(guilty.items())
        )
        + "\n\nA connection that knows a primary syncs on the driver's own "
        "schedule, so a read waits on a round trip at times nobody picked. "
        "Measured on the old persistum: 587ms inside one read, against under a "
        "millisecond for the same read most of the time. Open the local file by "
        "path here, and let session_open_channel hold the synced connection."
    )


def test_the_read_door_never_opens_a_synced_connection() -> None:
    """One door opens local files and one opens the channel. Not both."""
    synced = _every_call_to(WHAT_THE_READ_DOOR_SAYS, SYNCED)

    assert not synced, (
        f"{_THE_READ_DOOR.name} calls {SYNCED} at line "
        f"{', '.join(str(call.lineno) for call in synced)}, so a connection a "
        f"caller reads on is a connection that talks to the primary. That is the "
        f"shape the old persistum had, and it put half a second into random reads."
    )


def test_the_sync_door_is_the_one_that_names_a_primary() -> None:
    """The gate above means nothing if no door syncs at all.

    Without this, deleting replication outright would make the two tests above
    pass, and a package that never reaches the cloud passes every test about not
    waiting for it.
    """
    synced = _every_call_to(WHAT_THE_SYNC_DOOR_SAYS, SYNCED)

    assert synced, (
        f"{_THE_SYNC_DOOR.name} makes no call to {SYNCED}, so nothing in this "
        f"package opens a connection to a primary and the two tests above are "
        f"true of a package that cannot replicate at all"
    )
    assert any(_how_a_call_names_a_primary(call) for call in synced), (
        f"{_THE_SYNC_DOOR.name} calls {SYNCED} and names no primary, which opens "
        f"a local file through the sync door and replicates to nowhere"
    )


# WHAT A PLANTED DOOR LOOKS LIKE, one per way a primary reaches the driver. Each
# row is a reader somebody handed the primary to save an open.
WHAT_EACH_PLANTED_DOOR_PASSES: dict[str, str] = {
    "a second positional argument": "async def a(t):\n    return await turso.aio.connect(p, url)\n",
    "remote_url": "async def a(t):\n    return await turso.aio.connect(p, remote_url=url)\n",
    "auth_token": "async def a(t):\n    return await turso.aio.connect(p, auth_token=tok)\n",
}


@pytest.mark.parametrize("how", sorted(WHAT_EACH_PLANTED_DOOR_PASSES))
def test_the_gate_finds_a_planted_primary(how: str) -> None:
    """A reader handed the primary, written each way, and the gate names it."""
    found = [
        _how_a_call_names_a_primary(call)
        for call in _every_call_to(ast.parse(WHAT_EACH_PLANTED_DOOR_PASSES[how]), LOCAL)
    ]

    assert found == [[how]], (
        f"a read door passing {how} to the driver was read as {found}, so the "
        f"gate would not catch it"
    )


def test_the_gate_leaves_a_path_alone() -> None:
    """One argument is a local file, and reporting that is the gate crying wolf."""
    found = [
        _how_a_call_names_a_primary(call)
        for call in _every_call_to(
            ast.parse("async def a(t):\n    return await turso.aio.connect(p)\n"), LOCAL
        )
    ]

    assert found == [[]], f"opening a local file by path alone was reported as {found}"
