"""`declaro migrate-remote` computes a migration from a local replica.

If that replica is not fully caught up, the migration is computed against a
partial view of the cloud schema: tables the primary has and the copy has not
yet fetched look absent, and the plan proposes creating them.

MEASURED 2026-08-17, two defects in four lines:

    try:
        await conn.sync()
    except Exception:
        await conn.pull()

`ConnectionSync.sync` does not exist on pyturso. `hasattr(ConnectionSync,
"sync")` is False, so `await conn.sync()` raises `AttributeError` on every
single run and the `except` does the real work. An AttributeError is being
used as feature detection, and `AttributeError` is one of the types that mean
WE called something wrongly.

And `pull()` is called ONCE. The package already knows better:
`turso_database._pull_until_level` exists precisely because pyturso's `pull()`
fetches one batch and returns whether there may be more. Its own docstring
records a replica that showed nothing after connect, gained the missing table
on pull #1, and reported empty on pull #2. One call was never enough, and this
command makes exactly one.

So the command asks a question the driver cannot answer, catches the resulting
bug in our own code, then does half the work it needed.
"""

from __future__ import annotations

import inspect

import pytest

pytestmark = pytest.mark.precommit

turso_sync = pytest.importorskip("turso.aio.sync", reason="pyturso is not installed")


def test_the_driver_has_no_sync_method() -> None:
    """The fact the old fallback depended on, asserted rather than assumed.

    If pyturso ever grows `sync`, the command should call it deliberately
    rather than discovering it by not raising.
    """
    from turso.lib_sync import ConnectionSync

    assert not hasattr(ConnectionSync, "sync"), (
        "pyturso now has `sync`. `cmd_migrate_remote` pulls instead; decide "
        "which it should call rather than letting an exception choose."
    )


def test_the_driver_does_have_pull() -> None:
    from turso.lib_sync import ConnectionSync

    assert hasattr(ConnectionSync, "pull")
    # Presence only. `ConnectionSync.pull` read off the CLASS is not a
    # coroutine function, though an instance's is -- measured against a live
    # replica and pinned in
    # `test_the_replication_stand_ins_match_the_real_driver.py`. Asserting
    # awaitability here reads the wrong level and fails on a correct driver.


def test_migrate_remote_does_not_call_sync() -> None:
    """Asserted on the source, because the call is inside a command body.

    The module imports cleanly whether or not this line is there, so importing
    it proves nothing. The AST is read rather than the text so a mention in the
    docstring above does not count as a call.
    """
    import ast
    import pathlib

    source = pathlib.Path("src/declaro_persistum/cli/commands.py").read_text()

    calls = [
        node.lineno
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "sync"
    ]

    assert calls == [], (
        f"`.sync()` is called at line(s) {calls}. pyturso has no such method, "
        f"so every call raises AttributeError and whatever catches it is doing "
        f"feature detection with a bug of ours."
    )


def test_migrate_remote_pulls_until_the_replica_is_level() -> None:
    """One `pull()` fetches one batch. The command must use the loop.

    `_pull_until_level` is the package's own answer to this and has been since
    2026-08-13. A second caller reaching for a bare `pull()` is the same defect
    reappearing somewhere the first fix did not reach.
    """
    import ast
    import pathlib

    source = pathlib.Path("src/declaro_persistum/cli/commands.py").read_text()
    tree = ast.parse(source)

    bare_pulls = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "pull"
    ]
    assert bare_pulls == [], (
        f"`conn.pull()` is called directly at line(s) {bare_pulls}. One pull "
        f"fetches one batch, so the replica may still be behind and the "
        f"migration is computed against a partial schema. Use "
        f"`_pull_until_level`."
    )

    assert "_pull_until_level" in source, (
        "the command does not reach for the convergence loop at all"
    )


def test_the_convergence_loop_is_importable_from_the_cli() -> None:
    """The floor: the fix above must name something that exists."""
    from declaro_persistum.turso_database import _pull_until_level

    assert inspect.iscoroutinefunction(_pull_until_level)
    assert len(inspect.signature(_pull_until_level).parameters) == 1
