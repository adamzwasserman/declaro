"""No module-level mutable state, and no state retained on an exception.

The Slop Audit's L1.18 measures how much of a package's state is finitely
testable. Two sites here were "undetermined — drives a decision", which is the
band between provably bounded and provably endless: the analyzer could not
follow the value, so it could not promise the package is exhaustively testable.

    shutdown.py   _TRAPPED           a module-level dict of signal handlers
    errors.py     self.alternatives  a list retained on an exception instance

Both are the same shape as the machinery this package spent a week removing:
state living somewhere a caller cannot see, read later to make a decision.
"""

from __future__ import annotations

import ast
import inspect

import pytest

pytestmark = pytest.mark.precommit


def test_shutdown_holds_no_module_level_state():
    """The restore path must not need a global to remember what it replaced.

    `signal.getsignal()` is already the source of truth for what is installed.
    A second copy in a module dict is a cache of something the OS will answer
    directly, and it is shared by every Database in the process — so two
    databases trapping shutdown would overwrite each other's entry.
    """
    import declaro_persistum.shutdown as shutdown

    assert not _written_at_runtime(shutdown), (
        f"module-level state written at runtime in shutdown.py: "
        f"{sorted(_written_at_runtime(shutdown))}. It is shared by every "
        f"Database in the process and invisible at every call site."
    )


def _written_at_runtime(module: object) -> set[str]:
    """Module-level containers that something assigns into after import.

    IT USED TO NAME EVERY DICT, LIST AND SET, and that became wrong on
    2026-08-22. Removing the last conditional from `shutdown.py` replaced two
    `if` statements with `_TRAP`, a dispatch table of
    functions built once at import and never touched again. The old test
    failed on them, so two gates were asking for opposite things: one
    required the table, the other forbade any dict.

    The defect this was written for is not a dict. It is `_TRAPPED`, which
    accumulated signal handlers while the program ran, so two databases
    trapping shutdown overwrote each other's entry and the restore path read
    a value somebody else had put there. What distinguishes the two is
    whether anything writes to the container after import, so that is what is
    measured: a subscript assignment, a `del`, or a call to a method that
    mutates.
    """
    source = inspect.getsource(module)
    tree = ast.parse(source)
    names = {
        target.id
        for node in tree.body
        if isinstance(node, ast.Assign | ast.AnnAssign)
        for target in (
            node.targets if isinstance(node, ast.Assign) else [node.target]
        )
        if isinstance(target, ast.Name)
    }
    mutating = {"append", "extend", "insert", "add", "update", "pop",
                "popitem", "remove", "discard", "clear", "setdefault", "sort"}
    written = {
        held
        for node in ast.walk(tree)
        for held in _name_written_by(node, mutating)
    }
    return names & written


def _name_written_by(node: ast.AST, mutating: set[str]) -> list[str]:
    """The module-level name this node writes to, if it writes to one."""
    subscripts = [
        target.value.id
        for target in (
            node.targets if isinstance(node, ast.Assign) else []
        )
        if isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name)
    ]
    deleted = [
        target.value.id
        for target in (node.targets if isinstance(node, ast.Delete) else [])
        if isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name)
    ]
    called = [
        node.func.value.id
        for _ in [node]
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr in mutating
        and isinstance(node.func.value, ast.Name)
    ]
    return subscripts + deleted + called


def test_the_scan_still_catches_the_shape_it_was_written_for() -> None:
    """The floor. A scan that matches nothing passes the test above.

    The original defect, in miniature: a module-level dict that a function
    writes into while the program runs.
    """
    sample = ast.parse(
        "_TRAPPED = {}\n"
        "_TABLE = {True: len, False: id}\n"
        "def trap(sig, handler):\n"
        "    _TRAPPED[sig] = handler\n"
    )

    class _Sample:
        pass

    found = _written_names(sample)

    assert "_TRAPPED" in found, "the scan no longer sees a dict written at runtime"
    assert "_TABLE" not in found, "the scan is flagging a table nobody writes to"


def _written_names(tree: ast.Module) -> set[str]:
    """The names `_written_at_runtime` would report for an already-parsed tree."""
    names = {
        target.id
        for node in tree.body
        if isinstance(node, ast.Assign | ast.AnnAssign)
        for target in (
            node.targets if isinstance(node, ast.Assign) else [node.target]
        )
        if isinstance(target, ast.Name)
    }
    mutating = {"append", "extend", "insert", "add", "update", "pop",
                "popitem", "remove", "discard", "clear", "setdefault", "sort"}
    written = {
        held for node in ast.walk(tree) for held in _name_written_by(node, mutating)
    }
    return names & written


def test_the_unsupported_error_retains_nothing():
    """The alternatives are used to build the message and then let go.

    Nothing outside the exception ever read `.alternatives`; it existed only to
    format the text. Keeping it made an exception carry mutable state a caller
    could reach into and change after the fact.
    """
    from declaro_persistum.exceptions import NotSupportedError

    err = NotSupportedError("no stored procedures on SQLite", ["use a function"])

    assert not hasattr(err, "alternatives"), (
        "the exception still retains its alternatives list"
    )
    assert "use a function" in str(err), (
        "the alternatives were dropped instead of being folded into the message"
    )


def test_the_alternatives_argument_has_no_implicit_default():
    """Rule 14. A caller states whether there are alternatives to offer."""
    from declaro_persistum.exceptions import NotSupportedError

    params = inspect.signature(NotSupportedError.__init__).parameters
    assert params["alternatives"].default is inspect.Parameter.empty, (
        "alternatives has a default, so 'there are none' and 'I did not think "
        "about it' are indistinguishable at the call site"
    )


def test_there_is_one_exception_hierarchy_not_two():
    """Two modules declared SchemaError, ValidationError and MigrationError.

    A caller catching `exceptions.SchemaError` never caught `errors.SchemaError`,
    and nothing in the type system says so — the names match exactly.
    """
    import declaro_persistum

    package_dir = __import__("pathlib").Path(declaro_persistum.__file__).parent
    assert not (package_dir / "errors.py").exists(), (
        "errors.py is back alongside exceptions.py; three of its four classes "
        "share a name with an exceptions.py class, so a caller catching one "
        "silently misses the other"
    )
