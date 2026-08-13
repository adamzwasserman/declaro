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

    mutable = {
        name: value
        for name, value in vars(shutdown).items()
        if not name.startswith("__")
        and isinstance(value, (dict, list, set))
        and not inspect.ismodule(value)
    }
    assert not mutable, (
        f"module-level mutable state in shutdown.py: {sorted(mutable)}. "
        f"It is shared by every Database in the process and invisible at "
        f"every call site."
    )


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
