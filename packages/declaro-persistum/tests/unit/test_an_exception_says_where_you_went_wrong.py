"""Every exception's message is assembled, and none of it was tested.

`exceptions.py` audits at 99.1% silence over 111 facets, the worst ratio in
the package. I guessed the instrument was miscounting a module of plain
exception classes. It was not. The arithmetic:

    11 __init__ methods, 32 constructor parameters
    12 of them `str | None`, 8 `str`, 4 `Exception | None`, the rest sequences

Each parameter carries its canonical regions, which is roughly 96 before a
single branch is counted. The module is simply not exercised.

AND THE CONSTRUCTORS DO REAL WORK. They are not field assignments. Each one
assembles a human-readable message from whichever context it was given, with
one `if` per optional field, and `MigrationError` truncates SQL at 500
characters. That is the module's entire purpose, stated in its own docstring:
"A programmer should not have to be an expert stack tracer to figure out where
they went wrong." A wrong message is the failure it exists to prevent, and
nothing checked one.

These are pure functions over data. No database, no async, no fixture. There
was never a reason for them to be the least-tested module here beyond nobody
looking.

WHAT THIS FILE DOES NOT DO. It does not assert exact wording. Wording changes
and a test that pins it teaches people to update the test rather than read it.
It asserts that every piece of context you hand an exception appears in what
the programmer reads, which is the property that matters.
"""

from __future__ import annotations

import pytest

from declaro_persistum.exceptions import (
    AmbiguityError,
    ConnectionError as DeclaroConnectionError,
    CycleError,
    DatabaseClosedError,
    DeclaroError,
    DriftError,
    LoaderError,
    MigrationError,
    NotSupportedError,
    RollbackError,
    SchemaError,
    TransferError,
    ValidationError,
)

pytestmark = pytest.mark.precommit


# One row per constructor: the exception, the arguments, and the fragments a
# programmer must be able to find in the message. Table-driven because the
# property is identical for every one of them, and fifteen near-identical test
# functions would hide that.
CASES = [
    (SchemaError, ("bad schema",), {"file": "models.py", "line": 42},
     ["bad schema", "models.py", "42"]),
    (DeclaroConnectionError, ("unreachable",), {"dialect": "postgresql"},
     ["unreachable", "postgresql"]),
    (MigrationError, ("apply failed",),
     {"sql": "CREATE TABLE t (v INT)", "original_error": ValueError("nope")},
     ["apply failed", "CREATE TABLE t", "ValueError", "nope"]),
    (RollbackError, ("rollback failed",),
     {"sql": "DROP TABLE t", "rollback_error": RuntimeError("stuck")},
     ["rollback failed", "DROP TABLE t", "stuck"]),
    (ValidationError, ("bad column",),
     {"table": "users", "column": "email", "reference": "accounts.id"},
     ["bad column", "users", "email", "accounts.id"]),
    (LoaderError, ("cannot load",), {"path": "/models/user.py"},
     ["cannot load", "/models/user.py"]),
    (TransferError, ("transfer failed",),
     {"phase": "copy", "table": "orders", "original_error": OSError("disk")},
     ["transfer failed", "copy", "orders", "disk"]),
    (CycleError, (["a", "b", "a"], {"a", "b"}), {}, ["a", "b"]),
    (NotSupportedError, ("no arrays here", ["use a junction table"]), {},
     ["no arrays here", "use a junction table"]),
    (AmbiguityError, ([{"type": "possible_rename", "table": "users",
                        "from_column": "name", "to_column": "full_name",
                        "column": None, "confidence": 0.9,
                        "message": "rename or drop?"}],), {},
     ["rename or drop?"]),
    # The shape is {"symbol", "description"}, as cli/commands.py prints it.
    # My first draft passed {"table", "difference"} and the message came back
    # reading "~ Unknown", which is how the `.get(..., "Unknown")` fallback
    # below was found.
    (DriftError, ([{"symbol": "~", "description": "users.email added"}],),
     {"last_snapshot": "2026-08-14", "current_time": "2026-08-15"},
     ["users.email added", "2026-08-14", "2026-08-15"]),
]


@pytest.mark.parametrize(
    "kind,args,kwargs,fragments",
    CASES,
    ids=[c[0].__name__ for c in CASES],
)
def test_every_piece_of_context_reaches_the_message(
    kind: type[Exception], args: tuple, kwargs: dict, fragments: list[str]
) -> None:
    """Context you hand an exception must be readable in what it prints.

    Dropping a field silently is the failure mode: the exception still raises,
    the programmer still gets a traceback, and the one detail that would have
    told them where they went wrong is missing.
    """
    message = str(kind(*args, **kwargs))
    missing = [f for f in fragments if f not in message]
    assert not missing, f"{kind.__name__} dropped {missing} from:\n{message}"


@pytest.mark.parametrize("kind,args,kwargs,_fragments", CASES,
                         ids=[c[0].__name__ for c in CASES])
def test_every_exception_is_catchable_as_one_declaro_error(
    kind: type[Exception], args: tuple, kwargs: dict, _fragments: list[str]
) -> None:
    """The hierarchy's whole promise, in one assertion.

    The module docstring says "All exceptions inherit from DeclaroError for
    easy catching". A caller writing `except DeclaroError` gets that or gets a
    crash, and nothing checked it.
    """
    assert isinstance(kind(*args, **kwargs), DeclaroError)


@pytest.mark.parametrize("kind,args,_kwargs,_fragments", CASES,
                         ids=[c[0].__name__ for c in CASES])
def test_the_optional_context_is_optional(
    kind: type[Exception], args: tuple, _kwargs: dict, _fragments: list[str]
) -> None:
    """With nothing optional supplied, the message is still readable.

    Every `if` in these constructors guards one optional field, so the
    all-absent path is a real branch. It is also the commonest one in
    production, where an exception is raised with a message and no context.
    """
    message = str(kind(*args))
    assert message.strip(), f"{kind.__name__} with no context prints nothing"
    assert "None" not in message, (
        f"{kind.__name__} leaked a None into what the programmer reads:\n{message}"
    )


class TestTheSqlTruncation:
    """`MigrationError` truncates SQL at 500 characters. A boundary is a
    branch, and both sides of it are what "very long string" means."""

    def test_short_sql_is_kept_whole(self) -> None:
        sql = "S" * 499
        assert "..." not in str(MigrationError("boom", sql=sql))

    def test_long_sql_is_truncated_and_marked(self) -> None:
        message = str(MigrationError("boom", sql="S" * 600))
        shown = message.split("SQL: ")[1].split("\n")[0]
        assert shown.endswith("...")
        assert len(shown) == 503, "500 characters plus the ellipsis"

    def test_the_untruncated_sql_survives_on_the_exception(self) -> None:
        """Truncation is for reading. A debugger still wants the whole thing."""
        error = MigrationError("boom", sql="S" * 600)
        assert len(error.sql) == 600

    def test_exactly_500_is_truncated(self) -> None:
        """Which side the boundary falls on, stated rather than assumed."""
        assert "..." in str(MigrationError("boom", sql="S" * 500))


def test_an_exception_with_no_constructor_still_carries_its_message() -> None:
    """`DatabaseClosedError` and friends add no `__init__`. They inherit one,
    and inheriting is not the same as working."""
    assert "closed" in str(DatabaseClosedError("the database is closed"))
    assert isinstance(DatabaseClosedError("x"), DeclaroError)
