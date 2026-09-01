"""The engine probe reads the driver's module path, and nothing else.

`_conn_module` was `getattr(conn, "_declaro_engine", type(conn).__module__)`,
and its docstring said the attribute is "set on driver-owned async wrappers
like TursoAsyncConnection". Nothing set it.

    _declaro_engine in the package, outside the probe itself:  0
    _declaro_engine in any installed driver:                   0
    _declaro_engine in docs or README:                         0

It was added in 4b4098c for `LibSQLAsyncConnection` and `TursoAsyncConnection`.
7a5c9ac deleted every class in the package and libsql was eradicated, so both
classes that set it are gone. The first half of that `getattr` has been
unreachable since, while the docstring went on promising it.

THE COST OF LEAVING IT is not the wasted lookup. It is that the package
advertised an extension point it does not have: an integrator reading that
docstring would put `_declaro_engine` on their own wrapper, watch it work,
and have no way to know they were relying on a line that was about to be
deleted as dead. A promise nothing keeps is worse than no promise.

So the attribute is not consulted, and a connection this package cannot place
by its module path is refused by name rather than quietly taking its word.
"""

from __future__ import annotations

import ast
import pathlib
from collections.abc import Callable
from typing import cast

import pytest

from declaro_persistum.query.executor import detect_engine

pytestmark = pytest.mark.precommit

SRC = pathlib.Path(__file__).resolve().parents[2] / "src"


class _ClaimsToBeTurso:
    """A connection carrying the attribute the probe used to believe.

    NOT A MOCK OF A DRIVER. Nothing here stands in for a query result. It is
    an object with one attribute, which is the entire input under test.
    """

    _declaro_engine = "turso"


def test_the_attribute_is_not_consulted() -> None:
    """Carrying `_declaro_engine` does not make a connection a Turso one."""
    with pytest.raises(ValueError) as refused:
        detect_engine(_ClaimsToBeTurso())

    assert "_ClaimsToBeTurso" in str(refused.value) or "test_the_engine_probe" in str(
        refused.value
    ), f"the refusal should name what it could not place, and says: {refused.value}"


def test_a_real_driver_is_still_placed_by_its_module() -> None:
    """The path that does work still works. Without this, the test above
    passes just as well against a probe that refuses everything."""
    import aiosqlite

    conn = aiosqlite.Connection(lambda: None, iter_chunk_size=1)

    assert detect_engine(conn) == "sqlite"


def test_nothing_in_the_package_reads_or_sets_the_attribute() -> None:
    """The gate that keeps the removal true.

    IT READS THE CODE, NOT THE PROSE. A first version grepped for the name and
    failed on the docstring that explains why the name is gone, which would
    have made the rule "you may delete this, but never say you did". So it
    parses: an attribute access, an assignment, or the name handed to
    getattr/hasattr/setattr as a string. A docstring mentioning it is a
    string constant that no such node points at.

    Asserts a NUMBER of files first: if the scan itself breaks, zero offenders
    is indistinguishable from a clean tree.
    """
    scanned = list(SRC.rglob("*.py"))
    assert len(scanned) > 50, f"the scan found only {len(scanned)} files and is broken"

    found = _offenders_in(scanned, SRC)

    assert found == [], (
        f"_declaro_engine is back in the package as code at {found}. The probe "
        f"no longer reads it, so setting it does nothing and the two will drift."
    )


def _touches_the_attribute(node: ast.AST) -> bool:
    """Whether this node reads, writes or names `_declaro_engine` as code."""
    return _AS_CODE.get(type(node), _not_this_node)(node)


def _attribute_named(node: ast.AST) -> bool:
    return cast("ast.Attribute", node).attr == NAME


def _annotated_assignment(node: ast.AST) -> bool:
    target = cast("ast.AnnAssign", node).target
    return isinstance(target, ast.Name) and target.id == NAME


def _plain_assignment(node: ast.AST) -> bool:
    return any(
        isinstance(target, ast.Name) and target.id == NAME
        for target in cast("ast.Assign", node).targets
    )


def _handed_to_a_builtin(node: ast.AST) -> bool:
    call = cast("ast.Call", node)
    named = isinstance(call.func, ast.Name) and call.func.id in _BUILTINS
    return named and any(isinstance(arg, ast.Constant) and arg.value == NAME for arg in call.args)


def _not_this_node(_node: ast.AST) -> bool:
    """Every other kind of node, which cannot touch the attribute."""
    return False


NAME = "_declaro_engine"
_BUILTINS = frozenset({"getattr", "hasattr", "setattr", "delattr"})
_AS_CODE: dict[type, Callable[[ast.AST], bool]] = {
    ast.Attribute: _attribute_named,
    ast.AnnAssign: _annotated_assignment,
    ast.Assign: _plain_assignment,
    ast.Call: _handed_to_a_builtin,
}


def _offenders_in(paths: list[pathlib.Path], root: pathlib.Path) -> list[str]:
    """Every place these files touch the attribute as code."""
    return [
        f"{path.relative_to(root)}:{node.lineno}"
        for path in paths
        for node in ast.walk(ast.parse(path.read_text()))
        if _touches_the_attribute(node)
    ]


def test_the_scan_finds_one_when_there_is_one(tmp_path) -> None:
    """The companion. Without it, "zero offenders" and "the scan is blind"
    are the same green, which this suite has shipped before.

    Every shape the gate claims to catch is planted, and the docstring that
    must NOT be caught is planted beside them.
    """
    planted = tmp_path / "planted.py"
    planted.write_text(
        '''"""A docstring naming _declaro_engine, which is not code."""


def reads(conn):
    return conn._declaro_engine


def asks(conn):
    return getattr(conn, "_declaro_engine", None)


def checks(conn):
    return hasattr(conn, "_declaro_engine")


class Wrapper:
    _declaro_engine: str = "turso"


class Plain:
    _declaro_engine = "turso"
'''
    )

    found = _offenders_in([planted], tmp_path)

    assert len(found) == 5, (
        f"the gate found {len(found)} of the 5 planted offenders: {found}. "
        f"It is not catching every shape it claims to, so a clean run of the "
        f"test above means nothing."
    )
    assert all(line.startswith("planted.py:") for line in found), found
