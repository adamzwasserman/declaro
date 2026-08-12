"""Every holder method the driver calls must exist on the holder.

THE BUG THIS EXISTS FOR, caught by hand on 2026-08-12 and not by the suite.

Renaming `_TursoConnectionHolder.sync` to `replicate` left one caller behind:

    await self._loop.run_in_executor(self._executor, self._holder.sync)

`self._holder.sync` is an attribute LOOKUP, evaluated only when that line
runs. It is on the executor path, which no test exercises, so 1405 tests
passed with a guaranteed AttributeError sitting in the file.

This is the same shape as the `PoolClosedError` NameError from the module
split: a name referenced but never defined, invisible because the branch
holding it is untested. `raise SomeError(...)` and `obj.some_method` both fail
only when reached. A test that waits to reach them will not find them.

So this asserts the wiring directly, without executing the branch: every
attribute the driver names on `self._holder` must exist on the holder class.
It is deliberately independent of coverage — adding a test for the executor
path would fix today's instance, and this fixes the class.

TERMINOLOGY, since it is what caused the rename: in this package "sync" means
SYNCHRONOUS and nothing else. Bringing a replica and the cloud primary into
conformity is REPLICATION. pyturso's own API spells it `conn.sync()`, which
cannot be renamed here and is the one place the old sense survives.
"""

from __future__ import annotations

import ast
import inspect
import pathlib

import pytest

from declaro_persistum.turso_driver import (
    TursoAsyncConnection,
    _TursoConnectionHolder,
)

DRIVER = pathlib.Path(inspect.getfile(TursoAsyncConnection))


def _attributes_named_on_the_holder() -> set[str]:
    """Every `X._holder.<name>` and `self._holder.<name>` in the driver."""
    tree = ast.parse(DRIVER.read_text())
    found: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Attribute):
            continue
        base = node.value
        if isinstance(base, ast.Attribute) and base.attr == "_holder":
            found.add(node.attr)
    return found


def test_the_driver_names_at_least_one_holder_attribute():
    """Guards the guard: an empty set would make the real test vacuous."""
    assert _attributes_named_on_the_holder(), (
        "found no `self._holder.<name>` references — the AST walk is wrong, "
        "and the assertion below would pass no matter what the driver does"
    )


def _holder_attributes() -> set[str]:
    """Class attributes AND instance attributes assigned in the holder.

    `hasattr(cls, ...)` alone is not enough: `conn` is set as `self.conn` in
    `__init__` and never exists on the class, so a class-only check reports a
    perfectly valid reference as missing.
    """
    names = {n for n in dir(_TursoConnectionHolder) if not n.startswith("__")}
    src = inspect.getsource(_TursoConnectionHolder)
    for node in ast.walk(ast.parse(src)):
        if isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Store):
            if isinstance(node.value, ast.Name) and node.value.id == "self":
                names.add(node.attr)
    return names


@pytest.mark.parametrize("name", sorted(_attributes_named_on_the_holder()))
def test_the_holder_has_every_attribute_the_driver_calls(name):
    """One case per attribute, so a failure names the missing one."""
    assert name in _holder_attributes(), (
        f"turso_driver calls `self._holder.{name}`, which does not exist on "
        f"_TursoConnectionHolder. It fails only when that line runs, so an "
        f"untested branch hides it — which is exactly how the sync/replicate "
        f"rename shipped a dangling `self._holder.sync`."
    )


def test_the_replication_method_is_not_called_sync():
    """The terminology rule, asserted where it can regress.

    `sync` is reserved for synchronous. A holder that grows a `sync` method
    again is either reintroducing the old vocabulary or shadowing pyturso's.
    """
    assert hasattr(_TursoConnectionHolder, "replicate"), (
        "the holder lost `replicate`; replication is what this method does"
    )
    assert not hasattr(_TursoConnectionHolder, "sync"), (
        "the holder has a `sync` method again. In this package sync means "
        "SYNCHRONOUS; bringing a replica and the primary into conformity is "
        "REPLICATION. pyturso spells its own call `conn.sync()` and that one "
        "stays, but ours does not."
    )
