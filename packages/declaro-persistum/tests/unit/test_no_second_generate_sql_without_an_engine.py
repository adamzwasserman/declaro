"""There is one `generate_sql` and it takes an engine.

MEASURED 2026-08-19. There were two:

    applier/__init__.py:79   generate_sql(operations, execution_order, engine)
    applier/shared.py:499    generate_sql(operations, execution_order)

The second was called by nothing and reported dark by coverage. It renders
through `shared.generate_operation_sql`, which is the SQLite and Turso
renderer, so a caller who imported it and passed PostgreSQL operations would
get SQLite SQL and no error. `_alter_column_sql` differs entirely between the
two: PostgreSQL emits `ALTER TABLE ... ALTER COLUMN`, the shared one emits a
reconstruction comment.

WHY A COPY IS THE THING TO GUARD AGAINST HERE. The surviving function's own
docstring records what happened last time:

    ONE LOOP, NOT THREE. This was a member of each applier module, and only
    one of the three modules had it ... `declaro generate` calls it, so the
    command worked on PostgreSQL and raised AttributeError on the two
    backends a developer is likeliest to be running.

It was moved up and given a dispatch table precisely so the copies could not
drift. Leaving one behind in `shared.py` re-creates the condition, silently
this time: the old failure raised AttributeError, and this one would emit the
wrong engine's SQL and exit 0.

This is a gate, not a test of behavior. It reads the package rather than
asserting on any output, so it fails the day a second one appears.
"""

from __future__ import annotations

import inspect
import pkgutil
from importlib import import_module

import pytest

import declaro_persistum.applier as applier_package

pytestmark = [pytest.mark.precommit]


def _every_generate_sql() -> dict[str, object]:
    """Every callable named `generate_sql` anywhere in the applier package."""
    found: dict[str, object] = {}
    modules = [applier_package]
    for info in pkgutil.iter_modules(applier_package.__path__):
        modules.append(import_module(f"{applier_package.__name__}.{info.name}"))

    for module in modules:
        function = getattr(module, "generate_sql", None)
        if function is None or not callable(function):
            continue
        # An import of the package-level one is the same object, not a copy.
        if any(function is other for other in found.values()):
            continue
        found[f"{module.__name__}.{function.__name__}"] = function
    return found


def test_there_is_exactly_one() -> None:
    found = _every_generate_sql()

    assert len(found) == 1, (
        f"{len(found)} distinct generate_sql functions: {sorted(found)}. "
        f"The loop was moved to the package level and given a dispatch table "
        f"so the copies could not drift."
    )


def test_it_requires_a_engine() -> None:
    """A renderer that does not ask which engine renders for the wrong one."""
    for name, function in _every_generate_sql().items():
        parameters = inspect.signature(function).parameters

        assert "engine" in parameters, f"{name}{inspect.signature(function)}"


def test_the_engine_has_no_default() -> None:
    """Defaulting it would choose an engine for a caller who did not.

    That is the same trade `apply(dry_run=...)` used to make, where the
    direction that writes DDL to a live database was the one you got by
    forgetting there was a choice.
    """
    for name, function in _every_generate_sql().items():
        engine = inspect.signature(function).parameters["engine"]

        assert engine.default is inspect.Parameter.empty, (
            f"{name} defaults engine to {engine.default!r}"
        )
