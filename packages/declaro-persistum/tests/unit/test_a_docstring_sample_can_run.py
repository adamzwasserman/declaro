"""A call written in a `src/` docstring must be a call that works.

WRITTEN BEFORE THE CODE, against samples measured broken.

`test_documented_code_can_run.py` checks `docs/*.md` and `README.md`. It reads
no Python file, so every example living in a docstring was unchecked, and a
docstring is the documentation a reader is MOST likely to see: it is what the
IDE shows on hover and what `help()` prints.

MEASURED 2026-08-17, over 38 resolvable call samples in `src/`:

    cutover.py           await begin_cutover(source, target, ...)
                         missing schema_path, target_dialect
    differ/ambiguity.py  detect_ambiguities(current, target)   missing dialect
    differ/core.py       diff(current, target)                 missing dialect

A FOURTH WAS INVISIBLE TO THE FIRST VERSION OF THIS SCANNER, and it is the more
interesting one. `applier/__init__.py` opened with

    result = await apply(conn, operations, "sqlite")

which binds `"sqlite"` to `execution_order` and never supplies `dialect`. The
scanner skipped it because `apply` took `**kwargs`, and a signature with
`**kwargs` accepts any keyword, so nothing can be asserted about a call to it.
That is the cost of `**kwargs` stated precisely: it does not merely lose type
information, it removes the function from the reach of every instrument that
checks call sites. `apply` now names its parameters and the sample is checked.

I WROTE THAT SAMPLE THIS SESSION, in the docstring explaining why the dispatch
table was better than the class it replaced.

WHAT THIS CANNOT SEE, stated so its silence is not read as proof: a sample
whose arguments all bind and whose result is wrong; a sample calling a method
rather than a module-level function; and a multi-line call, since it matches
one line at a time.
"""

from __future__ import annotations

import ast
import importlib
import inspect
import pathlib
import re

import pytest

pytestmark = pytest.mark.precommit

ROOT = pathlib.Path(__file__).resolve().parents[2] / "src" / "declaro_persistum"

# `name(...)`, optionally awaited and optionally assigned.
CALL = re.compile(r"^(?:\w[\w\.]*\s*=\s*)?(?:await\s+)?(\w+)\((.*)\)\s*$")


def _callables() -> dict[str, object]:
    found: dict[str, object] = {}
    for path in ROOT.rglob("*.py"):
        module = ".".join(path.relative_to(ROOT.parent).with_suffix("").parts).removesuffix(
            ".__init__"
        )
        try:
            loaded = importlib.import_module(module)
        except Exception:
            continue
        for name in dir(loaded):
            obj = getattr(loaded, name)
            if callable(obj) and not isinstance(obj, type):
                found.setdefault(name, obj)
    return found


def _samples():
    """Every line in a `src/` docstring that calls a name the package defines."""
    known = _callables()
    for path in sorted(ROOT.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(
                node,
                ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
            ):
                continue
            doc = ast.get_docstring(node)
            if not doc:
                continue
            for raw in doc.splitlines():
                line = raw.strip().removeprefix(">>> ").removeprefix("... ")
                match = CALL.match(line)
                if not match:
                    continue
                fn = known.get(match.group(1))
                if fn is None:
                    continue
                yield path, line, match.group(1), fn


def test_the_gate_can_see_the_samples_at_all() -> None:
    """A scanner that matches nothing passes forever."""
    found = list(_samples())
    assert len(found) > 25, [s[1] for s in found]


def test_no_docstring_sample_calls_a_function_the_wrong_way() -> None:
    """The three measured samples, as a rule over all of them."""
    wrong: list[str] = []
    for path, line, _name, fn in _samples():
        try:
            tree = ast.parse(line.removeprefix("await "))
        except SyntaxError:
            continue
        call = next((n for n in ast.walk(tree) if isinstance(n, ast.Call)), None)
        if call is None:
            continue
        try:
            params = inspect.signature(fn).parameters
        except (ValueError, TypeError):
            continue
        if any(p.kind == p.VAR_KEYWORD for p in params.values()):
            continue
        given = {k.arg for k in call.keywords if k.arg}
        unknown = given - set(params)
        required = {
            n
            for n, p in params.items()
            if p.default is p.empty and p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
        }
        missing = required - given - set(list(params)[: len(call.args)])
        if unknown or missing:
            wrong.append(
                f"{path.relative_to(ROOT.parent)}: {line}\n"
                f"      missing={sorted(missing)} unknown={sorted(unknown)}"
            )

    assert not wrong, (
        "docstring samples that raise TypeError if a reader runs them:\n  "
        + "\n  ".join(sorted(set(wrong)))
    )


def test_the_package_apply_can_be_checked_at_all() -> None:
    """`**kwargs` removes a function from every call-site instrument.

    This is not a style preference. The scanner above must skip any signature
    with `**kwargs`, because such a signature accepts every keyword and so
    proves nothing about any call. `apply` is the package's main entry point,
    so hiding it was the single most expensive place to spend a `**kwargs`.
    """
    from declaro_persistum.applier import apply

    params = inspect.signature(apply).parameters
    variadic = [n for n, p in params.items() if p.kind == p.VAR_KEYWORD]

    assert not variadic, (
        f"apply takes {variadic}, so no gate can check any call to it, and the "
        f"sample in its own module docstring was wrong for a full session "
        f"without anything noticing"
    )
    assert "dry_run" in params, sorted(params)


def test_running_a_migration_is_asked_for_rather_than_defaulted() -> None:
    """`dry_run=False` decided the destructive direction by omission.

    Every production caller omitted it, so none of them recorded a choice, and
    the one direction that writes DDL to a database was the one you got by
    forgetting. Rule 14: the caller says which they want.
    """
    from declaro_persistum.applier import apply

    dry_run = inspect.signature(apply).parameters["dry_run"]

    assert dry_run.default is dry_run.empty, (
        f"dry_run defaults to {dry_run.default!r}, so a caller who omits it "
        f"executes DDL and cannot be distinguished from one who chose to"
    )
