"""No function in `src/` declares a default argument value.

WHY. A default absorbs the caller's omission. Afterwards nothing can tell a
caller who chose from a caller who forgot, and the path nobody chose is the path
nobody tested. It is catch-and-swallow pointed at the inputs, and it makes an
unexercised region of the function by construction.

`None` IS STILL A DEFAULT. `token: str | None = None` reads as declared absence
and is not: the caller who passed `None` and the caller who passed nothing are
the same caller once the body starts. Absence is declared as `NOTHING` in the
type and resolved at a boundary, where a test can reach it.

THE GATE COUNTS WHAT IT READ. A run that found no offenders and a run that read
no files report the same thing, so this one asserts a number.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

pytestmark = pytest.mark.precommit

_SRC = pathlib.Path(__file__).resolve().parents[2] / "src" / "declaro_persistum"

# WHAT A DEFAULT IS ALLOWED TO BE, AND THERE IS ONE. A nested function whose
# parameter defaults to a name from the scope around it is binding that value at
# definition time, which is the opposite of absorbing an omission: nobody calls
# it with that argument at all. `session_connect` builds its steps this way.
A_NAME_FROM_THE_SCOPE_AROUND_IT = ast.Name


def _every_module() -> dict[str, ast.Module]:
    return {
        str(path.relative_to(_SRC)): ast.parse(path.read_text(encoding="utf-8"))
        for path in sorted(_SRC.rglob("*.py"))
    }


def _the_defaults_in(tree: ast.Module) -> list[tuple[str, str, int]]:
    """Every parameter carrying a default, with the function and the line."""
    return [
        (node.name, parameter.arg, node.lineno)
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        for parameter, given in _the_pairs_in(node)
        if not isinstance(given, A_NAME_FROM_THE_SCOPE_AROUND_IT)
    ]


def _the_pairs_in(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[tuple[ast.arg, ast.expr]]:
    """Each parameter that has a default, paired with the default it has.

    Python lines the defaults up with the LAST parameters, so the two lists are
    zipped from the right. Kept-word parameters carry their own list, and a
    `None` in it means that one has no default.
    """
    takes_one = len(node.args.args) - len(node.args.defaults)
    positional = list(zip(node.args.args[takes_one:], node.args.defaults, strict=True))
    kept = [
        (parameter, given)
        for parameter, given in zip(node.args.kwonlyargs, node.args.kw_defaults, strict=True)
        if given is not None
    ]
    return [*positional, *kept]


# EVERY MODULE, PARSED ONCE, WHEN THIS FILE LOADS. Collecting this file is the
# edge, and a function that globs and parses is I/O buried where a reader of the
# tests below cannot see it.
WHAT_EACH_MODULE_DECLARES: dict[str, list[tuple[str, str, int]]] = {
    name: _the_defaults_in(tree) for name, tree in _every_module().items()
}


def test_no_function_takes_a_default() -> None:
    """A caller who left an argument out hears about it, every time."""
    taking = {name: found for name, found in WHAT_EACH_MODULE_DECLARES.items() if found}

    assert not taking, (
        "these functions take a default, so a caller who forgot an argument and "
        "a caller who chose one are the same caller once the body starts:\n"
        + "\n".join(
            f"    {name}:{line} {function}({parameter}=...)"
            for name, found in sorted(taking.items())
            for function, parameter, line in sorted(found)
        )
        + "\n\nDeclare the absence instead. Put `| NOTHING` in the type, make "
        "the caller pass it, and resolve it at a boundary where a test can "
        "reach the case."
    )


def test_the_gate_read_the_package() -> None:
    """A run that read nothing and a run that found nothing report the same."""
    read = len(WHAT_EACH_MODULE_DECLARES)

    assert read > 60, (
        f"the gate read {read} modules, and the package has more than that. The "
        f"glob is broken and the test above is passing on an empty set."
    )


def test_the_gate_finds_a_planted_default(tmp_path: pathlib.Path) -> None:
    """One planted default of every kind the feature names, and it finds them all.

    A gate reporting zero offenders and a gate finding nothing read the same
    from outside. This is the difference.
    """
    planted = tmp_path / "planted.py"
    planted.write_text(
        "def a(timeout: int = 30): pass\n"
        "def b(items: list = []): pass\n"
        "def c(token: str | None = None): pass\n"
        "def d(*, named: int = 1): pass\n"
        "def e(nothing_at_all: int): pass\n",
        encoding="utf-8",
    )
    found = {
        function for function, _parameter, _line in _the_defaults_in(ast.parse(planted.read_text()))
    }

    assert found == {"a", "b", "c", "d"}, (
        f"the gate claims to catch a number, a mutable, a None and a kept-word "
        f"default, and it found {sorted(found)}"
    )


def test_a_closure_over_an_enclosing_name_is_allowed(tmp_path: pathlib.Path) -> None:
    """Binding a value at definition time is the opposite of absorbing an omission.

    Nobody calls the inner function with that argument at all. It is how
    `session_connect` builds one step per row of its table, and reporting it
    would be the gate counting the cure.
    """
    planted = tmp_path / "closure.py"
    planted.write_text(
        "def outer(fills):\n"
        "    def inner(connecting, fills=fills): return fills\n"
        "    return inner\n",
        encoding="utf-8",
    )
    found = _the_defaults_in(ast.parse(planted.read_text()))

    assert found == [], f"a closure over an enclosing name was reported: {found}"
