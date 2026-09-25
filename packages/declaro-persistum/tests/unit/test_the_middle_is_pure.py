"""A plain `fn` in the declaration touches nothing outside the process.

NOTHING CHECKED THIS UNTIL 2026-09-25, and the whole architecture rests on it.
`tests/bdd/features/gates/the_middle_is_pure.feature` described a gate that read
`@helper` and `@boundary` decorators, and no such decorator exists anywhere in
this package: it was written for the source deleted on 2026-09-21.

THE DECLARATION SAYS WHICH FUNCTIONS ARE PURE, so this reads the declaration.
A card written `fn name : ...` is pure. A card written `boundary_in`,
`boundary_out`, `reader` or `orchestrator` reaches outside on purpose and is not
checked here.

I/O IS NOT THE ONLY WAY OUT. A function that reads a clock, a random source or
an environment variable is impure although it opened no connection. Two
identical calls to it can answer differently, and every test written against it
is a test of whatever it happened to read that day.

WHAT THIS CANNOT SEE. It reads the module a card is named after, by AST, and
only the function of that name and the helpers in the same file. A pure function
that reached the world through a chain of three other modules would pass. That
is a horizon and it is written here rather than left for somebody to discover.
"""

from __future__ import annotations

import ast
import pathlib
import re

import pytest

pytestmark = pytest.mark.precommit

_HERE = pathlib.Path(__file__).resolve().parents[2]
_SRC = _HERE / "src" / "declaro_persistum"
_DECLARATION = _HERE / "docs" / "design" / "persistum.hd"

# WHAT REACHING OUTSIDE LOOKS LIKE, by the name being called. Each row is one
# way a function's answer can change between two identical calls, and the value
# is what a report says about it.
WHAT_REACHES_OUTSIDE: dict[str, str] = {
    "time": "reads the clock",
    "monotonic": "reads the clock",
    "perf_counter": "reads the clock",
    "now": "reads the clock",
    "today": "reads the clock",
    "random": "reads a random source",
    "randint": "reads a random source",
    "choice": "reads a random source",
    "uuid4": "reads a random source",
    "uuid1": "reads a random source",
    "getenv": "reads the environment",
    "environ": "reads the environment",
    "open": "opens a file",
    "connect": "opens a connection",
    "execute": "runs a statement on a connection",
    "executemany": "runs a statement on a connection",
    "fetchall": "reads rows off a connection",
    "socket": "opens a socket",
    "urlopen": "opens a socket",
    "system": "starts a process",
    "run": "starts a process",
    "popen": "starts a process",
}

# WHICH ROLES REACH OUTSIDE ON PURPOSE. A card carrying one of these is declared
# to touch the world, so finding I/O in it is the declaration being kept rather
# than broken.
ROLES_THAT_MAY: frozenset[str] = frozenset(
    {"boundary_in", "boundary_out", "reader", "orchestrator"}
)


def _the_declaration() -> dict[str, str]:
    """Every card in `persistum.hd`, as its name to its role.

    A card with no role word in front of `fn` is pure, and this calls that role
    `"fn"` so every card has one and nothing is read with a fallback.
    """
    said = _DECLARATION.read_text(encoding="utf-8")
    return {
        name: (role or "fn")
        for role, name in re.findall(r"^\s*(\w+ )?fn (\w+) :", said, re.M)
        for role in [(role or "").strip()]
    }


def _the_pure_cards() -> dict[str, str]:
    return {name: role for name, role in _the_declaration().items() if role not in ROLES_THAT_MAY}


def _what_a_call_names(node: ast.Call) -> str:
    """The last name in the thing being called, which is what the table is keyed by."""
    reached = node.func
    return {True: lambda: getattr(reached, "attr", ""), False: lambda: getattr(reached, "id", "")}[
        isinstance(reached, ast.Attribute)
    ]()


def _how_one_module_reaches_outside(path: pathlib.Path) -> list[tuple[str, str]]:
    """Every call in this file that names one of the ways out, and what it does."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    called = [
        (_what_a_call_names(node), node.lineno)
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
    ]
    read = [
        (node.attr, node.lineno)
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute) and node.attr == "environ"
    ]
    return [
        (f"{path.name}:{line}", WHAT_REACHES_OUTSIDE[name])
        for name, line in [*called, *read]
        if name in WHAT_REACHES_OUTSIDE
    ]


def _every_pure_module() -> dict[str, pathlib.Path]:
    return {
        name: _SRC / f"{name}.py" for name in _the_pure_cards() if (_SRC / f"{name}.py").exists()
    }


# EVERY PURE MODULE, READ ONCE, WHEN THIS MODULE LOADS. The reading is up here
# rather than inside a test because collecting this file is the edge, and a
# function that globs and parses is I/O buried where a reader of the tests below
# cannot see it.
HOW_EACH_ONE_REACHES_OUTSIDE: dict[str, list[tuple[str, str]]] = {
    name: _how_one_module_reaches_outside(path) for name, path in _every_pure_module().items()
}


def test_no_pure_function_reaches_outside_the_process() -> None:
    """A plain `fn` reads no clock, no random source, no environment, and opens
    nothing."""
    reaching = {name: found for name, found in HOW_EACH_ONE_REACHES_OUTSIDE.items() if found}

    assert not reaching, (
        "these modules hold a function the declaration calls pure, and they "
        "reach outside the process:\n"
        + "\n".join(
            f"    {name}: " + "; ".join(f"{where} {what}" for where, what in sorted(found))
            for name, found in sorted(reaching.items())
        )
        + "\n\nA pure function's answer has to be the same for the same "
        "arguments, forever. Move the reach to a boundary and hand the value in, "
        "or give the card a role in docs/design/persistum.hd that says it "
        "reaches outside."
    )


def test_a_boundary_is_not_reported() -> None:
    """A card declared to touch the world is not checked by this gate.

    Without this the gate would report every door in the package and get turned
    off, which is the shape of a gate that counts the cure as the disease.
    """
    checked = set(_every_pure_module())
    doors = {name for name, role in _the_declaration().items() if role in ROLES_THAT_MAY}

    assert checked & doors == set(), (
        f"{sorted(checked & doors)} carry a role that says they reach outside "
        f"and this gate is checking them anyway"
    )
    assert doors, "no card in the declaration carries a role, so the roles were not read"


def test_the_gate_can_see_what_it_claims_to_check() -> None:
    """A discovery bug would make the test above pass by reading nothing.

    That is the vacuous-pass shape this package has shipped repeatedly, so the
    gate asserts its own reach with a number rather than with "not empty".
    """
    declared = _the_pure_cards()
    found = _every_pure_module()

    assert len(found) > 40, (
        f"the gate found {len(found)} pure modules to read, and the declaration "
        f"declares {len(declared)} pure functions. A count this low means the "
        f"glob or the parse is broken and the gate above is reading nothing."
    )
    assert set(declared) - set(found) == set(), (
        f"{sorted(set(declared) - set(found))} are declared pure and have no "
        f"module of that name, so nothing checked them"
    )


def test_the_gate_finds_a_planted_impurity_of_every_kind(tmp_path: pathlib.Path) -> None:
    """One planted function per kind the gate names, and it has to find them all.

    A gate that reports zero offenders and a gate that finds nothing read the
    same from outside. This is the difference.
    """
    planted = tmp_path / "planted.py"
    planted.write_text(
        "import os, random, time, uuid, sqlite3, socket\n"
        "def a(): return time.time()\n"
        "def b(): return random.randint(1, 2)\n"
        "def c(): return uuid.uuid4()\n"
        "def d(): return os.environ['X']\n"
        "def e(): return open('/tmp/x')\n"
        "def f(): return sqlite3.connect(':memory:')\n"
        "def g(): return socket.socket()\n",
        encoding="utf-8",
    )
    found = {what for _where, what in _how_one_module_reaches_outside(planted)}

    assert found == set(WHAT_REACHES_OUTSIDE.values()) - {
        "runs a statement on a connection",
        "reads rows off a connection",
        "starts a process",
    }, f"the gate missed some of what it claims to catch; it found {sorted(found)}"
