"""Follow each argument of a public function to what becomes of it.

WHAT THIS IS FOR. HOW-IT-SHOULD-WORK.md states a test any function can be held
to, and until now nothing ran it. Three strings carrying structure reached the
public surface and were removed one at a time by hand: a WHERE clause given as
text, a column name with a minus stuck to the front, and `field(check=...)`,
which is still there. Three of one thing is not three mistakes, it is a rule
nobody checks, so this is the check.

IT DECIDES BY FATE, NEVER BY APPEARANCE, and that is the whole design. The
obvious approach is to recognise SQL, and it fails in both directions. Scanning
for the twenty-odd verbs finds every statement and also finds `created_at` and
`update_count`, which are columns. It misses every fragment, because `total >=
0` and `name ASC` contain no verb and parse as nothing, and the fragments are
exactly the escape hatches. So this module names no SQL verb anywhere, and
`the gate never scans for SQL verbs` reads its own source to prove it.

WHAT IT CAN SEE, AND WHAT IT CANNOT. It reads one function at a time and finds
an argument that reaches an f-string or a concatenation in that function's own
body. An argument handed to a helper that does the pasting one call down is
invisible to it. That is a real horizon and not a claim of safety: it catches
the shape all three known instances took, and a fourth could hide behind a call.

Nothing here imports the package. It reads source, so a test can point it at a
module it wrote itself and prove it fails when it should.
"""

from __future__ import annotations

import ast
import pathlib
from collections.abc import Callable
from typing import NamedTuple

# WHAT A CONNECTION IS CALLED, read off types.py rather than guessed. A
# parameter annotated with one of these is a live connection the caller opened
# and drove, which is the imperative surface stage 1 deleted.
A_CONNECTION = ("DbapiConnection", "DbapiCursor", "FetchingConnection", "ReplicaConnection")

# WHAT AN ENGINE IS CALLED. A `Database` already carries its engine, so a
# second place to name one is a second thing that can be wrong, and the two can
# disagree without anything noticing until the wrong introspector runs.
AN_ENGINE = ("Engine",)
# BY SUBSTRING, NOT BY EQUALITY. `begin_cutover(source_engine: str, ...)` had
# the offence and the loose type both, so an exact-name check missed it and the
# annotation check missed it too. It stayed hidden until the two functions
# either side of it were fixed.
AN_ENGINE_BY_NAME = ("engine", "engine", "backend")


class Offence(NamedTuple):
    """One argument whose fate is not one of the two it is allowed."""

    where: str
    function: str
    argument: str
    becomes: str

    def said(self) -> str:
        return f"{self.where}: {self.function}({self.argument}) {self.becomes}"


Asked = Callable[[ast.FunctionDef | ast.AsyncFunctionDef, str], list[Offence]]


def _public(tree: ast.AST) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    """Every function a caller outside this package could reach by name."""
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        and not node.name.startswith("_")
    ]


def _arguments(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[ast.arg]:
    """Every argument, however the caller is allowed to pass it."""
    spec = node.args
    named = spec.posonlyargs + spec.args + spec.kwonlyargs
    return named + [one for one in (spec.vararg, spec.kwarg) if one is not None]


def _spelled(annotation: ast.expr | None) -> str:
    """The annotation as it was written, or nothing when there was none."""
    return "" if annotation is None else ast.unparse(annotation)


def _takes_a_connection(node: ast.FunctionDef | ast.AsyncFunctionDef, where: str) -> list[Offence]:
    """A caller who has to open and drive a connection is writing the library's half."""
    return [
        Offence(where, node.name, arg.arg, f"is a connection, typed {_spelled(arg.annotation)}")
        for arg in _arguments(node)
        if any(named in _spelled(arg.annotation) for named in A_CONNECTION)
    ]


def _hands_one_back(node: ast.FunctionDef | ast.AsyncFunctionDef, where: str) -> list[Offence]:
    """Handing one out is the same hole facing the other way."""
    given = _spelled(node.returns)
    named = [one for one in A_CONNECTION if one in given]
    return [
        Offence(where, node.name, "its return", f"hands back a connection, typed {given}")
        for _ in named[:1]
    ]


def _takes_an_engine(node: ast.FunctionDef | ast.AsyncFunctionDef, where: str) -> list[Offence]:
    """The engine is decided once, where the database is opened."""
    return [
        Offence(where, node.name, arg.arg, "names an engine the Database already carries")
        for arg in _arguments(node)
        if any(named in arg.arg.lower() for named in AN_ENGINE_BY_NAME)
        or any(named in _spelled(arg.annotation) for named in AN_ENGINE)
    ]


def _an_f_string(spot: ast.AST) -> bool:
    return isinstance(spot, ast.JoinedStr)


def _a_concatenation(spot: ast.AST) -> bool:
    """A `+` with a string on one side. `size + 1` is arithmetic, not a statement."""
    return (
        isinstance(spot, ast.BinOp)
        and isinstance(spot.op, ast.Add)
        and any(
            isinstance(side, ast.Constant) and isinstance(side.value, str)
            for side in (spot.left, spot.right)
        )
    )


# THE TWO WAYS A STRING GETS BUILT OUT OF AN ARGUMENT, as a table rather than a
# pair of branches, so a third way is a row and not an edit. Both are one hole:
# a string whose shape the caller decided is a string that was neither looked up
# against a declaration nor bound as a parameter.
_PASTING: dict[Callable[[ast.AST], bool], str] = {
    _an_f_string: "is pasted into an f-string",
    _a_concatenation: "is pasted into a concatenation",
}


def _pasted_into_a_string(
    node: ast.FunctionDef | ast.AsyncFunctionDef, where: str
) -> list[Offence]:
    """An argument that reaches a string this function builds."""
    mine = {arg.arg for arg in _arguments(node)}
    found = [
        Offence(where, node.name, read.id, becomes)
        for builds, becomes in _PASTING.items()
        for spot in ast.walk(node)
        if builds(spot)
        for read in ast.walk(spot)
        if isinstance(read, ast.Name) and read.id in mine
    ]
    return list(dict.fromkeys(found))


# EVERY QUESTION THIS GATE ASKS OF ONE FUNCTION. A row per rule the document
# states, so a rule with no row is visible as an absence rather than assumed.
_ASKED: tuple[Asked, ...] = (
    _takes_a_connection,
    _hands_one_back,
    _takes_an_engine,
    _pasted_into_a_string,
)


def offences_in_source(source: str, where: str) -> list[Offence]:
    """Every argument in this source whose fate is not one of the two allowed."""
    tree = ast.parse(source)
    return [offence for node in _public(tree) for asks in _ASKED for offence in asks(node, where)]


def offences_in_file(path: pathlib.Path) -> list[Offence]:
    """The same question asked of a file on disk."""
    return offences_in_source(path.read_text(), str(path))
