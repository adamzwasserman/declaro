"""A code sample in the docs must call names that exist.

`usage.md` and `README.md` both opened by teaching you to declare a schema:

    from declaro_persistum import table, field

    @table("users")
    class User(BaseModel):
        id: UUID = field(primary=True)

`field` had never existed. `git log -S"def field("` over the whole history of
the package returned nothing — it was not lost in the de-classing, it was
documented and never written. Both names in that import line were wrong, in
different ways, in the first example anyone read. Both work now.

WHAT THIS GATE IS AND IS NOT. It does not run the samples — most are fragments
that assume a database, an event loop, or names from a neighbouring block.
It asks the one question that needs no context: is this called name defined
anywhere in the package, imported in this block, defined anywhere in this
document, or a builtin? A name that is none of those cannot work for any
reader.

PINNED AT TWO, AND THE TWO ARE DELIBERATE. The list was 41 on 2026-08-13 and
is 2 on 2026-08-14, closed by fixing the documents. What remains is one
anti-example the architecture document is right to contain. Both directions
are asserted: a NEW broken sample fails here, and so does removing one of the
two, so the list cannot rot in either direction.

THE INSTRUMENT WAS WRONG THREE TIMES BEFORE IT WAS RIGHT.

    counted called names, ignoring block-local assignment    15   (true: 6)
    treated package imports as block-local                    6   (true: 41)
    scanned each fence alone, blind to the document           41  (true: 40)

Each correction moved the number by more than the defect it was measuring,
which is the reason a count from this file is quoted with the version of the
file that produced it.

WHAT IT STILL CANNOT SEE, stated so its silence is not read as proof:
a sample that calls a real name with wrong arguments; a bash or SQL block; a
fence that only a reader copying it alone would find broken; and any document
carrying the historical-record marker, which is exempt by design.
"""

from __future__ import annotations

import ast
import builtins
import importlib
import pathlib
import re

import pytest

pytestmark = pytest.mark.precommit

ROOT = pathlib.Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"

# docs/standards/ is excluded on the evidence in the files themselves: three of
# its six documents carry "**Scope**: react2htmx ...", and the other three use
# react2htmx names (CaptureResult, extract_dom, extract_relevant_tsx). They are
# a different project's documents filed here, not claims about this API, and
# counting their 27 absent names would have made this gate report a number that
# is four times the real one.

# Names the docs mention ON PURPOSE that the package does not have. The list
# went 41 -> 2 on 2026-08-14 by fixing the documents, not by widening this set.
#
# What is left is one anti-example. `declaro_persistum_architecture.md` shows a
# FORBIDDEN shape — a function that reaches for a global connection and infers
# its dialect — to say do not write this. Those two names are absent BECAUSE
# the document is right, so they belong here permanently and the gate must not
# be read as reporting debt when it prints them.
DELIBERATELY_ABSENT = {
    "get_global_connection",
    "infer_dialect",
}


def _package_names() -> set[str]:
    """Everything importable from anywhere in the package."""
    import declaro_persistum as pkg

    names = set(dir(pkg))
    src = pathlib.Path(pkg.__file__).parent
    for p in src.rglob("*.py"):
        rel = p.relative_to(src.parent).with_suffix("")
        mod = ".".join(rel.parts).removesuffix(".__init__")
        try:
            names |= set(dir(importlib.import_module(mod)))
        except Exception:
            continue
    return names


def _defined_names(source: str) -> set[str]:
    """What a block defines, so a later block in the same document may use it.

    A document is read top to bottom, and a helper defined in one fence and
    called in the next is a normal way to write one. Without this the FastAPI
    example in usage.md reports `tenant_db` as absent from the package, which
    is not what the gate is for.

    WHAT THIS COSTS. A reader who copies ONE fence out of the middle gets a
    NameError the gate no longer sees. That defect is real and this test does
    not claim to catch it; it catches names the PACKAGE lacks, and a name
    defined in the same document is not one of those.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return set()
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef)
    }


def _absent_names(source: str, known: set[str]) -> set[str]:
    """Names this block claims the package has, and it does not.

    TWO KINDS OF NAME, AND THE FIRST VERSION OF THIS CONFLATED THEM.

    `from declaro_persistum import field` IS a claim about this API, so the
    imported name is checked. `from datetime import datetime` is not, so it is
    excluded along with everything the block defines itself.

    The first version treated every import as block-local. That made the
    primary case invisible: a sample that imports a nonexistent name from the
    package and calls it reported nothing, which is exactly the shape of the
    `field` example. It caught `field` only from some other block that used it
    without importing it, so the number it produced was an artefact of where
    the docs happen to break their code fences.
    """
    tree = ast.parse(source)
    local: set[str] = set()
    claimed: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            from_package = (node.module or "").startswith("declaro_persistum")
            for a in node.names:
                (claimed if from_package else local).add(a.asname or a.name)
        elif isinstance(node, ast.Import):
            local |= {(a.asname or a.name).split(".")[0] for a in node.names}
        elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            local.add(node.name)
        elif isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
            local.add(node.id)
        elif isinstance(node, ast.arg):
            local.add(node.arg)

    called = {
        n for node in ast.walk(tree)
        if isinstance(node, ast.Call) and (n := getattr(node.func, "id", None))
    }
    return (claimed | (called - local - set(dir(builtins)))) - known


# A document that says in its own first lines that it records a past state is
# not making a claim about the current API, and editing its samples to name
# today's functions would destroy the record it exists to keep. The marker has
# to be explicit and near the top: a date alone is not enough, because a dated
# guide is still a guide.
HISTORICAL = "**Status**: historical record"
HISTORICAL_ALT = "**Status**: Implemented, then superseded"


def _is_historical(text: str) -> bool:
    head = text[:1500]
    return HISTORICAL in head or HISTORICAL_ALT in head


def _documents() -> list[pathlib.Path]:
    docs = [
        p
        for p in sorted(DOCS.rglob("*.md"))
        if "standards" not in p.parts and not _is_historical(p.read_text())
    ]
    return [*docs, ROOT / "README.md"]


def test_the_gate_can_see_the_samples_at_all() -> None:
    """A gate that parses nothing passes forever."""
    blocks = sum(
        len(re.findall(r"```python\n(.*?)```", p.read_text(), re.S))
        for p in _documents() if p.exists()
    )
    assert blocks > 60, f"found only {blocks} python blocks across the docs"


def test_no_new_sample_calls_a_name_the_package_lacks() -> None:
    known = _package_names()

    absent: set[str] = set()
    for path in _documents():
        if not path.exists():
            continue
        blocks = re.findall(r"```python\n(.*?)```", path.read_text(), re.S)
        # Names the document defines for itself, gathered before checking, so
        # the order the fences happen to fall in changes nothing.
        in_document = known | {n for b in blocks for n in _defined_names(b)}
        for block in blocks:
            try:
                absent |= _absent_names(block, in_document)
            except SyntaxError:
                continue  # a deliberate fragment, not a claim about the API

    appeared = absent - DELIBERATELY_ABSENT
    assert not appeared, (
        f"{sorted(appeared)} are called by a documented example and exist "
        f"nowhere in the package, so the sample cannot work for any reader"
    )

    fixed = DELIBERATELY_ABSENT - absent
    assert not fixed, (
        f"{sorted(fixed)} now resolve, or the samples stopped calling them. "
        f"Remove them from KNOWN_ABSENT so the list keeps meaning what it says"
    )


def _callables() -> dict[str, object]:
    """Every public function the package exposes, by name."""
    import declaro_persistum as pkg

    found: dict[str, object] = {}
    root = pathlib.Path(pkg.__file__).parent
    for path in root.rglob("*.py"):
        rel = path.relative_to(root.parent).with_suffix("")
        module = ".".join(rel.parts).removesuffix(".__init__")
        try:
            loaded = importlib.import_module(module)
        except Exception:
            continue
        for name in dir(loaded):
            obj = getattr(loaded, name)
            if callable(obj) and not isinstance(obj, type):
                found.setdefault(name, obj)
    return found


def _documented_calls():
    """Every call in a documented sample, paired with the real function.

    A NAME THE DOCUMENT DEFINES IS THE DOCUMENT'S, NOT THE PACKAGE'S.
    `crew-recipe.md` sketches its own `drainer(room, path, stop)`, which is a
    recipe rather than a call to `crew.drainer`. Resolving by name alone
    reported it as a call with three missing arguments.
    """
    import ast

    known = _callables()
    for path in _documents():
        if not path.exists():
            continue
        blocks = re.findall(r"```python\n(.*?)```", path.read_text(), re.S)
        local = {n for b in blocks for n in _defined_names(b)}
        for block in blocks:
            try:
                tree = ast.parse(block)
            except SyntaxError:
                continue
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                name = getattr(node.func, "id", None)
                if not name or name in local:
                    continue
                fn = known.get(name)
                if fn is not None:
                    yield path, name, node, fn


def test_no_sample_calls_a_function_the_wrong_way() -> None:
    """A name that exists is not the same as a call that works.

    The name gate above proves `open_turso` exists. It cannot see
    `open_turso(path)` missing the required `shutdown`, or a keyword the
    signature does not have. This one reads the signature.

    It found three stale `diff(...)` calls the moment it was written, left
    behind when `dialect` became required.
    """
    import ast
    import inspect

    wrong: list[str] = []
    for path, name, node, fn in _documented_calls():
        try:
            params = inspect.signature(fn).parameters
        except (ValueError, TypeError):
            continue
        if any(p.kind == p.VAR_KEYWORD for p in params.values()):
            continue
        given = {k.arg for k in node.keywords if k.arg}
        unknown = given - set(params)
        required = {
            n
            for n, p in params.items()
            if p.default is p.empty
            and p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
        }
        missing = required - given - set(list(params)[: len(node.args)])
        if unknown or missing:
            wrong.append(
                f"{path.name}: {name}() unknown={sorted(unknown)} "
                f"missing={sorted(missing)}"
            )
    assert not wrong, "documented calls that cannot run:\n  " + "\n  ".join(
        sorted(set(wrong))
    )


def test_no_sample_passes_a_value_outside_its_vocabulary() -> None:
    """The last thing the two gates above cannot see: a WRONG VALUE.

    `shutdown="wait_forever"` passes both of them — the name exists, the
    keyword exists — and raises nothing until it runs, because it is a
    `Literal` and Python does not enforce those. This checks documented
    literal arguments against the Literal types that accept them.

    Verified by injection before being trusted: putting
    `shutdown="wait_forever"` into README.md is caught, with the two valid
    values named.
    """
    import ast
    import typing

    def vocabulary(fn, param):
        try:
            hint = typing.get_type_hints(fn).get(param)
        except Exception:
            return None
        if typing.get_origin(hint) is typing.Literal:
            return set(typing.get_args(hint))
        return None

    wrong: list[str] = []
    for path, name, node, fn in _documented_calls():
        for keyword in node.keywords:
            if not keyword.arg or not isinstance(keyword.value, ast.Constant):
                continue
            allowed = vocabulary(fn, keyword.arg)
            if allowed and keyword.value.value not in allowed:
                wrong.append(
                    f"{path.name}: {name}({keyword.arg}="
                    f"{keyword.value.value!r}) is not one of {sorted(allowed)}"
                )
    assert not wrong, "documented values outside their vocabulary:\n  " + "\n  ".join(
        sorted(set(wrong))
    )
