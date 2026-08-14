"""A code sample in the docs must call names that exist.

`usage.md` opens by teaching you to declare a schema:

    from declaro_persistum import table, field

    @table("users")
    class User(BaseModel):
        id: UUID = field(primary=True)

`field` has never existed. `git log -S"def field("` over the whole history of
the package returns nothing — it was not lost in the de-classing, it was
documented and never written. `table` does exist, but it is
`table(name, schema)`, a query-side schema lookup; used as `@table("users")` it
raises TypeError for the missing argument. Both names in that import line are
wrong, in different ways, in the first example anyone reads.

`extract_field_metadata`'s own docstring cites `field()` too, so the source
repeats it.

WHAT THIS GATE IS AND IS NOT. It does not run the samples — most are fragments
that assume a database, an event loop, or names from a neighbouring block.
It asks the one question that needs no context: is this called name defined
anywhere in the package, imported in this block, defined in this block, or a
builtin? A name that is none of those cannot work for any reader.

FROZEN, NOT PINNED AT ZERO. Six names fail today and fixing them is not a docs
edit: either `field()` and `@table` get written to match the documentation, or
the documentation is rewritten around `__tablename__` and
`Field(json_schema_extra=...)`, which changes what every consumer's models file
looks like. That is a product decision. Meanwhile a NEW broken sample fails
here, and repairing one of these fails too, so the list cannot rot.

THE INSTRUMENT WAS WRONG TWICE BEFORE IT WAS RIGHT. Counting called names
without accounting for imports and assignments inside the same block reported
15; the true figure is 6. `uuid4`, `main` and `declarative_base` were mine, not
the docs'. The block-local pass below is the correction.
"""

from __future__ import annotations

import ast
import builtins
import importlib
import pathlib
import re

import pytest

pytestmark = pytest.mark.precommit

DOCS = pathlib.Path(__file__).resolve().parents[2] / "docs"
USAGE = DOCS / "usage.md"

# Verified 2026-08-13 by running the first example: the import line itself
# raises. Each entry is a name a sample calls that the package does not define.
KNOWN_ABSENT = {
    "field",                              # never implemented, 4 calls
    "view",                               # never implemented, 2 calls
    "uuid4",                              # stdlib, imported in a sibling block
    "generate_refresh_materialized_view",  # deleted with the classes
    "PostgreSQLInspector",                 # deleted with the classes
    "PostgreSQLApplier",                   # deleted with the classes
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


def _called_but_undefined(source: str, known: set[str]) -> set[str]:
    """Names this block calls that it neither imports, defines, nor can find.

    Block-local imports, assignments, parameters and defs are all excluded —
    leaving them in is what made the first count more than twice too high.
    """
    tree = ast.parse(source)
    local: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import | ast.ImportFrom):
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
    return called - local - set(dir(builtins)) - known


def test_the_gate_can_see_the_samples_at_all() -> None:
    """A gate that parses nothing passes forever."""
    blocks = re.findall(r"```python\n(.*?)```", USAGE.read_text(), re.S)
    assert len(blocks) > 40, f"found only {len(blocks)} python blocks in {USAGE}"


def test_no_new_sample_calls_a_name_the_package_lacks() -> None:
    known = _package_names()
    blocks = re.findall(r"```python\n(.*?)```", USAGE.read_text(), re.S)

    absent: set[str] = set()
    for block in blocks:
        try:
            absent |= _called_but_undefined(block, known)
        except SyntaxError:
            continue  # a deliberate fragment, not a claim about the API

    appeared = absent - KNOWN_ABSENT
    assert not appeared, (
        f"{sorted(appeared)} are called by a documented example and exist "
        f"nowhere in the package, so the sample cannot work for any reader"
    )

    fixed = KNOWN_ABSENT - absent
    assert not fixed, (
        f"{sorted(fixed)} now resolve, or the samples stopped calling them. "
        f"Remove them from KNOWN_ABSENT so the list keeps meaning what it says"
    )
