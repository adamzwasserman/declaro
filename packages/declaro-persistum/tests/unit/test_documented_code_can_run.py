"""A code sample in the docs must call names that exist.

`usage.md` and `README.md` both open by teaching you to declare a schema:

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

FROZEN, NOT PINNED AT ZERO. Forty names fail today, across usage.md,
README.md, hooks.md, two architecture records and two bug records and fixing them is not a docs
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

ROOT = pathlib.Path(__file__).resolve().parents[2]
DOCS = ROOT / "docs"

# docs/standards/ is excluded on the evidence in the files themselves: three of
# its six documents carry "**Scope**: react2htmx ...", and the other three use
# react2htmx names (CaptureResult, extract_dom, extract_relevant_tsx). They are
# a different project's documents filed here, not claims about this API, and
# counting their 27 absent names would have made this gate report a number that
# is four times the real one.

# Verified 2026-08-13 by running the first example: the import line itself
# raises. Each entry is a name a sample calls that the package does not define.
KNOWN_ABSENT = {
    # `field` and `@table` were here and are now implemented, so the docs'
    # opening example works. `view` is still absent: the docs show a `@view`
    # decorator for declaring a view, and nothing reads one.
    "view",
    # Deleted with the classes. usage.md, hooks.md and the architecture records
    # still import them by name.
    "ConnectionPool",
    "MirrorPool",
    "PoolClosedError",
    "PoolConnectionError",
    "PoolError",
    "PoolExhaustedError",
    "PostgreSQLApplier",
    "PostgreSQLInspector",
    "SQLiteApplier",
    "SQLiteInspector",
    "TursoApplier",
    "TursoCloudManager",
    "TursoInspector",
    # The query builder classes. __init__.py's own comment says these "went
    # with the query builder classes and return with Group A of the map".
    "DeleteQuery",
    "InsertQuery",
    "SelectQuery",
    "UpdateQuery",
    "increment",
    "table_factory",
    # Never defined; importing it raised, which is how it was found.
    "execute_reconstruction_sync",
    # Cited by dated design and bug records: 007-table-reconstruction-v1.md,
    # declaro_persistum_architecture.md, table_reconstruction.md,
    # BUGFIX-sqlite-applier-consistency.md. Whether a historical record should
    # be edited at all is a separate question from whether the API exists.
    "apply_operation_to_schema",
    "clear_decisions",
    "dataclass",
    "generator",
    "get_global_connection",
    "index",
    "infer_dialect",
    "parse_columns",
    "parse_to_column_dict",
    # Deleted generators and validators.
    "apply_rls",
    "generate_refresh_materialized_view",
    "validate_concurrent_refresh",
    "validate_references",
    # SQLAlchemy, in a migration-from section. Not a claim about this package.
    "Boolean",
    "DateTime",
    "Session",
    "String",
    "declarative_base",
    # stdlib, imported in a sibling block.
    "uuid4",
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


def _documents() -> list[pathlib.Path]:
    docs = [p for p in sorted(DOCS.rglob("*.md")) if "standards" not in p.parts]
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
        for block in re.findall(r"```python\n(.*?)```", path.read_text(), re.S):
            try:
                absent |= _absent_names(block, known)
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
