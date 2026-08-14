"""Every module must be reachable from a real entry point, or be declared dead.

THE FAILURE THIS EXISTS TO STOP, stated as the shape rather than the instance:

A module is "alive" if something imports it. A test imports the module it
tests. So a module that NO production path reaches still has importers, still
runs, and still passes — forever, with no signal anywhere that it is
unreachable. Coverage makes it worse rather than better: writing tests for an
uncovered module is exactly what cements an orphan, and that is not
hypothetical, it is what happened here (see the L1.19 coverage commit, which
added tests to `compat/sqlalchemy_shim.py` and `observability/`).

It has now happened four times in this package:

  declaro_persistum.functions      a second set of SQL function factories,
                                   with signatures incompatible with the live
                                   ones in query/table.py. DELETED 2026-08-12.
  query/sqlalchemy.py              a SQLAlchemy declarative_base/Session/Query
                                   emulation, 739 LOC, imported by nothing.
  compat/sqlalchemy_shim.py        a SECOND SQLAlchemy compat layer, Base and
                                   SessionLocal, imported by nothing but its
                                   own package __init__.
  observability/                   QueryObserver, Timer, fingerprint_query —
                                   duplicating instrumentation.py, which IS
                                   reachable and is what the database actually uses.

Four instances of one class of failure means the architecture is the bug, not
the modules. The missing structure is this test: nothing anywhere asserted that
a module is reachable from somewhere a user can actually get to.

REACHABILITY is computed from two roots, because a library has two:
  - `declaro_persistum/__init__.py`, for anything imported as a library
  - every `[project.scripts]` target in pyproject.toml, for the CLI

That second root matters. A narrower check reported the whole CLI — cli.main,
cli.commands, loader, validator, fk_ordering, 2287 LOC — as dead. It is not:
`declaro = declaro_persistum.cli.main:main` is a declared console script.

KNOWN_ORPHANS is a RATCHET, not an allowlist to grow. Adding a name to it is
a decision to ship code no entry point reaches; removing one is either wiring
it up or deleting it. A new orphan fails this test on the commit that creates
it, which is the only moment anyone still remembers why.
"""

from __future__ import annotations

import ast
import pathlib
import tomllib
from collections import defaultdict

PKG = "declaro_persistum"
ROOT = pathlib.Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / PKG

KNOWN_ORPHANS: dict[str, str] = {
}
"""One entry, and the aim is to get back to none.

All four original entries were DELETED rather than listed — 1450 LOC of source
plus the broken example that demonstrated one of them (2026-08-12). An entry
here is a decision to ship code no entry point reaches, which needs a reason
better than "it was already written".
"""


def _module_name(path: pathlib.Path) -> str:
    rel = path.relative_to(SRC.parent)
    return str(rel.with_suffix("")).replace("/", ".").removesuffix(".__init__")


def _all_modules() -> dict[str, pathlib.Path]:
    return {_module_name(p): p for p in SRC.rglob("*.py")}


def _absolute(module: str | None, level: int, current: str,
              mods: dict[str, pathlib.Path]) -> str:
    """Resolve a possibly-relative import to an absolute module name."""
    if not level:
        return module or ""
    parts = current.split(".")
    is_package = mods[current].name == "__init__.py"
    pkg = parts if is_package else parts[:-1]
    base = pkg[: len(pkg) - (level - 1)] if level > 1 else pkg
    return ".".join(base + ([module] if module else []))


def _import_graph(mods: dict[str, pathlib.Path]) -> dict[str, set[str]]:
    graph: dict[str, set[str]] = defaultdict(set)
    for name, path in mods.items():
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom):
                base = _absolute(node.module, node.level or 0, name, mods)
                if not base.startswith(PKG):
                    continue
                graph[name].add(base)
                # `from package import submodule` imports the submodule too,
                # and missing this form is what made an earlier version of
                # this check report live modules as dead.
                for alias in node.names:
                    graph[name].add(f"{base}.{alias.name}")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name.startswith(PKG):
                        graph[name].add(alias.name)
    return graph


def _entry_points() -> set[str]:
    """Module roots a user can reach: the package, plus every console script."""
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    roots = {PKG}
    for target in data.get("project", {}).get("scripts", {}).values():
        roots.add(target.split(":")[0])
    return roots


def _reachable(mods: dict[str, pathlib.Path], graph: dict[str, set[str]]) -> set[str]:
    seen: set[str] = set()
    stack = [r for r in _entry_points() if r in mods]
    while stack:
        current = stack.pop()
        if current in seen:
            continue
        seen.add(current)
        for target in graph[current]:
            candidates = [target]
            # importing a.b.c executes a and a.b as well
            parts = target.split(".")
            candidates += [".".join(parts[:i]) for i in range(1, len(parts))]
            stack += [c for c in candidates if c in mods and c not in seen]
    return seen


def _orphans() -> set[str]:
    mods = _all_modules()
    return set(mods) - _reachable(mods, _import_graph(mods))


def test_no_new_module_is_unreachable():
    """The ratchet. A module no entry point reaches fails on the commit that adds it."""
    new = sorted(_orphans() - set(KNOWN_ORPHANS))
    assert not new, (
        "these modules are reachable from no entry point — neither "
        f"{PKG}/__init__.py nor any [project.scripts] target — and are kept "
        "alive only by their own tests:\n  "
        + "\n  ".join(new)
        + "\n\nWire it into a real path, delete it, or add it to KNOWN_ORPHANS "
        "with the reason it ships unreachable."
    )


def test_the_orphan_list_does_not_outlive_its_entries():
    """The other direction: a name that is no longer an orphan must come off.

    Without this the list silently becomes fiction, which is the same defect
    one level up — a record that nothing forces to match reality.
    """
    stale = sorted(set(KNOWN_ORPHANS) - _orphans())
    assert not stale, (
        "these are listed as unreachable but something now reaches them; "
        "remove them from KNOWN_ORPHANS:\n  " + "\n  ".join(stale)
    )


def test_every_orphan_carries_a_reason():
    """A bare name records that someone noticed, not what they decided."""
    empty = sorted(k for k, v in KNOWN_ORPHANS.items() if len(v.strip()) < 20)
    assert not empty, f"orphans listed with no real reason: {empty}"


def test_the_cli_is_not_mistaken_for_dead_code():
    """Guards the bug this check itself had.

    A reachability test rooted only at the package __init__ reports the entire
    CLI as unreachable — 2287 LOC of live code. If someone narrows the roots
    again, this fails immediately instead of licensing a deletion.
    """
    mods = _all_modules()
    reachable = _reachable(mods, _import_graph(mods))
    for name in (f"{PKG}.cli.main", f"{PKG}.cli.commands", f"{PKG}.loader",
                 f"{PKG}.validator", f"{PKG}.fk_ordering"):
        assert name in reachable, (
            f"{name} is reported unreachable, but the `declaro` console script "
            f"reaches it. The entry-point roots are wrong, not the module."
        )
