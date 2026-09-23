"""Only a function the declaration hands an `Engine` may name one or write SQL.

THE FAILURE THIS EXISTS TO STOP. Everything before the last step is engine-free:
a model declares what exists, a grammar declares what you want, and neither
carries a word belonging to PostgreSQL, SQLite or Turso. That property is what
lets one declaration serve three engines and four grammars. It is also invisible
by reading any single file. A `SELECT` pasted together inside a grammar builder
looks like ordinary code there, and the first thing to notice is an engine
refusing a statement nobody can find the source of.

THE DECLARATION IS THE PERMISSION, AND NOTHING HERE IS HAND-WRITTEN. A function
that takes an `engine: Engine` has been handed the decision on purpose, and so
has an outward function handed a `Database`, which carries the engine the open
decided. Every other function has neither, so naming an engine inside one is a
decision made somewhere the map says it is not yet made.
`docs/design/persistum.hd` is read for that list, which means adding a function
to the exempt set is done by changing the declaration, in the open, rather than
by adding a name to a list in a test.

WHY THIS IS A GATE AND NOT A SCENARIO. It was written as a scenario in
`compile_statement.feature` on 2026-09-21 and could not be satisfied: no value
that function returns says anything about the other thirty modules. A claim
about every module is answered by reading every module, which is what this does.

THE READING HAPPENS IN A FIXTURE, which is the edge. Every decision below it is
a function of the text it was handed.
"""

from __future__ import annotations

import ast
import pathlib
import re

import pytest

pytestmark = pytest.mark.precommit

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_SRC = _ROOT / "src" / "declaro_persistum"
_DECLARATION = _ROOT / "docs" / "design" / "persistum.hd"

# `types.py` holds the vocabularies, so it names the three engines as members of
# a type. Naming them is the whole of what it is for.
THE_VOCABULARY = "types.py"

# SQL AS THIS PACKAGE EMITS IT, which is upper case. Matched case-sensitively and
# on word boundaries, because `select` and `where` are two grammars' own words: a
# caller writes `people.select("id")`, and a refusal that quotes it back is not a
# statement. An earlier version of this gate upper-cased everything first and
# reported both grammar builders, which was the gate misreading rather than the
# code misbehaving.
SQL_WORDS = ("SELECT", "INSERT INTO", "UPDATE", "DELETE FROM", "CREATE TABLE", "ON CONFLICT")
_SQL = re.compile("|".join(rf"\b{re.escape(word)}\b" for word in SQL_WORDS))

ENGINE_NAMES = frozenset({"postgresql", "sqlite", "turso"})

# `fn name : (…, engine: Engine …` — the signature is what grants the permission.
_TAKES_AN_ENGINE = re.compile(r"\bfn\s+([a-z_]+)\s*:\s*\([^)]*\bengine\s*:\s*Engine\b")

# A CARD HANDED A `Database` HAS BEEN HANDED THE ENGINE, and this gate could not
# see it until 2026-09-22, when three boundaries landed in one afternoon and all
# three were reported: `query_run_statement`, `schema_inspect` and the writer
# that opens a drainer's connection. A `Database` carries `engine: Engine`,
# decided once at open, which is the declaration's own sentence about it, so a
# function handed one is downstream of the decision rather than making it early.
#
# ONLY AN OUTWARD CARD, so the widening stays where the drivers are. `fn
# replicates` and `fn progress_line` are also handed a database and are pure, and
# a pure function that pasted a SELECT is still the failure this gate exists for.
_REACHES_A_DRIVER = re.compile(
    r"\b(?:boundary_out|reader)\s+fn\s+([a-z_]+)\s*:\s*\([^)]*\bdb\s*:\s*Database\b"
)


@pytest.fixture(scope="module")
def package() -> tuple[dict[pathlib.Path, str], frozenset[str]]:
    """Every module as path to text, and the module names the declaration exempts."""
    modules = {p: p.read_text(encoding="utf-8") for p in sorted(_SRC.rglob("*.py"))}
    declaration = _DECLARATION.read_text(encoding="utf-8")
    declared = _TAKES_AN_ENGINE.findall(declaration) + _REACHES_A_DRIVER.findall(declaration)
    # `engine_of` is the function that works the engine out, so it returns one
    # rather than taking one. Its own name is the permission.
    permitted = frozenset(f"{name}.py" for name in [*declared, "engine_of"])
    return modules, permitted


def _strings_in(text: str) -> list[str]:
    """Every string literal, docstrings left out.

    A docstring explains the module to a reader. A string literal in the code is
    something the module emits, and that is what this gate asks about.
    """
    tree = ast.parse(text)
    docstrings = {
        node.body[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef)
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
        and isinstance(node.body[0].value.value, str)
    }
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node not in docstrings
    ]


def test_no_module_the_declaration_kept_engine_free_builds_a_statement(package):
    """A grammar builder that pasted a SELECT would pass its own feature."""
    modules, permitted = package
    building = {
        path.name: sorted({found for value in _strings_in(text) for found in _SQL.findall(value)})
        for path, text in modules.items()
        if path.name not in permitted
    }
    building = {name: words for name, words in building.items() if words}

    assert not building, (
        "these modules carry SQL in a string literal, and the declaration hands "
        "none of them an engine:\n"
        + "\n".join(f"    {name}  {words}" for name, words in sorted(building.items()))
        + "\n\nEverything before the last step is engine-free. Move the building "
        "into a function the declaration gives an `engine: Engine`, where the "
        "engine is already known."
    )


def test_no_module_the_declaration_kept_engine_free_names_an_engine(package):
    """Naming an engine outside those functions is a decision made too early."""
    modules, permitted = package
    naming = {
        path.name: sorted(
            {name for value in _strings_in(text) if (name := value.lower()) in ENGINE_NAMES}
        )
        for path, text in modules.items()
        if path.name not in permitted and path.name != THE_VOCABULARY
    }
    naming = {name: engines for name, engines in naming.items() if engines}

    assert not naming, (
        "these modules name an engine in a string literal:\n"
        + "\n".join(f"    {name}  {engines}" for name, engines in sorted(naming.items()))
        + "\n\nOnly a function the declaration hands an `engine: Engine` knows "
        "which engine it is talking to. Give it the engine in the declaration, "
        "or take the decision out of this module."
    )


def test_this_gate_can_actually_see_the_package(package):
    """A discovery bug would make both tests above pass on an empty set.

    That is the vacuous-pass shape this package has shipped before, so the gate
    asserts its own reach with numbers rather than with truthiness checks.
    """
    modules, permitted = package

    assert len(modules) >= 15, (
        f"only {len(modules)} modules found under {_SRC}; the two gates above "
        "would pass by examining almost nothing"
    )
    assert len(permitted) >= 6, (
        f"the declaration yielded only {sorted(permitted)}; if the signature "
        "spelling changed, every module looks exempt from nothing and the gates "
        "would report the renderer itself"
    )
    assert "compile_statement.py" in permitted, (
        "the renderer is not in the permitted set, so the declaration no longer "
        "hands it an engine or this gate no longer reads the declaration"
    )

    # THE WORD LIST IS PROVED AGAINST WHICHEVER PERMITTED MODULE IS BUILT.
    # Changed 2026-09-23: this read `compile_statement.py` by name and the
    # renderer was deleted that morning, so the gate's own proof raised
    # FileNotFoundError instead of saying the renderer was unbuilt. The point is
    # that `SQL_WORDS` still matches how statements are spelled, and any module
    # the declaration hands an engine can show that.
    written = [
        name
        for name in sorted(permitted)
        if (_SRC / name).exists()
        and _SQL.search(" ".join(_strings_in((_SRC / name).read_text(encoding="utf-8"))))
    ]
    assert written, (
        f"not one of the {len(permitted)} modules the declaration hands an "
        f"engine carries any of {list(SQL_WORDS)}. Either the word list no "
        f"longer matches how statements are spelled and both gates above are "
        f"blind, or every one of them is unbuilt, in which case there is nothing "
        f"here to guard and this gate should say so rather than pass."
    )
