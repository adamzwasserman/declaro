"""Every table keyed by engine names all three engines.

WHY. What differs between engines lives in a table with one row per engine, read
by subscript, so an engine nobody wrote a row for raises at the lookup. Adding an
engine means adding a row to each table, and until today nothing checked that you
did. Add a row to twenty tables, miss two, and a customer on the engine you added
gets a crash the first time they open a database.

WHICH ENGINES EXIST comes from `set engines` in `docs/design/persistum.hd`, not
from a list in here. Two lists of the same three words drift apart, and the
declaration is the one the rest of the package is written from.

WHICH TABLES THESE ARE is found by shape: a dict whose keys are plain strings and
whose keys are all engine words. A table keyed by a pair counts when the first
half of every key is an engine word. Nobody keeps a list of the tables, because a
list somebody forgets to add to is the defect this gate catches, one level up.

THE GATE COUNTS WHAT IT READ. A run that found no table and a run that found no
fault report the same thing, so this one asserts a number.
"""

from __future__ import annotations

import ast
import pathlib
import re
from collections.abc import Callable

import pytest

pytestmark = pytest.mark.precommit

_HERE = pathlib.Path(__file__).resolve().parents[2]
_SRC = _HERE / "src" / "declaro_persistum"
_DECLARATION = _HERE / "docs" / "design" / "persistum.hd"

# A TABLE WHOSE KEYS INCLUDE AN ENGINE WORD AND ARE NOT ONLY ENGINE WORDS, with
# the reason it is left alone. `_ENGINE_BY_SCHEME` is keyed by what a caller
# writes in a target, and two of those words are also engine names. Without this
# the gate would either report it forever or skip every such table in silence,
# and silence is where a table that lost an engine would hide.
KEYED_BY_SOMETHING_ELSE: dict[str, str] = {
    "_ENGINE_BY_SCHEME": (
        "keyed by the scheme in a target, and a scheme is not an engine: a bare "
        "path and the word sqlite both answer sqlite"
    ),
}


def _the_engine_words() -> frozenset[str]:
    """The three words `set engines` names, read off the declaration."""
    said = _DECLARATION.read_text(encoding="utf-8")
    line = re.search(r"^\s*set engines = \{(.*?)\}", said, re.M | re.S)
    assert line, "the declaration has no `set engines`, so nothing below knows what an engine is"
    return frozenset(re.findall(r'"(\w+)"\s*(?::|,|\})', line.group(1)))


ENGINES = _the_engine_words()


def _a_word(node: ast.expr | None) -> str | None:
    """The string a key spells, or nothing when that key is not a plain string."""
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _how_a_table_is_keyed(table: ast.Dict) -> dict[str, list[str | None]]:
    """The words this table is keyed by, whole and by the first half of a pair."""
    return {
        "by a word": [_a_word(key) for key in table.keys],
        "by the first of a pair": [
            _a_word(key.elts[0]) if key.elts else None
            for key in table.keys
            if isinstance(key, ast.Tuple)
        ],
    }


def _named_here(node: ast.Assign | ast.AnnAssign) -> str:
    """What this table is called, which is what a failure has to print."""
    targets = [node.target] if isinstance(node, ast.AnnAssign) else node.targets
    return next(
        (one.id for one in targets if isinstance(one, ast.Name)),
        "a table with no name of its own",
    )


# WHAT EACH READING ASKS FOR. One wants the tables whose keys are engine words
# and nothing else. The other wants the tables naming an engine word beside a
# word that is not one, which is either a different vocabulary sharing a word or
# an engine table somebody added a stray word to. Written as two functions they
# were one function differing by a `not`.
WHAT_EACH_READING_ASKS_FOR: dict[str, Callable[[set[str | None]], bool]] = {
    "keyed by engine": lambda keys: keys <= set(ENGINES),
    "naming an engine and something else": lambda keys: not keys <= set(ENGINES),
}


def _the_tables_in(where: str, tree: ast.Module, asked: str) -> dict[str, frozenset[str]]:
    """The tables in this file the reading asks for, as name and line to keys.

    A key that is not a plain string reads as a word that is not an engine, so a
    table mixing a computed key with engine words comes back from the second
    reading rather than passing the first.
    """
    wanted = WHAT_EACH_READING_ASKS_FOR[asked]
    return {
        f"{where}:{node.lineno} {_named_here(node)}": frozenset(words)
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign | ast.AnnAssign)
        if isinstance(node.value, ast.Dict) and node.value.keys
        for keyed in [_how_a_table_is_keyed(node.value)]
        for said in keyed.values()
        if said
        for words in [{one for one in said if one is not None}]
        if words & ENGINES and wanted(set(said))
    }


def _every_lookup_with_a_fallback(tree: ast.Module) -> list[tuple[int, str]]:
    """Every `.get(key, something)` in this file, with the line and the name read.

    One argument is a lookup that answers nothing and the caller has to look at
    what came back. Two arguments answer for a key the table never held.
    """
    return [
        (node.lineno, getattr(node.func.value, "id", "something with no name"))
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "get"
        if len(node.args) > 1
    ]


def _every_module() -> dict[str, ast.Module]:
    return {
        path.name: ast.parse(path.read_text(encoding="utf-8"))
        for path in sorted(_SRC.rglob("*.py"))
    }


# EVERY MODULE, READ ONCE, WHEN THIS FILE LOADS. Collecting this file is the edge.
# A function that globs and parses hides the reading where nobody looking at the
# tests below would see it.
WHAT_EACH_MODULE_SAYS: dict[str, ast.Module] = _every_module()

WHAT_EACH_TABLE_IS_KEYED_BY: dict[str, frozenset[str]] = {
    name: keys
    for where, tree in WHAT_EACH_MODULE_SAYS.items()
    for name, keys in _the_tables_in(where, tree, "keyed by engine").items()
}

WHAT_NAMES_AN_ENGINE_AND_SOMETHING_ELSE: dict[str, frozenset[str]] = {
    name: keys
    for where, tree in WHAT_EACH_MODULE_SAYS.items()
    for name, keys in _the_tables_in(where, tree, "naming an engine and something else").items()
}

NAMES_OF_THE_TABLES = frozenset(name.split(" ", 1)[1] for name in WHAT_EACH_TABLE_IS_KEYED_BY)

WHERE_A_LOOKUP_TAKES_A_FALLBACK: dict[str, list[tuple[int, str]]] = {
    where: found
    for where, tree in WHAT_EACH_MODULE_SAYS.items()
    for found in [_every_lookup_with_a_fallback(tree)]
    if found
}


def test_every_table_names_every_engine() -> None:
    """A row missed in one table crashes the first customer who reaches it."""
    short = {
        name: sorted(ENGINES - keys)
        for name, keys in WHAT_EACH_TABLE_IS_KEYED_BY.items()
        if ENGINES - keys
    }

    assert not short, (
        "these tables are keyed by engine and do not name every engine:\n"
        + "\n".join(
            f"    {name} has no row for {', '.join(missing)}"
            for name, missing in sorted(short.items())
        )
        + f"\n\nThe engines are {', '.join(sorted(ENGINES))}, from `set engines` "
        "in docs/design/persistum.hd. Every table read by engine needs a row for "
        "each one, or the lookup raises the first time somebody uses it."
    )


def test_a_table_keyed_by_something_else_is_named_with_its_reason() -> None:
    """A table sharing a word with the engines is either declared or reported.

    Skipping every such table in silence is where a table that lost an engine and
    gained a stray word would hide.
    """
    unexplained = {
        name: sorted(keys)
        for name, keys in WHAT_NAMES_AN_ENGINE_AND_SOMETHING_ELSE.items()
        if not any(said in name for said in KEYED_BY_SOMETHING_ELSE)
    }

    assert not unexplained, (
        "these tables name an engine and a word that is not an engine:\n"
        + "\n".join(
            f"    {name} is keyed by {', '.join(keys)}"
            for name, keys in sorted(unexplained.items())
        )
        + "\n\nEither it is keyed by engine and somebody added a word that is not "
        "an engine, or it is keyed by a different vocabulary and belongs in "
        "KEYED_BY_SOMETHING_ELSE in this file, with the reason."
    )


def test_no_engine_table_is_read_with_a_fallback() -> None:
    """A fallback answers for a key the table never held.

    `.get(key, something)` reopens the space the table closed: an engine nobody
    wrote a row for gets an answer instead of a refusal.
    """
    reading = {
        where: [(line, named) for line, named in found if named in NAMES_OF_THE_TABLES]
        for where, found in WHERE_A_LOOKUP_TAKES_A_FALLBACK.items()
        if [named for _line, named in found if named in NAMES_OF_THE_TABLES]
    }

    assert not reading, (
        "these tables are keyed by engine and read with a fallback:\n"
        + "\n".join(
            f"    {where}:{line} {named}.get(key, ...)"
            for where, found in sorted(reading.items())
            for line, named in found
        )
        + "\n\nRead it by subscript. An engine with no row is a KeyError naming "
        "the word, which is the refusal the table was written to give."
    )


def test_the_gate_found_the_tables() -> None:
    """A discovery bug makes every test above pass by reading nothing."""
    found = len(WHAT_EACH_TABLE_IS_KEYED_BY)

    assert found > 15, (
        f"the gate found {found} tables keyed by engine, and the package holds "
        f"more than that. The glob or the shape rule is broken, and the tests "
        f"above are agreeing with an empty set."
    )
    assert sorted(ENGINES) == ["postgresql", "sqlite", "turso"], (
        f"the declaration names {sorted(ENGINES)} as the engines, and this gate "
        f"was written when there were three. Adding one is fine; say so here."
    )


# WHAT A PLANTED TABLE MISSING TURSO LOOKS LIKE, one per way a table can be
# keyed. Written as two tests they were one test differing by a quoted string.
WHAT_EACH_PLANTED_TABLE_LOOKS_LIKE: dict[str, str] = {
    "keyed by a word": 'WHAT_EACH_ONE_DOES = {"postgresql": 1, "sqlite": 2}\n',
    "keyed by a pair": 'WHAT_EACH_ONE_DOES = {("postgresql", "x"): 1, ("sqlite", "x"): 2}\n',
}


@pytest.mark.parametrize("shape", sorted(WHAT_EACH_PLANTED_TABLE_LOOKS_LIKE))
def test_the_gate_finds_a_table_missing_an_engine(shape: str) -> None:
    """One planted table short a row, keyed each way, and the gate names it."""
    found = _the_tables_in(
        "planted.py",
        ast.parse(WHAT_EACH_PLANTED_TABLE_LOOKS_LIKE[shape]),
        "keyed by engine",
    )

    assert [sorted(ENGINES - keys) for keys in found.values()] == [["turso"]], (
        f"a table {shape}, carrying postgresql and sqlite and not turso, was read as {found}"
    )


def test_the_gate_finds_a_table_naming_an_engine_nobody_declared() -> None:
    """A word in one table and no other is a word the declaration never said."""
    found = _the_tables_in(
        "planted.py",
        ast.parse('WHAT_EACH_ONE_DOES = {"postgresql": 1, "sqlite": 2, "turso": 3, "duckdb": 4}\n'),
        "naming an engine and something else",
    )

    assert [sorted(keys) for keys in found.values()] == [
        ["duckdb", "postgresql", "sqlite", "turso"]
    ], f"a table naming an engine the declaration never declared was read as {found}"


def test_the_gate_finds_a_lookup_with_a_fallback() -> None:
    """A two-argument lookup is found and a one-argument lookup is left alone."""
    found = _every_lookup_with_a_fallback(
        ast.parse(
            "def a(engine):\n"
            "    return WHAT_EACH_ONE_DOES.get(engine, 0)\n"
            "def b(engine):\n"
            "    return WHAT_EACH_ONE_DOES.get(engine)\n"
        )
    )

    assert [named for _line, named in found] == ["WHAT_EACH_ONE_DOES"], (
        f"one lookup takes a fallback and one does not, and the gate found {found}"
    )
