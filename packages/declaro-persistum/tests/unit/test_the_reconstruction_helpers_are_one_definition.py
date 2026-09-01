"""Two reconstruction modules must not each keep their own copy of a helper.

MEASURED 2026-08-17. `abstractions/reconstruction.py` (504 lines) and
`abstractions/table_reconstruction.py` (446 lines) both exist, both are
imported across the package, and three of their functions are near-copies:

    generate_create_table_sql   vs  _generate_create_table_sql    84%
    _get_table_columns_async    vs  _get_table_columns            98%
    _get_table_indexes_async    vs  _get_table_indexes            99%

The last two differ ONLY in the function name and one word of docstring. Their
bodies are character-for-character the same, and both are `async def`, so the
`_async` suffix distinguishes nothing.

WHY THIS IS A DEFECT AND NOT UNTIDINESS. A fix applied to one copy misses the
other, silently, and this package has already paid for that: the `Literal`
type `_map_type` crash on 2026-03-06 had to be fixed in three places for one
bug. Two copies of a PRAGMA reader is the same shape waiting.

BOTH COPIES DELEGATE TO `pragma_compat`, which is where they belong: it already
holds `pragma_index_list` and `pragma_table_info`, and these are thin readers
over exactly those. One definition there, imported by both.

THE 84% PAIR IS LEFT ALONE. `generate_create_table_sql` and its twin differ in
sixteen lines of real behavior, so merging them is a decision about which
behavior is right rather than a deduplication. Recorded below so the number
cannot drift unnoticed.
"""

from __future__ import annotations

import ast
import pathlib
from typing import Any

import pytest

pytestmark = pytest.mark.precommit

SRC = pathlib.Path(__file__).resolve().parents[2] / "src" / "declaro_persistum"
MODULES = (
    SRC / "abstractions" / "reconstruction.py",
    SRC / "abstractions" / "table_reconstruction.py",
)


def _functions(path: pathlib.Path) -> dict[str, str]:
    return {
        node.name: ast.unparse(node)
        for node in ast.parse(path.read_text()).body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    }


def _body_only(source: str) -> str:
    """The function without its `def` line or docstring.

    Two copies that differ only in name and docstring are one function wearing
    two labels, and comparing whole sources would score them as different.
    """
    tree = ast.parse(source).body[0]
    body = list(tree.body)
    if (
        body
        and isinstance(body[0], ast.Expr)
        and isinstance(body[0].value, ast.Constant)
        and isinstance(body[0].value.value, str)
    ):
        body = body[1:]
    return "\n".join(ast.unparse(stmt) for stmt in body)


def test_both_modules_still_exist() -> None:
    """The floor. If one is deleted this whole file is measuring nothing."""
    for module in MODULES:
        assert module.is_file(), module


def test_no_helper_body_is_duplicated_between_them() -> None:
    """The assertion that fails until the copies become one definition.

    Compares BODIES, so renaming one copy does not make the duplication
    disappear from this test while leaving it in the code.
    """
    first, second = (_functions(m) for m in MODULES)

    duplicates = []
    for name_a, source_a in first.items():
        for name_b, source_b in second.items():
            if _body_only(source_a) == _body_only(source_b):
                duplicates.append(f"{name_a} == {name_b}")

    assert duplicates == [], (
        f"these functions have identical bodies in both modules: "
        f"{duplicates}. A fix to one misses the other, which is how the "
        f"`_map_type` crash needed fixing in three places."
    )


def test_the_shared_readers_live_beside_the_pragmas_they_wrap() -> None:
    """One definition, in the module that already owns PRAGMA reading."""
    from declaro_persistum.abstractions import pragma_compat

    for name in ("table_column_names", "create_index_statements"):
        assert hasattr(pragma_compat, name), (
            f"pragma_compat has no {name}; the shared readers were moved "
            f"somewhere else and this test no longer says where"
        )


def test_both_modules_use_the_shared_readers() -> None:
    """Importing them is the point. A copy left behind defeats the move."""
    for module in MODULES:
        text = module.read_text()
        assert "pragma_compat import" in text or "pragma_compat" in text, module


# A column definition for each property the two generators read, plus the
# combinations that once diverged.
_SHAPES: dict[str, dict[str, Any]] = {
    "a plain column": {"c": {"type": "text"}},
    "a primary key": {"id": {"type": "integer", "primary_key": True}},
    "a composite key": {
        "a": {"type": "integer", "primary_key": True},
        "b": {"type": "integer", "primary_key": True},
    },
    "not null": {"c": {"type": "text", "nullable": False}},
    "unique": {"c": {"type": "text", "unique": True}},
    "a default": {"c": {"type": "text", "default": "'pending'"}},
    "an expression default": {"c": {"type": "text", "default": "(datetime('now'))"}},
    "a numeric default": {"c": {"type": "integer", "default": 0}},
    "no default at all": {"c": {"type": "text", "default": None}},
    "a check": {"c": {"type": "integer", "check": "c > 0"}},
    "a reference": {"c": {"type": "integer", "references": "p.id"}},
    "a reference with actions": {
        "c": {
            "type": "integer",
            "references": "p.id",
            "on_delete": "CASCADE",
            "on_update": "SET NULL",
        }
    },
    "every property at once": {
        "id": {"type": "integer", "primary_key": True},
        "c": {
            "type": "text",
            "nullable": False,
            "unique": True,
            "default": "(datetime('now'))",
            "check": "length(c) > 0",
            "references": "p.name",
            "on_delete": "CASCADE",
        },
    },
}


@pytest.mark.parametrize("shape", sorted(_SHAPES), ids=sorted(_SHAPES))
def test_the_two_generators_emit_the_same_sql(shape: str) -> None:
    """The two create-table generators must agree on what they write.

    IT USED TO COMPARE THEIR SOURCE TEXT, holding the similarity between 70
    and 95 percent on the reasoning that drift in either direction meant
    somebody had changed one and not the other. On 2026-08-22 both were
    rewritten, independently, from `if` chains into dispatch tables. They came
    out 51 percent alike while writing the same SQL, so the test failed on a
    property nobody cares about and would have passed two generators that had
    quietly diverged as long as they still read alike.

    Comparing the output is the question the text was standing in for. It also
    caught something the ratio never could: `table_reconstruction` wrote its
    DEFAULT clause raw while the other called `sqlite_default`, so an
    expression default lost a layer of parentheses on every rebuild.
    """
    from declaro_persistum.abstractions.reconstruction import (
        generate_create_table_sql,
    )
    from declaro_persistum.abstractions.table_reconstruction import (
        _generate_create_table_sql,
    )
    from declaro_persistum.types import NOTHING

    columns = _SHAPES[shape]

    assert generate_create_table_sql("t", columns, NOTHING) == _generate_create_table_sql(
        "t", columns, NOTHING
    ), (
        f"the two create-table generators disagree on {shape}; a rebuild "
        f"through one of them writes a table the other would not"
    )


# ---------------------------------------------------------------------------
# The shared readers themselves
# ---------------------------------------------------------------------------
#
# ADDED BECAUSE A CONTROL DID NOT FIRE. After the two copies became one, I broke
# the auto-index skip in the shared reader and 35 tests still passed. Both
# reconstruction modules call it, and neither exercised that branch: the one
# BDD step that checks auto-indexes reads `sqlite_master` directly instead.
#
# A helper that two modules depend on and nothing tests is worse deduplicated
# than duplicated, because now one break reaches both.


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_the_column_reader_names_every_column() -> None:
    import aiosqlite

    from declaro_persistum.abstractions.pragma_compat import table_column_names

    async with aiosqlite.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b INT)")

        assert await table_column_names(conn, "t") == ["id", "a", "b"], (
            "the reader must return the columns in declaration order, because "
            "a reconstruction copies rows positionally"
        )


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_the_index_reader_returns_explicit_indexes_only() -> None:
    """The branch the control caught nothing on.

    A UNIQUE column gives SQLite an automatic `sqlite_autoindex_*`, which
    belongs to the constraint and is recreated by the CREATE TABLE. Reissuing
    it as `CREATE INDEX sqlite_autoindex_...` is a syntax error, so a
    reconstruction that copied every index would fail outright.
    """
    import aiosqlite

    from declaro_persistum.abstractions.pragma_compat import (
        create_index_statements,
        pragma_index_list,
    )

    async with aiosqlite.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, email TEXT UNIQUE, name TEXT)")
        await conn.execute("CREATE INDEX ix_name ON t(name)")

        listed = {row[1] for row in await pragma_index_list(conn, "t")}
        assert any(n.startswith("sqlite_autoindex_") for n in listed), (
            f"no automatic index exists on a table with a UNIQUE column, so "
            f"this test is no longer exercising the skip: {listed}"
        )

        statements = await create_index_statements(conn, "t")

        assert len(statements) == 1, f"expected only the explicit index, got {statements}"
        assert "ix_name" in statements[0], statements
        assert not any("sqlite_autoindex" in s for s in statements), (
            f"an automatic index was reissued as a CREATE INDEX, which is a "
            f"syntax error when the reconstruction replays it: {statements}"
        )


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_a_table_with_no_indexes_returns_nothing() -> None:
    import aiosqlite

    from declaro_persistum.abstractions.pragma_compat import create_index_statements

    async with aiosqlite.connect(":memory:") as conn:
        await conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")

        assert await create_index_statements(conn, "t") == []
