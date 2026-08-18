"""`COUNT(*) AS c` is an expression, not a column called `COUNT(*) AS c`.

MEASURED 2026-08-17, running a scalar query against a real SQLite file:

    select("COUNT(*) AS c", from_table="t")
    ->  SELECT "COUNT(*) AS c" FROM "t"
    ->  'COUNT(*) AS c'

The value that comes back is the LITERAL TEXT of the expression. SQLite treats
a double-quoted string it cannot resolve as a string literal, so the query
succeeds, returns the wrong answer, and says nothing. On PostgreSQL the same
SQL raises `column "COUNT(*) AS c" does not exist`, which is at least loud.

`_quote_column` quoted everything that was not `*` or `table.column`, so every
aggregate, every `AS` alias and every function call in a SELECT list was
broken. `count_`, `sum_`, `avg_`, `min_`, `max_` and `now_` all produce exactly
this shape, and all three shipped example apps import `count_`.

AN EXPRESSION IS PASSED THROUGH UNQUOTED, which is the only thing that can
work, and it is worth being explicit about what that means: a SELECT list is
developer-authored, like a `Literal` vocabulary, and is not a place to put
user input. Quoting cannot make it safe either -- a quoted expression is not
an expression, it is a wrong answer.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest

from declaro_persistum.query.builder import select

pytestmark = pytest.mark.precommit

EXPRESSIONS = [
    "COUNT(*) AS c",
    "COUNT(*)",
    "SUM(amount) AS total",
    "MAX(n)",
    "COUNT(DISTINCT id) AS uniques",
    "n * 2 AS doubled",
]


@pytest.mark.parametrize("expression", EXPRESSIONS)
def test_an_expression_is_not_wrapped_in_quotes(expression: str) -> None:
    sql = select(expression, from_table="t")["sql"]

    assert f'"{expression}"' not in sql, (
        f"the expression was quoted as an identifier, so the engine returns "
        f"its text instead of evaluating it: {sql}"
    )


@pytest.mark.parametrize("expression", EXPRESSIONS)
def test_an_expression_evaluates_against_a_real_engine(expression: str) -> None:
    """The floor. Not quoting it is only right if the engine then runs it.

    A string assertion cannot see the difference between valid SQL and SQL
    that parses into the wrong thing.
    """
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id TEXT, n INTEGER, amount INTEGER)")
    conn.execute("INSERT INTO t VALUES ('1', 5, 100), ('2', 7, 200)")

    value = conn.execute(select(expression, from_table="t")["sql"]).fetchone()[0]

    assert value != expression, (
        f"the engine returned the text of the expression rather than its "
        f"value: {value!r}"
    )
    assert isinstance(value, int), f"{expression} produced {value!r}"


def test_a_count_is_the_number_of_rows() -> None:
    """The specific answer, not merely a number."""
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (id TEXT)")
    conn.execute("INSERT INTO t VALUES ('1'), ('2'), ('3')")

    sql = select("COUNT(*) AS c", from_table="t")["sql"]

    assert conn.execute(sql).fetchone()[0] == 3


# ---------------------------------------------------------------------------
# What must keep being quoted
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "column", ["id", "user_id", "created_at", "select", "order", "group"]
)
def test_a_plain_column_is_still_quoted(column: str) -> None:
    """Quoting is what lets a column be named `order` or `select`."""
    sql = select(column, from_table="t")["sql"]

    assert f'"{column}"' in sql, sql


def test_a_reserved_word_column_still_works_against_an_engine() -> None:
    conn = sqlite3.connect(":memory:")
    conn.execute('CREATE TABLE t ("order" INTEGER)')
    conn.execute("INSERT INTO t VALUES (7)")

    sql = select("order", from_table="t")["sql"]

    assert conn.execute(sql).fetchone()[0] == 7


def test_a_qualified_column_is_quoted_in_both_halves() -> None:
    sql = select("users.id", from_table="users")["sql"]

    assert '"users"."id"' in sql, sql


def test_a_star_is_left_alone() -> None:
    assert '"*"' not in select("*", from_table="t")["sql"]


def test_a_column_named_like_a_function_is_still_a_column() -> None:
    """`count` with no parentheses is a column name, and must stay quoted."""
    sql = select("count", from_table="t")["sql"]

    assert '"count"' in sql, sql


# ---------------------------------------------------------------------------
# Through the executor, which is where it was found
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.sqlite
async def test_a_scalar_count_comes_back_as_a_number() -> None:
    """The measured failure, end to end."""
    import aiosqlite

    from declaro_persistum.query.executor import execute_scalar

    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        await conn.execute("CREATE TABLE t (id TEXT)")
        await conn.execute("INSERT INTO t VALUES ('1'), ('2')")
        await conn.commit()

        assert await execute_scalar(select("COUNT(*) AS c", from_table="t"), conn) == 2
    finally:
        await conn.close()
        os.unlink(path)
