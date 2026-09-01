"""Both parameter converters rewrote text that was not a placeholder.

MEASURED 2026-08-20. `_convert_to_asyncpg` and `_convert_to_turso` turn the
`:name` placeholders a query builder emits into the form each driver takes.
Neither knew where a placeholder could legally appear, so both rewrote colons
that belonged to something else:

    :id and :id_2      pg  SELECT ... id = $1 AND id_2 = $1_2
    'time :name here'  pg  s = 'time $1 here'     tu  s = 'time ? here'
    n::text, :text     pg  SELECT n:$1            tu  SELECT n:?

The first is the worst kind, because it depends on dictionary order. Declaring
`{"id": 1, "id_2": 2}` corrupts the SQL and declaring `{"id_2": 2, "id": 1}`
does not, so the same query works or fails according to the order its
parameters happened to be built in.

THE SECOND IS THE ONE THAT LOSES QUIETLY. A colon inside a string literal is
data. Rewriting `'time :name here'` changes the value the query compares
against, so the statement still parses, still runs, and returns the wrong
rows. Nothing raises. On Turso it also appends a second value to the bind
list, so the list no longer matches the placeholders and the driver binds
everything after it one position out.

THE TWO CONVERTERS DISAGREED, WHICH IS THE STANDING SHAPE HERE. Turso already
scanned with a regex that respected word boundaries, so it got the first case
right; asyncpg used `str.replace` under a comment claiming it handled word
boundaries. They now share one scanner, so a rule cannot be learned by one and
not the other.
"""

import os
import re
import tempfile

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from declaro_persistum.query.executor import (
    _convert_to_asyncpg,
    _convert_to_turso,
)
from tests.conftest import probe_url, refuse_without_postgresql


@pytest.mark.parametrize(
    "params",
    [{"id": 1, "id_2": 2}, {"id_2": 2, "id": 1}],
    ids=["shorter name declared first", "longer name declared first"],
)
def test_a_name_that_is_a_prefix_of_another_is_not_eaten(params: dict) -> None:
    """`:id` must not match the start of `:id_2`, in either declared order.

    Both orders are run because the defect was invisible in one of them. A
    single-order test would have passed against the broken code half the time,
    depending on which way the dict was written that day.
    """
    sql = "SELECT * FROM t WHERE id = :id AND id_2 = :id_2"

    converted, values = _convert_to_asyncpg(sql, params)

    assert "$1_2" not in converted
    assert converted.count("$") == 2
    assert sorted(values) == [1, 2]


@pytest.mark.parametrize(
    "convert", [_convert_to_asyncpg, _convert_to_turso], ids=["asyncpg", "turso"]
)
def test_a_colon_inside_a_string_literal_is_data(convert) -> None:
    """The literal is compared against, so rewriting it changes the answer.

    This is asserted on both converters rather than on the one that was worse,
    because the rule belongs to SQL and not to a driver.
    """
    sql = "SELECT * FROM t WHERE s = 'time :name here' AND n = :name"

    converted, values = convert(sql, {"name": "v"})

    assert "'time :name here'" in converted
    assert list(values) == ["v"]


@pytest.mark.parametrize(
    "convert", [_convert_to_asyncpg, _convert_to_turso], ids=["asyncpg", "turso"]
)
def test_a_cast_is_not_a_placeholder(convert) -> None:
    """`n::text` is a cast, and a parameter happening to be called `text`
    must not turn it into `n:$1`, which no engine parses."""
    sql = "SELECT n::text FROM t WHERE s = :text"

    converted, values = convert(sql, {"text": "a"})

    assert "n::text" in converted
    assert list(values) == ["a"]


@pytest.mark.parametrize(
    "convert", [_convert_to_asyncpg, _convert_to_turso], ids=["asyncpg", "turso"]
)
def test_a_quoted_identifier_holding_a_colon_is_a_name(convert) -> None:
    """`"a:b"` is one column called `a:b`, not a column and a placeholder."""
    sql = 'SELECT "a:b" FROM t WHERE n = :n'

    converted, values = convert(sql, {"n": 1})

    assert '"a:b"' in converted
    assert list(values) == [1]


def test_a_name_used_twice_binds_twice_for_turso_and_once_for_asyncpg() -> None:
    """The one place the two converters must legitimately differ.

    `$1` can be written twice and refer to one value; `?` cannot, so each
    occurrence needs its own entry. A shared scanner has to keep this
    difference rather than flatten it, and asserting both here is what stops
    the fix from making one of them wrong.
    """
    sql = "SELECT * FROM t WHERE a = :x OR b = :x"

    pg_sql, pg_values = _convert_to_asyncpg(sql, {"x": 7})
    tu_sql, tu_values = _convert_to_turso(sql, {"x": 7})

    assert pg_sql == "SELECT * FROM t WHERE a = $1 OR b = $1"
    assert list(pg_values) == [7]
    assert tu_sql == "SELECT * FROM t WHERE a = ? OR b = ?"
    assert list(tu_values) == [7, 7]


@pytest.mark.parametrize(
    "convert", [_convert_to_asyncpg, _convert_to_turso], ids=["asyncpg", "turso"]
)
def test_an_unknown_name_is_left_alone(convert) -> None:
    """A `:name` with no value is not this function's error to raise.

    Turso already left one in place and asyncpg already ignored one, so the
    behavior predates the fix. It is written down because a scanner that
    replaced it would turn a missing parameter into a silently wrong query
    rather than the bind error the driver raises.
    """
    sql = "SELECT * FROM t WHERE a = :known AND b = :unknown"

    converted, values = convert(sql, {"known": 1})

    assert ":unknown" in converted
    assert list(values) == [1]


# ---------------------------------------------------------------- properties


_NAME = st.from_regex(r"\A[a-z][a-z0-9_]{0,6}\Z")
_LITERAL = st.text(alphabet=" :abc_", min_size=0, max_size=8)


@st.composite
def _statements(draw):
    """A statement built from parts, so what it should contain is known.

    Generating SQL text and then asserting about it would only test the
    generator. This builds from a list of names and literals and keeps the
    list, so the expected placeholder count and the expected literals are
    facts rather than a second parse.
    """
    names = draw(st.lists(_NAME, min_size=0, max_size=4, unique=True))
    literals = draw(st.lists(_LITERAL, min_size=0, max_size=3))
    parts = [f"a = :{name}" for name in names]
    parts += [f"s = '{literal}'" for literal in literals]
    parts += ["n::text = 'x'"]
    draw(st.randoms()).shuffle(parts)
    return "SELECT * FROM t WHERE " + " AND ".join(parts), names, literals


@given(_statements())
@settings(max_examples=200, deadline=None)
def test_every_literal_survives_both_converters(case) -> None:
    """A quoted literal comes out byte for byte, whatever it holds.

    This is the property the string-literal defect broke, stated so that a
    colon in a literal is only one of the shapes it has to hold for.
    """
    sql, names, literals = case
    params = dict.fromkeys(names, 1)

    for convert in (_convert_to_asyncpg, _convert_to_turso):
        converted, _ = convert(sql, params)
        for literal in literals:
            assert f"'{literal}'" in converted, (
                f"{convert.__name__} rewrote the literal {literal!r}: {converted}"
            )
        assert "n::text" in converted, f"{convert.__name__} broke a cast: {converted}"


@given(_statements())
@settings(max_examples=200, deadline=None)
def test_turso_binds_one_value_for_each_question_mark(case) -> None:
    """The count has to match, or every later bind is one position out.

    A `?` carries no number, so a bind list longer or shorter than the marks
    is not an error the driver can recover from; it silently shifts the rest.
    """
    sql, names, _ = case
    params = dict.fromkeys(names, 1)

    converted, values = _convert_to_turso(sql, params)

    assert converted.count("?") == len(values)


@given(_statements())
@settings(max_examples=200, deadline=None)
def test_asyncpg_numbers_every_placeholder_it_emits(case) -> None:
    """`$N` must never exceed the number of values passed."""
    sql, names, _ = case
    params = dict.fromkeys(names, 1)

    converted, values = _convert_to_asyncpg(sql, params)

    emitted = {int(n) for n in re.findall(r"\$(\d+)", converted)}
    assert not emitted or max(emitted) <= len(values)
    assert emitted == set(range(1, len(values) + 1)) or not values


# -------------------------------------------------- against the real drivers


_STATEMENT = "SELECT id FROM b WHERE id = :id AND tag = :id_2 AND note = 'has :id inside'"
_PARAMS = {"id": 1, "id_2": "x"}


@pytest.mark.postgresql
async def test_asyncpg_accepts_and_answers_the_converted_statement() -> None:
    """The converter's output has to be SQL asyncpg parses, not merely SQL.

    Every assertion above this line reads the converted string. This one hands
    it to the driver, which is the only thing that can say the string is
    valid. Before the fix the driver was handed `$1_2` and raised, and it was
    handed a literal whose contents had been rewritten and did not.
    """
    import asyncpg

    url = os.environ.get(
        "TEST_POSTGRESQL_URL",
        probe_url(),
    )
    try:
        conn = await asyncpg.connect(url)
    except (OSError, asyncpg.PostgresError) as exc:
        refuse_without_postgresql(url, exc)

    try:
        await conn.execute("DROP TABLE IF EXISTS b CASCADE")
        await conn.execute("CREATE TABLE b (id INTEGER, tag TEXT, note TEXT)")
        await conn.execute("INSERT INTO b VALUES (1,'x','has :id inside'),(2,'y','other')")

        sql, values = _convert_to_asyncpg(_STATEMENT, _PARAMS)
        rows = await conn.fetch(sql, *values)

        assert [dict(row) for row in rows] == [{"id": 1}]
    finally:
        await conn.execute("DROP TABLE IF EXISTS b CASCADE")
        await conn.close()


@pytest.mark.turso
async def test_turso_accepts_and_answers_the_converted_statement() -> None:
    """The same, for the driver whose bind list has to match its marks."""
    import turso.aio

    conn = await turso.aio.connect(os.path.join(tempfile.mkdtemp(), "b.db"))
    try:
        await conn.execute("CREATE TABLE b (id INTEGER, tag TEXT, note TEXT)")
        await conn.execute("INSERT INTO b VALUES (1,'x','has :id inside')")
        await conn.execute("INSERT INTO b VALUES (2,'y','other')")

        sql, values = _convert_to_turso(_STATEMENT, _PARAMS)
        cursor = await conn.execute(sql, values)

        assert await cursor.fetchall() == [(1,)]
    finally:
        await conn.close()
