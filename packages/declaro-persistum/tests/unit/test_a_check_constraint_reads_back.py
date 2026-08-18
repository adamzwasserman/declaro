"""Reading a CHECK back off a database, which no inspector could do.

A CHECK is created and enforced and neither inspector reported it, so the
differ could not compare one. Comparing it anyway is what produced a migration
loop earlier today: the declared value against `None`, forever, with
`alter_column` rebuilding the table each deploy to apply a constraint that was
already there.

THE COMPARISON STAYS OFF UNTIL BOTH ENGINES CAN READ ONE BACK. That is the
lesson from shipping the half-wired version: a property compared on one side
only is worse than a property ignored.

THE TWO ENGINES SPELL IT DIFFERENTLY, measured:

    SQLite      CREATE TABLE a (id TEXT, n INTEGER CHECK (n > 0))
    PostgreSQL  pg_get_constraintdef -> CHECK ((n > 0))

so both are normalised to the expression alone before anything compares them,
the same way a declared type is spelled per engine before `same_type` asks.

THE FORMS SQLITE ACTUALLY PRODUCES, taken from a real database rather than
imagined, because a parser tested against invented input tests the invention:

    n INTEGER CHECK (n > 0)                  a space before the paren
    n INTEGER CHECK(n > 0)                   none
    s TEXT CHECK (s IN ('x', 'y'))           nested parens, string literals
    id TEXT, n INTEGER, CHECK (n > 0)        TABLE-level, belongs to no column
    n INTEGER DEFAULT 0 CHECK (n >= 0) NOT NULL   in the middle of the column
"""

from __future__ import annotations

import sqlite3

import pytest

pytestmark = pytest.mark.precommit


def _ddl(statement: str) -> str:
    """The CREATE TABLE text SQLite stores, not the one I typed."""
    conn = sqlite3.connect(":memory:")
    conn.execute(statement)
    return conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table'"
    ).fetchone()[0]


@pytest.mark.parametrize(
    "statement,column,expected",
    [
        ("CREATE TABLE t (id TEXT, n INTEGER CHECK (n > 0))", "n", "n > 0"),
        ("CREATE TABLE t (id TEXT, n INTEGER CHECK(n > 0))", "n", "n > 0"),
        (
            "CREATE TABLE t (id TEXT, s TEXT CHECK (s IN ('x', 'y')))",
            "s",
            "s IN ('x', 'y')",
        ),
        (
            "CREATE TABLE t (id TEXT, n INTEGER DEFAULT 0 CHECK (n >= 0) NOT NULL)",
            "n",
            "n >= 0",
        ),
    ],
    ids=["spaced", "tight", "nested-parens", "mid-column"],
)
def test_a_column_check_is_read_off_the_ddl(statement, column, expected) -> None:
    from declaro_persistum.inspector.shared import checks_from_ddl

    assert checks_from_ddl(_ddl(statement)).get(column) == expected


def test_two_columns_each_keep_their_own_check() -> None:
    from declaro_persistum.inspector.shared import checks_from_ddl

    ddl = _ddl(
        "CREATE TABLE t (id TEXT, n INTEGER CHECK(n > 0), m INTEGER CHECK (m < 100))"
    )

    assert checks_from_ddl(ddl) == {"n": "n > 0", "m": "m < 100"}


def test_a_table_level_check_belongs_to_no_column() -> None:
    """`CHECK (n > 0)` written as its own clause constrains the ROW.

    Attributing it to `n` would make the differ propose removing a column
    constraint that was never declared on the column.
    """
    from declaro_persistum.inspector.shared import checks_from_ddl

    ddl = _ddl("CREATE TABLE t (id TEXT, n INTEGER, CHECK (n > 0))")

    assert checks_from_ddl(ddl) == {}


def test_a_table_with_no_checks_reads_back_empty() -> None:
    """The floor. Returning something for every column would pass above."""
    from declaro_persistum.inspector.shared import checks_from_ddl

    assert checks_from_ddl(_ddl("CREATE TABLE t (id TEXT, n INTEGER)")) == {}


def test_a_column_whose_name_contains_check_is_not_mistaken_for_one() -> None:
    """`checksum` starts with the word and declares no constraint."""
    from declaro_persistum.inspector.shared import checks_from_ddl

    ddl = _ddl("CREATE TABLE t (id TEXT, checksum TEXT, checked INTEGER)")

    assert checks_from_ddl(ddl) == {}


def test_a_default_string_containing_the_word_is_not_a_check() -> None:
    """A literal is not syntax, and a parser that scans text must know it."""
    from declaro_persistum.inspector.shared import checks_from_ddl

    ddl = _ddl("CREATE TABLE t (id TEXT, note TEXT DEFAULT 'CHECK (x > 1)')")

    assert checks_from_ddl(ddl) == {}


# ---------------------------------------------------------------------------
# Normalisation, because the two engines answer differently
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("n > 0", "n > 0"),
        ("(n > 0)", "n > 0"),
        ("CHECK ((n > 0))", "n > 0"),
        ("CHECK (n > 0)", "n > 0"),
        ("  n  >  0  ", "n > 0"),
        ("((n > 0))", "n > 0"),
    ],
    ids=lambda v: str(v)[:18],
)
def test_a_check_normalises_to_its_expression(raw, expected) -> None:
    """SQLite gives the expression, PostgreSQL wraps it twice and names it."""
    from declaro_persistum.inspector.shared import normalize_check

    assert normalize_check(raw) == expected


@pytest.mark.parametrize(
    "expression",
    [
        "(a OR b) AND c",
        "(a OR b) AND (c OR d)",
        "(a) + (b)",
    ],
    ids=["trailing-word", "both-ends-parenthesised", "two-groups"],
)
def test_normalisation_does_not_unwrap_a_meaningful_paren(expression) -> None:
    """Stripping by position turns `(a OR b) AND (c OR d)` into
    `a OR b) AND (c OR d` and changes what it means.

    FOUND BY A MUTATION CONTROL. My first input here was `(a OR b) AND c`,
    which does not END in a parenthesis, so the guard was never reached and
    deleting it left every test passing. The middle case is the one that
    starts and ends with a paren while its outermost pair closes early, and it
    is the only shape the guard exists for.
    """
    from declaro_persistum.inspector.shared import normalize_check

    assert normalize_check(expression) == expression
