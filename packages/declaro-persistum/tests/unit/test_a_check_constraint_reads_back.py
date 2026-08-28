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
    return conn.execute("SELECT sql FROM sqlite_master WHERE type='table'").fetchone()[0]


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

    ddl = _ddl("CREATE TABLE t (id TEXT, n INTEGER CHECK(n > 0), m INTEGER CHECK (m < 100))")

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


# ---------------------------------------------------------------------------
# The edges a hand-written scanner is most likely to get wrong
# ---------------------------------------------------------------------------
#
# `checks_from_ddl` scans SQL text; it is not a parser. I listed these as
# untested when the scanner shipped, then measured them against a real SQLite
# rather than reasoning about them. All seven already held, which is worth
# knowing precisely: the scanner is sturdier than I credited it, and now the
# claim is pinned instead of assumed.


@pytest.mark.parametrize(
    "statement,expected",
    [
        (
            "CREATE TABLE t (id TEXT, n INTEGER CONSTRAINT positive CHECK (n > 0))",
            {"n": "n > 0"},
        ),
        (
            "CREATE TABLE t (id TEXT, n INTEGER, g INTEGER GENERATED ALWAYS AS (n * 2))",
            {},
        ),
        (
            "CREATE TABLE t (id TEXT, n INTEGER CHECK (n > 0), "
            "g INTEGER GENERATED ALWAYS AS (n * 2))",
            {"n": "n > 0"},
        ),
        (
            "CREATE TABLE t (\n  id TEXT,\n  n INTEGER\n    CHECK (n > 0)\n)",
            {"n": "n > 0"},
        ),
        (
            "CREATE TABLE t (id TEXT, n INTEGER, CONSTRAINT positive CHECK (n > 0))",
            {},
        ),
        (
            "CREATE TABLE t (id TEXT, s TEXT COLLATE NOCASE CHECK (s <> ''))",
            {"s": "s <> ''"},
        ),
    ],
    ids=[
        "named-column-check",
        "generated-column",
        "generated-beside-a-check",
        "newlines",
        "named-table-check",
        "collate-before-check",
    ],
)
def test_the_scanner_holds_at_its_edges(statement, expected) -> None:
    from declaro_persistum.inspector.shared import checks_from_ddl

    assert checks_from_ddl(_ddl(statement)) == expected


def test_a_comma_inside_a_quoted_identifier_does_not_split_the_clause() -> None:
    """`"odd,name"` is one column, and splitting on its comma makes two.

    This is the case `_split_top_level` tracks quotes for, and it is easy to
    write a scanner that only tracks single ones.
    """
    from declaro_persistum.inspector.shared import checks_from_ddl

    ddl = _ddl("""CREATE TABLE t ("odd,name" TEXT CHECK ("odd,name" <> ''))""")

    assert checks_from_ddl(ddl) == {"odd,name": "\"odd,name\" <> ''"}


def test_a_type_name_ending_in_check_is_not_a_constraint() -> None:
    """SQLite accepts any type name, so `MYCHECK(10)` is a legal column type.

    FOUND BY A MUTATION CONTROL. Deleting the word-boundary guard around the
    keyword left every test passing, because my input was `checksum TEXT` and
    what follows the letters there is not `(` -- the `rest.startswith("(")`
    check rescued it, so the guard was never reached.

    `n MYCHECK(10)` is the shape that needs it: the letters are followed
    immediately by a parenthesis, so without the boundary check the scanner
    reports a constraint of `10` on a column that declares none.
    """
    from declaro_persistum.inspector.shared import checks_from_ddl

    assert checks_from_ddl(_ddl("CREATE TABLE t (id TEXT, n MYCHECK(10))")) == {}


# ---------------------------------------------------------------------------
# The identifier quoting SQLite accepts, which is more than one form
# ---------------------------------------------------------------------------
#
# MEASURED 2026-08-17, and these were wrong:
#
#     [odd name] INTEGER CHECK ([odd name] > 0)   column reported as 'odd'
#     `odd name` INTEGER CHECK (`odd name` > 0)   column reported as 'odd'
#     [a,b]      INTEGER CHECK ([a,b] > 0)        column reported as 'b'
#
# The scanner took `words[0]` off a whitespace split, so a quoted name
# containing a space ended at the space. And `_split_top_level` tracked `'` and
# `"` but not brackets, so a comma inside `[a,b]` split the clause in two.
#
# THE CONSEQUENCE IS THE LOOP AGAIN, not a wrong name in a dict. A name that
# does not match a column is dropped by the `if name in columns` guard, so the
# CHECK goes unreported, and an unreported CHECK is compared against None on
# every deploy -- which is exactly the failure that took a commit to ship and
# a commit to remove earlier today. A bracketed column name would have brought
# it back for anyone using one.
#
# SQLite prohibits a subquery in a CHECK, measured, so there is no case there.


@pytest.mark.parametrize(
    "statement,expected",
    [
        (
            "CREATE TABLE t (id TEXT, [odd name] INTEGER CHECK ([odd name] > 0))",
            {"odd name": "[odd name] > 0"},
        ),
        (
            "CREATE TABLE t (id TEXT, `odd name` INTEGER CHECK (`odd name` > 0))",
            {"odd name": "`odd name` > 0"},
        ),
        (
            'CREATE TABLE t (id TEXT, "odd name" INTEGER CHECK ("odd name" > 0))',
            {"odd name": '"odd name" > 0'},
        ),
        (
            "CREATE TABLE t (id TEXT, [a,b] INTEGER CHECK ([a,b] > 0))",
            {"a,b": "[a,b] > 0"},
        ),
        # A COMMA, NOT A SPACE, and in each quoting form. A mutation control
        # found that dropping the backtick from the splitter's quote set
        # changed nothing, because the backtick case above holds a space and
        # the splitter only splits on commas. The guard is observable only
        # when the quoted name contains one.
        (
            "CREATE TABLE t (id TEXT, `a,b` INTEGER CHECK (`a,b` > 0))",
            {"a,b": "`a,b` > 0"},
        ),
        (
            'CREATE TABLE t (id TEXT, "a,b" INTEGER CHECK ("a,b" > 0))',
            {"a,b": '"a,b" > 0'},
        ),
    ],
    ids=[
        "brackets",
        "backticks",
        "double-quotes",
        "comma-in-brackets",
        "comma-in-backticks",
        "comma-in-double-quotes",
    ],
)
def test_a_quoted_column_name_survives_the_scan(statement, expected) -> None:
    from declaro_persistum.inspector.shared import checks_from_ddl

    assert checks_from_ddl(_ddl(statement)) == expected


def test_a_comma_inside_a_check_expression_does_not_split_the_clause() -> None:
    """`substr(s, 1, 2)` has two commas and is one clause."""
    from declaro_persistum.inspector.shared import checks_from_ddl

    ddl = _ddl("CREATE TABLE t (id TEXT, s TEXT CHECK (substr(s, 1, 2) <> 'xx'))")

    assert checks_from_ddl(ddl) == {"s": "substr(s, 1, 2) <> 'xx'"}
