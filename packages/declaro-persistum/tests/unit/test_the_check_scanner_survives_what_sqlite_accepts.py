"""Fuzz the CHECK scanner, with SQLite as the oracle.

`checks_from_ddl` scans SQL text. Every test for it so far used a form I
thought of, and twice that was not enough: a quoted column name ending at its
first space, and a comma inside `[a,b]` splitting a clause in two. Both were
found by writing down what I had NOT tested and going back, which is a method
that runs out when imagination does.

SO THE ENGINE PICKS THE INPUTS. Each case builds a CREATE TABLE, hands it to
SQLite, and reads back the text SQLite stored -- which is the only text this
scanner ever sees in production. A statement SQLite rejects is discarded
rather than asserted on, so the corpus is exactly "what this engine accepts".

TWO PROPERTIES, and the second is the one that matters:

    the declared CHECK is reported, against the column that declared it
    the schema converges -- introspect, diff against itself, no operations

The second is the failure this scanner can actually cause. A CHECK it fails to
report is compared against None on every deploy, and `alter_column` rebuilds
the table each time, which is a defect that shipped once today and was one
quoted column name away from shipping twice.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from declaro_persistum.inspector.shared import checks_from_ddl
from declaro_persistum.types import NOTHING

pytestmark = pytest.mark.precommit

# Names SQLite accepts, in each quoting form, including the shapes that broke
# the scanner: a space, a comma, and a word that starts with the keyword.
BARE = st.from_regex(r"\A[a-z][a-z0-9_]{0,7}\Z", fullmatch=True)
AWKWARD = st.sampled_from(
    ["odd name", "a,b", "check", "checksum", "select", "a b c", "x,y,z", "CHECK"]
)
QUOTING = st.sampled_from(['"{}"', "`{}`", "[{}]"])


@st.composite
def _column(draw):
    """A column name and the text that declares it."""
    name = draw(st.one_of(BARE, AWKWARD))
    needs_quoting = not name.replace("_", "").isalnum() or name.upper() in (
        "CHECK",
        "SELECT",
    )
    if needs_quoting or draw(st.booleans()):
        return name, draw(QUOTING).format(name)
    return name, name


EXPRESSIONS = st.sampled_from(
    [
        "{q} > 0",
        "{q} <> ''",
        "{q} IN ('x', 'y')",
        "length({q}) > 2",
        "substr({q}, 1, 2) <> 'xx'",
        "({q} > 0)",
        "{q} > 0 AND {q} < 100",
    ]
)


def _stored(statement: str) -> str | None:
    """The DDL SQLite keeps, or None if SQLite refuses the statement."""
    conn = sqlite3.connect(":memory:")
    try:
        conn.execute(statement)
    except sqlite3.Error:
        return None
    return conn.execute("SELECT sql FROM sqlite_master WHERE type='table'").fetchone()[0]


@settings(max_examples=250, suppress_health_check=[HealthCheck.too_slow], deadline=None)
@given(column=_column(), template=EXPRESSIONS)
def test_a_declared_check_is_reported_against_its_own_column(column, template) -> None:
    name, declaration = column
    expression = template.format(q=declaration)
    stored = _stored(f"CREATE TABLE t (id TEXT, {declaration} INTEGER CHECK ({expression}))")
    if stored is None:
        return  # SQLite refused it, so it is not an input this scanner sees

    checks = checks_from_ddl(stored)

    assert name in checks, (
        f"the CHECK on {name!r} was not reported, so the differ compares it "
        f"against None on every deploy: {stored}"
    )


@settings(max_examples=250, suppress_health_check=[HealthCheck.too_slow], deadline=None)
@given(column=_column(), template=EXPRESSIONS)
def test_no_column_gains_a_check_it_did_not_declare(column, template) -> None:
    """The other direction. Reporting one on the wrong column proposes
    removing a constraint nobody wrote."""
    name, declaration = column
    expression = template.format(q=declaration)
    stored = _stored(f"CREATE TABLE t (id TEXT, {declaration} INTEGER CHECK ({expression}))")
    if stored is None:
        return

    assert set(checks_from_ddl(stored)) <= {name}, checks_from_ddl(stored)


@settings(max_examples=100, suppress_health_check=[HealthCheck.too_slow], deadline=None)
@given(column=_column(), template=EXPRESSIONS)
@pytest.mark.asyncio
async def test_the_schema_converges_whatever_sqlite_accepted(column, template) -> None:
    """The failure this scanner can cause, asserted directly.

    Introspect a real table, diff the result against itself, and require no
    operations. A CHECK the scanner misreads makes the two sides disagree and
    the migration repeats forever.
    """
    import aiosqlite

    from declaro_persistum.differ import diff
    from declaro_persistum.inspector import introspect

    _name, declaration = column
    expression = template.format(q=declaration)
    statement = f"CREATE TABLE t (id TEXT PRIMARY KEY, {declaration} INTEGER CHECK ({expression}))"
    if _stored(statement) is None:
        return

    path = tempfile.mktemp(suffix=".db")
    conn = await aiosqlite.connect(path)
    try:
        await conn.execute(statement)
        await conn.commit()

        actual = await introspect(conn, "sqlite")
        again = diff(actual, actual, dialect="sqlite", decisions=NOTHING)

        assert again["operations"] == [], (
            f"{statement}\nproduced {[(o['op'], o['details']) for o in again['operations']]}"
        )
    finally:
        await conn.close()
        os.unlink(path)
