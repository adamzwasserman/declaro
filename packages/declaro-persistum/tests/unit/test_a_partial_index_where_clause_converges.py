"""A partial index proposed dropping and rebuilding itself on every run.

MEASURED 2026-08-20 against PostgreSQL 17.10. An index created as

    CREATE UNIQUE INDEX "ix_live" ON "pw_users"("email") WHERE "deleted_at" IS NULL

is introspected as `where: '(deleted_at IS NULL)'`. PostgreSQL does not store
the text an author wrote; it stores a parsed expression and renders it back
through `pg_get_expr`, which adds the outer parentheses and drops the quoting
from any identifier that does not need it. The differ compared the whole index
definition dict with `!=`, so the rendered form never equalled the declared one
and every run emitted:

    drop_index pw_users {'index': 'ix_live'}
    add_index  pw_users {'index': 'ix_live', 'definition': {... 'where': ...}}

Applying them rebuilds the identical index, so the next run emits the same two.

THIS IS THE THIRD TIME THE SAME SHAPE HAS BEEN FOUND. An engine rewrites the
SQL text an author wrote, and something compares the two spellings as strings.
Defaults got `_canonical_spacing`, CHECK expressions got `normalize_check`, and
each was written for its own case. `_canonical_spacing` cannot serve here: it
was built for `datetime ('now')` and deletes the space before an opening paren,
so `(a > 0) AND (b > 0)` becomes `(a > 0) AND(b > 0)` and folds to nothing
PostgreSQL would produce. The architectural cause is that there is no one
answer to "are these the same SQL predicate", only a normalizer per caller.

WHAT THIS FIX DOES NOT DO. Quoting is folded only where unquoting cannot change
which column is named: a quoted identifier that is already lowercase and needs
no quotes. `"Email"` and `"my col"` keep their quotes, because on PostgreSQL
the quoting is what makes them mean that column, and folding them would call
two different predicates equal. A model that writes `WHERE "Email" IS NULL`
against PostgreSQL still loops, and that is a narrower gap than the one closed.
"""

import pathlib

import pytest

from declaro_persistum import close, open_sqlite, reading, writing
from declaro_persistum.differ.core import diff
from declaro_persistum.inspector import INSPECTORS
from declaro_persistum.types import NOTHING

_TABLE = 'CREATE TABLE "pw_users" ("id" INTEGER PRIMARY KEY, "email" TEXT, "deleted_at" TEXT)'
_INDEX = 'CREATE UNIQUE INDEX "ix_live" ON "pw_users"("email") WHERE "deleted_at" IS NULL'


async def _live(tmp_path: pathlib.Path) -> dict:
    database = await open_sqlite(
        str(tmp_path / "t.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    try:
        async with writing(database) as conn:
            await conn.execute(_TABLE)
            await conn.execute(_INDEX)
            await conn.commit()
        async with reading(database) as conn:
            return await INSPECTORS["sqlite"](conn, schema_name=NOTHING, include_views=False)
    finally:
        await close(database)


def _declaring(where: str, live: dict) -> dict:
    return {
        "pw_users": {
            "columns": dict(live["pw_users"]["columns"]),
            "indexes": {"ix_live": {"columns": ["email"], "unique": True, "where": where}},
        }
    }


@pytest.mark.parametrize(
    "rendered",
    [
        "(deleted_at IS NULL)",
        "deleted_at IS NULL",
        "( deleted_at IS NULL )",
        "deleted_at   IS   NULL",
        '"deleted_at" IS NULL',
    ],
    ids=[
        "postgres renders it",
        "no parens",
        "loose outer spacing",
        "loose inner spacing",
        "as declared",
    ],
)
async def test_the_engines_rendering_is_the_declared_clause(
    tmp_path: pathlib.Path, rendered: str
) -> None:
    """Every spelling of one predicate must compare equal to the declaration.

    The introspected schema is stood in for rather than read from four
    databases, because the difference under test is the text of the clause and
    not the engine that produced it. The first case is the exact string
    PostgreSQL 17.10 returned when measured.
    """
    live = await _live(tmp_path)
    live["pw_users"]["indexes"]["ix_live"]["where"] = rendered

    result = diff(
        live, _declaring('"deleted_at" IS NULL', live), dialect="postgresql", decisions=NOTHING
    )

    assert result["operations"] == []


async def test_a_genuinely_different_clause_still_migrates(
    tmp_path: pathlib.Path,
) -> None:
    """The negative control, so the comparison is narrowed and not disabled."""
    live = await _live(tmp_path)

    result = diff(
        live, _declaring('"deleted_at" IS NOT NULL', live), dialect="postgresql", decisions=NOTHING
    )

    assert [op["op"] for op in result["operations"]] == ["drop_index", "add_index"]


async def test_a_quoted_name_that_needs_its_quotes_is_not_folded(
    tmp_path: pathlib.Path,
) -> None:
    """`"Deleted_At"` and `deleted_at` are different columns on PostgreSQL.

    An unquoted identifier is folded to lower case, so the quotes are what
    make the mixed-case one mean that column. Treating the two as one
    predicate would report a real difference as no change, which is the
    failure this whole file exists to avoid, pointing the other way.
    """
    live = await _live(tmp_path)
    live["pw_users"]["indexes"]["ix_live"]["where"] = "deleted_at IS NULL"

    result = diff(
        live, _declaring('"Deleted_At" IS NULL', live), dialect="postgresql", decisions=NOTHING
    )

    assert [op["op"] for op in result["operations"]] == ["drop_index", "add_index"]


async def test_a_mixed_case_name_is_not_the_lower_case_one(
    tmp_path: pathlib.Path,
) -> None:
    """`"Email"` and a bare `Email` name different columns on PostgreSQL.

    This is the control on the quoting rule itself, and it is the one that
    fails if quotes are stripped from every identifier rather than only from
    the ones that carry nothing. PostgreSQL keeps the quotes on a mixed-case
    name when it renders a clause, precisely because dropping them would fold
    the name to lower case and point at something else. A fold that took them
    off both sides would call two different predicates one, and report a real
    difference as no change.
    """
    live = await _live(tmp_path)
    live["pw_users"]["indexes"]["ix_live"]["where"] = '"Deleted_At" IS NULL'

    result = diff(
        live, _declaring("Deleted_At IS NULL", live), dialect="postgresql", decisions=NOTHING
    )

    assert [op["op"] for op in result["operations"]] == ["drop_index", "add_index"]
