"""A view declared as its author wrote it migrates on every run, forever.

MEASURED 2026-08-20 against PostgreSQL 17.10. The view

    CREATE VIEW "v_live" AS SELECT "id", "n" FROM "pv_users" WHERE "n" > 0

reads back from `pg_views` as

    SELECT id, n FROM pv_users WHERE (n > 0);

PostgreSQL stores no copy of the text an author wrote. It stores a parse tree
and renders it back, dropping the quoting, parenthesising the predicate and
appending a semicolon. `diff_views` compares the two bodies with `!=`, so it
emits a drop and a create every run. Applying them recreates the same view,
and the next run emits the same two.

    view declared as written              -> 2 operations
    view declared as postgres reports it  -> 0 operations

The second line is the only way out today: copy PostgreSQL's rendering back
into the schema and keep it in step with whatever that renderer does next.

WHY THIS IS PINNED RATHER THAN FIXED. The three earlier cases of this shape
were foldable, because each compared one small expression. This one is a whole
SELECT: the renderer also expands `*`, qualifies names with their schema and
reformats. A normalizer would be a guess at PostgreSQL's renderer, and would
be wrong in a way that reports a real edit as no change, which is worse than
looping. Fixing it means either asking the engine to render the declared text
too, which needs write access during a read-only diff, or stamping the
author's text beside the view, which needs a migration. Adam's decision.

THE XFAIL IS STRICT. If this starts passing, somebody changed the comparison,
and this file must be read before that change is kept.
"""

import pytest

from declaro_persistum.differ.extended import diff_views

_AS_POSTGRESQL_RENDERS_IT = "SELECT id, n FROM pv_users WHERE (n > 0);"
_AS_AN_AUTHOR_WRITES_IT = 'SELECT "id", "n" FROM "pv_users" WHERE "n" > 0'


@pytest.mark.xfail(
    strict=True,
    reason="measured 2026-08-20: a postgresql view body loops, awaiting a decision",
)
def test_a_view_declared_as_written_migrates_nothing() -> None:
    """What the package would have to do for a declared view to converge."""
    live = {"v_live": {"name": "v_live", "query": _AS_POSTGRESQL_RENDERS_IT}}
    declared = {"v_live": {"name": "v_live", "query": _AS_AN_AUTHOR_WRITES_IT}}

    assert diff_views(live, declared) == []


def test_a_view_declared_as_postgresql_renders_it_converges() -> None:
    """The only spelling that converges today, recorded so the gap is legible.

    A reader who sees the xfail above needs to know the loop is not total. It
    stops as soon as the schema holds PostgreSQL's own rendering, which is why
    the workaround is to paste that string back rather than to stop using
    views.
    """
    live = {"v_live": {"name": "v_live", "query": _AS_POSTGRESQL_RENDERS_IT}}
    declared = {"v_live": {"name": "v_live", "query": _AS_POSTGRESQL_RENDERS_IT}}

    assert diff_views(live, declared) == []


def test_an_edited_view_still_migrates() -> None:
    """The behaviour that must survive whatever decision is taken.

    Editing a view has to emit a drop and a create. A fix that quietened the
    loop by comparing less would take this with it, and a view edit that
    migrates nothing while reporting success is the defect this package
    already found once, on 2026-08-14.
    """
    live = {"v_live": {"name": "v_live", "query": _AS_POSTGRESQL_RENDERS_IT}}
    declared = {"v_live": {"name": "v_live", "query": "SELECT id FROM pv_users;"}}

    assert [op["op"] for op in diff_views(live, declared)] == [
        "drop_view",
        "create_view",
    ]
