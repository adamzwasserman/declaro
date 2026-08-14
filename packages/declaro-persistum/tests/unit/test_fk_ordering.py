"""Foreign-key ordering: parents before children, children before parents.

`fk_ordering.py` carried 34 branches and 0% coverage — Slop Audit L1.19
measured the package at 49.7% decision-space coverage and this module was
one of six holding the gap (declaro-xu0).

Everything here except `execute_fk_ordered` is a pure function over a
schema dict, so every test is `assert f(input) == expected` with no mock
anywhere. The one I/O function takes a database, so it gets a recording fake
that asserts on the SQL actually issued.

Ordering is deterministic by construction: `_toposort` sorts the ready
queue on every pass, so a schema with several independent tables has ONE
correct answer, not an arbitrary one. The tests rely on that.
"""

import pytest

from declaro_persistum.fk_ordering import (
    _build_fk_graph,
    _toposort,
    execute_fk_ordered,
    fk_delete_order,
    fk_insert_order,
    sort_operations,
    strip_foreign_keys,
)


def _t(**columns):
    """A table definition carrying only what FK ordering reads."""
    return {"columns": columns}


def _col(references=None, **extra):
    col = {"type": "text", **extra}
    if references is not None:
        col["references"] = references
    return col


# users <- posts <- comments, plus an unrelated island.
CHAIN = {
    "users": _t(id=_col()),
    "posts": _t(id=_col(), author=_col(references="users.id")),
    "comments": _t(id=_col(), post=_col(references="posts.id")),
    "settings": _t(id=_col()),
}















