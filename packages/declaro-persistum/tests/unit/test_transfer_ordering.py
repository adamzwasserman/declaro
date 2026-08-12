"""Table ordering for bulk transfer: parents loaded before children.

`transfer.py` sat at 13% coverage with 74 branches — part of the Slop Audit
L1.19 gap (declaro-xu0). The two graph functions are pure, so they are
covered here by assertion; the async transfer machinery around them is not.

This is a SECOND topological sort, separate from the one in
`fk_ordering.py`, and it behaves differently on the case that matters: a
cycle here is RETURNED as `circular_refs` rather than raised. Bulk transfer
wants to load what it can and report the rest; DML ordering cannot proceed
at all. The tests pin that difference so the two are not "unified" by
someone who assumes they are duplicates.

This graph also reads a second schema shape — the introspected
`foreign_keys` list — which `fk_ordering` does not.
"""

from declaro_persistum.transfer import _build_table_fk_graph, _toposort_tables


def _t(columns=None, foreign_keys=None):
    d = {"columns": columns or {}}
    if foreign_keys is not None:
        d["foreign_keys"] = foreign_keys
    return d


def _ref(table_col):
    return {"type": "integer", "references": table_col}


CHAIN = {
    "users": _t({"id": {"type": "integer"}}),
    "posts": _t({"author": _ref("users.id")}),
    "comments": _t({"post": _ref("posts.id")}),
}




