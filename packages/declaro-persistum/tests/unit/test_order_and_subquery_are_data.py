"""ORDER BY terms and subquery expressions are data.

Strangler slices two, three and four on `query/table.py`, following
`JoinClause`. All three classes are PRODUCED by the fluent API — `col.asc()`,
`case_(...).desc()`, `subquery(q)` — and never constructed by name, so the
consumer API does not change when they become TypedDicts.

Each carries a `kind` tag, which is the point of the slice rather than
decoration. The rendering code dispatched on SHAPE:

    if hasattr(o, "to_sql_fragment"):     # select.py — CaseOrderBy
        ...
    else:                                 # OrderBy
        ...

    if isinstance(self.value, SubqueryExpr):   # table.py — Condition

`hasattr` dispatch silently reclassifies anything that grows or loses a
method, and `isinstance` cannot be used on a TypedDict at all. Both become a
dict lookup on `kind` (Rule 1), which fails loudly on an unknown tag instead
of falling through to the other branch.

The ORDER BY term vocabulary is bounded — a plain column term or a CASE term
— so both members are rendered here rather than one.
"""

from typing import get_type_hints

import pytest

from declaro_persistum.query.table import (
    CaseOrderBy,
    OrderBy,
    SubqueryExpr,
    case_,
    subquery,
    table,
)

SCHEMA = {
    "users": {"columns": {"id": {"type": "int"}, "name": {"type": "text"},
                          "age": {"type": "int"}}},
    "roles": {"columns": {"user_id": {"type": "int"}, "name": {"type": "text"}}},
}

SHAPES = [
    (OrderBy, {"kind", "column", "direction"}),
    (CaseOrderBy, {"kind", "expr", "direction"}),
    (SubqueryExpr, {"kind", "query"}),
]


@pytest.mark.parametrize("cls,fields", SHAPES)
def test_it_is_a_typed_dict_with_a_kind_tag(cls, fields):
    assert issubclass(cls, dict), f"{cls.__name__} is still a class"
    assert set(get_type_hints(cls)) == fields


def _users():
    return table("users", schema=SCHEMA)




