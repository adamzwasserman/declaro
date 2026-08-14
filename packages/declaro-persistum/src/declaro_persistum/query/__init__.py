"""Query building: functions over data. No proxies, no operators, no classes.

    from declaro_persistum.query import select, insert, update, delete, table

    q = select("users", ["id", "email"], where={"status": "active"})

A WHERE clause is a DICT, not a Python expression:

    {"age": {"gt": 18}, "status": "active"}
    {"or": [{"status": "active"}, {"status": "pending"}]}

The fluent layer that spelled this `users.age > 18` is gone — `TableProxy`,
`ColumnProxy`, `Condition`, `ConditionGroup`, `SelectQuery`, `InsertQuery`,
`UpdateQuery`, `DeleteQuery`, `QuerySet` and `PrismaQueryBuilder`. Between
them they carried eight dunder methods whose only purpose was to borrow
Python\'s expression grammar so the interpreter would assemble an AST. A dict
is already that AST.

`builder.py` — the pure select/insert/update/delete — was in this package the
whole time, exported and called by nothing. It is now the API.
"""

from declaro_persistum.query.builder import (
    Query,
    delete,
    insert,
    raw,
    select,
    update,
    with_limit,
    with_offset,
    with_params,
)
from declaro_persistum.query.conditions import COMBINATORS, OPERATORS
from declaro_persistum.query.conditions import render as render_condition
from declaro_persistum.query.executor import (
    detect_dialect,
    execute,
    execute_many,
    execute_one,
    execute_scalar,
)
from declaro_persistum.query.expressions import (
    CaseOrderBy,
    JoinClause,
    OrderBy,
    SubqueryExpr,
    is_subquery,
    render_order_term,
    render_subquery,
)
from declaro_persistum.query.table import (
    CaseExpression,
    SQLFunction,
    avg_,
    case_,
    count_,
    max_,
    min_,
    now_,
    render_case,
    render_function,
    subquery,
    sum_,
    table,
)

__all__ = [
    # building
    "Query",
    "select",
    "insert",
    "update",
    "delete",
    "raw",
    "with_limit",
    "with_offset",
    "with_params",
    # execution — the boundary
    "execute",
    "execute_one",
    "execute_scalar",
    "execute_many",
    "detect_dialect",
    # conditions, as data
    "render_condition",
    "OPERATORS",
    "COMBINATORS",
    # expressions, as data
    "SQLFunction",
    "CaseExpression",
    "OrderBy",
    "CaseOrderBy",
    "JoinClause",
    "SubqueryExpr",
    "table",
    "count_",
    "sum_",
    "avg_",
    "min_",
    "max_",
    "now_",
    "case_",
    "subquery",
    # rendering
    "render_function",
    "render_case",
    "render_order_term",
    "render_subquery",
    "is_subquery",
]
