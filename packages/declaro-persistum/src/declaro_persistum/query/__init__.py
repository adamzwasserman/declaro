"""
Query building and execution layer.

Provides a functional query builder that generates parameterized SQL
without ORM overhead.

Two APIs are available:

1. Schema-validated dot notation (recommended):
   ```python
   from declaro_persistum.query import table

   schema = load_schema("./schema")
   users = table("users", schema)

   results = await (
       users
       .select(users.id, users.email)
       .where(users.status == "active")
       .execute(conn)
   )
   ```

2. Legacy string-based functions (for compatibility):
   ```python
   from declaro_persistum.query import select, execute

   q = select("id", "email", from_table="users", where="status = :status", params={"status": "active"})
   results = await execute(q, conn)
   ```
"""

# New schema-validated API
# Legacy string-based API (still supported)
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
from declaro_persistum.query.delete import DeleteQuery

# Django-style API
from declaro_persistum.query.django_style import (
    DoesNotExist,
    MultipleObjectsReturned,
    QuerySet,
)
from declaro_persistum.query.executor import execute, execute_many, execute_one, execute_scalar
from declaro_persistum.query.insert import InsertQuery

# Prisma-style API
from declaro_persistum.query.prisma_style import PrismaQueryBuilder
from declaro_persistum.query.select import SelectQuery
from declaro_persistum.query.table import (
    CaseExpression,
    CaseOrderBy,
    ColumnProxy,
    Condition,
    ConditionGroup,
    JoinClause,
    OrderBy,
    SQLFunction,
    SubqueryExpr,
    TableProxy,
    avg_,
    # Aggregate functions
    case_,
    count_,
    max_,
    min_,
    now_,
    subquery,
    sum_,
    table,
)
from declaro_persistum.query.update import UpdateQuery

__all__ = [
    # New API (fluent SQL-like)
    "table",
    "TableProxy",
    "ColumnProxy",
    "Condition",
    "ConditionGroup",
    "OrderBy",
    "JoinClause",
    "SQLFunction",
    "SelectQuery",
    "InsertQuery",
    "UpdateQuery",
    "DeleteQuery",
    # Expression classes
    "CaseExpression",
    "CaseOrderBy",
    "SubqueryExpr",
    # Functions
    "count_",
    "sum_",
    "avg_",
    "min_",
    "max_",
    "now_",
    "case_",
    "subquery",
    # Django-style API
    "QuerySet",
    "DoesNotExist",
    "MultipleObjectsReturned",
    # Prisma-style API
    "PrismaQueryBuilder",
    # Legacy string-based API
    "select",
    "insert",
    "update",
    "delete",
    "raw",
    "with_limit",
    "with_offset",
    "with_params",
    "Query",
    "execute",
    "execute_one",
    "execute_scalar",
    "execute_many",
]
