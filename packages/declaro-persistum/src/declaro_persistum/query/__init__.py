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

# SQLAlchemy-style API
from declaro_persistum.query.sqlalchemy import (
    BLOB,
    CHAR,
    JSON,
    JSONB,
    TIMESTAMP,
    UUID,
    VARCHAR,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    # Types
    Integer,
    LargeBinary,
    ModelBase,
    Numeric,
    Session,
    SmallInteger,
    String,
    Text,
    Time,
    clear_model_registry,
    declarative_base,
    get_model_schema,
    get_registered_models,
)
from declaro_persistum.query.sqlalchemy import (
    Query as SAQuery,
)
from declaro_persistum.query.table import (
    ColumnProxy,
    Condition,
    ConditionGroup,
    JoinClause,
    OrderBy,
    SQLFunction,
    TableProxy,
    avg_,
    # Aggregate functions
    count_,
    max_,
    min_,
    now_,
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
    # Functions
    "count_",
    "sum_",
    "avg_",
    "min_",
    "max_",
    "now_",
    # Django-style API
    "QuerySet",
    "DoesNotExist",
    "MultipleObjectsReturned",
    # Prisma-style API
    "PrismaQueryBuilder",
    # SQLAlchemy-style API
    "declarative_base",
    "Column",
    "ForeignKey",
    "Session",
    "SAQuery",
    "ModelBase",
    "get_model_schema",
    "get_registered_models",
    "clear_model_registry",
    "Integer",
    "BigInteger",
    "SmallInteger",
    "String",
    "Text",
    "Boolean",
    "Float",
    "Numeric",
    "DateTime",
    "Date",
    "Time",
    "LargeBinary",
    "JSON",
    "JSONB",
    "UUID",
    "VARCHAR",
    "CHAR",
    "TIMESTAMP",
    "BLOB",
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
