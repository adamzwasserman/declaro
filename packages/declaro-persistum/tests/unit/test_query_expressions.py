"""
Unit tests for advanced query builder expressions.

Tests SQL generation for:
- CaseExpression (case_() factory)
- CaseOrderBy
- SubqueryExpr (subquery() factory)
- SQLFunction.to_sql_fragment() with CaseExpression args
- TableProxy.alias()
- Condition.to_sql() with SubqueryExpr
"""


from declaro_persistum.types import Schema

# ---------------------------------------------------------------------------
# Minimal schema
# ---------------------------------------------------------------------------

_SCHEMA: Schema = {
    "tickets": {
        "columns": {
            "id": {"type": "integer", "primary_key": True},
            "severity": {"type": "text"},
            "status": {"type": "text"},
            "amount": {"type": "integer"},
            "user_id": {"type": "integer"},
        }
    },
    "users": {
        "columns": {
            "id": {"type": "integer", "primary_key": True},
            "email": {"type": "text"},
        }
    },
    "roles": {
        "columns": {
            "user_id": {"type": "integer"},
            "name": {"type": "text"},
        }
    },
    "comments": {
        "columns": {
            "id": {"type": "integer", "primary_key": True},
            "parent_id": {"type": "integer"},
            "body": {"type": "text"},
        }
    },
}


# ---------------------------------------------------------------------------
# Feature #6: TableProxy.alias()
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Feature #1: CaseExpression
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Feature #1: CaseOrderBy
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Feature #4: sum_(case_(...))
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Feature #5: subquery() in .in_()
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Feature #3: Compound OR in JOIN WHERE (verify already works)
# ---------------------------------------------------------------------------




# ---------------------------------------------------------------------------
# Feature #2: Self-JOIN with GROUP BY + aggregate
# ---------------------------------------------------------------------------


