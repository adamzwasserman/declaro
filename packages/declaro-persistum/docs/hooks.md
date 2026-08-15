# Query Hooks

**There are no hooks any more, and nothing was lost.** `table_factory`, `SelectQuery`, `InsertQuery`, `UpdateQuery` and `DeleteQuery` were deleted in `264cedd` ("a WHERE clause is data, not a Python expression"), which took `query/hooks.py` with them. Every example in the old version of this document raised `ImportError`.

What replaced them is smaller. A query is now a `Query`: a dict of `sql`, `params` and `dialect`. So a pre-hook is a function from `Query` to `Query`, and a post-hook is a function from rows to rows. There is no slot to register them in because there is nothing to register — you call them.

## Row-level security

```python
from declaro_persistum import Query, execute, reading, select

def scope_to_owner(query: Query, owner: str) -> Query:
    return {
        **query,
        "sql": query["sql"] + " AND owner = :owner",
        "params": {**query["params"], "owner": owner},
    }

q = select("id", from_table="items", where={"status": "active"})

async with reading(db) as conn:
    rows = await execute(scope_to_owner(q, current_user_id), conn)
```

Measured on a two-row table: the unscoped query returns both rows, the scoped one returns Alice's. The function is pure, so its test is `assert scope_to_owner(q, "alice") == expected` with no database in sight.

## Audit logging

A post-hook is a function you call on the result:

```python
def log_audit(rows: list, sql: str) -> list:
    audit_log.append({"sql": sql, "rows": len(rows)})
    return rows

async with reading(db) as conn:
    rows = log_audit(await execute(q, conn), q["sql"])
```

## Soft delete

The old document made much of a pre-hook that could rewrite a DELETE into an UPDATE by returning a different query type. With queries as data, that is one function returning a different value:

```python
from declaro_persistum import Query, delete, update

def soft(query: Query) -> Query:
    """Callers write `delete(...)`; the table never loses a row."""
    if not query["sql"].startswith("DELETE FROM"):
        return query
    head, _, where = query["sql"].partition(" WHERE ")
    table = head.split('"')[1]
    return update(table, {"deleted_at": "now()"},
                  where=where, params=query["params"])
```

```text
in:  DELETE FROM "items" WHERE id = :w_0_id
out: UPDATE "items" SET "deleted_at" = :set_deleted_at WHERE id = :w_0_id
```

Carry the WHERE across. A first draft of this passed `where="1=1"` and kept the original parameters, which reads as correct and soft-deletes the whole table.

## Applying several

Composition is ordinary Python. No registry, no import-time side effects, no ordering rules to learn:

```python
from functools import reduce

def apply_hooks(query: Query, *hooks) -> Query:
    return reduce(lambda q, h: h(q), hooks, query)
```

## Why this is the better shape

The old design had a real virtue — hooks were passed in, not registered — and it needed 127 lines and five query classes to deliver it. The virtue came from the hooks being ordinary functions, not from the machinery around them. Once the query itself is data, the machinery has nothing left to do.

The cost is that a hook now works on rendered SQL rather than on an unrendered builder, so a rewrite is string work rather than structural. For scoping and auditing that is what the old builder hooks did anyway. For a rewrite that has to understand the query's structure, build the second query from the same inputs instead of editing the first one's text.
