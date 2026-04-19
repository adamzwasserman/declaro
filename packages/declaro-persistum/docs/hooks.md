# Query Hooks

Declaro-persistum gives you two hook slots per table:

- **pre-hook** — runs before SQL is built. Receives the query builder object, returns a (possibly modified) query object.
- **post-hook** — runs after the DB returns rows. Receives `(rows, QueryMeta)`, returns (possibly transformed) rows.

This is the primitive you build row-level security, audit logging, soft deletes, tenant isolation, dev/test query rewriting, and read-replica routing on top of. Declaro itself ships none of those policies — you write them in your app, where the auth model lives.

## Design: hooks are passed in, not registered

Hooks are ordinary function references that you **pass as arguments** to `table_factory(...)` (or directly to `.execute()`). They are not decorators, not entries in a registry, not attached to a class by metaclass magic. The control flow is explicit at the call site:

```python
# Hooks are data. You bind them where the table proxy is created:
get_table = table_factory(schema, pool, pre=apply_rls, post=log_audit)

# …and they are visible function references, testable in isolation:
assert apply_rls(some_query).to_sql("sqlite")[0] == expected_sql
```

There is no module-level registry of hooks. There is no decorator like `@register_pre_hook("items")` that runs at import time. Declaro never reaches outside the arguments you handed it — no `ContextVar` reads inside the library, no reflection over module globals, no side effects from importing.

This is deliberate. It means:

- **You can trace every hook to a line of your code.** No "where did this filter come from?" archaeology.
- **You can compose externally.** If you want two pre-hooks, you write `pre=lambda q: second(first(q))`. Declaro does not pick an ordering for you because ordering is application semantics.
- **You can test hooks as pure functions.** A hook is `(query) -> query` or `(rows, meta) -> rows`. No declaro setup, no mocks, no fixtures — just `assert apply_rls(q).to_sql(...) == expected`.
- **Different scopes use different factories.** Web requests use `make_request_table(...)` with RLS; background jobs use `make_job_table(...)` with no RLS; admin scripts use plain `table(...)` with nothing. Each is a different factory binding different hook functions. Same declaro, different composition.
- **Declaro never mutates its own state.** Calling `table_factory(...)` twice with different hooks produces two independent closures. There is nothing global to be out of sync with, nothing to clear between tests, nothing that can leak from one request into another.

The inverse — decorator-based registration — was considered and rejected. A `@pre_hook(table="items")` decorator would require a module-level registry, which means declaro carrying state the user cannot see, initialization order dependencies, and the exact "action at a distance" Honest Code avoids.

## Quick example

```python
from declaro_persistum import table_factory
from declaro_persistum.query.select import SelectQuery
from declaro_persistum.query.table import table

# Your app-defined hook. Pure function. Testable without declaro.
def apply_rls(query):
    user = current_user_id.get()
    if user is None:
        raise PermissionError("no authenticated user")
    if isinstance(query, SelectQuery):
        proxy = table(query._table, query._schema, query._pool)
        return query.where(proxy.owner == user)
    return query

def log_audit(rows, meta):
    audit_log.append({"sql": meta["sql"], "row_count": len(rows)})
    return rows

# At app startup — one place:
get_table = table_factory(schema, pool, pre=apply_rls, post=log_audit)

# In every route — normal usage, hooks applied automatically:
items = get_table("items")
rows = await items.select().where(items.owner == user_id).execute()
```

## API

### `table_factory(schema, pool, *, pre=None, post=None) -> Callable[[str], TableProxy]`

Pure function. Returns a closure that produces `TableProxy` instances with hooks pre-wired. Flow into every query builder the proxy creates, into `.execute()`, and get called in order: **pre → SQL execution → post**.

### `table(name, schema, pool=None, *, pre=None, post=None) -> TableProxy`

Same hooks, one table at a time. Use this when you only need hooks on a single table; use `table_factory` when you want the same hooks on many tables.

### `.execute(pre=None, post=None, without_hooks=False)` on every query builder

Override or bypass per-call:

```python
# Override factory-level pre with a different one (post stays from factory)
await items.select().execute(pre=special_rls)

# Bypass all hooks (admin script, background job, migration repair)
await items.delete().where(items.id == 42).execute(without_hooks=True)
```

The same kwargs are available on `.execute_one()` and `.execute_scalar()`.

## Semantics

### Pre-hook signature

```python
PreHook = Callable[[Query], Query]
# Query is one of: SelectQuery | InsertQuery | UpdateQuery | DeleteQuery
```

- Receives the query builder object as-is (pre-`.to_sql()`).
- Returns a query object. Can return the **same** object (e.g., after `.where(...)`), or a **different query type** entirely. A pre-hook on DELETE can return an UpdateQuery to implement soft delete — the executor runs whatever comes back.
- Returning `None` raises `TypeError`. There is no silent no-op.

### Post-hook signature

```python
PostHook = Callable[[list[dict[str, Any]], QueryMeta], list[dict[str, Any]]]

class QueryMeta(TypedDict):
    operation: Literal["select", "insert", "update", "delete"]
    table: str
    sql: str
    params: dict[str, Any]
    dialect: str
```

- Always receives `list[dict]`, regardless of whether the caller used `.execute()`, `.execute_one()`, or `.execute_scalar()`. Mode reduction happens **after** the post-hook runs.
- `QueryMeta.sql` and `QueryMeta.params` reflect the SQL **actually executed** — i.e., the post-pre-hook query. An audit logger gets the truth, not the pre-transform version.
- **Mode reduction caveat**: a post-hook that drops rows changes what `.execute_one()` returns. If the raw query returned one row and your post-hook returns `[]`, then `.execute_one()` returns `None`. Same for `.execute_scalar()`. This is consistent behavior, but worth naming so it's not a surprise.

### Hook layering

- Factory-level hook: set via `table_factory(..., pre=X, post=Y)` or `table(..., pre=X, post=Y)`.
- Execute-level hook: set via `.execute(pre=Z, post=W)`.
- **Execute-level replaces factory-level.** There is no auto-composition. Choosing an order or merging references is application semantics — declaro stays out of it.
- If you want to compose, do it explicitly: `.execute(pre=lambda q: factory_pre(extra_pre(q)))`.

### Bypass

`.execute(without_hooks=True)` skips both pre and post for that single call. Useful for admin scripts, data migrations, or reconstructing state when you need to see the raw database.

Nesting semantics: `without_hooks` is a per-call boolean. There is no nested re-entry concept — each `.execute()` call is independent.

### Avoiding recursion when hooks rewrite queries

When a pre-hook returns a different query object (e.g., DELETE → UPDATE), the executor calls the returned object's **`_run_raw()`** internal seam, not its public `.execute()`. That means the returned query's hooks do **not** re-fire — standard middleware pattern, no infinite loop.

`_run_raw()` is an underscore-prefixed internal method. Tools doing introspection-style query execution may want to call it directly, but it is not a stable public API; its return shape is `list[dict]` pre-mode-reduction and may change.

## Example: row-level security

```python
from contextvars import ContextVar
from declaro_persistum import table_factory
from declaro_persistum.query.select import SelectQuery
from declaro_persistum.query.update import UpdateQuery
from declaro_persistum.query.insert import InsertQuery
from declaro_persistum.query.delete import DeleteQuery
from declaro_persistum.query.table import table

current_user_id: ContextVar[str | None] = ContextVar("current_user_id", default=None)

def apply_rls(query):
    user = current_user_id.get()
    if user is None:
        raise PermissionError("no authenticated user in context")

    # Re-derive column proxies for the table under attack.
    proxy = table(query._table, query._schema, query._pool)

    if isinstance(query, SelectQuery):
        # Owner OR public OR shared with me
        return query.where(
            (proxy.owner == user)
            | (proxy.public == True)  # noqa: E712
            | proxy.shared_with.like(f"%{user}%")
        )

    if isinstance(query, DeleteQuery):
        # Rewrite as soft delete — return UPDATE instead.
        soft = proxy.update(deleted_at="now").where(proxy.id == proxy.id)
        if query._where is not None:
            soft = soft.where(query._where)
        return soft.where(proxy.owner == user)

    if isinstance(query, (InsertQuery, UpdateQuery)):
        # Stamp owner/timestamps
        now_values = {"updated_at": "now"}
        if isinstance(query, InsertQuery) and "owner" not in query._values:
            now_values["owner"] = user
            now_values["created_at"] = "now"
        # ... merge into query._values carefully ...
        return query

    return query
```

Test it with plain assertions — no mocks needed:

```python
def test_rls_adds_owner_filter():
    user_token = current_user_id.set("alice")
    try:
        proxy = table("items", schema, pool=None)
        q = proxy.select(proxy.id)

        transformed = apply_rls(q)
        sql, params = transformed.to_sql("sqlite")

        assert "owner" in sql
        assert "alice" in params.values()
    finally:
        current_user_id.reset(user_token)
```

## Example: audit logging

```python
def log_audit(rows, meta):
    # Reflects the SQL that actually ran, including any pre-hook transformations.
    log.info({
        "op":     meta["operation"],
        "table":  meta["table"],
        "sql":    meta["sql"],
        "params": meta["params"],
        "rows":   len(rows),
    })
    return rows  # Always return rows — the caller relies on them.
```

## Testing hooks

Because hooks are pure functions taking query objects, test them with assertions — no mocks:

```python
def test_pre_hook_adds_where():
    items = table("items", schema, pool=None)
    q = items.select(items.id)

    transformed = apply_rls(q)
    sql, params = transformed.to_sql("sqlite")

    assert "owner" in sql
```

For integration tests, use a real in-memory SQLite pool and exercise the full flow — `tests/unit/test_hooks.py` in this repo does exactly that.

## Performance

- One Python function call per query for pre-hook.
- One call per query for post-hook.
- No reflection, no dict lookup on the hot path, no `ContextVar` reads inside declaro.
- When no hooks are configured, the `.execute()` path is one `if` away from the pre-hook code — negligible overhead.
- When a post-hook is set on a call that would otherwise use `mode="one"` or `mode="scalar"`, the executor upgrades internally to `mode="all"` (so the post-hook can see the full `list[dict]`). If the query has no `LIMIT`, this may pull more rows than `execute_one`/`execute_scalar` would alone. Add `.limit(n)` to the query when you care.

## Non-goals

- No auto-registration, decorators, or import-time side effects.
- No ContextVar use inside declaro. Users read ContextVars inside their hook.
- No fail-closed error handling in declaro. User's hook raises what it wants.
- No hook composition helpers. Ordinary Python.
- No schema-level `__hooks__` attribute on Pydantic models. Hooks are composition, not schema metadata.
- No multiple pre/post hooks per slot. Use one; compose externally if needed.
