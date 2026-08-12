# Feature Request: Per-Table Row-Level-Security Policies

**Requested by**: honest-starter (FastAPI/HTMX multi-user starter)
**Priority**: High — blocks auth-first starter kit; currently requires either an app-layer TableProxy wrapper (fragile, bypassable) or monkey-patching Declaro's execute methods (fragile, modifies vendored lib).
**Context**: Apps built on `honest-starter` are multi-user by design. Every non-public row needs: `created_by_user_id`, `public`, `shared_with_user_ids`, `created_date`, `last_changed_date`, `deleted_date`. Every SELECT must filter by ownership. Every INSERT must auto-set ownership + timestamps. Every UPDATE must auto-bump `last_changed_date`. Every DELETE must soft-delete (set `deleted_date = now()` instead of removing the row). Today SQLAlchemy apps do this with `before_compile` / `before_flush` events. Declaro has no equivalent, so the enforcement either leaks into every route (easy to forget) or requires a TableProxy shim.

---

## Proposed API

Per-table policies declared alongside the Pydantic model, resolved at schema-load time, applied automatically on every query against that table.

### Minimal example

```python
from contextvars import ContextVar
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field
from declaro_persistum import policy, fn

# App provides a context source. Declaro calls this to learn "who is asking?"
current_user_id: ContextVar[str | None] = ContextVar("current_user_id", default=None)


class Item(BaseModel):
    __tablename__ = "items"
    __policy__ = policy(
        context=current_user_id.get,

        # Applied to every SELECT + every UPDATE + every DELETE touching this table.
        # None or empty = no filter (equivalent to `public` rows only). Use a raw
        # SQL fragment; :user is a reserved placeholder bound to context().
        row_filter=(
            "public = TRUE "
            "OR created_by_user_id = :user "
            "OR position(:user in shared_with_user_ids) > 0"
        ),

        # Applied on every INSERT unless the caller passes the column explicitly.
        auto_insert={
            "created_by_user_id": fn.context(),  # value comes from context() at exec time
            "created_date": fn.now(),
            "last_changed_date": fn.now(),
        },

        # Applied on every UPDATE unless the caller passes the column explicitly.
        auto_update={
            "last_changed_date": fn.now(),
        },

        # Convert DELETE into UPDATE with this column bumped to now().
        # None = hard delete (current behavior).
        soft_delete="deleted_date",

        # Universal "hide soft-deleted rows" filter, AND-ed with row_filter on SELECT.
        # Omit to disable auto-hiding.
        soft_delete_hides=True,
    )

    id: UUID = Field(json_schema_extra={"primary": True, "default": "gen_random_uuid()"})
    name: str = Field(min_length=1)

    # Ownership columns — Declaro doesn't require these, but row_filter / auto_*
    # clauses reference them so the schema MUST declare them.
    created_by_user_id: UUID = Field(json_schema_extra={"nullable": False})
    public: bool = Field(json_schema_extra={"default": "FALSE", "nullable": False})
    shared_with_user_ids: str = Field(json_schema_extra={"default": "''", "nullable": False})
    created_date: datetime = Field(json_schema_extra={"default": "now()"})
    last_changed_date: datetime = Field(json_schema_extra={"default": "now()"})
    deleted_date: datetime | None = Field(default=None)
```

### Behavior at runtime

When a route handler does:

```python
items = table("items", schema, pool)
rows = await items.select(items.id, items.name).where(items.category == "hardware").execute()
```

Declaro composes the final SQL roughly as:

```sql
SELECT id, name
FROM items
WHERE category = :p_1
  AND (public = TRUE OR created_by_user_id = :user OR position(:user in shared_with_user_ids) > 0)
  AND deleted_date IS NULL
```

`:user` is bound to `current_user_id.get()` at execute-time. If the ContextVar is unset, Declaro raises `PolicyContextMissingError` — **fail-closed**, never silently elide the filter.

INSERT auto-fields work identically:

```python
await items.insert(name="Widget", category="hardware").execute()
# Declaro adds created_by_user_id, created_date, last_changed_date from policy.auto_insert.
```

Explicit values passed by the caller win over auto defaults (so admin scripts can set `created_by_user_id` directly if needed — Declaro doesn't reject this, but logs a warning at `WARN` level so it's visible).

DELETE converts to UPDATE:

```python
await items.delete().where(items.id == item_id).execute()
# Declaro actually emits:
#   UPDATE items SET deleted_date = now() WHERE id = :p_1
#     AND (public = TRUE OR created_by_user_id = :user OR position(:user in shared_with_user_ids) > 0)
#     AND deleted_date IS NULL
```

### Policy dataclass

```python
@dataclass(frozen=True)
class Policy:
    context: Callable[[], str | None]
    row_filter: str | None = None
    auto_insert: dict[str, Any] = field(default_factory=dict)
    auto_update: dict[str, Any] = field(default_factory=dict)
    soft_delete: str | None = None            # column name, or None
    soft_delete_hides: bool = False           # auto-add "col IS NULL" to row_filter
    placeholder_name: str = "user"            # the placeholder in row_filter (default ":user")
```

`policy(...)` is a thin constructor for `Policy`. `fn.context()` / `fn.now()` / `fn.uuid()` are sentinel values the insert/update composer recognizes and replaces.

### Context source

A per-policy `context: Callable[[], Any]`. Apps set it to a `ContextVar.get` bound method. Declaro calls it once per query execution, caches the result for that query only. No global state inside Declaro.

When the context callable returns `None`:

- **Queries with row_filter**: raise `PolicyContextMissingError`. Fail-closed.
- **Queries with auto_insert referencing `fn.context()`**: raise `PolicyContextMissingError`. Fail-closed.
- **Queries only using soft_delete_hides (no row_filter referencing user)**: proceed — no context needed.

### Escape hatch

One explicit, visible way to bypass all policies (for admin scripts, migrations, data repair):

```python
with pool.without_policies():
    # Inside this block, Declaro emits raw SQL with no row_filter, auto_insert,
    # or soft_delete conversion. Every write or read logs a WARN line saying the
    # policy was bypassed.
    rows = await items.select().execute()
```

Nested `without_policies()` blocks are legal (re-entrant). Outside this context, policies are always active.

## What to record

- **Test:** Unit tests under `tests/unit/` showing row_filter, auto_insert, auto_update, soft_delete, and PolicyContextMissingError behavior.
- **Migration:** No schema changes in Declaro itself — this is all query composition. The user's schema still declares the ownership columns manually.
- **Documentation:** One section in the top-level README showing the minimal example above.

## Non-goals

- **No automatic column injection.** Declaro does NOT add `created_by_user_id` columns to tables that lack them. The user declares the schema; policies reference it.
- **No multi-tenant isolation beyond row_filter.** If you need table-per-tenant or schema-per-tenant, build it on top.
- **No caching of context resolution.** Each query resolves context fresh (modulo the one-query cache mentioned above). Context is cheap — a ContextVar read.
- **No hooks/events beyond these five policy slots.** This is not a general event system. If you need arbitrary hooks later, extend the Policy dataclass.

## Open questions for the maintainer

1. Should `row_filter` accept a `ConditionGroup` object instead of a SQL string? More type-safe but harder to express across dialects. Current proposal: string + named placeholder. Maintainer decision.
2. Should `policy(...)` be applied at Pydantic model level (`__policy__`) or registered separately via `register_policy(table_name, policy)`? Model-level is more declarative; separate registration is more flexible for retrofit. Current proposal: model-level.
3. Does `soft_delete` need to work with CASCADE / parent-child relationships? Current proposal: no, it's per-table. Cascading soft-deletes are the app's responsibility.
