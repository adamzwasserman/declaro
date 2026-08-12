# Bug: `update_many(... increment=...)` raises `TypeError: object of type 'int' has no len()`

## Summary

`TableProxy.update_many()` introduced in 0.1.5 raises `TypeError: object of type 'int' has no len()` on every call. The Prisma-style wrapper at `declaro_persistum/query/prisma_style.py:412` does `return len(rows)` on a value that is already an `int` (the affected-row count returned by `_build_update_query(...).execute()`).

`update_one(..., increment=...)` is not affected — it routes through `self.prisma.update(...)` and works correctly.

## Reproduction

Any call to `update_many` on a TableProxy. Minimal repro:

```python
import asyncio
from declaro_persistum import ConnectionPool

async def main():
    pool = await ConnectionPool.turso(":memory:")
    await pool.execute_sql(
        "CREATE TABLE counters (id TEXT PRIMARY KEY, n INTEGER NOT NULL DEFAULT 0)"
    )
    await pool.counters.create(data={"id": "a", "n": 0})

    # This raises TypeError:
    await pool.counters.update_many(
        where={"id": {"in": ["a"]}},
        increment={"n": 1},
    )

asyncio.run(main())
```

## Observed traceback

```
File ".../declaro_persistum/query/prisma_style.py", line 412, in update_many
    return len(rows)
           ^^^^^^^^^
TypeError: object of type 'int' has no len()
```

The two lines before the bug:

```python
values = self._compose_update_values(data, increment)
rows = await self._build_update_query(where, values).execute()
return len(rows)
```

## Analysis

`_build_update_query(...).execute()` returns an `int` (the affected-row count), not a list. The Prisma-style wrapper assumes it received the row list and tries to `len()` it.

The wrapper's declared return type is `int` (rows-updated count, per the docstring example):

```python
async def update_many(
    self,
    *,
    where: dict[str, Any],
    data: dict[str, Any] | None = None,
    increment: dict[str, Any] | None = None,
) -> int:
```

So whatever the fix shape, the caller-facing contract is correct — it's just the internal coercion that's broken.

## Suggested fixes

### Fix A: `update_many` returns the int directly

If the fluent `.execute()` already returns the affected-row count:

```python
values = self._compose_update_values(data, increment)
return await self._build_update_query(where, values).execute()
```

### Fix B: defensive coercion

```python
result = await self._build_update_query(where, values).execute()
return result if isinstance(result, int) else len(result)
```

Fix A is cleaner if the fluent contract is stable; Fix B is a one-line patch that's robust if `.execute()` semantics differ across update vs select-returning-update.

## Affected versions

declaro-persistum **0.1.5** (PyPI). `update_one(..., increment=...)` works in the same release; only `update_many` is broken.

## Impact

`update_many` is unusable in 0.1.5. Downstream consumers that planned to collapse per-row `update_one` loops into a single atomic `update_many(... increment=...)` cannot ship until the wrapper is patched.
