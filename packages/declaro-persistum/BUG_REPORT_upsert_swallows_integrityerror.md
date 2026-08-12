# Bug: `.create` / `.upsert` silently swallow `IntegrityError` (silent data loss)

## Summary

`TableProxy.create()` and `TableProxy.upsert()` return **success** for a write
that violates a database constraint (foreign key, and by the same mechanism any
constraint surfaced at row-fetch time), while writing **nothing**. A failed
constrained write is therefore indistinguishable from a successful one — silent
data loss.

The equivalent raw `INSERT` (via `connection.execute`) correctly raises
`IntegrityError`. Only the ORM create/upsert path swallows it, because it uses
`INSERT ... RETURNING *`.

## Environment

- `declaro-persistum` **0.1.8**
- `pyturso` **0.5.1**
- Python 3.13, macOS (arm64)
- Turso embedded pool (`ConnectionPool.turso`), local-only or cloud-synced —
  reproduces on **local-only** (see repro), so no cloud is required.

## Severity

**High.** A constraint-violating create/upsert is reported as success and the
row never lands. Callers that trust the return value (e.g. "write a row, then
read it back") observe a phantom success followed by an empty read, which can
cascade into ret/redirect loops, orphaned parents, or lost records.

## Minimal reproduction (no cloud needed)

`swallow_schema.py`:

```python
from pydantic import BaseModel, Field


class Parent(BaseModel):
    __tablename__ = "parent"
    id: str = Field(json_schema_extra={"primary_key": True, "nullable": False})


class Child(BaseModel):
    __tablename__ = "child"
    id: str = Field(json_schema_extra={"primary_key": True, "nullable": False})
    parent_id: str = Field(
        json_schema_extra={"nullable": False, "references": "parent.id"}
    )
```

`repro.py`:

```python
import asyncio
import tempfile
from pathlib import Path

from declaro_persistum import ConnectionPool, load_models_from_module
from declaro_persistum.query.table import table

SCHEMA = Path(__file__).parent / "swallow_schema.py"


async def main():
    pool = await ConnectionPool.turso(str(Path(tempfile.mkdtemp()) / "t.db"))
    async with pool.acquire() as c:
        await c.execute("PRAGMA foreign_keys = ON")
        await c.execute("CREATE TABLE parent (id TEXT PRIMARY KEY)")
        await c.execute(
            "CREATE TABLE child (id TEXT PRIMARY KEY, "
            "parent_id TEXT NOT NULL REFERENCES parent(id))"
        )
        await c.commit()

    schema = load_models_from_module(SCHEMA)
    child = table("child", schema, pool)

    # (1) proxy upsert whose parent_id does not exist -> FK violation
    try:
        await child.upsert(
            where={"id": "c1"},
            create={"id": "c1", "parent_id": "ghost"},
            update={"parent_id": "ghost"},
        )
        print("(1) proxy .upsert : returned SUCCESS  <-- IntegrityError swallowed")
    except Exception as e:
        print(f"(1) proxy .upsert : RAISED {type(e).__name__}: {e}")

    # (2) was anything actually written?
    row = await table("child", schema, pool).find_one(where={"id": "c1"})
    print(f"(2) row written? {row is not None}")

    # (3) raw INSERT of the same violating row for contrast
    try:
        async with pool.acquire() as c:
            await c.execute("INSERT INTO child (id, parent_id) VALUES ('c2', 'ghost')")
            await c.commit()
        print("(3) raw INSERT    : returned SUCCESS")
    except Exception as e:
        print(f"(3) raw INSERT    : RAISED {type(e).__name__}  <-- correct")

    await pool.close()


asyncio.run(main())
```

### Observed output

```
(1) proxy .upsert : returned SUCCESS  <-- IntegrityError swallowed
(2) row written? False
(3) raw INSERT    : RAISED IntegrityError  <-- correct
```

### Expected

`(1)` should raise `IntegrityError` (like `(3)` does). At minimum, a create that
writes nothing must not return a success value.

## Root cause

The create/upsert path issues `INSERT ... RETURNING *`. With pyturso 0.5.1 the
constraint violation for `INSERT ... RETURNING` surfaces **at fetch time**, not
at `execute()` time. `TursoAsyncConnection.execute` (native-async path,
`pool.py` ~lines 162–166) wraps the fetch in a bare `except Exception` that
turns any fetch error into an empty result set:

```python
cursor = await self._holder.conn.execute(sql, parameters)
try:
    rows = await cursor.fetchall()
except Exception:
    rows = []          # <-- swallows IntegrityError raised while fetching RETURNING
return TursoAsyncCursor(rows, ...)
```

So the `IntegrityError` pyturso raises while materialising the `RETURNING` rows
is discarded, `create()` gets `rows == []`, and
`prisma_style.create()` returns the input `data` via
`return result or data` — reporting success.

### Direct confirmation of the mechanism

```
INSERT ... (no RETURNING)      -> RAISES IntegrityError      (error surfaces at execute())
INSERT ... RETURNING *         -> NO ERROR, rows=[]          (error surfaces at fetchall(), swallowed)
```

This is why raw `connection.execute("INSERT ...")` (no RETURNING) behaves
correctly while `.create` / `.upsert` do not.

## Suggested fix direction

The bare `except Exception: rows = []` in `TursoAsyncConnection.execute` is too
broad. It should only absorb the "statement produced no result set to fetch"
case, and must **re-raise** genuine driver errors (constraint violations, etc.).
Options:

- Narrow the catch to the specific "no rows / no result set" pyturso signal and
  let everything else propagate; or
- Detect whether the statement is expected to return rows (it has `RETURNING`)
  and, if so, never swallow fetch errors; or
- Execute constrained writes without depending on `RETURNING` for
  error-surfacing (surface errors from `execute()`/`commit()`).

Any of these makes `.create` / `.upsert` fail loudly on constraint violations,
matching raw `INSERT` and the documented contract.
