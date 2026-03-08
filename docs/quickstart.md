# Quick Start

This document shows the minimal steps to begin using Declaro's persistence toolkit. It assumes you have Python 3.11+.

## Install

```bash
pip install declaro-persistum[all]
```

## Create a pool and run a query

```python
from uuid import uuid4
from declaro_persistum import ConnectionPool
from declaro_persistum.query import table

async def main():
    pool = await ConnectionPool.sqlite("./example.db")
    users = table("users")
    async with pool.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT)")
        await conn.commit()
        await users.insert(id=str(uuid4()), name="alice").execute(conn)
        rows = await users.select().execute(conn)
        print(rows)
    await pool.close()

# run with uv run python - <<'PY'
# import asyncio; asyncio.run(main())
# PY
```

## Schema migrations

Schemas are defined with Pydantic models and `@table`.  After you create or modify models, use the CLI:

```bash
# show differences between models and database
declaro diff -c sqlite://./example.db

# apply the changes
declaro apply -c sqlite://./example.db
```

## Next steps

- Read the [Manifesto](../MANIFESTO.md) to understand the philosophy.
- Browse the `dataos-site/blog` posts for essays on design and performance.
- Explore tests in `packages/declaro-persistum/tests` for examples of query APIs and multi‑backend behaviour.