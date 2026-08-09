# Declaro

[![PyPI version](https://img.shields.io/pypi/v/declaro-persistum.svg)](https://pypi.org/project/declaro-persistum/) [![Python versions](https://img.shields.io/pypi/pyversions/declaro-persistum.svg)](https://pypi.org/project/declaro-persistum/)

> **Note:** Declaro is available on PyPI as early-access software. The APIs are still unstable and under active development, so pin your version and expect breaking changes before 1.0. Not yet recommended for production.

**You declare what you want. The library works out how.**

That is what the name means, and it is the first principle of everything here. You describe the state you want — a schema, a query, a type — and the tools derive the steps. You do not write the steps.

## Vision

Declaro is built on three commitments, in this order.

**1. Declarative.** You declare outcomes, not procedures. You never hand-write a migration: you edit your models to say what the schema should be, and the library reads the live database and derives the operations. You describe the rows you want rather than assembling SQL. Anything that pushes you back into writing procedures is a defect, however convenient it looks.

**2. One surface over many backends.** A single API spans PostgreSQL, SQLite and Turso, so changing database is a change of configuration rather than a refactor — SQLite in development, PostgreSQL in production, Turso at the edge, without touching your code. Where a database lacks a capability, the difference is absorbed by the library rather than reaching your application.

**3. Tools that survive a team.** Migrations that do not collide when two people branch. Errors that say what is wrong rather than where it threw. Behaviour that a second developer can predict from the code in front of them.

The functional style — data as dicts and TypedDicts, pure functions, explicit types, no hidden state — is how these are achieved and how they stay testable. It is the discipline, not the goal.

## Packages

| Package | Description | Status |
| ------- | ----------- | ------ |
| `declaro-persistum` | Declarative database toolkit — schema, migrations, queries, pooling | Published, in active use |
| `declaro-ximinez` | Type enforcement with memorable errors | In development, not yet released |
| `declaro-observe` | Event sourcing observability | Pre-alpha, not yet released |
| `declaro-api` | FastAPI integration | Planned |

Only `declaro-persistum` is on PyPI today. The others are in this repository but not yet published.

## Install

```bash
pip install declaro-persistum

# with a database driver
pip install "declaro-persistum[postgresql]"
pip install "declaro-persistum[sqlite]"
pip install "declaro-persistum[turso]"
pip install "declaro-persistum[all]"
```

The `declaro` meta-package exists on PyPI as a placeholder and does not yet install the stack. Install the package you want directly.

## Philosophy

```python
# Not this (classes, state, magic)
class User(BaseModel):
    email: str

    @validator("email")
    def validate_email(cls, v):
        ...

# This (data, functions, clarity)
User = TypedDict("User", {"email": str})

def validate_user(user: dict) -> list[Error]:
    return check_email(user.get("email", ""))
```

Declaro takes its cues from the “banana, monkey, jungle” problem: libraries should not plant bananas in the environment and then ask you to babysit them. State is owned by the caller; caches in the core are limited to pools and prepared statements, not query results. If you require application‑specific caching strategies, put them in a sibling package such as tablix or handle them yourself. This keeps the persistence façade lean and predictable.
### Getting started

```bash
pip install "declaro-persistum[sqlite]"
```

```python
from uuid import uuid4
from declaro_persistum import ConnectionPool
from declaro_persistum.query import table

# The schema is data — a dict describing your tables. In a real project you
# declare it as Pydantic models and load it with load_schema().
schema = {
    "users": {
        "columns": {
            "id": {"type": "text", "primary_key": True},
            "name": {"type": "text", "nullable": True},
        },
        "primary_key": ["id"],
        "indexes": {},
    }
}

# Switching to PostgreSQL or Turso later changes this line and nothing else.
pool = await ConnectionPool.sqlite("./data.db")

async with pool.acquire() as conn:
    await conn.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT)")
    await conn.commit()

# The pool is bound at table creation, so queries take no connection.
users = table("users", schema, pool)

await users.insert(id=str(uuid4()), name="alice").execute()
rows = await users.select().execute()
print(rows)   # [{'id': '...', 'name': 'alice'}]

await pool.close()
```

A fuller quick start lives in [`packages/declaro-persistum/README.md`](packages/declaro-persistum/README.md), including declaring your schema as models and letting the diff engine derive migrations from it.

## Manifesto & Additional Reading

For the full philosophy (pure functions, explicit types, banana/monkey/jungle, caching, etc.) see [MANIFESTO.md](MANIFESTO.md).

### Further reading

Several essays and blog posts expand on these ideas. You can find them at https://dataos.software:

- [BIG STATE / Declarative Interfaces](https://dataos.software/blog/big-state-declarative-interfaces.html) – why state in libraries is dangerous
- [Classes Broke My Object](https://dataos.software/blog/classes-broke-my-object.html) – the thread that became "Classes Considered Harmful"
- [DATAOS Explained](https://dataos.software/blog/DATAOS-explained.html) – background on the DATAOS project that accompanies this stack

Feel free to copy or republish these posts; they’re a good way to onboard others to the philosophy.

Blog posts are stored as Markdown in `dataos-site/blog/` and rendered to HTML via the static site.

## Contributing

1. `uv sync` to install dependencies.
2. Run the test suite from the root or individual packages via `uv run pytest`.
3. Add new features or fix bugs — keep functions pure and avoid hidden state.
4. Update docs and MANIFESTO.md when you change core principles.

Pull requests are welcome; see `CONTRIBUTING.md` for additional guidelines.

## Manifesto

Read our [MANIFESTO.md](MANIFESTO.md) for the full philosophy.

## License

MIT

---

Built by [Adam Zachary Wasserman](https://adamzacharywasserman.com).
