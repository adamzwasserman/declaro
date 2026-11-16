# Declaro

[![PyPI version](https://img.shields.io/pypi/v/declaro-persistum.svg)](https://pypi.org/project/declaro-persistum/) [![Python versions](https://img.shields.io/pypi/pyversions/declaro-persistum.svg)](https://pypi.org/project/declaro-persistum/)

> **Note:** Declaro is available on PyPI as early-access software. The APIs are still unstable and under active development, so pin your version and expect breaking changes before 1.0. Not yet recommended for production. `declaro-persistum` is being rebuilt from an architecture declaration, and the 0.1.x API below will be replaced by the one described under [Where persistum is going](#where-persistum-is-going).

**You declare what you want. The library works out how.**

That is what the name means, and it is the first principle of everything here. You describe the state you want — a schema, a query, a type — and the tools derive the steps. You do not write the steps.

## Vision

Declaro is built on three commitments, in this order.

**1. Declarative.** You declare outcomes, not procedures. You never hand-write a migration: you edit your models to say what the schema should be, and the library reads the live database and derives the operations. You describe the rows you want rather than assembling SQL. Anything that pushes you back into writing procedures is a defect, however convenient it looks.

**2. One surface over many backends.** A single API spans PostgreSQL, SQLite and Turso. The engine is read off the target string you open with, so moving from SQLite in development to PostgreSQL in production to Turso at the edge changes one argument and nothing else. Where a database lacks a capability, the library absorbs the difference rather than letting it reach your application.

**3. Tools that survive a team.** Migrations that do not collide when two people branch, which falls out of deriving them rather than writing them down. Alembic's files are a linked list: each one names the one before it, and two developers who branch produce two files claiming the same parent. Merging gives you two heads, a refusal to run, and a hand-written merge revision that picks an order neither person tested. Derive instead and there is no list to collide, because models merge the way any other Python merges. Add to that errors saying what went wrong and what to do about it, not where they threw, and behaviour a second developer can predict from the code in front of them.

The functional style — data as dicts and TypedDicts, pure functions, explicit types, no hidden state — is how these are achieved and how they stay testable. It is the discipline, not the goal.

## Packages

| Package | Description | Status |
| ------- | ----------- | ------ |
| `declaro-persistum` | Declarative database toolkit — schema, migrations, queries | Published, in active use |
| `declaro-ximinez` | Type enforcement with memorable errors | In development, not yet released |
| `declaro-observe` | Event sourcing observability | Pre-alpha, not yet released |
| `declaro-tablix` | Declarative table and filter system with CSS Grid layouts | In development, not yet released |
| `declaro-advise` | Notification and messaging for the stack | In development, not yet released |
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

Declaro takes its cues from the “banana, monkey, jungle” problem, which Joe Armstrong named: you wanted a banana, and what you got was a gorilla holding the banana and the entire jungle. Ask an object for one small thing and you have to take the object, everything it owns and the environment it needs to run. A function that takes a dict and returns a dict hands you the banana.

So this library does not hold your rows. You ask for rows, you get rows, and nothing is kept. If you want them a second time without going back to the database, you keep them, or a package above this one keeps them for you. That way the only thing that can serve you a stale row is code you wrote.

### Getting started

This is the released API, which is what `pip install` gives you today.

```bash
pip install "declaro-persistum[sqlite]"
```

```python
from uuid import uuid4
from declaro_persistum import ConnectionPool
from declaro_persistum.query import table

# The schema is data — a dict describing your tables. In a real project you
# declare it as Pydantic models and load it with load_schema_from_models().
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

Those two hand-written lines of `CREATE TABLE` are the reason for the rewrite below. The first commitment on this page says you never write schema by hand, and here you do.

A fuller quick start lives in [`packages/declaro-persistum/README.md`](packages/declaro-persistum/README.md).

## Where persistum is going

The library is being rebuilt from an architecture declaration, `docs/design/persistum.hd`, which is the design in a form the tooling reads rather than a document that drifts from the code. Both the declaration and the rebuilt code live on a branch and are not released, so nothing below installs yet and neither file is on `main`. It is here because it changes the API you will be upgrading to.

**Your Pydantic models become the only declaration of your schema.** A model is a Pydantic model carrying `__tablename__`, and there is nothing else to learn. The columns are the annotations you already wrote, and a field with no `| None` gets a not-null constraint derived for it.

```python
from pydantic import BaseModel

class Person(BaseModel):
    __tablename__ = "people"
    id: int
    name: str
    nickname: str | None
```

You already wrote this class to validate your data. Writing the schema a second time, in a migration file or a schema dict, is a second declaration of the same thing, and two declarations drift. This is the only one.

**Migration stops being a command and becomes what opening does.** There is no migrate step, no migration file and no version table. The library asks the process which models it declares, asks the database what it currently holds, works out the difference, and runs it before the open returns.

```python
from declaro_persistum import session_open

db = await session_open("./data.db", shutdown="exit_immediately", primary=None, token=None)
```

That call creates the table from the class above:

```sql
CREATE TABLE "people" ("id" INTEGER NOT NULL, "name" TEXT NOT NULL, "nickname" TEXT)
```

Nothing is remembered between runs, because the database is the record. Open it again after editing the class and you get the difference between what is there and what you now declare.

A change that would destroy data you did not ask to lose stops the open. You ask to lose a column by naming it in `__dropping__`, and a whole table by naming it in `__dropping_tables__`. Everything else refuses, which is why this is the one call in the package that raises: a server handed a fault at startup can ignore it and serve requests against nothing.

**One door takes both questions and writes**, in whichever of four spellings you find familiar.

```python
from declaro_persistum import query_ask

rows = await query_ask(db, person, "pseudo_sql", {"model": "people", "wanted": "all"})
```

A write answers with how many rows it touched. A question that goes wrong comes back as a fault carrying what went wrong and what to do instead, rather than raising. You never close the database: opening installs the shutdown handler, so the platform's signal stops the workers and, on a replicated database, pushes every local commit to the primary first.

## Manifesto & Additional Reading

For the full philosophy (pure functions, explicit types, banana/monkey/jungle, caching, etc.) see [MANIFESTO.md](MANIFESTO.md).

### Further reading

Several essays and blog posts expand on these ideas. You can find them at https://dataos.software:

- [BIG STATE / Declarative Interfaces](https://dataos.software/blog/big-state-declarative-interfaces.html) – why state in libraries is dangerous
- [Classes Broke My Object](https://dataos.software/blog/classes-broke-my-object.html) – the thread that became "Classes Considered Harmful"
- [DATAOS Explained](https://dataos.software/blog/DATAOS-explained.html) – background on the DATAOS project that accompanies this stack

Feel free to copy or republish these posts; they’re a good way to onboard others to the philosophy.

## Contributing

1. `uv sync` to install dependencies.
2. Run the test suite from the root or individual packages via `uv run pytest`.
3. Add new features or fix bugs — keep functions pure and avoid hidden state.
4. Update docs and MANIFESTO.md when you change core principles.

Pull requests are welcome; see `CONTRIBUTING.md` for additional guidelines.

## License

MIT

---

Built by [Adam Zachary Wasserman](https://adamzacharywasserman.com).
