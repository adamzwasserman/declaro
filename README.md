# Declaro

> **Note:** This repository is public but not yet released. The APIs are unstable and packages are under active development. If you're looking for production-ready software, check back later.

**The Functional Python Stack**

> Pure functions. Typed data. No class magic.

## Vision

Declaro is a collection of tools for developers who believe:

- **Data is just data** - Dicts and TypedDicts, not objects with hidden state
- **Functions transform data** - Pure functions with no side effects
- **Types should be explicit** - Declared upfront, enforced everywhere
- **Testing should be trivial** - Same input, same output, always

## Packages

| Package | Description | Status |
| ------- | ----------- | ------ |
| `declaro-persistum` | Schema-first database toolkit | In development |
| `declaro-ximinez` | Type enforcement with memorable errors | In development |
| `declaro-observe` | Event sourcing observability | Pre-alpha |
| `declaro-api` | FastAPI integration | Planned |

## Installation

```bash
# Install everything
pip install declaro[all]

# Or pick what you need
pip install declaro-persistum
pip install declaro-ximinez
pip install declaro-api
```

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

Use `declaro-persistum` below as an example; the other packages follow the same pattern.

```bash
# install the core persistence toolkit
pip install declaro-persistum[all]
```

A longer quick‑start walkthrough lives in [docs/quickstart.md](docs/quickstart.md).

```python
from uuid import uuid4
from declaro_persistum import ConnectionPool
from declaro_persistum.query import table

# create an SQLite pool (later you can switch to Postgres/Turso without changing this code)
pool = await ConnectionPool.sqlite("./data.db")

users = table("users")

async with pool.acquire() as conn:
    # make sure your schema exists
    await conn.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, name TEXT)")
    await conn.commit()

    # simple insert
    await users.insert(id=str(uuid4()), name="alice").execute(conn)
    # select rows
    rows = await users.select().execute(conn)
    print(rows)

await pool.close()
```

you can find a more complete quick start in `packages/declaro-persistum/README.md`.

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
