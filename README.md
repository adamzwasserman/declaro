# Declaro

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
|---------|-------------|--------|
| `declaro-persistum` | Schema-first database toolkit | In development |
| `declaro-ximinez` | Type enforcement with memorable errors | Planned |
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

## Manifesto

Read our [MANIFESTO.md](MANIFESTO.md) for the full philosophy.

## License

MIT
