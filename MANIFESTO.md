# The Declaro Manifesto

> **Keep YOUR state out of MY code.**

## The Enemy: Big State

Every stateful library is a Trojan horse. You import a function, but you inherit state you didn't ask for—connection pools, caches, registries, singletons. The library's internal state becomes your problem. Its bugs become your bugs. Its assumptions become your constraints.

**Big State** is the pattern of libraries that export their internal state management to users. It's ORMs with "attached" and "detached" objects. It's connection managers with hidden retry logic. It's cache invalidation as someone else's policy applied to your data.

Declaro exists because we're tired of debugging other people's state.

## The Lie of Object-Oriented Programming

Java didn't invent OOP. It bastardized it.

Smalltalk's vision was message-passing between autonomous agents. What we got was **Class-Oriented Programming (COP)**—a taxonomy of nouns with methods bolted on. COP gives you:

- **Inheritance hierarchies** that become prisons
- **Encapsulation theater** (private members accessible via `_underscore_convention`)
- **State corruption** because every object is a petri dish for mutation

A class is not an abstraction. It's a liability. Every method is a potential mutation site. Every instance is state you now own.

## Pure Functions: The Original Abstraction

Pure functions provide everything OOP promised and never delivered:

### Encapsulation
- Classes: `user._email = "hacked"` works—"private" is just a naming convention
- Closures: Mathematically perfect encapsulation. Variables are truly inaccessible.

### Polymorphism
- Classes: Require interface inheritance, abstract base classes, ceremony
- Functions: Structural compatibility. If the types match, it works. Duck typing without the quacking.

### Reuse
- Classes: Rigid inheritance trees, the fragile base class problem, favor-composition-over-inheritance articles written for 30 years
- Functions: Compose. Chain. Pipe. No inheritance needed.

### Testability
- Classes: Mocks, fixtures, setup/teardown, test databases, dependency injection frameworks
- Pure functions: `assert f(input) == expected_output`. Done.

## Data is Just Data

```python
# Not this (state wrapped in methods pretending to be data)
class User(BaseModel):
    email: str

    @validator("email")
    def validate_email(cls, v):
        ...

# This (data is data, functions transform it)
User = TypedDict("User", {"email": str})

def validate_user(user: dict) -> list[Error]:
    return check_email(user.get("email", ""))
```

If you can't `json.dumps()` it, it's too clever.

## Failure Recovery Over Failure Avoidance

The Java philosophy: Prevent failure at all costs. Checked exceptions. Defensive programming. Wrap everything in try-catch.

The Erlang philosophy: Let it crash. Recover fast. Supervision trees. The system heals itself.

Declaro follows Erlang. We don't try to prevent every possible error. We make state so minimal and functions so pure that recovery is trivial. When something breaks:

1. You know exactly what input caused it
2. You can reproduce it instantly
3. You fix the function
4. No state to clean up, no caches to invalidate, no sessions to reconnect

## The Performance Lie

"But objects are faster because they cache state!"

Every cache is a bug waiting to happen. Every memoized result is a stale answer in disguise. The performance argument for OOP is actually an argument against it:

- Cache invalidation is one of the two hard problems in computer science
- Most "performance" code is premature optimization
- A cache miss in production is worse than no cache at all
- Stateless functions are embarrassingly parallelizable

If you need performance, measure first. Then optimize the hot path. Don't scatter state across your entire codebase for a cache hit you might never need.

## Declarative Interfaces, Imperative Internals

Imperative code is inevitable. At some point, the CPU executes instructions. The database runs queries. Bytes hit the network. **Imperative code is not the enemy.**

**Imperative interfaces are the enemy.**

When a library forces you to orchestrate its operations—manage its connections, call its methods in the right order, handle its state transitions—it has failed at being a library. It has exported its implementation to you. That's not encapsulation. That's Big State wearing a trench coat.

A **true library** is a black box with a declarative interface:
- You state WHAT you want
- It figures out HOW to do it
- You never touch its internals

```python
# Imperative interface - YOU manage the library's concerns
session = Session()
try:
    user = session.query(User).filter_by(id=1).first()
    user.name = "new"
    session.commit()
except:
    session.rollback()
finally:
    session.close()

# Declarative interface - you state intent, library handles the rest
user = await query.select("users").where(id=1).one()
await query.update("users").where(id=1).set(name="new")
```

The imperative code still exists—inside the library. But the interface is declarative. You declare what you want. The library delivers.

```toml
# Schema declaration - the ultimate declarative interface
[user]
table = "users"

[user.fields]
id = { type = "uuid" }
email = { type = "str", validate = ["email"] }
```

TOML defines the schema. The system derives:
- TypedDicts for Python typing
- Validation functions
- Database migrations
- OpenAPI specifications
- Query builders

One declaration. Everything else follows.

## The Stack

| Package | Purpose |
|---------|---------|
| `declaro-persistum` | Schema-first database toolkit |
| `declaro-ximenez` | Type enforcement with memorable errors |
| `declaro-api` | FastAPI integration for functional Python |

## The Spell

```
DECLARO = "I declare"
```

You declare your intent. We handle the rest.

No classes. No hidden state. No magic.

Just functions and data.

---

*"If you need a debugger, your code is too clever."*
