# The Declaro Manifesto

> **Keep YOUR state out of MY code.**

## The Enemy: Big State

Every stateful library is a Trojan horse. You import a function, but you inherit state you didn't ask for—connection pools, caches, registries, singletons. The library's internal state becomes your problem. Its bugs become your bugs. Its assumptions become your constraints. This is sometime called the Banana, Monkey, Jungle problem. All you want is the banana, but to get it, you have to import the monkey that is holding it, and the jungle that the monkey is in. This is a fundamental broken model.

**Big State** is the pattern of libraries that export their internal state management to users. It's ORMs with "attached" and "detached" objects. It's connection managers with hidden retry logic. It's cache invalidation as someone else's policy applied to your data.

Declaro exists because we're tired of debugging other people's state.

## The Lie of Object-Oriented Programming

Java didn't invent OOP. It bastardized it.

Java was originally based on small talk. But Smalltalk's main vision was message-passing between autonomous agents. Java itself, is a small compact language, that is actually pretty nice. But Java has come to mean notably the language primitives, but enormous class libraries like J2EE. So in the end, what we got with Java was **Class-Oriented Programming (COP)**—a taxonomy of nouns with methods bolted on. COP gives you:

- **Inheritance hierarchies** that become prisons
- **Encapsulation theater** (private members accessible via `_underscore_convention`)
- **State corruption** because every object is a petri dish for mutation

A class is not an abstraction. It's a liability. Every method is a potential mutation site. Every instance is state you now own.

### Classes Considered Harmful

Performance and quality are the two reasons the argument against classes keeps resurfacing.

If you look at a typical object graph – customers holding orders holding line items – you are scattering pointers all over the heap. Your CPU's cache lines (64 bytes at a time) are being ignored; each `new MyClass()` is a cache miss waiting to happen. Replace that structure with three flat arrays and the same algorithm often runs tens of times faster. Real‑world benchmarks show a 50× speedup merely by keeping related data contiguous rather than chasing object references.

Beyond speed, every class you write adds a “defect factory”. Ten methods touching five fields mean combinatorial state: which methods must run in which order, which fields are valid together, what happens when two threads invoke them concurrently? Making a class thread‑safe is not a matter of sprinkling locks around like pixie dust; you end up with lock ordering protocols, documentation about which mutex to hold, and servers waiting on deadlocks. Pure functions operating on immutable data never deadlock.

That is not to say classes are never ever the correct tool in the toolbox. File handles, network connections, game entities: those are inherently stateful and a small, explicit class makes sense. But for the vast majority of application code you don’t need a class at all. Functions are faster, simpler, easier to test (much easier to test), and they tell the truth about what they do: input in, output out. Classes, by contrast, lie about their internal state, and they lie loudly when you try to reason about them.

If you ever find yourself reaching for a `Customer` class just to hold some attributes and maybe a helper method, stop and ask whether a `TypedDict` plus a couple of pure functions would do the job. The patterns you admire—factories, visitors, decorators—are really just functions in disguise. Take off the OOP costume and the code becomes honest and maintainable.

## Pure Functions: The Original Abstraction

Pure functions provide everything OOP promised and never delivered:

### Encapsulation
- Classes: `user._email = "hacked"` works—"private" is just a naming convention
- Functions (Closures): Mathematically perfect encapsulation. Variables are truly inaccessible.

### Polymorphism
- Classes: Require interface inheritance, abstract base classes, ceremony
- Functions: A function with early returns *is* polymorphism. An `if` that returns handles one case; code that passes through handles the next. No class hierarchy, no dispatch table: just a function that tests conditions and lets execution fall through to the general case. This is the same pattern as pattern matching, guard clauses, and multimethod dispatch, but without the abstraction tax. If the types match, it works. Duck typing without the quacking.

### Reuse
- Classes: Rigid inheritance trees, the fragile base class problem, favor-composition-over-inheritance articles written for 30 years on the problems.
- Functions: Compose. Chain. Pipe. No inheritance needed. In languages where functions are first class, just pass them in. Composition is reuse done right; inheritance is reuse done wrong

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

If you can't `json.dumps()` it, it's too clever by half.

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
- Objects are sprinkled across the heap like garbage spilling from a truck on the freeway. Constant cache misses, poor locality, and huge performance penalties in real workloads 

Stateless functions are embarrassingly parallelizable, allocate linearly or on the stack, and stay hot in cache.


## Bananas, Monkeys, and Jungles

The banana-monkey-jungle problem is simple and brutal:  You only want the banana.
But in most systems—especially big OOP codebases or heavyweight libraries—to get the banana you have to take the monkey that's holding it... and the entire jungle the monkey lives in.The banana is the piece of data or functionality you actually care about.

The monkey is the object/class/package forced to own, guard, and manage that state or logic.
The jungle is the undifferentiated mess of transitive dependencies, shared state, caches, pools, registries, lifecycle hooks, and implicit couplings that come along for the ride, and that will eventually fight over, corrupt, leak, or lose your banana.

Declaro solves this at both levels:

- Libraries never hoard bananas (state belongs to the caller, never to declaro packages).  
- Declaro itself is a monorepo of completely standalone packages. Each one is a pure, self-sufficient banana — install and use just the one you need. No forced monkeys, no surprise jungles of transitive dependencies or shared state managers.

Grab one package for connection pooling? That's all you get. No pulling in the entire declaro ecosystem.
No package ever forces you to take the whole jungle just to get one banana.

This design keeps your dependency tree lean, your builds fast, your mental model clear, and your code free of the hidden coupling that turns "simple libraries" into unmanageable vendor lock-in.

## Caching is Policy, Not Plumbing

This leads directly into our stance on caching. You will find a handful of caches inside declaro‑persistum—connection pools, schema caches, prepared‑statement caches—but they are *infrastructure caches*. They make the polymorphic façade cheap and eliminate obvious duplicate work. They never change the observable semantics of a query.

**Any other caching belongs upstream, where the domain is understood.**

A generic result cache bolted onto the ORM is a recipe for stale data and bewildered users. If you need a widget layout cached for five seconds, or a time‑series aggregated daily, that logic belongs in a sibling package (tablix, for example) or in your application layer. Those components can implement use‑case‑specific strategies such as LRU, windowing, event–sourced invalidation, or Redis‑backed memoisation without polluting the persistence façade with opinionated policy.

This separation mirrors the “banana, monkey, jungle” rule: you choose the banana, you choose the monkey, and you decide how the jungle behaves. Declaro gives you the clean jungle floor; the rest is your business.

If you need performance, measure first. Then optimize the hot path. Don't scatter state across your entire codebase for a cache hit you might never need.

## Declarative Interfaces, Imperative Internals

Imperative code is inevitable. At some point, the CPU executes instructions. The database runs queries. Bytes hit the network. **Imperative code is not the enemy.**

**Imperative interfaces are the enemy.**

When a library forces you to orchestrate its operations—manage its connections, call its methods in the right order, handle its state transitions—it has failed at being a library. It has exported its implementation to you, that's not encapsulation. Big State is coming for you again.

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

The imperative code still exists. Inside the library. But the interface is declarative. You declare what you want. The library delivers.

```python
# Schema declaration - the ultimate declarative interface
from pydantic import BaseModel
from declaro_persistum import table, field

@table("users")
class User(BaseModel):
    id: UUID = field(primary=True)
    email: str = field(unique=True, validate=["email"])
```

Pydantic defines the schema. The system derives:
- Validation functions
- Database migrations
- OpenAPI specifications
- Query builders

One declaration. Everything else follows.

## The Stack

| Package | Purpose |
|---------|---------|
| `declaro-persistum` | Schema-first database toolkit |
| `declaro-ximinez` | Type enforcement with memorable errors |
| `declaro-observe` | Event sourcing observability |
| `declaro-api` | FastAPI integration for functional Python |

## The Spell

```
DECLARO = "I declare"
```

You declare your intent. We handle the rest.

No classes. No hidden state. No magic.

Just functions and data.

---

*Declaro ergo fit.*

"I declare, therefore it becomes."
