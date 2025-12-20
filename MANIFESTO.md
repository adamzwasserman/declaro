# The Declaro Manifesto

## Core Beliefs

### 1. Data is just data
- Dicts and TypedDicts, not objects with hidden state
- No `__init__` magic, no descriptors, no metaclasses
- If you can't `json.dumps()` it, it's too clever

### 2. Functions transform data
- Pure functions: same input, same output, always
- No mutation of arguments
- No side effects hidden behind method calls

### 3. State corruption is impossible when there is no state
- A class is a petri dish for state corruption
- A dict is just data
- Objects are state wrapped in methods pretending to be data

### 4. Types should be explicit and declared upfront
- No implicit `Any`
- No type inference where clarity matters
- Block declarations over scattered annotations

### 5. Testability is not a feature—it's a consequence of purity
- Pure functions are trivially testable
- No mocks needed for stateless code
- Same input, same output, in CI and on your laptop

### 6. Encapsulation and polymorphism don't require classes
- Closures provide mathematically perfect encapsulation
- Structural typing provides polymorphism without inheritance
- Function composition replaces class hierarchies

### 7. If you need a debugger, your code is too clever

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

One declaration. Schema, types, validation, persistence—all derived from it.

No classes. No magic. Just functions and data.
