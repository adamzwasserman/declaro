# Contributing to Declaro

Thank you for your interest in contributing to Declaro! This document provides guidelines and information for contributors.

## Development Setup

1. Fork the repository
2. Clone your fork: `git clone https://github.com/yourusername/declaro.git`
3. Install dependencies: `uv sync`
4. Run tests: `uv run pytest`

## Development Workflow

1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Make your changes
3. Add tests for new functionality
4. Ensure all tests pass: `uv run pytest`
5. Run type checks: `uv run mypy`
6. Commit your changes with a descriptive message
7. Push to your fork
8. Create a Pull Request

## Code Style

- **Pure functions only** — no classes, no mutable state
- **Data is data** — use TypedDicts, not objects with methods
- **Explicit types** — all function signatures must be typed
- **Declarative interfaces** — users declare intent, library handles implementation
- Keep functions small and focused
- Use meaningful variable names

## Testing

Declaro uses BDD and unit testing:

- **pytest-bdd** - Human-readable feature specifications
- **pytest** - Unit tests for pure functions

```bash
# Run all tests
uv run pytest

# Run unit tests only
uv run pytest tests/unit/

# Run BDD tests
uv run pytest tests/bdd/

# Run with coverage
uv run pytest --cov
```

### Test Guidelines

- Add unit tests for all new functionality
- Write BDD scenarios for user-facing features
- Ensure existing tests still pass
- Test edge cases and error conditions
- Use descriptive test names
- Pure functions are trivially testable: `assert f(input) == expected`

## Pull Request Guidelines

- Provide a clear description of the changes
- Reference any related issues
- Ensure CI checks pass
- Follow the existing architecture patterns
- Keep PRs focused — one feature or fix per PR

## Architecture Principles

When contributing, please follow these core principles:

1. **No Classes** — TypedDicts and pure functions only
2. **No Hidden State** — if you can't `json.dumps()` it, it's too clever
3. **Declarative Interfaces** — users say WHAT, library figures out HOW
4. **Imperative Internals** — implementation details stay inside the library
5. **TOML as Source of Truth** — schemas declared in TOML, everything derived

## Package Structure

Declaro is a monorepo with multiple packages:

| Package | Purpose |
|---------|---------|
| `declaro-persistum` | Schema-first database toolkit |
| `declaro-ximenez` | Type enforcement with memorable errors |
| `declaro-api` | FastAPI integration for functional Python |
| `declaro-http` | Functional HTTP client |

Each package lives in `packages/<package-name>/`.

## Code of Conduct

Please follow our [Code of Conduct](CODE_OF_CONDUCT.md) in all interactions.

## Questions?

Feel free to open an issue for questions or discussions.

---

*Declaro ergo fit.*
