# Python Coding Standards for react2htmx

**Document Version**: 1.0
**Date**: 2025-12-13
**Status**: MANDATORY FOR ALL PYTHON DEVELOPMENT
**Scope**: react2htmx CLI Tool Implementation

---

---
**IMPLEMENTATION STATUS**: PLANNED
**LAST VERIFIED**: 2025-12-13
**IMPLEMENTATION EVIDENCE**: Not yet implemented - new project
---

## Philosophy: Clarity and Correctness

This document establishes coding standards for the react2htmx CLI tool. The tool converts Figma Make React applications into Buckler-compatible HTMX/FastAPI modules. Code must be clear, testable, and maintainable.

**Core Principle**: Favor explicit over implicit. Every function should have clear inputs, outputs, and no hidden side effects.

---

## 1. Function-First Architecture

### 1.1 Pure Functions as Default

**MANDATORY PATTERNS**:
```python
# Preferred: Pure function with explicit dependencies
def extract_element_styles(
    element: ElementCapture,
    viewport_sizes: tuple[int, ...],
) -> dict[str, ComputedStyles]:
    """Extract computed styles for an element across viewports.

    Args:
        element: Captured DOM element data
        viewport_sizes: Tuple of viewport widths to analyze

    Returns:
        Mapping of viewport size to computed styles
    """
    return {
        str(size): compute_styles_for_viewport(element, size)
        for size in viewport_sizes
    }


# Preferred: Function composition
def process_capture_pipeline(
    url: str,
    config: CaptureConfig,
) -> CaptureResult:
    """Process a full capture pipeline."""
    dom_tree = capture_dom(url, config)
    styles = extract_all_styles(dom_tree, config.viewports)
    interactions = capture_interactions(dom_tree, config.interaction_states)

    return CaptureResult(
        dom=dom_tree,
        styles=styles,
        interactions=interactions,
    )
```

**FORBIDDEN PATTERNS**:
```python
# NEVER - Hidden state mutation
class BadStyleExtractor:
    def __init__(self):
        self._cache = {}  # Hidden state

    def extract(self, element):
        # Mutates internal state unexpectedly
        self._cache[element.id] = element
        return self._process(element)


# NEVER - Global state
_global_config = {}  # Module-level mutable state

def bad_capture(url):
    # Relies on global state
    return process(url, _global_config)
```

### 1.2 When Classes Are Appropriate

**APPROVED CLASS USAGE**:

1. **Pydantic Models** - Data validation and serialization
```python
from pydantic import BaseModel, Field

class ElementCapture(BaseModel):
    """Captured DOM element with metadata."""
    id: str = Field(..., description="Unique element identifier")
    tag: str = Field(..., description="HTML tag name")
    attributes: dict[str, str] = Field(default_factory=dict)
    computed_styles: dict[str, str] = Field(default_factory=dict)
    children: list["ElementCapture"] = Field(default_factory=list)

    model_config = {"frozen": True}  # Immutable by default
```

2. **Context Managers** - Resource management
```python
from contextlib import contextmanager
from playwright.sync_api import sync_playwright

@contextmanager
def browser_context(headless: bool = True):
    """Managed browser context for captures."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        try:
            yield context
        finally:
            context.close()
            browser.close()
```

3. **Protocols** - Interface definitions
```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class StyleExtractor(Protocol):
    """Protocol for style extraction implementations."""

    def extract(
        self,
        element: ElementCapture,
        viewport: int,
    ) -> dict[str, str]:
        """Extract styles for an element at a viewport size."""
        ...
```

4. **Click Commands** - CLI structure (required by framework)
```python
import click

@click.group()
def cli():
    """react2htmx - Convert React to HTMX."""
    pass

@cli.command()
@click.argument("url")
@click.option("--output", "-o", type=click.Path())
def capture(url: str, output: str | None):
    """Capture DOM and styles from a React application."""
    result = run_capture(url)
    save_capture(result, output)
```

---

## 2. Type Annotations

### 2.1 Mandatory Type Hints

**ALL functions must have complete type annotations**:

```python
from typing import TypeVar, Callable, Sequence
from collections.abc import Iterator

T = TypeVar("T")
U = TypeVar("U")


def map_elements(
    elements: Sequence[ElementCapture],
    transform: Callable[[ElementCapture], T],
) -> list[T]:
    """Apply transform to all elements."""
    return [transform(el) for el in elements]


def traverse_dom(root: ElementCapture) -> Iterator[ElementCapture]:
    """Depth-first traversal of DOM tree."""
    yield root
    for child in root.children:
        yield from traverse_dom(child)
```

### 2.2 Use Modern Type Syntax

```python
# Preferred: Python 3.10+ syntax
def process(
    items: list[str],
    config: dict[str, Any] | None = None,
) -> tuple[int, list[str]]:
    ...

# Avoid: Old typing module syntax
from typing import List, Dict, Optional, Tuple

def process(
    items: List[str],
    config: Optional[Dict[str, Any]] = None,
) -> Tuple[int, List[str]]:
    ...
```

---

## 3. Error Handling

### 3.1 Custom Exception Hierarchy

```python
class React2HtmxError(Exception):
    """Base exception for react2htmx."""

    def __init__(self, message: str, context: dict[str, Any] | None = None):
        super().__init__(message)
        self.context = context or {}


class CaptureError(React2HtmxError):
    """Error during DOM/style capture."""
    pass


class StyleExtractionError(React2HtmxError):
    """Error extracting or processing styles."""
    pass


class LLMAnalysisError(React2HtmxError):
    """Error during LLM behavioral analysis."""
    pass


class CodeGenerationError(React2HtmxError):
    """Error generating output code."""
    pass
```

### 3.2 Result Type Pattern

For operations that may fail predictably, use a Result type:

```python
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")
E = TypeVar("E", bound=Exception)


@dataclass(frozen=True)
class Ok(Generic[T]):
    """Successful result."""
    value: T

    def is_ok(self) -> bool:
        return True

    def is_err(self) -> bool:
        return False


@dataclass(frozen=True)
class Err(Generic[E]):
    """Error result."""
    error: E

    def is_ok(self) -> bool:
        return False

    def is_err(self) -> bool:
        return True


Result = Ok[T] | Err[E]


# Usage
def parse_react_component(source: str) -> Result[ComponentAST, ParseError]:
    """Parse React component source code."""
    try:
        ast = do_parse(source)
        return Ok(ast)
    except SyntaxError as e:
        return Err(ParseError(str(e), {"source": source[:100]}))
```

---

## 4. Immutability and Data Structures

### 4.1 Prefer Immutable Structures

```python
from dataclasses import dataclass
from typing import FrozenSet

# Preferred: Frozen dataclass
@dataclass(frozen=True)
class StyleRule:
    """Immutable CSS rule."""
    selector: str
    properties: tuple[tuple[str, str], ...]  # Immutable

    def with_property(self, name: str, value: str) -> "StyleRule":
        """Return new rule with added property."""
        return StyleRule(
            selector=self.selector,
            properties=self.properties + ((name, value),),
        )


# Preferred: Use frozenset for sets
def find_common_styles(
    elements: Sequence[ElementCapture],
) -> FrozenSet[str]:
    """Find style properties common to all elements."""
    if not elements:
        return frozenset()

    common = set(elements[0].computed_styles.keys())
    for element in elements[1:]:
        common &= set(element.computed_styles.keys())

    return frozenset(common)
```

### 4.2 Defensive Copying

```python
def process_elements(
    elements: list[ElementCapture],
) -> list[ProcessedElement]:
    """Process elements without modifying input."""
    # Work with a copy if modification is needed internally
    working_copy = list(elements)

    # Never modify the input
    return [transform(el) for el in working_copy]
```

---

## 5. Async Patterns

### 5.1 Playwright Operations

```python
import asyncio
from playwright.async_api import async_playwright, Page


async def capture_viewport(
    page: Page,
    viewport_width: int,
) -> ViewportCapture:
    """Capture DOM state at a specific viewport."""
    await page.set_viewport_size({"width": viewport_width, "height": 800})
    await page.wait_for_load_state("networkidle")

    dom = await page.evaluate("() => document.documentElement.outerHTML")
    styles = await extract_computed_styles(page)

    return ViewportCapture(width=viewport_width, dom=dom, styles=styles)


async def capture_all_viewports(
    url: str,
    viewports: tuple[int, ...] = (320, 768, 1024, 1440),
) -> list[ViewportCapture]:
    """Capture at multiple viewports concurrently."""
    async with async_playwright() as p:
        browser = await p.chromium.launch()

        # Create separate contexts for parallel capture
        async def capture_one(viewport: int) -> ViewportCapture:
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto(url)
            result = await capture_viewport(page, viewport)
            await context.close()
            return result

        results = await asyncio.gather(
            *[capture_one(vp) for vp in viewports]
        )

        await browser.close()
        return list(results)
```

### 5.2 LLM API Calls

```python
from anthropic import AsyncAnthropic


async def analyze_behavior(
    tsx_source: str,
    dom_capture: str,
    client: AsyncAnthropic,
) -> BehaviorAnalysis:
    """Analyze React behavior using LLM."""
    response = await client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": build_analysis_prompt(tsx_source, dom_capture),
            }
        ],
    )

    return parse_analysis_response(response.content[0].text)
```

---

## 6. Configuration Management

### 6.1 Configuration as Data

```python
from pydantic import BaseModel, Field
from pathlib import Path


class CaptureConfig(BaseModel):
    """Configuration for capture stage."""
    viewports: tuple[int, ...] = (320, 768, 1024, 1440)
    interaction_states: tuple[str, ...] = ("hover", "focus", "active", "disabled")
    wait_for_idle: bool = True
    timeout_ms: int = 30000

    model_config = {"frozen": True}


class LLMConfig(BaseModel):
    """Configuration for LLM analysis."""
    provider: str = "claude"
    model: str = "claude-sonnet-4-20250514"
    max_tokens: int = 4096
    temperature: float = 0.0

    model_config = {"frozen": True}


class React2HtmxConfig(BaseModel):
    """Root configuration."""
    capture: CaptureConfig = Field(default_factory=CaptureConfig)
    llm: LLMConfig = Field(default_factory=LLMConfig)
    output_dir: Path = Path("./output")

    @classmethod
    def from_file(cls, path: Path) -> "React2HtmxConfig":
        """Load configuration from YAML/JSON file."""
        import yaml

        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.model_validate(data)
```

---

## 7. Testing Requirements

### 7.1 Test Organization

```
tests/
├── unit/
│   ├── capture/
│   │   ├── test_dom_extraction.py
│   │   └── test_style_capture.py
│   ├── extract/
│   │   ├── test_style_manifest.py
│   │   └── test_css_generation.py
│   └── generate/
│       ├── test_route_generation.py
│       └── test_template_generation.py
├── integration/
│   ├── test_capture_pipeline.py
│   └── test_full_conversion.py
├── features/
│   ├── capture.feature
│   └── conversion.feature
└── conftest.py
```

### 7.2 Test Patterns

```python
import pytest
from react2htmx.capture.styles import extract_element_styles


class TestExtractElementStyles:
    """Tests for style extraction."""

    def test_extracts_styles_for_single_viewport(
        self,
        sample_element: ElementCapture,
    ):
        """Should extract styles for a single viewport."""
        result = extract_element_styles(sample_element, (1024,))

        assert "1024" in result
        assert isinstance(result["1024"], dict)

    def test_extracts_styles_for_multiple_viewports(
        self,
        sample_element: ElementCapture,
    ):
        """Should extract styles for all specified viewports."""
        viewports = (320, 768, 1024)
        result = extract_element_styles(sample_element, viewports)

        assert len(result) == 3
        assert all(str(vp) in result for vp in viewports)

    def test_handles_empty_viewports(
        self,
        sample_element: ElementCapture,
    ):
        """Should return empty dict for no viewports."""
        result = extract_element_styles(sample_element, ())

        assert result == {}


@pytest.fixture
def sample_element() -> ElementCapture:
    """Provide a sample element for testing."""
    return ElementCapture(
        id="el-001",
        tag="button",
        attributes={"class": "btn-primary"},
        computed_styles={"background-color": "blue", "color": "white"},
    )
```

---

## 8. Code Organization

### 8.1 Module Structure

```
react2htmx/
├── __init__.py
├── cli.py                    # Click CLI entry point
├── config.py                 # Configuration models
├── capture/
│   ├── __init__.py
│   ├── browser.py            # Playwright orchestration
│   ├── dom.py                # DOM tree extraction
│   └── styles.py             # Computed style capture
├── extract/
│   ├── __init__.py
│   ├── manifest.py           # Inverted index builder
│   ├── css.py                # Vanilla CSS output
│   └── naming.py             # Class name derivation
├── analyze/
│   ├── __init__.py
│   ├── functional_units.py   # React pattern detection
│   ├── gherkin.py            # Feature file generation
│   └── llm.py                # LLM API wrapper
├── generate/
│   ├── __init__.py
│   ├── domain.py             # Pydantic model generation
│   ├── services.py           # Service stub generation
│   ├── routes.py             # FastAPI route generation
│   └── templates.py          # Jinja2 template generation
└── models/
    ├── __init__.py
    ├── capture.py            # Capture-related models
    ├── analysis.py           # Analysis-related models
    └── output.py             # Output-related models
```

### 8.2 File Size Guidelines

- **Target**: 300-500 lines per file
- **Maximum**: 700 lines (requires justification)
- **Split criteria**: Logical domain boundaries, not arbitrary line counts

---

## 9. Documentation

### 9.1 Docstring Format

```python
def generate_css_class(
    style_hash: str,
    element_ids: frozenset[str],
    naming_context: NamingContext,
) -> CSSClass:
    """Generate a CSS class from shared styles.

    Derives a meaningful class name based on the naming context and
    generates CSS properties from the style hash.

    Args:
        style_hash: Hash of the canonical style representation
        element_ids: Set of element IDs sharing this style
        naming_context: Context for deriving class names (Figma data, etc.)

    Returns:
        CSSClass with derived name and properties

    Raises:
        StyleExtractionError: If style hash is invalid

    Example:
        >>> ctx = NamingContext(component_name="LoginForm", prop_context="submit")
        >>> css = generate_css_class("abc123", frozenset(["el-001"]), ctx)
        >>> css.name
        'login-form__submit-button'
    """
```

---

## 10. Forbidden Patterns

### 10.1 Anti-Patterns That Result in Code Rejection

```python
# FORBIDDEN - Mutable default arguments
def bad_func(items: list = []):  # NEVER
    items.append("x")
    return items


# FORBIDDEN - Star imports
from playwright.sync_api import *  # NEVER


# FORBIDDEN - Bare except
try:
    risky_operation()
except:  # NEVER - catches KeyboardInterrupt, SystemExit
    pass


# FORBIDDEN - Type: ignore without explanation
result = sketchy_function()  # type: ignore


# FORBIDDEN - Global mutable state
_cache = {}  # Module-level mutable dict

def get_cached(key):
    return _cache.get(key)


# FORBIDDEN - Print for logging
def process(data):
    print(f"Processing {data}")  # Use logging module


# FORBIDDEN - String formatting with %
message = "Error: %s" % error  # Use f-strings or .format()
```

---

## 11. Code Quality Enforcement

### 11.1 Required Tools

```toml
# pyproject.toml
[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = [
    "E",    # pycodestyle errors
    "W",    # pycodestyle warnings
    "F",    # pyflakes
    "I",    # isort
    "B",    # flake8-bugbear
    "C4",   # flake8-comprehensions
    "UP",   # pyupgrade
    "ARG",  # flake8-unused-arguments
    "SIM",  # flake8-simplify
]

[tool.mypy]
python_version = "3.11"
strict = true
warn_return_any = true
warn_unused_ignores = true

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-v --cov=react2htmx --cov-report=term-missing"
```

### 11.2 Pre-Commit Hooks

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.1.6
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.7.1
    hooks:
      - id: mypy
        additional_dependencies: [pydantic, click]
```

### 11.3 Mandatory Code Review Checklist

- [ ] All functions have complete type annotations
- [ ] All public functions have docstrings
- [ ] No mutable default arguments
- [ ] No bare except clauses
- [ ] No star imports
- [ ] No global mutable state
- [ ] Tests cover happy path and error cases
- [ ] Async code uses proper patterns
- [ ] Configuration is immutable (frozen Pydantic models)

---

## 12. Conclusion

These standards ensure the react2htmx CLI tool is:

- **Maintainable**: Clear, typed, well-documented code
- **Testable**: Pure functions with explicit dependencies
- **Reliable**: Proper error handling and immutable data structures
- **Consistent**: Enforced through tooling and pre-commit hooks

Follow these patterns to produce code that other developers can understand, extend, and trust.
