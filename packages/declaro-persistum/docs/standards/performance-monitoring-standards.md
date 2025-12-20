# Performance Monitoring Standards

**Document Type**: Standards & Guidelines
**Date**: 2025-12-13
**Version**: 1.0
**Status**: ACTIVE
**Owner**: Engineering Team

---

---
**IMPLEMENTATION STATUS**: PLANNED
**LAST VERIFIED**: 2025-12-13
**IMPLEMENTATION EVIDENCE**: Not yet implemented - new project
---

## Executive Summary

This document establishes performance monitoring standards for the react2htmx CLI tool. Performance is measured across four dimensions: CLI execution time, Playwright capture efficiency, LLM API usage, and generated code quality.

### Core Principles

1. **Total Conversion Time**: Full pipeline should complete in under 5 minutes for typical apps
2. **LLM Cost Efficiency**: Minimize token usage while maintaining analysis quality
3. **Memory Efficiency**: Handle large React apps without excessive memory consumption
4. **Generated Code Quality**: Output must meet Buckler production standards

---

## Performance Budgets

### CLI Execution Time Targets

| Stage | Target | Warning | Critical |
|-------|--------|---------|----------|
| **Capture** (4 viewports) | < 30s | 30-60s | > 60s |
| **Style Extraction** | < 10s | 10-20s | > 20s |
| **LLM Analysis** | < 60s | 60-120s | > 120s |
| **Code Generation** | < 5s | 5-15s | > 15s |
| **Total Pipeline** | < 2min | 2-5min | > 5min |

### LLM Token Usage Targets

| Analysis Type | Input Tokens | Output Tokens | Total Cost Target |
|---------------|--------------|---------------|-------------------|
| Behavioral extraction | < 10,000 | < 4,000 | < $0.15 |
| Gherkin generation | < 5,000 | < 2,000 | < $0.07 |
| Class naming | < 3,000 | < 1,000 | < $0.04 |
| **Total per conversion** | < 20,000 | < 8,000 | < $0.30 |

### Memory Usage Targets

| Operation | Target | Warning | Critical |
|-----------|--------|---------|----------|
| Playwright capture | < 500MB | 500MB-1GB | > 1GB |
| DOM tree in memory | < 100MB | 100-200MB | > 200MB |
| Style manifest | < 50MB | 50-100MB | > 100MB |
| Peak total | < 1GB | 1-2GB | > 2GB |

---

## Stage 1: Capture Performance

### Playwright Optimization

```python
# Performance-optimized capture configuration
CAPTURE_CONFIG = {
    "viewports": (320, 768, 1024, 1440),
    "wait_strategy": "networkidle",  # NOT "load" - too slow
    "timeout_ms": 30000,
    "parallel_viewports": True,  # Capture viewports concurrently
    "skip_offscreen": True,  # Don't capture hidden elements
}
```

### Capture Timing Instrumentation

```python
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class CaptureMetrics:
    """Metrics for capture stage performance."""
    url: str
    viewport_count: int
    element_count: int
    navigation_time_ms: float
    hydration_wait_ms: float
    dom_extraction_ms: float
    style_extraction_ms: float
    total_capture_ms: float

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "viewport_count": self.viewport_count,
            "element_count": self.element_count,
            "navigation_time_ms": round(self.navigation_time_ms, 2),
            "hydration_wait_ms": round(self.hydration_wait_ms, 2),
            "dom_extraction_ms": round(self.dom_extraction_ms, 2),
            "style_extraction_ms": round(self.style_extraction_ms, 2),
            "total_capture_ms": round(self.total_capture_ms, 2),
        }


def capture_with_metrics(url: str, config: CaptureConfig) -> tuple[CaptureResult, CaptureMetrics]:
    """Capture with performance instrumentation."""
    start_total = time.perf_counter()

    start_nav = time.perf_counter()
    page = navigate_to_url(url)
    navigation_time = (time.perf_counter() - start_nav) * 1000

    start_hydration = time.perf_counter()
    wait_for_hydration(page)
    hydration_time = (time.perf_counter() - start_hydration) * 1000

    start_dom = time.perf_counter()
    dom_tree = extract_dom(page)
    dom_time = (time.perf_counter() - start_dom) * 1000

    start_styles = time.perf_counter()
    styles = extract_styles(page, config.viewports)
    style_time = (time.perf_counter() - start_styles) * 1000

    total_time = (time.perf_counter() - start_total) * 1000

    metrics = CaptureMetrics(
        url=url,
        viewport_count=len(config.viewports),
        element_count=count_elements(dom_tree),
        navigation_time_ms=navigation_time,
        hydration_wait_ms=hydration_time,
        dom_extraction_ms=dom_time,
        style_extraction_ms=style_time,
        total_capture_ms=total_time,
    )

    return CaptureResult(dom=dom_tree, styles=styles), metrics
```

### Capture Performance Checklist

- [ ] Parallel viewport capture enabled
- [ ] Network idle wait (not full load)
- [ ] Unnecessary elements skipped (ads, analytics)
- [ ] Browser context reused across viewports
- [ ] Screenshot capture disabled unless needed

---

## Stage 2: Style Extraction Performance

### Inverted Index Optimization

```python
from collections import defaultdict
from hashlib import sha256


def build_style_manifest_optimized(
    elements: list[ElementCapture],
) -> StyleManifest:
    """Build style manifest with O(n) complexity.

    Uses hash-based grouping to avoid O(n²) comparisons.
    """
    # Hash-based grouping: O(n)
    style_groups: dict[str, list[str]] = defaultdict(list)

    for element in elements:
        # Canonical style representation
        style_key = compute_style_hash(element.computed_styles)
        style_groups[style_key].append(element.id)

    # Build manifest from groups
    manifest = StyleManifest()
    for style_hash, element_ids in style_groups.items():
        if len(element_ids) > 1:  # Only dedupe shared styles
            manifest.add_shared_style(style_hash, frozenset(element_ids))

    return manifest


def compute_style_hash(styles: dict[str, str]) -> str:
    """Compute deterministic hash of style properties."""
    # Sort for determinism
    canonical = sorted(styles.items())
    serialized = str(canonical).encode()
    return sha256(serialized).hexdigest()[:16]
```

### Style Extraction Metrics

```python
@dataclass
class StyleExtractionMetrics:
    """Metrics for style extraction performance."""
    element_count: int
    unique_style_count: int
    shared_style_count: int
    deduplication_ratio: float  # shared / total
    manifest_build_ms: float
    css_generation_ms: float

    @property
    def styles_per_second(self) -> float:
        total_ms = self.manifest_build_ms + self.css_generation_ms
        return (self.element_count / total_ms) * 1000 if total_ms > 0 else 0
```

---

## Stage 3: LLM Analysis Performance

### Token Optimization Strategies

```python
# Prompt optimization: minimize tokens while maintaining quality

BEHAVIORAL_ANALYSIS_PROMPT = """
Analyze this React component for behavioral patterns.

TSX Source (relevant excerpt):
{tsx_excerpt}

Captured DOM State:
{dom_summary}

Identify:
1. State variables and their classification (UI/form/session/persistent)
2. Event handlers and their HTMX equivalents
3. Conditional rendering patterns
4. Data fetching patterns

Output JSON format:
{{"state": [...], "handlers": [...], "conditionals": [...], "fetching": [...]}}
"""


def optimize_prompt_input(
    tsx_source: str,
    dom_capture: str,
    max_tokens: int = 8000,
) -> tuple[str, str]:
    """Optimize input to stay within token budget.

    Strategies:
    1. Extract only relevant TSX (hooks, handlers, render)
    2. Summarize DOM structure instead of full tree
    3. Remove comments and whitespace
    """
    # Extract relevant portions
    tsx_excerpt = extract_relevant_tsx(tsx_source, max_lines=200)
    dom_summary = summarize_dom_structure(dom_capture, max_depth=5)

    return tsx_excerpt, dom_summary
```

### LLM Call Instrumentation

```python
import time
from anthropic import Anthropic


@dataclass
class LLMCallMetrics:
    """Metrics for a single LLM API call."""
    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    latency_ms: float
    estimated_cost_usd: float

    @classmethod
    def from_response(
        cls,
        response,
        model: str,
        latency_ms: float,
    ) -> "LLMCallMetrics":
        prompt_tokens = response.usage.input_tokens
        completion_tokens = response.usage.output_tokens

        # Claude pricing (approximate)
        cost = estimate_cost(model, prompt_tokens, completion_tokens)

        return cls(
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
            latency_ms=latency_ms,
            estimated_cost_usd=cost,
        )


def estimate_cost(model: str, input_tokens: int, output_tokens: int) -> float:
    """Estimate USD cost based on model pricing."""
    # Pricing per 1M tokens (as of 2025)
    pricing = {
        "claude-sonnet-4-20250514": {"input": 3.0, "output": 15.0},
        "claude-haiku-35-20241022": {"input": 0.25, "output": 1.25},
    }

    if model not in pricing:
        return 0.0

    rates = pricing[model]
    input_cost = (input_tokens / 1_000_000) * rates["input"]
    output_cost = (output_tokens / 1_000_000) * rates["output"]

    return round(input_cost + output_cost, 4)


async def analyze_with_metrics(
    client: Anthropic,
    prompt: str,
    model: str = "claude-sonnet-4-20250514",
) -> tuple[str, LLMCallMetrics]:
    """Make LLM call with metrics collection."""
    start = time.perf_counter()

    response = await client.messages.create(
        model=model,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )

    latency_ms = (time.perf_counter() - start) * 1000

    metrics = LLMCallMetrics.from_response(response, model, latency_ms)
    content = response.content[0].text

    return content, metrics
```

### LLM Performance Budget Enforcement

```python
@dataclass
class LLMBudget:
    """Budget constraints for LLM usage."""
    max_total_tokens: int = 30000
    max_cost_usd: float = 0.50
    max_calls: int = 10

    tokens_used: int = 0
    cost_used: float = 0.0
    calls_made: int = 0

    def can_make_call(self, estimated_tokens: int) -> bool:
        """Check if call is within budget."""
        return (
            self.tokens_used + estimated_tokens <= self.max_total_tokens
            and self.calls_made < self.max_calls
        )

    def record_call(self, metrics: LLMCallMetrics) -> None:
        """Record a completed call against budget."""
        self.tokens_used += metrics.total_tokens
        self.cost_used += metrics.estimated_cost_usd
        self.calls_made += 1

    def remaining(self) -> dict:
        """Get remaining budget."""
        return {
            "tokens": self.max_total_tokens - self.tokens_used,
            "cost_usd": round(self.max_cost_usd - self.cost_used, 4),
            "calls": self.max_calls - self.calls_made,
        }
```

---

## Stage 4: Code Generation Performance

### Template Rendering Optimization

```python
from jinja2 import Environment, FileSystemLoader, select_autoescape


def create_optimized_jinja_env(template_dir: str) -> Environment:
    """Create Jinja2 environment optimized for code generation."""
    return Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(disabled_extensions=["py", "j2"]),
        auto_reload=False,  # Disable in production
        cache_size=100,  # Cache compiled templates
        trim_blocks=True,
        lstrip_blocks=True,
    )
```

### Generation Metrics

```python
@dataclass
class GenerationMetrics:
    """Metrics for code generation stage."""
    files_generated: int
    total_lines: int
    routes_generated: int
    templates_generated: int
    domain_models_generated: int
    service_stubs_generated: int
    css_rules_generated: int
    gherkin_scenarios_generated: int
    generation_time_ms: float

    @property
    def lines_per_second(self) -> float:
        return (self.total_lines / self.generation_time_ms) * 1000 if self.generation_time_ms > 0 else 0
```

---

## Pipeline Metrics Aggregation

### Full Pipeline Metrics

```python
@dataclass
class PipelineMetrics:
    """Aggregated metrics for full conversion pipeline."""
    source_url: str
    timestamp: str

    # Stage metrics
    capture: CaptureMetrics
    style_extraction: StyleExtractionMetrics
    llm_analysis: list[LLMCallMetrics]
    generation: GenerationMetrics

    # Aggregates
    total_time_ms: float
    peak_memory_mb: float

    def summary(self) -> dict:
        """Generate summary for logging/reporting."""
        llm_totals = {
            "calls": len(self.llm_analysis),
            "tokens": sum(m.total_tokens for m in self.llm_analysis),
            "cost_usd": sum(m.estimated_cost_usd for m in self.llm_analysis),
            "latency_ms": sum(m.latency_ms for m in self.llm_analysis),
        }

        return {
            "url": self.source_url,
            "total_time_seconds": round(self.total_time_ms / 1000, 2),
            "capture_time_seconds": round(self.capture.total_capture_ms / 1000, 2),
            "llm_tokens_used": llm_totals["tokens"],
            "llm_cost_usd": round(llm_totals["cost_usd"], 4),
            "files_generated": self.generation.files_generated,
            "lines_generated": self.generation.total_lines,
            "peak_memory_mb": round(self.peak_memory_mb, 1),
        }

    def check_budgets(self) -> list[str]:
        """Check metrics against budgets, return warnings."""
        warnings = []

        if self.total_time_ms > 300_000:  # 5 min
            warnings.append(f"Total time exceeded: {self.total_time_ms/1000:.1f}s > 300s")

        if self.capture.total_capture_ms > 60_000:
            warnings.append(f"Capture time exceeded: {self.capture.total_capture_ms/1000:.1f}s > 60s")

        llm_tokens = sum(m.total_tokens for m in self.llm_analysis)
        if llm_tokens > 30_000:
            warnings.append(f"LLM tokens exceeded: {llm_tokens} > 30,000")

        llm_cost = sum(m.estimated_cost_usd for m in self.llm_analysis)
        if llm_cost > 0.50:
            warnings.append(f"LLM cost exceeded: ${llm_cost:.2f} > $0.50")

        if self.peak_memory_mb > 2000:
            warnings.append(f"Memory exceeded: {self.peak_memory_mb:.0f}MB > 2000MB")

        return warnings
```

---

## Memory Profiling

### Memory Tracking

```python
import tracemalloc
from contextlib import contextmanager


@contextmanager
def track_memory():
    """Context manager for memory tracking."""
    tracemalloc.start()
    try:
        yield
    finally:
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        print(f"Current memory: {current / 1024 / 1024:.1f}MB")
        print(f"Peak memory: {peak / 1024 / 1024:.1f}MB")


def get_peak_memory_mb() -> float:
    """Get peak memory usage in MB."""
    import resource
    # On Unix systems
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_maxrss / 1024  # Convert KB to MB on macOS
```

### Memory Optimization Patterns

```python
# Stream large DOM trees instead of loading fully into memory
def stream_elements(dom_tree: ElementCapture) -> Iterator[ElementCapture]:
    """Depth-first traversal without full tree in memory."""
    stack = [dom_tree]
    while stack:
        element = stack.pop()
        yield element
        # Add children in reverse to maintain order
        stack.extend(reversed(element.children))


# Use generators for style processing
def process_styles_streaming(
    elements: Iterator[ElementCapture],
) -> Iterator[tuple[str, dict]]:
    """Process styles without loading all elements into memory."""
    for element in elements:
        yield element.id, element.computed_styles
```

---

## CLI Performance Reporting

### Verbose Mode Output

```
$ react2htmx convert https://example.figma.site/ -o ./output --verbose

react2htmx v1.0.0
================

[1/4] Capturing DOM and styles...
      Viewports: 320, 768, 1024, 1440
      Navigation: 1.2s
      Hydration wait: 2.4s
      DOM extraction: 0.8s
      Style extraction: 3.2s
      ✓ Capture complete (7.6s, 847 elements)

[2/4] Extracting style manifest...
      Unique styles: 234
      Shared styles: 89 (deduplication: 38%)
      ✓ Extraction complete (2.1s)

[3/4] Analyzing behavior with LLM...
      Model: claude-sonnet-4-20250514
      Call 1: 4,521 tokens, $0.08, 12.3s (behavioral analysis)
      Call 2: 2,103 tokens, $0.04, 8.7s (Gherkin generation)
      Call 3: 1,287 tokens, $0.02, 5.2s (class naming)
      ✓ Analysis complete (26.2s, 7,911 tokens, $0.14)

[4/4] Generating code...
      Routes: 8 files
      Templates: 12 files
      Domain models: 4 files
      Services: 4 files
      CSS: 1 file (1,247 lines)
      Gherkin: 3 features (18 scenarios)
      ✓ Generation complete (1.8s, 31 files, 4,521 lines)

================
Conversion complete!

Summary:
  Total time: 37.7s
  LLM cost: $0.14
  Files generated: 31
  Peak memory: 412MB

Output: ./output/
```

### JSON Metrics Output

```bash
$ react2htmx convert https://example.figma.site/ -o ./output --metrics-json

# Writes to ./output/metrics.json
{
  "version": "1.0",
  "timestamp": "2025-12-13T10:30:00Z",
  "source_url": "https://example.figma.site/",
  "stages": {
    "capture": {
      "total_ms": 7600,
      "elements": 847,
      "viewports": 4
    },
    "extraction": {
      "total_ms": 2100,
      "unique_styles": 234,
      "shared_styles": 89
    },
    "analysis": {
      "total_ms": 26200,
      "llm_calls": 3,
      "tokens_used": 7911,
      "cost_usd": 0.14
    },
    "generation": {
      "total_ms": 1800,
      "files": 31,
      "lines": 4521
    }
  },
  "totals": {
    "time_seconds": 37.7,
    "cost_usd": 0.14,
    "peak_memory_mb": 412
  },
  "warnings": []
}
```

---

## Continuous Performance Monitoring

### CI/CD Integration

```yaml
# .github/workflows/performance.yml
name: Performance Benchmarks

on:
  pull_request:
  push:
    branches: [main]

jobs:
  benchmark:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install uv
          uv sync

      - name: Run benchmark suite
        run: |
          uv run pytest tests/benchmarks/ --benchmark-json=benchmark.json

      - name: Check performance regression
        run: |
          uv run python scripts/check_performance_regression.py benchmark.json

      - name: Upload benchmark results
        uses: actions/upload-artifact@v4
        with:
          name: benchmark-results
          path: benchmark.json
```

### Benchmark Tests

```python
# tests/benchmarks/test_capture_performance.py
import pytest


@pytest.mark.benchmark
def test_capture_small_page(benchmark, sample_small_url):
    """Benchmark capture of small React app (~50 elements)."""
    result = benchmark(capture_url, sample_small_url)
    assert result.element_count < 100
    assert benchmark.stats["mean"] < 10.0  # seconds


@pytest.mark.benchmark
def test_capture_medium_page(benchmark, sample_medium_url):
    """Benchmark capture of medium React app (~500 elements)."""
    result = benchmark(capture_url, sample_medium_url)
    assert result.element_count < 1000
    assert benchmark.stats["mean"] < 30.0  # seconds


@pytest.mark.benchmark
def test_style_extraction_performance(benchmark, large_element_set):
    """Benchmark style manifest building."""
    result = benchmark(build_style_manifest, large_element_set)
    assert benchmark.stats["mean"] < 5.0  # seconds
```

---

## Performance Optimization Checklist

### Before Release

- [ ] All stages within time budgets
- [ ] LLM token usage within budget
- [ ] Memory usage within budget
- [ ] No performance regressions from baseline
- [ ] Benchmark suite passing
- [ ] Verbose mode shows accurate metrics

### Per-Stage Optimization

**Capture**:
- [ ] Parallel viewport capture
- [ ] Network idle wait (not full load)
- [ ] Browser context reuse
- [ ] Offscreen element skipping

**Style Extraction**:
- [ ] O(n) hash-based grouping
- [ ] Streaming for large DOMs
- [ ] Efficient CSS generation

**LLM Analysis**:
- [ ] Prompt optimization (minimal tokens)
- [ ] Use Haiku for simple tasks
- [ ] Budget enforcement
- [ ] Caching for repeated patterns

**Code Generation**:
- [ ] Template caching enabled
- [ ] Streaming file writes
- [ ] Parallel generation where possible

---

## Conclusion

Performance monitoring ensures react2htmx remains fast and cost-effective. Key metrics to track:

1. **Total conversion time**: < 5 minutes for typical apps
2. **LLM cost per conversion**: < $0.50
3. **Memory usage**: < 2GB peak
4. **Generated code quality**: Production-ready output

Regular benchmark testing prevents regressions and guides optimization efforts.
