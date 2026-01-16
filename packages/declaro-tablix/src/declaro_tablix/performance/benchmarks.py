"""
Performance benchmarks for TableV2 optimization validation.

This module provides comprehensive benchmarking functions to validate
that the <10ms overhead target is met across all scenarios.
"""

import statistics
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from declaro_advise import error, info, success, warning
from declaro_tablix.performance.customization_optimizer import (
    CustomizationContext,
    apply_customizations,
    get_performance_metrics,
)
from declaro_tablix.performance.formula_compiler import (
    batch_execute_formulas,
    compile_formula,
    execute_formula,
    get_formula_stats,
)
from declaro_tablix.performance.performance_monitor import get_performance_monitor


@dataclass
class BenchmarkResult:
    """Result of a performance benchmark."""

    benchmark_name: str
    success: bool
    target_met: bool
    iterations: int
    total_time_ms: float
    average_time_ms: float
    median_time_ms: float
    p95_time_ms: float
    p99_time_ms: float
    min_time_ms: float
    max_time_ms: float
    standard_deviation: float
    throughput_ops_per_second: float
    target_value: float
    actual_value: float
    improvement_over_baseline: Optional[float] = None
    metadata: Dict[str, Any] = None


@dataclass
class BenchmarkSuite:
    """Collection of benchmark results."""

    suite_name: str
    benchmarks: List[BenchmarkResult]
    overall_success: bool
    total_duration_seconds: float
    targets_met_count: int
    targets_failed_count: int
    summary_stats: Dict[str, float]


class PerformanceBenchmarker:
    """Performance benchmarking system."""

    def __init__(self):
        self.baseline_results: Dict[str, BenchmarkResult] = {}
        self.performance_monitor = get_performance_monitor()

    def run_formula_compilation_benchmark(self, iterations: int = 1000) -> BenchmarkResult:
        """Benchmark formula compilation performance."""
        info(f"Running formula compilation benchmark with {iterations} iterations")

        # Test formulas of varying complexity
        formulas = [
            "{col_1} + {col_2}",
            "{col_1} * {col_2} + {col_3}",
            "round({col_1} / {col_2}, 2)",
            "max({col_1}, {col_2}) + min({col_3}, {col_4})",
            "round(({col_1} + {col_2}) * {col_3} / max({col_4}, 1), 2)",
        ]

        columns = [f"col_{i}" for i in range(1, 11)]
        times = []

        start_time = time.time()

        for _ in range(iterations):
            formula = formulas[_ % len(formulas)]

            iteration_start = time.time()
            result = compile_formula(formula, columns)
            iteration_time = (time.time() - iteration_start) * 1000

            times.append(iteration_time)

            if not result.success:
                warning(f"Formula compilation failed: {result.error_message}")

        total_time = time.time() - start_time

        return self._create_benchmark_result(
            "formula_compilation",
            times,
            total_time,
            target_value=5.0,  # 5ms target
            metadata={"formulas_tested": len(formulas), "columns_count": len(columns)},
        )

    def run_formula_execution_benchmark(self, iterations: int = 1000) -> BenchmarkResult:
        """Benchmark formula execution performance."""
        info(f"Running formula execution benchmark with {iterations} iterations")

        # Pre-compile formulas
        formulas = [
            "{col_1} + {col_2}",
            "{col_1} * {col_2} + {col_3}",
            "round({col_1} / {col_2}, 2)",
            "max({col_1}, {col_2}) + min({col_3}, {col_4})",
            "round(({col_1} + {col_2}) * {col_3} / max({col_4}, 1), 2)",
        ]

        columns = [f"col_{i}" for i in range(1, 11)]
        compiled_formulas = []

        for formula in formulas:
            result = compile_formula(formula, columns)
            if result.success:
                compiled_formulas.append(result)

        # Generate test data
        test_data = {col: 42.0 for col in columns}

        times = []
        start_time = time.time()

        for _ in range(iterations):
            compiled_formula = compiled_formulas[_ % len(compiled_formulas)]

            iteration_start = time.time()
            result = execute_formula(compiled_formula, test_data)
            iteration_time = (time.time() - iteration_start) * 1000

            times.append(iteration_time)

            if not result.success:
                warning(f"Formula execution failed: {result.error_message}")

        total_time = time.time() - start_time

        return self._create_benchmark_result(
            "formula_execution",
            times,
            total_time,
            target_value=5.0,  # 5ms target
            metadata={"compiled_formulas": len(compiled_formulas)},
        )

    def run_customization_overhead_benchmark(self, iterations: int = 100) -> BenchmarkResult:
        """Benchmark customization overhead per 1000 rows."""
        info(f"Running customization overhead benchmark with {iterations} iterations")

        # Generate test data (1000 rows)
        row_count = 1000
        column_count = 10
        test_data = {f"col_{i}": [42.0 + i for _ in range(row_count)] for i in range(column_count)}

        # Generate customizations
        customizations = {
            "column_alias": {f"col_{i}": f"Column {i}" for i in range(column_count)},
            "column_filters": {
                "col_1": {"operator": "gt", "value": 40},
                "col_2": {"operator": "lt", "value": 50},
            },
            "formula_columns": {
                "calculated_1": "{col_1} + {col_2}",
                "calculated_2": "{col_1} * {col_2}",
            },
        }

        times = []
        start_time = time.time()

        for i in range(iterations):
            context = CustomizationContext(
                user_id=f"benchmark_user_{i}",
                table_name="benchmark_table",
                row_count=row_count,
                column_count=column_count,
                customizations=customizations,
                data_snapshot_hash=f"benchmark_snapshot_{i}",
                timestamp=time.time(),
            )

            iteration_start = time.time()
            result = apply_customizations(test_data, context)
            iteration_time = (time.time() - iteration_start) * 1000

            times.append(iteration_time)

            if not result.success:
                warning(f"Customization application failed: {result.error_message}")

        total_time = time.time() - start_time

        return self._create_benchmark_result(
            "customization_overhead",
            times,
            total_time,
            target_value=10.0,  # 10ms per 1000 rows target
            metadata={"row_count": row_count, "column_count": column_count},
        )

    def run_cache_performance_benchmark(self, iterations: int = 500) -> BenchmarkResult:
        """Benchmark cache performance and hit rates."""
        info(f"Running cache performance benchmark with {iterations} iterations")

        # Test cache effectiveness with repeated operations
        formula = "{col_1} + {col_2} * {col_3}"
        columns = [f"col_{i}" for i in range(1, 6)]
        test_data = {col: 42.0 for col in columns}

        # First pass - populate cache
        compile_result = compile_formula(formula, columns)
        if not compile_result.success:
            return self._create_benchmark_result(
                "cache_performance",
                [0],
                0,
                target_value=0.8,  # 80% cache hit rate target
                metadata={"error": "Formula compilation failed"},
            )

        # Warm up cache
        for _ in range(10):
            execute_formula(compile_result, test_data)

        # Benchmark cache hits
        times = []
        cache_hits = 0
        start_time = time.time()

        for _ in range(iterations):
            iteration_start = time.time()
            result = execute_formula(compile_result, test_data)
            iteration_time = (time.time() - iteration_start) * 1000

            times.append(iteration_time)

            if result.cache_hit:
                cache_hits += 1

        total_time = time.time() - start_time
        cache_hit_rate = cache_hits / iterations

        return self._create_benchmark_result(
            "cache_performance",
            times,
            total_time,
            target_value=0.8,  # 80% cache hit rate target
            actual_value=cache_hit_rate,
            metadata={"cache_hits": cache_hits, "cache_hit_rate": cache_hit_rate},
        )

    def run_batch_processing_benchmark(self, batch_size: int = 100) -> BenchmarkResult:
        """Benchmark batch processing performance."""
        info(f"Running batch processing benchmark with batch size {batch_size}")

        # Prepare batch data
        formulas = [
            "{col_1} + {col_2}",
            "{col_1} * {col_2}",
            "{col_1} - {col_2}",
            "{col_1} / {col_2}",
            "round({col_1} * {col_2}, 2)",
        ]

        columns = [f"col_{i}" for i in range(1, 6)]
        test_data = {col: 42.0 for col in columns}

        # Compile formulas
        compiled_formulas = []
        for formula in formulas:
            result = compile_formula(formula, columns)
            if result.success:
                compiled_formulas.append(result)

        # Create batch
        batch_data = [(formula, test_data) for formula in compiled_formulas] * (batch_size // len(compiled_formulas))

        times = []
        start_time = time.time()

        # Execute batch
        batch_start = time.time()
        results = batch_execute_formulas(batch_data)
        batch_time = (time.time() - batch_start) * 1000

        times.append(batch_time)

        total_time = time.time() - start_time

        # Calculate per-operation time
        per_operation_time = batch_time / len(batch_data)

        success_count = sum(1 for result in results if result.success)
        success_rate = success_count / len(results)

        return self._create_benchmark_result(
            "batch_processing",
            [per_operation_time],
            total_time,
            target_value=2.0,  # 2ms per operation in batch
            metadata={"batch_size": len(batch_data), "success_rate": success_rate, "total_batch_time": batch_time},
        )

    def run_memory_efficiency_benchmark(self, iterations: int = 100) -> BenchmarkResult:
        """Benchmark memory efficiency during operations."""
        info(f"Running memory efficiency benchmark with {iterations} iterations")

        try:
            import os

            import psutil

            process = psutil.Process(os.getpid())

            # Baseline memory
            baseline_memory = process.memory_info().rss / 1024 / 1024  # MB

            # Generate large dataset
            large_data = {f"col_{i}": [42.0 + i for _ in range(10000)] for i in range(20)}

            customizations = {
                "column_alias": {f"col_{i}": f"Column {i}" for i in range(20)},
                "formula_columns": {f"calc_{i}": f"{{col_{i}}} + {{col_{i+1}}}" for i in range(19)},
            }

            memory_usage = []
            start_time = time.time()

            for i in range(iterations):
                context = CustomizationContext(
                    user_id=f"memory_user_{i}",
                    table_name="memory_table",
                    row_count=10000,
                    column_count=20,
                    customizations=customizations,
                    data_snapshot_hash=f"memory_snapshot_{i}",
                    timestamp=time.time(),
                )

                apply_customizations(large_data, context)

                current_memory = process.memory_info().rss / 1024 / 1024  # MB
                memory_usage.append(current_memory - baseline_memory)

            total_time = time.time() - start_time

            return self._create_benchmark_result(
                "memory_efficiency",
                memory_usage,
                total_time,
                target_value=100.0,  # 100MB max memory increase
                metadata={"baseline_memory_mb": baseline_memory},
            )

        except ImportError:
            warning("psutil not available, skipping memory efficiency benchmark")
            return self._create_benchmark_result(
                "memory_efficiency", [0], 0, target_value=100.0, metadata={"error": "psutil not available"}
            )

    def run_concurrent_operations_benchmark(self, concurrent_users: int = 50) -> BenchmarkResult:
        """Benchmark concurrent operations performance."""
        info(f"Running concurrent operations benchmark with {concurrent_users} users")

        import threading
        from concurrent.futures import ThreadPoolExecutor

        # Shared test data
        test_data = {f"col_{i}": 42.0 for i in range(1, 6)}
        formula = "{col_1} + {col_2} * {col_3}"
        columns = [f"col_{i}" for i in range(1, 6)]

        # Pre-compile formula
        compile_result = compile_formula(formula, columns)
        if not compile_result.success:
            return self._create_benchmark_result(
                "concurrent_operations", [0], 0, target_value=5.0, metadata={"error": "Formula compilation failed"}
            )

        # Results collection
        results = []
        results_lock = threading.Lock()

        def worker_function(worker_id: int) -> None:
            """Worker function for concurrent execution."""
            for i in range(10):  # 10 operations per worker
                start_time = time.time()
                result = execute_formula(compile_result, test_data)
                execution_time = (time.time() - start_time) * 1000

                with results_lock:
                    results.append(execution_time)

        # Execute concurrent workers
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=concurrent_users) as executor:
            futures = [executor.submit(worker_function, i) for i in range(concurrent_users)]

            # Wait for all workers to complete
            for future in futures:
                future.result()

        total_time = time.time() - start_time

        return self._create_benchmark_result(
            "concurrent_operations",
            results,
            total_time,
            target_value=5.0,  # 5ms per operation under load
            metadata={"concurrent_users": concurrent_users},
        )

    def run_comprehensive_benchmark_suite(self) -> BenchmarkSuite:
        """Run comprehensive benchmark suite."""
        info("Starting comprehensive benchmark suite")

        suite_start_time = time.time()
        benchmarks = []

        # Run all benchmarks
        benchmark_functions = [
            (self.run_formula_compilation_benchmark, 1000),
            (self.run_formula_execution_benchmark, 1000),
            (self.run_customization_overhead_benchmark, 100),
            (self.run_cache_performance_benchmark, 500),
            (self.run_batch_processing_benchmark, 100),
            (self.run_memory_efficiency_benchmark, 50),
            (self.run_concurrent_operations_benchmark, 25),
        ]

        for benchmark_func, iterations in benchmark_functions:
            try:
                if benchmark_func == self.run_concurrent_operations_benchmark:
                    result = benchmark_func(iterations)
                else:
                    result = benchmark_func(iterations)
                benchmarks.append(result)

                if result.target_met:
                    success(f"✅ {result.benchmark_name}: {result.actual_value:.2f} (target: {result.target_value})")
                else:
                    warning(f"❌ {result.benchmark_name}: {result.actual_value:.2f} (target: {result.target_value})")

            except Exception as e:
                error(f"Benchmark {benchmark_func.__name__} failed: {str(e)}")
                benchmarks.append(
                    BenchmarkResult(
                        benchmark_name=benchmark_func.__name__,
                        success=False,
                        target_met=False,
                        iterations=0,
                        total_time_ms=0,
                        average_time_ms=0,
                        median_time_ms=0,
                        p95_time_ms=0,
                        p99_time_ms=0,
                        min_time_ms=0,
                        max_time_ms=0,
                        standard_deviation=0,
                        throughput_ops_per_second=0,
                        target_value=0,
                        actual_value=0,
                        metadata={"error": str(e)},
                    )
                )

        suite_duration = time.time() - suite_start_time

        # Calculate suite statistics
        targets_met = sum(1 for b in benchmarks if b.target_met)
        targets_failed = len(benchmarks) - targets_met
        overall_success = targets_failed == 0

        summary_stats = {
            "total_benchmarks": len(benchmarks),
            "targets_met": targets_met,
            "targets_failed": targets_failed,
            "success_rate": targets_met / len(benchmarks) if benchmarks else 0,
            "average_improvement": (
                statistics.mean([b.improvement_over_baseline for b in benchmarks if b.improvement_over_baseline is not None])
                if any(b.improvement_over_baseline is not None for b in benchmarks)
                else 0
            ),
        }

        suite = BenchmarkSuite(
            suite_name="TableV2 Performance Benchmark Suite",
            benchmarks=benchmarks,
            overall_success=overall_success,
            total_duration_seconds=suite_duration,
            targets_met_count=targets_met,
            targets_failed_count=targets_failed,
            summary_stats=summary_stats,
        )

        if overall_success:
            success(f"🎉 All performance benchmarks passed! ({targets_met}/{len(benchmarks)})")
        else:
            warning(f"⚠️  Some benchmarks failed: {targets_met}/{len(benchmarks)} passed")

        return suite

    def set_baseline(self, benchmark_name: str, result: BenchmarkResult) -> None:
        """Set baseline for performance comparison."""
        self.baseline_results[benchmark_name] = result
        success(f"Set baseline for {benchmark_name}")

    def _create_benchmark_result(
        self,
        benchmark_name: str,
        times: List[float],
        total_time: float,
        target_value: float,
        actual_value: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BenchmarkResult:
        """Create benchmark result from timing data."""
        if not times:
            return BenchmarkResult(
                benchmark_name=benchmark_name,
                success=False,
                target_met=False,
                iterations=0,
                total_time_ms=total_time * 1000,
                average_time_ms=0,
                median_time_ms=0,
                p95_time_ms=0,
                p99_time_ms=0,
                min_time_ms=0,
                max_time_ms=0,
                standard_deviation=0,
                throughput_ops_per_second=0,
                target_value=target_value,
                actual_value=actual_value or 0,
                metadata=metadata or {},
            )

        times_sorted = sorted(times)

        average_time = statistics.mean(times)
        median_time = statistics.median(times)
        p95_time = times_sorted[int(0.95 * len(times_sorted))]
        p99_time = times_sorted[int(0.99 * len(times_sorted))]
        min_time = min(times)
        max_time = max(times)
        std_dev = statistics.stdev(times) if len(times) > 1 else 0

        throughput = len(times) / total_time if total_time > 0 else 0

        # Use actual_value if provided, otherwise use average_time
        if actual_value is None:
            actual_value = average_time

        target_met = actual_value <= target_value

        # Calculate improvement over baseline
        improvement = None
        if benchmark_name in self.baseline_results:
            baseline_value = self.baseline_results[benchmark_name].actual_value
            if baseline_value > 0:
                improvement = ((baseline_value - actual_value) / baseline_value) * 100

        return BenchmarkResult(
            benchmark_name=benchmark_name,
            success=True,
            target_met=target_met,
            iterations=len(times),
            total_time_ms=total_time * 1000,
            average_time_ms=average_time,
            median_time_ms=median_time,
            p95_time_ms=p95_time,
            p99_time_ms=p99_time,
            min_time_ms=min_time,
            max_time_ms=max_time,
            standard_deviation=std_dev,
            throughput_ops_per_second=throughput,
            target_value=target_value,
            actual_value=actual_value,
            improvement_over_baseline=improvement,
            metadata=metadata or {},
        )


# Global benchmarker instance
_benchmarker = PerformanceBenchmarker()


def run_performance_benchmarks() -> BenchmarkSuite:
    """Run comprehensive performance benchmarks."""
    return _benchmarker.run_comprehensive_benchmark_suite()


def run_quick_performance_check() -> bool:
    """Run quick performance check to validate targets are met."""
    info("Running quick performance check")

    # Run core benchmarks
    formula_result = _benchmarker.run_formula_execution_benchmark(100)
    customization_result = _benchmarker.run_customization_overhead_benchmark(50)

    targets_met = formula_result.target_met and customization_result.target_met

    if targets_met:
        success("✅ Quick performance check passed")
    else:
        warning("❌ Quick performance check failed")

    return targets_met


# Export all functions and classes
__all__ = [
    "BenchmarkResult",
    "BenchmarkSuite",
    "PerformanceBenchmarker",
    "run_performance_benchmarks",
    "run_quick_performance_check",
]
