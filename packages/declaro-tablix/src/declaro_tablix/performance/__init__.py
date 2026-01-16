"""
TableV2 Performance Optimization Module.

This module provides comprehensive performance optimization for TableV2 customizations,
including formula compilation, caching, lazy evaluation, and performance monitoring.
"""

from .benchmarks import (
    BenchmarkResult,
    BenchmarkSuite,
    PerformanceBenchmarker,
    run_performance_benchmarks,
    run_quick_performance_check,
)
from .customization_optimizer import (
    CustomizationContext,
    CustomizationResult,
    PerformanceMetrics,
    analyze_performance_bottlenecks,
    apply_customizations,
    batch_apply_customizations,
    clear_customization_cache,
    configure_optimization_strategies,
    get_performance_metrics,
    reset_performance_metrics,
)
from .formula_compiler import (
    FormulaCompileResult,
    FormulaExecutionResult,
    FormulaStats,
    batch_execute_formulas,
    clear_formula_cache,
    compile_formula,
    execute_formula,
    get_formula_stats,
    get_hot_formulas,
    optimize_hot_formulas,
)
from .load_testing import (
    LoadTestConfig,
    LoadTestResult,
    LoadTestRunner,
    run_complex_formula_load_test,
    run_comprehensive_stress_test,
    run_large_table_load_test,
    run_performance_load_test,
)
from .performance_monitor import (
    AlertSeverity,
    MetricValue,
    PerformanceAlert,
    PerformanceMonitor,
    PerformanceThreshold,
    get_performance_monitor,
    start_performance_monitoring,
    stop_performance_monitoring,
)

# Performance optimization constants
PERFORMANCE_TARGETS = {
    "formula_evaluation_time_ms": 5.0,
    "customization_overhead_per_1000_rows_ms": 10.0,
    "cache_hit_rate_threshold": 0.7,
    "memory_usage_limit_mb": 1000.0,
    "concurrent_users_limit": 100,
}

# Export all public functions and classes
__all__ = [
    # Formula compiler
    "compile_formula",
    "execute_formula",
    "batch_execute_formulas",
    "get_formula_stats",
    "clear_formula_cache",
    "get_hot_formulas",
    "optimize_hot_formulas",
    "FormulaCompileResult",
    "FormulaExecutionResult",
    "FormulaStats",
    # Customization optimizer
    "apply_customizations",
    "batch_apply_customizations",
    "get_performance_metrics",
    "reset_performance_metrics",
    "configure_optimization_strategies",
    "clear_customization_cache",
    "analyze_performance_bottlenecks",
    "CustomizationContext",
    "CustomizationResult",
    "PerformanceMetrics",
    # Performance monitoring
    "PerformanceMonitor",
    "PerformanceAlert",
    "PerformanceThreshold",
    "AlertSeverity",
    "MetricValue",
    "get_performance_monitor",
    "start_performance_monitoring",
    "stop_performance_monitoring",
    # Load testing
    "LoadTestConfig",
    "LoadTestResult",
    "LoadTestRunner",
    "run_performance_load_test",
    "run_large_table_load_test",
    "run_complex_formula_load_test",
    "run_comprehensive_stress_test",
    # Benchmarks
    "BenchmarkResult",
    "BenchmarkSuite",
    "PerformanceBenchmarker",
    "run_performance_benchmarks",
    "run_quick_performance_check",
    # Constants
    "PERFORMANCE_TARGETS",
]
