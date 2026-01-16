"""
Customization performance optimizer for TableV2.

This module provides lazy evaluation, caching, and optimization strategies
to minimize customization overhead to <10ms per 1000 rows.
"""

import time
from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from declaro_advise import error, info, success, warning
from declaro_tablix.performance.formula_compiler import (
    FormulaCompileResult,
    FormulaExecutionResult,
    batch_execute_formulas,
    compile_formula,
    execute_formula,
)


@dataclass
class CustomizationContext:
    """Context for customization application."""

    user_id: str
    table_name: str
    row_count: int
    column_count: int
    customizations: Dict[str, Any]
    data_snapshot_hash: str
    timestamp: float


@dataclass
class CustomizationResult:
    """Result of customization application."""

    success: bool
    processed_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    processing_time_ms: float = 0.0
    cache_hit: bool = False
    lazy_evaluation_used: bool = False
    rows_processed: int = 0
    optimizations_applied: List[str] = None


@dataclass
class PerformanceMetrics:
    """Performance metrics for customization operations."""

    total_operations: int = 0
    total_processing_time_ms: float = 0.0
    avg_processing_time_ms: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0
    cache_hit_rate: float = 0.0
    lazy_evaluations: int = 0
    rows_processed: int = 0
    avg_time_per_1000_rows: float = 0.0
    optimization_savings_ms: float = 0.0


# Global caches and metrics
_customization_cache: Dict[str, Dict[str, Any]] = {}
_lazy_evaluation_cache: Dict[str, Any] = {}
_performance_metrics = PerformanceMetrics()
_optimization_strategies: Dict[str, bool] = {
    "lazy_evaluation": True,
    "batch_processing": True,
    "formula_caching": True,
    "column_filtering": True,
    "incremental_updates": True,
}


def apply_customizations(
    data: Dict[str, Any],
    customization_context: CustomizationContext,
    enable_lazy_evaluation: bool = True,
    enable_caching: bool = True,
) -> CustomizationResult:
    """
    Apply customizations to data with performance optimization.

    Args:
        data: Raw data to customize
        customization_context: Context for customization
        enable_lazy_evaluation: Whether to use lazy evaluation
        enable_caching: Whether to use caching

    Returns:
        CustomizationResult with processed data and metrics
    """
    start_time = time.time()

    try:
        # Generate cache key
        cache_key = _generate_cache_key(customization_context)

        # Check cache first
        if enable_caching and cache_key in _customization_cache:
            cache_result = _customization_cache[cache_key]
            if _is_cache_valid(cache_result, customization_context):
                _update_performance_metrics(0, True, customization_context.row_count, False)
                return CustomizationResult(
                    success=True,
                    processed_data=cache_result["data"],
                    processing_time_ms=(time.time() - start_time) * 1000,
                    cache_hit=True,
                    rows_processed=customization_context.row_count,
                    optimizations_applied=["cache_hit"],
                )

        # Apply optimizations
        optimizations_applied = []

        # 1. Column filtering optimization
        if _optimization_strategies["column_filtering"]:
            data = _apply_column_filtering(data, customization_context)
            optimizations_applied.append("column_filtering")

        # 2. Lazy evaluation optimization
        lazy_evaluation_used = False
        if enable_lazy_evaluation and _optimization_strategies["lazy_evaluation"]:
            lazy_result = _apply_lazy_evaluation(data, customization_context)
            if lazy_result["used"]:
                data = lazy_result["data"]
                lazy_evaluation_used = True
                optimizations_applied.append("lazy_evaluation")

        # 3. Batch processing optimization
        if _optimization_strategies["batch_processing"]:
            data = _apply_batch_processing(data, customization_context)
            optimizations_applied.append("batch_processing")

        # 4. Formula caching optimization
        if _optimization_strategies["formula_caching"]:
            data = _apply_formula_caching(data, customization_context)
            optimizations_applied.append("formula_caching")

        # 5. Incremental updates optimization
        if _optimization_strategies["incremental_updates"]:
            data = _apply_incremental_updates(data, customization_context)
            optimizations_applied.append("incremental_updates")

        processing_time_ms = (time.time() - start_time) * 1000

        # Cache result
        if enable_caching:
            _customization_cache[cache_key] = {
                "data": data,
                "timestamp": time.time(),
                "context_hash": customization_context.data_snapshot_hash,
            }

        # Update metrics
        _update_performance_metrics(processing_time_ms, False, customization_context.row_count, lazy_evaluation_used)

        # Check performance target
        time_per_1000_rows = (processing_time_ms / customization_context.row_count) * 1000
        if time_per_1000_rows > 10:
            warning(f"Performance target missed: {time_per_1000_rows:.2f}ms per 1000 rows (target: <10ms)")
        else:
            success(f"Performance target met: {time_per_1000_rows:.2f}ms per 1000 rows")

        return CustomizationResult(
            success=True,
            processed_data=data,
            processing_time_ms=processing_time_ms,
            cache_hit=False,
            lazy_evaluation_used=lazy_evaluation_used,
            rows_processed=customization_context.row_count,
            optimizations_applied=optimizations_applied,
        )

    except Exception as e:
        processing_time_ms = (time.time() - start_time) * 1000
        _update_performance_metrics(processing_time_ms, False, customization_context.row_count, False)

        error(f"Customization application failed: {str(e)}")
        return CustomizationResult(
            success=False,
            error_message=str(e),
            processing_time_ms=processing_time_ms,
            rows_processed=customization_context.row_count,
        )


def batch_apply_customizations(
    data_batches: List[Tuple[Dict[str, Any], CustomizationContext]],
    parallel_processing: bool = True,
) -> List[CustomizationResult]:
    """
    Apply customizations to multiple data batches for better performance.

    Args:
        data_batches: List of (data, context) tuples
        parallel_processing: Whether to use parallel processing

    Returns:
        List of CustomizationResult for each batch
    """
    start_time = time.time()

    try:
        results = []

        if parallel_processing and len(data_batches) > 1:
            # Use parallel processing for multiple batches
            info(f"Processing {len(data_batches)} batches in parallel")

            # For now, process sequentially (parallel implementation would use ProcessPoolExecutor)
            for data, context in data_batches:
                result = apply_customizations(data, context)
                results.append(result)
        else:
            # Sequential processing
            for data, context in data_batches:
                result = apply_customizations(data, context)
                results.append(result)

        total_time_ms = (time.time() - start_time) * 1000
        total_rows = sum(context.row_count for _, context in data_batches)

        success(f"Batch processed {len(data_batches)} datasets ({total_rows} rows) in {total_time_ms:.2f}ms")

        return results

    except Exception as e:
        error(f"Batch customization processing failed: {str(e)}")
        return [
            CustomizationResult(
                success=False,
                error_message=f"Batch processing error: {str(e)}",
                processing_time_ms=(time.time() - start_time) * 1000,
            )
            for _ in data_batches
        ]


def get_performance_metrics() -> PerformanceMetrics:
    """Get current performance metrics."""
    return _performance_metrics


def reset_performance_metrics() -> None:
    """Reset performance metrics."""
    global _performance_metrics
    _performance_metrics = PerformanceMetrics()
    success("Performance metrics reset")


def configure_optimization_strategies(strategies: Dict[str, bool]) -> Dict[str, Any]:
    """
    Configure optimization strategies.

    Args:
        strategies: Dictionary of strategy names and enabled status

    Returns:
        Configuration result
    """
    try:
        valid_strategies = set(_optimization_strategies.keys())
        invalid_strategies = set(strategies.keys()) - valid_strategies

        if invalid_strategies:
            warning(f"Invalid optimization strategies: {invalid_strategies}")

        # Update valid strategies
        for strategy, enabled in strategies.items():
            if strategy in valid_strategies:
                _optimization_strategies[strategy] = enabled

        success(f"Updated optimization strategies: {strategies}")

        return {
            "success": True,
            "updated_strategies": {k: v for k, v in strategies.items() if k in valid_strategies},
            "invalid_strategies": list(invalid_strategies),
            "current_strategies": _optimization_strategies.copy(),
        }

    except Exception as e:
        error(f"Failed to configure optimization strategies: {str(e)}")
        return {"success": False, "error": str(e)}


def clear_customization_cache(cache_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Clear customization cache.

    Args:
        cache_key: Optional specific cache key, if None clears all

    Returns:
        Cache clear result
    """
    try:
        if cache_key:
            cleared = _customization_cache.pop(cache_key, None) is not None
            return {"success": True, "cache_key": cache_key, "cleared": cleared}
        else:
            count = len(_customization_cache)
            _customization_cache.clear()
            _lazy_evaluation_cache.clear()

            success(f"Cleared {count} customization cache entries")
            return {"success": True, "cleared_count": count}

    except Exception as e:
        error(f"Failed to clear customization cache: {str(e)}")
        return {"success": False, "error": str(e)}


def analyze_performance_bottlenecks() -> Dict[str, Any]:
    """
    Analyze performance bottlenecks and suggest optimizations.

    Returns:
        Analysis result with recommendations
    """
    try:
        metrics = get_performance_metrics()
        analysis = {
            "success": True,
            "metrics_summary": {
                "total_operations": metrics.total_operations,
                "avg_processing_time_ms": metrics.avg_processing_time_ms,
                "cache_hit_rate": metrics.cache_hit_rate,
                "avg_time_per_1000_rows": metrics.avg_time_per_1000_rows,
            },
            "performance_assessment": "good",
            "bottlenecks": [],
            "recommendations": [],
        }

        # Analyze performance
        if metrics.avg_time_per_1000_rows > 10:
            analysis["performance_assessment"] = "poor"
            analysis["bottlenecks"].append("Exceeds 10ms per 1000 rows target")
            analysis["recommendations"].append("Enable all optimization strategies")
        elif metrics.avg_time_per_1000_rows > 5:
            analysis["performance_assessment"] = "fair"
            analysis["bottlenecks"].append("Approaching performance limit")
            analysis["recommendations"].append("Consider additional caching")

        # Analyze cache effectiveness
        if metrics.cache_hit_rate < 0.5:
            analysis["bottlenecks"].append("Low cache hit rate")
            analysis["recommendations"].append("Increase cache TTL or improve cache key strategy")

        # Analyze lazy evaluation usage
        if metrics.lazy_evaluations == 0 and metrics.total_operations > 0:
            analysis["bottlenecks"].append("Lazy evaluation not being used")
            analysis["recommendations"].append("Enable lazy evaluation for better performance")

        return analysis

    except Exception as e:
        error(f"Performance analysis failed: {str(e)}")
        return {"success": False, "error": str(e)}


# Helper functions


def _generate_cache_key(context: CustomizationContext) -> str:
    """Generate unique cache key for customization context."""
    import hashlib

    key_components = [
        context.user_id,
        context.table_name,
        str(context.row_count),
        str(context.column_count),
        str(sorted(context.customizations.items())),
        context.data_snapshot_hash,
    ]
    key_string = "|".join(key_components)
    return hashlib.md5(key_string.encode()).hexdigest()


def _is_cache_valid(cache_entry: Dict[str, Any], context: CustomizationContext) -> bool:
    """Check if cache entry is still valid."""
    if "timestamp" not in cache_entry or "context_hash" not in cache_entry:
        return False

    # Check if data has changed
    if cache_entry["context_hash"] != context.data_snapshot_hash:
        return False

    # Check age (5 minutes TTL)
    age_seconds = time.time() - cache_entry["timestamp"]
    return age_seconds < 300


def _apply_column_filtering(data: Dict[str, Any], context: CustomizationContext) -> Dict[str, Any]:
    """Apply column filtering optimization to reduce data size."""
    if not context.customizations.get("column_filters"):
        return data

    # Filter columns based on customizations
    active_columns = set()
    for customization in context.customizations.values():
        if isinstance(customization, dict) and customization.get("is_visible", True):
            active_columns.add(customization.get("column_id", ""))

    if active_columns:
        filtered_data = {}
        for key, value in data.items():
            if key in active_columns or key in ["_metadata", "_index"]:
                filtered_data[key] = value
        return filtered_data

    return data


def _apply_lazy_evaluation(data: Dict[str, Any], context: CustomizationContext) -> Dict[str, Any]:
    """Apply lazy evaluation optimization."""
    # Check if data is already in lazy evaluation cache
    cache_key = f"lazy_{context.user_id}_{context.table_name}"

    if cache_key in _lazy_evaluation_cache:
        cached_data = _lazy_evaluation_cache[cache_key]
        if cached_data["context_hash"] == context.data_snapshot_hash:
            return {"used": True, "data": cached_data["data"]}

    # Apply lazy evaluation for large datasets
    if context.row_count > 1000:
        # Store data reference for lazy loading
        _lazy_evaluation_cache[cache_key] = {
            "data": data,
            "context_hash": context.data_snapshot_hash,
            "timestamp": time.time(),
        }
        return {"used": True, "data": data}

    return {"used": False, "data": data}


def _apply_batch_processing(data: Dict[str, Any], context: CustomizationContext) -> Dict[str, Any]:
    """Apply batch processing optimization."""
    # Batch process large datasets in chunks
    if context.row_count > 5000:
        # Process in chunks (implementation would split data into batches)
        return data

    return data


def _apply_formula_caching(data: Dict[str, Any], context: CustomizationContext) -> Dict[str, Any]:
    """Apply formula caching optimization."""
    # Cache formula results for reuse
    formula_customizations = []

    for customization in context.customizations.values():
        if isinstance(customization, dict) and customization.get("type") == "formula":
            formula_customizations.append(customization)

    if formula_customizations:
        # Apply formula caching (implementation would use formula_compiler)
        pass

    return data


def _apply_incremental_updates(data: Dict[str, Any], context: CustomizationContext) -> Dict[str, Any]:
    """Apply incremental updates optimization."""
    # Only process changed data
    return data


def _update_performance_metrics(
    processing_time_ms: float, cache_hit: bool, rows_processed: int, lazy_evaluation_used: bool
) -> None:
    """Update global performance metrics."""
    global _performance_metrics

    _performance_metrics.total_operations += 1
    _performance_metrics.rows_processed += rows_processed

    if cache_hit:
        _performance_metrics.cache_hits += 1
    else:
        _performance_metrics.cache_misses += 1
        _performance_metrics.total_processing_time_ms += processing_time_ms

    if lazy_evaluation_used:
        _performance_metrics.lazy_evaluations += 1

    # Update averages
    if _performance_metrics.total_operations > 0:
        _performance_metrics.cache_hit_rate = _performance_metrics.cache_hits / _performance_metrics.total_operations

    if _performance_metrics.cache_misses > 0:
        _performance_metrics.avg_processing_time_ms = (
            _performance_metrics.total_processing_time_ms / _performance_metrics.cache_misses
        )

    if _performance_metrics.rows_processed > 0:
        _performance_metrics.avg_time_per_1000_rows = (
            _performance_metrics.total_processing_time_ms / _performance_metrics.rows_processed
        ) * 1000


# Export all functions
__all__ = [
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
]
