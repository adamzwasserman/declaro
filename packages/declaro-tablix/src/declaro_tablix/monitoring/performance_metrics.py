"""
Performance metrics collection module for TableV2 monitoring system.

This module provides comprehensive performance monitoring capabilities that aggregate
metrics from async processing, parallel optimization, and general table operations.
"""

import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from declaro_advise import error, info, success, warning
from declaro_tablix.performance.async_processing import get_async_performance_metrics
from declaro_tablix.performance.parallel_optimization import get_parallel_performance_metrics

# Global performance metrics storage
_performance_metrics = {
    "operation_counts": defaultdict(int),
    "operation_durations": defaultdict(list),
    "error_counts": defaultdict(int),
    "success_counts": defaultdict(int),
    "memory_usage": [],
    "cpu_usage": [],
    "last_reset": datetime.utcnow(),
    "total_operations": 0,
    "peak_memory_usage": 0.0,
    "peak_cpu_usage": 0.0,
    "average_response_time": 0.0,
    "throughput_per_second": 0.0,
}

# Thread-local storage for request-specific metrics
_thread_local = threading.local()
_metrics_lock = threading.Lock()


def track_operation_start(operation_name: str, **context) -> str:
    """
    Start tracking an operation's performance.

    Args:
        operation_name: Name of the operation being tracked
        **context: Additional context information

    Returns:
        Tracking ID for the operation
    """
    tracking_id = f"{operation_name}_{int(time.time() * 1000000)}"

    if not hasattr(_thread_local, "active_operations"):
        _thread_local.active_operations = {}

    _thread_local.active_operations[tracking_id] = {
        "operation_name": operation_name,
        "start_time": time.time(),
        "context": context,
        "thread_id": threading.get_ident(),
    }

    with _metrics_lock:
        _performance_metrics["operation_counts"][operation_name] += 1
        _performance_metrics["total_operations"] += 1

    return tracking_id


def track_operation_end(tracking_id: str, success: bool = True, error_message: str = None, **metrics) -> Dict[str, Any]:
    """
    End tracking an operation and record its metrics.

    Args:
        tracking_id: ID returned from track_operation_start
        success: Whether the operation succeeded
        error_message: Error message if operation failed
        **metrics: Additional metrics to record

    Returns:
        Operation performance summary
    """
    if not hasattr(_thread_local, "active_operations"):
        return {"error": "No active operations found"}

    operation_data = _thread_local.active_operations.get(tracking_id)
    if not operation_data:
        return {"error": f"Operation {tracking_id} not found"}

    # Calculate duration
    end_time = time.time()
    duration = end_time - operation_data["start_time"]
    operation_name = operation_data["operation_name"]

    with _metrics_lock:
        # Record duration
        _performance_metrics["operation_durations"][operation_name].append(duration)

        # Record success/error
        if success:
            _performance_metrics["success_counts"][operation_name] += 1
        else:
            _performance_metrics["error_counts"][operation_name] += 1

        # Update average response time
        total_durations = sum(sum(durations) for durations in _performance_metrics["operation_durations"].values())
        total_ops = _performance_metrics["total_operations"]
        _performance_metrics["average_response_time"] = total_durations / total_ops if total_ops > 0 else 0

    # Clean up
    del _thread_local.active_operations[tracking_id]

    result = {
        "tracking_id": tracking_id,
        "operation_name": operation_name,
        "duration": duration,
        "success": success,
        "error_message": error_message,
        "context": operation_data["context"],
        "metrics": metrics,
    }

    info(f"Operation {operation_name} completed in {duration:.3f}s (success: {success})")

    return result


def record_system_metrics(memory_usage_mb: float, cpu_usage_percent: float) -> None:
    """
    Record system-level performance metrics.

    Args:
        memory_usage_mb: Current memory usage in MB
        cpu_usage_percent: Current CPU usage percentage
    """
    with _metrics_lock:
        # Store recent metrics (keep last 100 measurements)
        _performance_metrics["memory_usage"].append({"timestamp": datetime.utcnow(), "value": memory_usage_mb})
        _performance_metrics["cpu_usage"].append({"timestamp": datetime.utcnow(), "value": cpu_usage_percent})

        # Keep only recent measurements
        if len(_performance_metrics["memory_usage"]) > 100:
            _performance_metrics["memory_usage"] = _performance_metrics["memory_usage"][-100:]
        if len(_performance_metrics["cpu_usage"]) > 100:
            _performance_metrics["cpu_usage"] = _performance_metrics["cpu_usage"][-100:]

        # Update peak values
        _performance_metrics["peak_memory_usage"] = max(_performance_metrics["peak_memory_usage"], memory_usage_mb)
        _performance_metrics["peak_cpu_usage"] = max(_performance_metrics["peak_cpu_usage"], cpu_usage_percent)


def get_operation_statistics(operation_name: str = None) -> Dict[str, Any]:
    """
    Get statistics for a specific operation or all operations.

    Args:
        operation_name: Name of operation to get stats for (None for all)

    Returns:
        Operation statistics
    """
    with _metrics_lock:
        if operation_name:
            # Stats for specific operation
            durations = _performance_metrics["operation_durations"].get(operation_name, [])
            return {
                "operation_name": operation_name,
                "total_executions": _performance_metrics["operation_counts"][operation_name],
                "successful_executions": _performance_metrics["success_counts"][operation_name],
                "failed_executions": _performance_metrics["error_counts"][operation_name],
                "success_rate": (
                    _performance_metrics["success_counts"][operation_name]
                    / _performance_metrics["operation_counts"][operation_name]
                    if _performance_metrics["operation_counts"][operation_name] > 0
                    else 0
                ),
                "average_duration": sum(durations) / len(durations) if durations else 0,
                "min_duration": min(durations) if durations else 0,
                "max_duration": max(durations) if durations else 0,
                "total_duration": sum(durations),
            }
        else:
            # Stats for all operations
            all_stats = {}
            for op_name in _performance_metrics["operation_counts"].keys():
                all_stats[op_name] = get_operation_statistics(op_name)
            return all_stats


def get_system_performance_summary() -> Dict[str, Any]:
    """
    Get comprehensive system performance summary.

    Returns:
        System performance metrics
    """
    with _metrics_lock:
        uptime = datetime.utcnow() - _performance_metrics["last_reset"]

        # Calculate throughput
        throughput = _performance_metrics["total_operations"] / uptime.total_seconds() if uptime.total_seconds() > 0 else 0

        # Get recent memory and CPU averages
        recent_memory = _performance_metrics["memory_usage"][-10:] if _performance_metrics["memory_usage"] else []
        recent_cpu = _performance_metrics["cpu_usage"][-10:] if _performance_metrics["cpu_usage"] else []

        avg_memory = sum(m["value"] for m in recent_memory) / len(recent_memory) if recent_memory else 0
        avg_cpu = sum(c["value"] for c in recent_cpu) / len(recent_cpu) if recent_cpu else 0

        return {
            "uptime": {
                "seconds": uptime.total_seconds(),
                "human_readable": str(uptime),
                "started_at": _performance_metrics["last_reset"].isoformat(),
            },
            "operations": {
                "total_operations": _performance_metrics["total_operations"],
                "operations_per_second": round(throughput, 2),
                "average_response_time": round(_performance_metrics["average_response_time"], 3),
                "total_success": sum(_performance_metrics["success_counts"].values()),
                "total_errors": sum(_performance_metrics["error_counts"].values()),
                "overall_success_rate": (
                    sum(_performance_metrics["success_counts"].values()) / _performance_metrics["total_operations"]
                    if _performance_metrics["total_operations"] > 0
                    else 0
                ),
            },
            "system_resources": {
                "current_memory_mb": round(avg_memory, 2),
                "peak_memory_mb": round(_performance_metrics["peak_memory_usage"], 2),
                "current_cpu_percent": round(avg_cpu, 2),
                "peak_cpu_percent": round(_performance_metrics["peak_cpu_usage"], 2),
                "memory_samples": len(_performance_metrics["memory_usage"]),
                "cpu_samples": len(_performance_metrics["cpu_usage"]),
            },
            "operation_breakdown": dict(_performance_metrics["operation_counts"]),
        }


def get_integrated_performance_metrics() -> Dict[str, Any]:
    """
    Get integrated performance metrics from all subsystems.

    Returns:
        Comprehensive performance metrics from all modules
    """
    try:
        # Get metrics from async processing module
        async_metrics = get_async_performance_metrics()

        # Get metrics from parallel optimization module
        parallel_metrics = get_parallel_performance_metrics()

        # Get local monitoring metrics
        monitoring_metrics = get_system_performance_summary()

        # Get operation statistics
        operation_stats = get_operation_statistics()

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "async_processing": async_metrics,
            "parallel_optimization": parallel_metrics,
            "monitoring_system": monitoring_metrics,
            "operation_statistics": operation_stats,
            "health_status": calculate_health_status(monitoring_metrics, async_metrics, parallel_metrics),
        }

    except Exception as e:
        error(f"Error getting integrated performance metrics: {str(e)}")
        return {
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
        }


def calculate_health_status(monitoring_metrics: Dict, async_metrics: Dict, parallel_metrics: Dict) -> Dict[str, Any]:
    """
    Calculate overall system health status based on metrics.

    Args:
        monitoring_metrics: Monitoring system metrics
        async_metrics: Async processing metrics
        parallel_metrics: Parallel optimization metrics

    Returns:
        Health status assessment
    """
    health_score = 100
    issues = []

    # Check success rate
    success_rate = monitoring_metrics["operations"]["overall_success_rate"]
    if success_rate < 0.95:
        health_score -= 20
        issues.append(f"Low success rate: {success_rate:.1%}")

    # Check response time
    avg_response_time = monitoring_metrics["operations"]["average_response_time"]
    if avg_response_time > 5.0:
        health_score -= 15
        issues.append(f"High response time: {avg_response_time:.2f}s")

    # Check memory usage
    current_memory = monitoring_metrics["system_resources"]["current_memory_mb"]
    peak_memory = monitoring_metrics["system_resources"]["peak_memory_mb"]
    if current_memory > 1000:  # 1GB
        health_score -= 10
        issues.append(f"High memory usage: {current_memory:.1f}MB")

    # Check CPU usage
    current_cpu = monitoring_metrics["system_resources"]["current_cpu_percent"]
    if current_cpu > 80:
        health_score -= 15
        issues.append(f"High CPU usage: {current_cpu:.1f}%")

    # Check throughput
    throughput = monitoring_metrics["operations"]["operations_per_second"]
    if throughput < 1:
        health_score -= 10
        issues.append(f"Low throughput: {throughput:.2f} ops/sec")

    # Determine status
    if health_score >= 90:
        status = "healthy"
    elif health_score >= 70:
        status = "warning"
    else:
        status = "critical"

    return {
        "status": status,
        "score": max(0, health_score),
        "issues": issues,
        "recommendations": generate_recommendations(issues),
        "last_updated": datetime.utcnow().isoformat(),
    }


def generate_recommendations(issues: List[str]) -> List[str]:
    """
    Generate recommendations based on identified issues.

    Args:
        issues: List of identified performance issues

    Returns:
        List of recommendations
    """
    recommendations = []

    for issue in issues:
        if "success rate" in issue:
            recommendations.append("Review error logs and fix failing operations")
        elif "response time" in issue:
            recommendations.append("Optimize slow operations or increase parallel processing")
        elif "memory usage" in issue:
            recommendations.append("Implement memory optimization or increase system memory")
        elif "CPU usage" in issue:
            recommendations.append("Scale horizontally or optimize CPU-intensive operations")
        elif "throughput" in issue:
            recommendations.append("Increase concurrency or optimize processing pipeline")

    return recommendations


def reset_performance_metrics() -> None:
    """Reset all performance metrics to initial state."""
    with _metrics_lock:
        _performance_metrics.update(
            {
                "operation_counts": defaultdict(int),
                "operation_durations": defaultdict(list),
                "error_counts": defaultdict(int),
                "success_counts": defaultdict(int),
                "memory_usage": [],
                "cpu_usage": [],
                "last_reset": datetime.utcnow(),
                "total_operations": 0,
                "peak_memory_usage": 0.0,
                "peak_cpu_usage": 0.0,
                "average_response_time": 0.0,
                "throughput_per_second": 0.0,
            }
        )

    info("Performance metrics reset")


def cleanup_old_metrics(hours_to_keep: int = 24) -> None:
    """
    Clean up old metrics data to prevent memory leaks.

    Args:
        hours_to_keep: Number of hours of data to keep
    """
    cutoff_time = datetime.utcnow() - timedelta(hours=hours_to_keep)

    with _metrics_lock:
        # Clean up memory usage data
        _performance_metrics["memory_usage"] = [
            m for m in _performance_metrics["memory_usage"] if m["timestamp"] > cutoff_time
        ]

        # Clean up CPU usage data
        _performance_metrics["cpu_usage"] = [c for c in _performance_metrics["cpu_usage"] if c["timestamp"] > cutoff_time]

        # Clean up operation durations (keep only recent data)
        for operation_name in _performance_metrics["operation_durations"]:
            durations = _performance_metrics["operation_durations"][operation_name]
            if len(durations) > 1000:  # Keep only last 1000 measurements
                _performance_metrics["operation_durations"][operation_name] = durations[-1000:]

    info(f"Cleaned up metrics older than {hours_to_keep} hours")


def performance_monitoring_decorator(operation_name: str):
    """
    Decorator to automatically track operation performance.

    Args:
        operation_name: Name of the operation being decorated

    Returns:
        Decorator function
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            tracking_id = track_operation_start(operation_name, args=len(args), kwargs=list(kwargs.keys()))

            try:
                result = func(*args, **kwargs)
                track_operation_end(tracking_id, success=True)
                return result
            except Exception as e:
                track_operation_end(tracking_id, success=False, error_message=str(e))
                raise

        return wrapper

    return decorator


# FastAPI dependency injection functions
def get_performance_metrics_for_dependency() -> Dict[str, callable]:
    """FastAPI dependency for performance metrics functions."""
    return {
        "track_operation_start": track_operation_start,
        "track_operation_end": track_operation_end,
        "record_system_metrics": record_system_metrics,
        "get_operation_statistics": get_operation_statistics,
        "get_system_performance_summary": get_system_performance_summary,
        "get_integrated_performance_metrics": get_integrated_performance_metrics,
        "reset_performance_metrics": reset_performance_metrics,
        "cleanup_old_metrics": cleanup_old_metrics,
    }


def get_performance_tracker_for_dependency() -> callable:
    """FastAPI dependency for performance tracking decorator."""
    return performance_monitoring_decorator


def get_performance_summary_for_dependency() -> callable:
    """FastAPI dependency for performance summary."""
    return get_integrated_performance_metrics
