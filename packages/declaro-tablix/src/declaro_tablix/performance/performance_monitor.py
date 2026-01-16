"""
Performance monitoring system for TableV2 customizations.

This module provides real-time monitoring, alerting, and performance tracking
for formula evaluation, cache performance, and system health.
"""

import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from declaro_advise import error, info, success, warning


class AlertSeverity(Enum):
    """Alert severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class PerformanceAlert:
    """Performance alert definition."""

    alert_id: str
    severity: AlertSeverity
    title: str
    message: str
    timestamp: float
    metric_name: str
    threshold_value: float
    actual_value: float
    resolved: bool = False
    resolved_timestamp: Optional[float] = None


@dataclass
class PerformanceThreshold:
    """Performance threshold configuration."""

    metric_name: str
    threshold_value: float
    comparison_operator: str  # 'gt', 'lt', 'eq', 'gte', 'lte'
    severity: AlertSeverity
    enabled: bool = True
    alert_cooldown_seconds: int = 300  # 5 minutes


@dataclass
class MetricValue:
    """Individual metric value with timestamp."""

    value: float
    timestamp: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PerformanceMetrics:
    """Comprehensive performance metrics."""

    # Formula metrics
    formula_evaluation_times: deque = field(default_factory=lambda: deque(maxlen=1000))
    formula_cache_hit_rate: float = 0.0
    formula_cache_hits: int = 0
    formula_cache_misses: int = 0
    hot_formula_count: int = 0

    # Customization metrics
    customization_overhead_ms: deque = field(default_factory=lambda: deque(maxlen=1000))
    customization_cache_hit_rate: float = 0.0
    lazy_evaluation_usage: int = 0

    # System metrics
    memory_usage_mb: deque = field(default_factory=lambda: deque(maxlen=100))
    cpu_usage_percent: deque = field(default_factory=lambda: deque(maxlen=100))
    concurrent_operations: int = 0

    # Performance targets
    avg_formula_time_ms: float = 0.0
    avg_customization_time_per_1000_rows: float = 0.0

    # Alert metrics
    active_alerts: int = 0
    total_alerts: int = 0


class PerformanceMonitor:
    """Performance monitoring system."""

    def __init__(self):
        self.metrics = PerformanceMetrics()
        self.thresholds: Dict[str, PerformanceThreshold] = {}
        self.alerts: List[PerformanceAlert] = []
        self.metric_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.alert_callbacks: List[Callable[[PerformanceAlert], None]] = []
        self.monitoring_enabled = False
        self.monitoring_thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()

        # Initialize default thresholds
        self._initialize_default_thresholds()

    def start_monitoring(self) -> None:
        """Start the performance monitoring system."""
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            warning("Performance monitoring already started")
            return

        self.monitoring_enabled = True
        self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitoring_thread.start()

        success("Performance monitoring started")

    def stop_monitoring(self) -> None:
        """Stop the performance monitoring system."""
        self.monitoring_enabled = False

        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)

        success("Performance monitoring stopped")

    def record_formula_evaluation(self, evaluation_time_ms: float, cache_hit: bool, formula_hash: str) -> None:
        """Record formula evaluation metrics."""
        with self.lock:
            self.metrics.formula_evaluation_times.append(
                MetricValue(
                    value=evaluation_time_ms,
                    timestamp=time.time(),
                    metadata={"formula_hash": formula_hash, "cache_hit": cache_hit},
                )
            )

            if cache_hit:
                self.metrics.formula_cache_hits += 1
            else:
                self.metrics.formula_cache_misses += 1

            # Update cache hit rate
            total_evaluations = self.metrics.formula_cache_hits + self.metrics.formula_cache_misses
            if total_evaluations > 0:
                self.metrics.formula_cache_hit_rate = self.metrics.formula_cache_hits / total_evaluations

            # Update average
            if self.metrics.formula_evaluation_times:
                recent_times = [m.value for m in list(self.metrics.formula_evaluation_times)[-100:]]
                self.metrics.avg_formula_time_ms = sum(recent_times) / len(recent_times)

        # Check thresholds
        self._check_threshold("formula_evaluation_time", evaluation_time_ms)
        self._check_threshold("formula_cache_hit_rate", self.metrics.formula_cache_hit_rate)

    def record_customization_performance(self, processing_time_ms: float, row_count: int, cache_hit: bool) -> None:
        """Record customization performance metrics."""
        with self.lock:
            # Calculate time per 1000 rows
            time_per_1000_rows = (processing_time_ms / row_count) * 1000 if row_count > 0 else 0

            self.metrics.customization_overhead_ms.append(
                MetricValue(
                    value=time_per_1000_rows,
                    timestamp=time.time(),
                    metadata={"row_count": row_count, "cache_hit": cache_hit},
                )
            )

            # Update average
            if self.metrics.customization_overhead_ms:
                recent_times = [m.value for m in list(self.metrics.customization_overhead_ms)[-100:]]
                self.metrics.avg_customization_time_per_1000_rows = sum(recent_times) / len(recent_times)

        # Check thresholds
        self._check_threshold("customization_overhead", time_per_1000_rows)

    def record_system_metrics(self, memory_usage_mb: float, cpu_usage_percent: float) -> None:
        """Record system performance metrics."""
        with self.lock:
            self.metrics.memory_usage_mb.append(MetricValue(value=memory_usage_mb, timestamp=time.time()))

            self.metrics.cpu_usage_percent.append(MetricValue(value=cpu_usage_percent, timestamp=time.time()))

        # Check thresholds
        self._check_threshold("memory_usage", memory_usage_mb)
        self._check_threshold("cpu_usage", cpu_usage_percent)

    def increment_concurrent_operations(self) -> None:
        """Increment concurrent operations counter."""
        with self.lock:
            self.metrics.concurrent_operations += 1

        self._check_threshold("concurrent_operations", self.metrics.concurrent_operations)

    def decrement_concurrent_operations(self) -> None:
        """Decrement concurrent operations counter."""
        with self.lock:
            self.metrics.concurrent_operations = max(0, self.metrics.concurrent_operations - 1)

    def add_threshold(self, threshold: PerformanceThreshold) -> None:
        """Add a performance threshold."""
        with self.lock:
            self.thresholds[threshold.metric_name] = threshold

        success(f"Added performance threshold for {threshold.metric_name}")

    def remove_threshold(self, metric_name: str) -> bool:
        """Remove a performance threshold."""
        with self.lock:
            removed = self.thresholds.pop(metric_name, None) is not None

        if removed:
            success(f"Removed performance threshold for {metric_name}")
        else:
            warning(f"No threshold found for {metric_name}")

        return removed

    def add_alert_callback(self, callback: Callable[[PerformanceAlert], None]) -> None:
        """Add an alert callback function."""
        self.alert_callbacks.append(callback)

    def get_current_metrics(self) -> PerformanceMetrics:
        """Get current performance metrics."""
        with self.lock:
            return self.metrics

    def get_metric_history(self, metric_name: str, limit: int = 100) -> List[MetricValue]:
        """Get historical data for a specific metric."""
        with self.lock:
            history = self.metric_history[metric_name]
            return list(history)[-limit:]

    def get_active_alerts(self) -> List[PerformanceAlert]:
        """Get list of active alerts."""
        with self.lock:
            return [alert for alert in self.alerts if not alert.resolved]

    def get_all_alerts(self, limit: int = 100) -> List[PerformanceAlert]:
        """Get all alerts (active and resolved)."""
        with self.lock:
            return self.alerts[-limit:]

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an active alert."""
        with self.lock:
            for alert in self.alerts:
                if alert.alert_id == alert_id and not alert.resolved:
                    alert.resolved = True
                    alert.resolved_timestamp = time.time()
                    self.metrics.active_alerts -= 1
                    success(f"Resolved alert {alert_id}")
                    return True

        warning(f"Alert {alert_id} not found or already resolved")
        return False

    def clear_alerts(self) -> int:
        """Clear all resolved alerts."""
        with self.lock:
            resolved_count = len([alert for alert in self.alerts if alert.resolved])
            self.alerts = [alert for alert in self.alerts if not alert.resolved]

        success(f"Cleared {resolved_count} resolved alerts")
        return resolved_count

    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary."""
        with self.lock:
            summary = {
                "timestamp": time.time(),
                "metrics": {
                    "formula_performance": {
                        "avg_evaluation_time_ms": self.metrics.avg_formula_time_ms,
                        "cache_hit_rate": self.metrics.formula_cache_hit_rate,
                        "hot_formula_count": self.metrics.hot_formula_count,
                    },
                    "customization_performance": {
                        "avg_time_per_1000_rows": self.metrics.avg_customization_time_per_1000_rows,
                        "cache_hit_rate": self.metrics.customization_cache_hit_rate,
                        "lazy_evaluation_usage": self.metrics.lazy_evaluation_usage,
                    },
                    "system_performance": {
                        "concurrent_operations": self.metrics.concurrent_operations,
                        "memory_usage_mb": (
                            list(self.metrics.memory_usage_mb)[-1].value if self.metrics.memory_usage_mb else 0
                        ),
                        "cpu_usage_percent": (
                            list(self.metrics.cpu_usage_percent)[-1].value if self.metrics.cpu_usage_percent else 0
                        ),
                    },
                },
                "alerts": {
                    "active_count": self.metrics.active_alerts,
                    "total_count": self.metrics.total_alerts,
                    "active_alerts": [
                        {
                            "id": alert.alert_id,
                            "severity": alert.severity.value,
                            "title": alert.title,
                            "metric": alert.metric_name,
                            "threshold": alert.threshold_value,
                            "actual": alert.actual_value,
                        }
                        for alert in self.alerts
                        if not alert.resolved
                    ],
                },
                "performance_assessment": self._assess_performance(),
            }

        return summary

    def _initialize_default_thresholds(self) -> None:
        """Initialize default performance thresholds."""
        default_thresholds = [
            PerformanceThreshold(
                metric_name="formula_evaluation_time",
                threshold_value=5.0,  # 5ms
                comparison_operator="gt",
                severity=AlertSeverity.WARNING,
            ),
            PerformanceThreshold(
                metric_name="customization_overhead",
                threshold_value=10.0,  # 10ms per 1000 rows
                comparison_operator="gt",
                severity=AlertSeverity.ERROR,
            ),
            PerformanceThreshold(
                metric_name="formula_cache_hit_rate",
                threshold_value=0.7,  # 70%
                comparison_operator="lt",
                severity=AlertSeverity.WARNING,
            ),
            PerformanceThreshold(
                metric_name="memory_usage",
                threshold_value=1000.0,  # 1GB
                comparison_operator="gt",
                severity=AlertSeverity.WARNING,
            ),
            PerformanceThreshold(
                metric_name="cpu_usage",
                threshold_value=80.0,  # 80%
                comparison_operator="gt",
                severity=AlertSeverity.WARNING,
            ),
            PerformanceThreshold(
                metric_name="concurrent_operations",
                threshold_value=100,  # 100 concurrent operations
                comparison_operator="gt",
                severity=AlertSeverity.WARNING,
            ),
        ]

        for threshold in default_thresholds:
            self.thresholds[threshold.metric_name] = threshold

    def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self.monitoring_enabled:
            try:
                # Collect system metrics
                self._collect_system_metrics()

                # Check for stale alerts
                self._check_stale_alerts()

                # Sleep for monitoring interval
                time.sleep(30)  # 30 seconds

            except Exception as e:
                error(f"Error in monitoring loop: {str(e)}")
                time.sleep(30)

    def _collect_system_metrics(self) -> None:
        """Collect system performance metrics."""
        try:
            import psutil

            # Get memory usage
            memory_info = psutil.virtual_memory()
            memory_usage_mb = memory_info.used / (1024 * 1024)

            # Get CPU usage
            cpu_usage = psutil.cpu_percent(interval=1)

            self.record_system_metrics(memory_usage_mb, cpu_usage)

        except ImportError:
            # psutil not available, use placeholder values
            self.record_system_metrics(0.0, 0.0)
        except Exception as e:
            warning(f"Failed to collect system metrics: {str(e)}")

    def _check_threshold(self, metric_name: str, value: float) -> None:
        """Check if a metric value exceeds threshold."""
        threshold = self.thresholds.get(metric_name)
        if not threshold or not threshold.enabled:
            return

        # Check if threshold is exceeded
        threshold_exceeded = False

        if threshold.comparison_operator == "gt":
            threshold_exceeded = value > threshold.threshold_value
        elif threshold.comparison_operator == "lt":
            threshold_exceeded = value < threshold.threshold_value
        elif threshold.comparison_operator == "gte":
            threshold_exceeded = value >= threshold.threshold_value
        elif threshold.comparison_operator == "lte":
            threshold_exceeded = value <= threshold.threshold_value
        elif threshold.comparison_operator == "eq":
            threshold_exceeded = value == threshold.threshold_value

        if threshold_exceeded:
            # Check for alert cooldown
            recent_alerts = [
                alert
                for alert in self.alerts
                if alert.metric_name == metric_name
                and not alert.resolved
                and time.time() - alert.timestamp < threshold.alert_cooldown_seconds
            ]

            if not recent_alerts:
                self._create_alert(threshold, value)

    def _create_alert(self, threshold: PerformanceThreshold, actual_value: float) -> None:
        """Create a new performance alert."""
        alert_id = f"{threshold.metric_name}_{int(time.time())}"

        alert = PerformanceAlert(
            alert_id=alert_id,
            severity=threshold.severity,
            title=f"Performance threshold exceeded: {threshold.metric_name}",
            message=f"Metric {threshold.metric_name} value {actual_value:.2f} exceeds threshold {threshold.threshold_value:.2f}",
            timestamp=time.time(),
            metric_name=threshold.metric_name,
            threshold_value=threshold.threshold_value,
            actual_value=actual_value,
        )

        with self.lock:
            self.alerts.append(alert)
            self.metrics.active_alerts += 1
            self.metrics.total_alerts += 1

        # Send notifications
        if threshold.severity == AlertSeverity.CRITICAL:
            error(alert.message)
        elif threshold.severity == AlertSeverity.ERROR:
            error(alert.message)
        elif threshold.severity == AlertSeverity.WARNING:
            warning(alert.message)
        else:
            info(alert.message)

        # Call alert callbacks
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                warning(f"Alert callback failed: {str(e)}")

    def _check_stale_alerts(self) -> None:
        """Check for stale alerts that should be auto-resolved."""
        current_time = time.time()

        with self.lock:
            for alert in self.alerts:
                if not alert.resolved and current_time - alert.timestamp > 3600:  # 1 hour
                    alert.resolved = True
                    alert.resolved_timestamp = current_time
                    self.metrics.active_alerts -= 1

    def _assess_performance(self) -> str:
        """Assess overall performance health."""
        if self.metrics.active_alerts == 0:
            return "excellent"

        critical_alerts = len([a for a in self.alerts if not a.resolved and a.severity == AlertSeverity.CRITICAL])
        error_alerts = len([a for a in self.alerts if not a.resolved and a.severity == AlertSeverity.ERROR])

        if critical_alerts > 0:
            return "critical"
        elif error_alerts > 0:
            return "poor"
        elif self.metrics.active_alerts > 5:
            return "fair"
        else:
            return "good"


# Global performance monitor instance
_performance_monitor = PerformanceMonitor()


def get_performance_monitor() -> PerformanceMonitor:
    """Get the global performance monitor instance."""
    return _performance_monitor


def start_performance_monitoring() -> None:
    """Start performance monitoring."""
    _performance_monitor.start_monitoring()


def stop_performance_monitoring() -> None:
    """Stop performance monitoring."""
    _performance_monitor.stop_monitoring()


# Export all functions and classes
__all__ = [
    "PerformanceMonitor",
    "PerformanceAlert",
    "PerformanceThreshold",
    "AlertSeverity",
    "MetricValue",
    "PerformanceMetrics",
    "get_performance_monitor",
    "start_performance_monitoring",
    "stop_performance_monitoring",
]
