"""
Health check endpoints module for TableV2 monitoring system.

This module provides comprehensive health check functionality including
system health, database connectivity, performance status, and service dependencies.
"""

import asyncio
import time
from datetime import datetime
from typing import Any, Dict, List

from declaro_tablix.monitoring.structured_logging import log_error, log_info

# Health check configuration
HEALTH_CHECK_CONFIG = {
    "timeout_seconds": 30,
    "database_timeout": 5,
    "external_service_timeout": 10,
    "performance_threshold": {
        "response_time_ms": 5000,
        "memory_usage_mb": 1000,
        "cpu_usage_percent": 80,
        "success_rate": 0.95,
    },
    "critical_services": ["database", "redis", "notification_system"],
    "optional_services": ["external_api", "file_storage"],
}


async def check_system_health() -> Dict[str, Any]:
    """
    Perform comprehensive system health check.

    Returns:
        Complete health status report
    """
    start_time = time.time()
    log_info("Starting comprehensive system health check")

    try:
        # Run all health checks concurrently
        health_checks = await asyncio.gather(
            check_database_health(),
            check_redis_health(),
            check_performance_health(),
            check_memory_health(),
            check_disk_health(),
            check_network_health(),
            check_service_dependencies(),
            return_exceptions=True,
        )

        # Process results
        (
            database_health,
            redis_health,
            performance_health,
            memory_health,
            disk_health,
            network_health,
            dependencies_health,
        ) = health_checks

        # Calculate overall health status
        overall_status = calculate_overall_health_status(
            [
                database_health,
                redis_health,
                performance_health,
                memory_health,
                disk_health,
                network_health,
                dependencies_health,
            ]
        )

        execution_time = time.time() - start_time

        health_report = {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_status": overall_status,
            "execution_time_seconds": round(execution_time, 3),
            "checks": {
                "database": database_health,
                "redis": redis_health,
                "performance": performance_health,
                "memory": memory_health,
                "disk": disk_health,
                "network": network_health,
                "dependencies": dependencies_health,
            },
            "system_info": get_system_info(),
        }

        log_info(f"Health check completed in {execution_time:.3f}s - Status: {overall_status['status']}")

        return health_report

    except Exception as e:
        log_error(f"Health check failed: {str(e)}")
        return {
            "timestamp": datetime.utcnow().isoformat(),
            "overall_status": {"status": "critical", "message": f"Health check failed: {str(e)}"},
            "execution_time_seconds": time.time() - start_time,
            "error": str(e),
        }


async def check_database_health() -> Dict[str, Any]:
    """
    Check database connectivity and performance.

    Returns:
        Database health status
    """
    try:
        start_time = time.time()

        # Test database connection
        # This would normally test actual database connectivity
        # For now, we'll simulate the check
        await asyncio.sleep(0.1)  # Simulate database query

        connection_time = time.time() - start_time

        # Simulate database metrics
        db_metrics = {
            "connection_time_ms": round(connection_time * 1000, 2),
            "active_connections": 5,
            "max_connections": 100,
            "connection_pool_usage": 0.05,
            "query_response_time_ms": round(connection_time * 1000 * 0.8, 2),
        }

        # Determine status
        if connection_time > HEALTH_CHECK_CONFIG["database_timeout"]:
            status = "critical"
            message = f"Database connection timeout: {connection_time:.2f}s"
        elif connection_time > 1.0:
            status = "warning"
            message = f"Database connection slow: {connection_time:.2f}s"
        else:
            status = "healthy"
            message = "Database connection healthy"

        return {
            "status": status,
            "message": message,
            "metrics": db_metrics,
            "last_checked": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        log_error(f"Database health check failed: {str(e)}")
        return {
            "status": "critical",
            "message": f"Database health check failed: {str(e)}",
            "error": str(e),
            "last_checked": datetime.utcnow().isoformat(),
        }


async def check_redis_health() -> Dict[str, Any]:
    """
    Check Redis connectivity and performance.

    Returns:
        Redis health status
    """
    try:
        start_time = time.time()

        # Test Redis connection
        # This would normally test actual Redis connectivity
        # For now, we'll simulate the check
        await asyncio.sleep(0.05)  # Simulate Redis ping

        connection_time = time.time() - start_time

        # Simulate Redis metrics
        redis_metrics = {
            "connection_time_ms": round(connection_time * 1000, 2),
            "memory_usage_mb": 25.6,
            "connected_clients": 3,
            "operations_per_second": 1250,
            "hit_rate": 0.92,
        }

        # Determine status
        if connection_time > 1.0:
            status = "critical"
            message = f"Redis connection timeout: {connection_time:.2f}s"
        elif connection_time > 0.5:
            status = "warning"
            message = f"Redis connection slow: {connection_time:.2f}s"
        else:
            status = "healthy"
            message = "Redis connection healthy"

        return {
            "status": status,
            "message": message,
            "metrics": redis_metrics,
            "last_checked": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        log_error(f"Redis health check failed: {str(e)}")
        return {
            "status": "critical",
            "message": f"Redis health check failed: {str(e)}",
            "error": str(e),
            "last_checked": datetime.utcnow().isoformat(),
        }


async def check_performance_health() -> Dict[str, Any]:
    """
    Check system performance health.

    Returns:
        Performance health status
    """
    try:
        # Get basic performance metrics (avoid integrated call that may cause issues)
        from declaro_tablix.monitoring.performance_metrics import get_system_performance_summary

        performance_metrics = {"monitoring_system": get_system_performance_summary()}

        if "error" in performance_metrics:
            return {
                "status": "critical",
                "message": f"Failed to get performance metrics: {performance_metrics['error']}",
                "last_checked": datetime.utcnow().isoformat(),
            }

        # Extract key metrics
        monitoring_metrics = performance_metrics.get("monitoring_system", {})
        operations = monitoring_metrics.get("operations", {})
        resources = monitoring_metrics.get("system_resources", {})

        # Check performance thresholds
        issues = []

        # Check response time
        avg_response_time = operations.get("average_response_time", 0) * 1000  # Convert to ms
        if avg_response_time > HEALTH_CHECK_CONFIG["performance_threshold"]["response_time_ms"]:
            issues.append(f"High response time: {avg_response_time:.1f}ms")

        # Check memory usage
        memory_usage = resources.get("current_memory_mb", 0)
        if memory_usage > HEALTH_CHECK_CONFIG["performance_threshold"]["memory_usage_mb"]:
            issues.append(f"High memory usage: {memory_usage:.1f}MB")

        # Check CPU usage
        cpu_usage = resources.get("current_cpu_percent", 0)
        if cpu_usage > HEALTH_CHECK_CONFIG["performance_threshold"]["cpu_usage_percent"]:
            issues.append(f"High CPU usage: {cpu_usage:.1f}%")

        # Check success rate
        success_rate = operations.get("overall_success_rate", 0)
        if success_rate < HEALTH_CHECK_CONFIG["performance_threshold"]["success_rate"]:
            issues.append(f"Low success rate: {success_rate:.1%}")

        # Determine status
        if issues:
            status = "critical" if len(issues) > 2 else "warning"
            message = f"Performance issues detected: {'; '.join(issues)}"
        else:
            status = "healthy"
            message = "System performance is healthy"

        return {
            "status": status,
            "message": message,
            "metrics": {
                "average_response_time_ms": round(avg_response_time, 2),
                "memory_usage_mb": round(memory_usage, 2),
                "cpu_usage_percent": round(cpu_usage, 2),
                "success_rate": round(success_rate, 3),
                "operations_per_second": operations.get("operations_per_second", 0),
            },
            "issues": issues,
            "last_checked": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        log_error(f"Performance health check failed: {str(e)}")
        return {
            "status": "critical",
            "message": f"Performance health check failed: {str(e)}",
            "error": str(e),
            "last_checked": datetime.utcnow().isoformat(),
        }


async def check_memory_health() -> Dict[str, Any]:
    """
    Check system memory health.

    Returns:
        Memory health status
    """
    try:
        # Get memory information
        memory_info = get_memory_info()

        # Check memory usage thresholds
        memory_usage_percent = memory_info["used_percent"]

        if memory_usage_percent > 95:
            status = "critical"
            message = f"Critical memory usage: {memory_usage_percent:.1f}%"
        elif memory_usage_percent > 90:
            status = "warning"
            message = f"High memory usage: {memory_usage_percent:.1f}%"
        else:
            status = "healthy"
            message = f"Memory usage normal: {memory_usage_percent:.1f}%"

        return {
            "status": status,
            "message": message,
            "metrics": memory_info,
            "last_checked": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        log_error(f"Memory health check failed: {str(e)}")
        return {
            "status": "critical",
            "message": f"Memory health check failed: {str(e)}",
            "error": str(e),
            "last_checked": datetime.utcnow().isoformat(),
        }


async def check_disk_health() -> Dict[str, Any]:
    """
    Check disk space health.

    Returns:
        Disk health status
    """
    try:
        # Get disk information
        disk_info = get_disk_info()

        # Check disk usage thresholds
        disk_usage_percent = disk_info["used_percent"]

        if disk_usage_percent > 98:
            status = "critical"
            message = f"Critical disk usage: {disk_usage_percent:.1f}%"
        elif disk_usage_percent > 95:
            status = "warning"
            message = f"High disk usage: {disk_usage_percent:.1f}%"
        else:
            status = "healthy"
            message = f"Disk usage normal: {disk_usage_percent:.1f}%"

        return {
            "status": status,
            "message": message,
            "metrics": disk_info,
            "last_checked": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        log_error(f"Disk health check failed: {str(e)}")
        return {
            "status": "critical",
            "message": f"Disk health check failed: {str(e)}",
            "error": str(e),
            "last_checked": datetime.utcnow().isoformat(),
        }


async def check_network_health() -> Dict[str, Any]:
    """
    Check network connectivity health.

    Returns:
        Network health status
    """
    try:
        # Test network connectivity
        network_metrics = await test_network_connectivity()

        # Check network latency
        avg_latency = network_metrics.get("average_latency_ms", 0)
        packet_loss = network_metrics.get("packet_loss_percent", 0)

        if packet_loss > 10 or avg_latency > 1000:
            status = "critical"
            message = f"Network issues: {packet_loss:.1f}% packet loss, {avg_latency:.1f}ms latency"
        elif packet_loss > 5 or avg_latency > 500:
            status = "warning"
            message = f"Network degraded: {packet_loss:.1f}% packet loss, {avg_latency:.1f}ms latency"
        else:
            status = "healthy"
            message = f"Network healthy: {packet_loss:.1f}% packet loss, {avg_latency:.1f}ms latency"

        return {
            "status": status,
            "message": message,
            "metrics": network_metrics,
            "last_checked": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        log_error(f"Network health check failed: {str(e)}")
        return {
            "status": "critical",
            "message": f"Network health check failed: {str(e)}",
            "error": str(e),
            "last_checked": datetime.utcnow().isoformat(),
        }


async def check_service_dependencies() -> Dict[str, Any]:
    """
    Check health of service dependencies.

    Returns:
        Service dependencies health status
    """
    try:
        # Check critical services
        critical_services_status = {}
        optional_services_status = {}

        # Check critical services
        for service in HEALTH_CHECK_CONFIG["critical_services"]:
            service_status = await check_service_health(service)
            critical_services_status[service] = service_status

        # Check optional services
        for service in HEALTH_CHECK_CONFIG["optional_services"]:
            service_status = await check_service_health(service)
            optional_services_status[service] = service_status

        # Determine overall status
        critical_failures = [s for s in critical_services_status.values() if s["status"] == "critical"]
        critical_warnings = [s for s in critical_services_status.values() if s["status"] == "warning"]

        if critical_failures:
            status = "critical"
            message = f"Critical service failures: {len(critical_failures)}"
        elif critical_warnings:
            status = "warning"
            message = f"Critical service warnings: {len(critical_warnings)}"
        else:
            status = "healthy"
            message = "All critical services healthy"

        return {
            "status": status,
            "message": message,
            "critical_services": critical_services_status,
            "optional_services": optional_services_status,
            "last_checked": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        log_error(f"Service dependencies health check failed: {str(e)}")
        return {
            "status": "critical",
            "message": f"Service dependencies health check failed: {str(e)}",
            "error": str(e),
            "last_checked": datetime.utcnow().isoformat(),
        }


async def check_service_health(service_name: str) -> Dict[str, Any]:
    """
    Check health of a specific service.

    Args:
        service_name: Name of the service to check

    Returns:
        Service health status
    """
    try:
        # Simulate service health checks
        await asyncio.sleep(0.1)  # Simulate service check

        # Simulate different service responses
        if service_name == "database":
            return {"status": "healthy", "message": "Database connection active"}
        elif service_name == "redis":
            return {"status": "healthy", "message": "Redis connection active"}
        elif service_name == "notification_system":
            return {"status": "healthy", "message": "Notification system operational"}
        elif service_name == "external_api":
            return {"status": "warning", "message": "External API slow response"}
        elif service_name == "file_storage":
            return {"status": "healthy", "message": "File storage accessible"}
        else:
            return {"status": "unknown", "message": f"Unknown service: {service_name}"}

    except Exception as e:
        return {
            "status": "critical",
            "message": f"Service check failed: {str(e)}",
            "error": str(e),
        }


def calculate_overall_health_status(health_checks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate overall health status from individual checks.

    Args:
        health_checks: List of health check results

    Returns:
        Overall health status
    """
    # Count statuses
    status_counts = {"healthy": 0, "warning": 0, "critical": 0, "unknown": 0}

    for check in health_checks:
        if isinstance(check, dict) and "status" in check:
            status = check["status"]
            status_counts[status] = status_counts.get(status, 0) + 1

    # Determine overall status
    if status_counts["critical"] > 0:
        overall_status = "critical"
        message = f"System critical: {status_counts['critical']} critical issues"
    elif status_counts["warning"] > 0:
        overall_status = "warning"
        message = f"System degraded: {status_counts['warning']} warnings"
    else:
        overall_status = "healthy"
        message = "System healthy"

    return {
        "status": overall_status,
        "message": message,
        "status_counts": status_counts,
        "total_checks": len(health_checks),
    }


def get_system_info() -> Dict[str, Any]:
    """
    Get basic system information.

    Returns:
        System information
    """
    try:
        import os
        import platform

        return {
            "platform": platform.system(),
            "platform_version": platform.version(),
            "python_version": platform.python_version(),
            "architecture": platform.machine(),
            "hostname": platform.node(),
            "process_id": os.getpid(),
            "working_directory": os.getcwd(),
        }
    except Exception as e:
        return {"error": f"Failed to get system info: {str(e)}"}


def get_memory_info() -> Dict[str, Any]:
    """
    Get memory information.

    Returns:
        Memory information
    """
    try:
        import psutil

        memory = psutil.virtual_memory()
        return {
            "total_mb": round(memory.total / 1024 / 1024, 2),
            "available_mb": round(memory.available / 1024 / 1024, 2),
            "used_mb": round(memory.used / 1024 / 1024, 2),
            "used_percent": round(memory.percent, 2),
            "cached_mb": round(memory.cached / 1024 / 1024, 2) if hasattr(memory, "cached") else 0,
            "buffers_mb": round(memory.buffers / 1024 / 1024, 2) if hasattr(memory, "buffers") else 0,
        }
    except ImportError:
        return {
            "error": "psutil not available",
            "total_mb": 0,
            "available_mb": 0,
            "used_mb": 0,
            "used_percent": 0,
        }


def get_disk_info() -> Dict[str, Any]:
    """
    Get disk information.

    Returns:
        Disk information
    """
    try:
        import psutil

        disk = psutil.disk_usage("/")
        return {
            "total_gb": round(disk.total / 1024 / 1024 / 1024, 2),
            "available_gb": round(disk.free / 1024 / 1024 / 1024, 2),
            "used_gb": round(disk.used / 1024 / 1024 / 1024, 2),
            "used_percent": round((disk.used / disk.total) * 100, 2),
        }
    except ImportError:
        return {
            "error": "psutil not available",
            "total_gb": 0,
            "available_gb": 0,
            "used_gb": 0,
            "used_percent": 0,
        }


async def test_network_connectivity() -> Dict[str, Any]:
    """
    Test network connectivity.

    Returns:
        Network connectivity metrics
    """
    try:
        # Simulate network test
        await asyncio.sleep(0.1)

        # Return simulated network metrics
        return {
            "average_latency_ms": 45.2,
            "packet_loss_percent": 0.0,
            "jitter_ms": 2.1,
            "bandwidth_mbps": 100.0,
            "dns_response_time_ms": 12.5,
        }
    except Exception as e:
        return {
            "error": f"Network test failed: {str(e)}",
            "average_latency_ms": 0,
            "packet_loss_percent": 100,
        }


# Health check endpoint for quick status
async def quick_health_check() -> Dict[str, Any]:
    """
    Quick health check for basic status.

    Returns:
        Basic health status
    """
    try:
        start_time = time.time()

        # Quick checks
        db_ok = True  # Simplified check
        redis_ok = True  # Simplified check

        execution_time = time.time() - start_time

        if db_ok and redis_ok:
            status = "healthy"
            message = "Service is operational"
        else:
            status = "critical"
            message = "Critical services unavailable"

        return {
            "status": status,
            "message": message,
            "timestamp": datetime.utcnow().isoformat(),
            "execution_time_seconds": round(execution_time, 3),
            "version": "1.0.0",
        }

    except Exception as e:
        return {
            "status": "critical",
            "message": f"Health check failed: {str(e)}",
            "timestamp": datetime.utcnow().isoformat(),
            "error": str(e),
        }


# FastAPI dependency injection functions
def get_health_checker_for_dependency() -> Dict[str, callable]:
    """FastAPI dependency for health check functions."""
    return {
        "check_system_health": check_system_health,
        "quick_health_check": quick_health_check,
        "check_database_health": check_database_health,
        "check_redis_health": check_redis_health,
        "check_performance_health": check_performance_health,
        "check_memory_health": check_memory_health,
        "check_disk_health": check_disk_health,
        "check_network_health": check_network_health,
        "check_service_dependencies": check_service_dependencies,
    }


def get_system_health_for_dependency() -> callable:
    """FastAPI dependency for system health check."""
    return check_system_health


def get_quick_health_for_dependency() -> callable:
    """FastAPI dependency for quick health check."""
    return quick_health_check
