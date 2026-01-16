"""
Advanced caching service for TableV2 with Redis integration.

This module provides comprehensive caching functionality including cache warming,
performance monitoring, and intelligent cache invalidation strategies.
"""

import json
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Dict, List, Optional

from declaro_advise import error, info, success, warning
from declaro_tablix.domain.models import TableConfig, TableData
from declaro_tablix.repositories.cache_repository import (
    cache_query_result,
    cache_table_config,
    cache_table_data,
    generate_cache_key,
    get_cache_stats,
    get_cached_query_result,
    get_cached_table_config,
    get_cached_table_data,
    get_redis_client,
    initialize_cache_connection,
    invalidate_cache_pattern,
)

# Cache performance metrics
_cache_metrics = {
    "hits": 0,
    "misses": 0,
    "operations": 0,
    "total_time": 0.0,
    "warming_operations": 0,
    "invalidation_operations": 0,
    "last_reset": datetime.utcnow(),
}

# Cache warming priorities
CACHE_PRIORITIES = {
    "critical": 1,
    "high": 2,
    "medium": 3,
    "low": 4,
}

# Default TTL values for different cache types
DEFAULT_TTL = {
    "table_data": 300,  # 5 minutes
    "table_config": 3600,  # 1 hour
    "query_results": 180,  # 3 minutes
    "aggregations": 600,  # 10 minutes
    "user_preferences": 7200,  # 2 hours
}


def cache_performance_decorator(func):
    """Decorator to track cache performance metrics."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        _cache_metrics["operations"] += 1

        try:
            result = func(*args, **kwargs)

            # Track hits/misses based on result
            if func.__name__.startswith("get_") and result is not None:
                _cache_metrics["hits"] += 1
            elif func.__name__.startswith("get_") and result is None:
                _cache_metrics["misses"] += 1

            return result

        finally:
            _cache_metrics["total_time"] += time.time() - start_time

    return wrapper


def initialize_caching_service(
    host: Optional[str] = None,
    port: int = 6379,
    db: int = 1,  # Use different DB for caching
    password: Optional[str] = None,
    max_connections: int = 30,
) -> bool:
    """Initialize caching service with Redis connection.

    Args:
        host: Redis server host
        port: Redis server port
        db: Redis database number (default: 1 for caching)
        password: Redis password (optional)
        max_connections: Maximum connection pool size

    Returns:
        True if initialization successful, False otherwise
    """
    try:
        info(f"Initializing advanced caching service on Redis {host}:{port}/{db}")

        # Initialize base cache connection
        if not initialize_cache_connection(host, port, db, password, max_connections):
            return False

        # Reset metrics
        reset_cache_metrics()

        # Test advanced functionality
        client = get_redis_client()
        if not client:
            return False

        # Set up cache warming pipeline
        if not setup_cache_warming_pipeline():
            warning("Cache warming pipeline setup failed, continuing without warming")

        success("Advanced caching service initialized successfully")
        return True

    except Exception as e:
        error(f"Error initializing caching service: {str(e)}")
        return False


def setup_cache_warming_pipeline() -> bool:
    """Set up cache warming pipeline for proactive data loading.

    Returns:
        True if setup successful, False otherwise
    """
    try:
        client = get_redis_client()
        if not client:
            return False

        # Create warming queue
        warming_queue = "cache_warming_queue"

        # Initialize warming queue if it doesn't exist
        if not client.exists(warming_queue):
            client.lpush(
                warming_queue,
                json.dumps(
                    {
                        "type": "initialization",
                        "timestamp": datetime.utcnow().isoformat(),
                        "priority": CACHE_PRIORITIES["medium"],
                    }
                ),
            )

        info("Cache warming pipeline initialized")
        return True

    except Exception as e:
        error(f"Error setting up cache warming pipeline: {str(e)}")
        return False


@cache_performance_decorator
def smart_cache_get(key: str, cache_type: str = "table_data", refresh_threshold: float = 0.8) -> Optional[Any]:
    """Smart cache retrieval with automatic refresh detection.

    Args:
        key: Cache key to retrieve
        cache_type: Type of cache data
        refresh_threshold: Threshold for TTL refresh (0.0-1.0)

    Returns:
        Cached data or None if not found
    """
    try:
        client = get_redis_client()
        if not client:
            return None

        # Get cached data based on type
        if cache_type == "table_data":
            cached_data = get_cached_table_data(key)
        elif cache_type == "table_config":
            cached_data = get_cached_table_config(key)
        elif cache_type == "query_results":
            cached_data = get_cached_query_result(key)
        else:
            # Generic cache retrieval
            full_key = f"{cache_type}:{key}"
            cached_raw = client.get(full_key)
            if cached_raw:
                cached_data = json.loads(cached_raw.decode("utf-8"))
            else:
                cached_data = None

        if cached_data is not None:
            # Check TTL for refresh scheduling
            full_key = f"{cache_type}:{key}"
            ttl = client.ttl(full_key)
            default_ttl = DEFAULT_TTL.get(cache_type, 300)

            if ttl > 0 and ttl < (default_ttl * refresh_threshold):
                # Schedule background refresh
                schedule_cache_refresh(key, cache_type, "low")

        return cached_data

    except Exception as e:
        error(f"Error in smart cache retrieval: {str(e)}")
        return None


@cache_performance_decorator
def smart_cache_set(
    key: str, data: Any, cache_type: str = "table_data", ttl_seconds: Optional[int] = None, priority: str = "medium"
) -> bool:
    """Smart cache storage with priority-based TTL.

    Args:
        key: Cache key
        data: Data to cache
        cache_type: Type of cache data
        ttl_seconds: Custom TTL (uses default if None)
        priority: Cache priority (affects TTL)

    Returns:
        True if successful, False otherwise
    """
    try:
        # Determine TTL based on priority and type
        if ttl_seconds is None:
            base_ttl = DEFAULT_TTL.get(cache_type, 300)
            priority_multiplier = {"critical": 2.0, "high": 1.5, "medium": 1.0, "low": 0.5}.get(priority, 1.0)
            ttl_seconds = int(base_ttl * priority_multiplier)

        # Cache based on type
        if cache_type == "table_data" and isinstance(data, TableData):
            result = cache_table_data(key, data, ttl_seconds)
        elif cache_type == "table_config" and isinstance(data, TableConfig):
            result = cache_table_config(key, data, ttl_seconds)
        elif cache_type == "query_results" and isinstance(data, list):
            result = cache_query_result(key, data, ttl_seconds)
        else:
            # Generic cache storage
            client = get_redis_client()
            if not client:
                return False

            full_key = f"{cache_type}:{key}"
            data_json = json.dumps(data, default=str)
            result = client.setex(full_key, ttl_seconds, data_json)

        if result:
            info(f"Cached {cache_type} with key '{key}', TTL: {ttl_seconds}s, priority: {priority}")

        return result

    except Exception as e:
        error(f"Error in smart cache storage: {str(e)}")
        return False


def schedule_cache_refresh(key: str, cache_type: str, priority: str = "medium") -> bool:
    """Schedule background cache refresh.

    Args:
        key: Cache key to refresh
        cache_type: Type of cache data
        priority: Refresh priority

    Returns:
        True if scheduled successfully, False otherwise
    """
    try:
        client = get_redis_client()
        if not client:
            return False

        # Create refresh task
        refresh_task = {
            "key": key,
            "cache_type": cache_type,
            "priority": CACHE_PRIORITIES.get(priority, 3),
            "timestamp": datetime.utcnow().isoformat(),
            "scheduled_for": (datetime.utcnow() + timedelta(seconds=30)).isoformat(),
        }

        # Add to warming queue
        queue_key = "cache_warming_queue"
        client.lpush(queue_key, json.dumps(refresh_task))

        _cache_metrics["warming_operations"] += 1
        info(f"Scheduled cache refresh for key '{key}' with priority {priority}")
        return True

    except Exception as e:
        error(f"Error scheduling cache refresh: {str(e)}")
        return False


def warm_cache_for_user(user_id: str, table_names: List[str], operations: List[str] = None) -> Dict[str, bool]:
    """Warm cache for specific user and tables.

    Args:
        user_id: User ID to warm cache for
        table_names: List of table names to warm
        operations: List of operations to warm (default: common operations)

    Returns:
        Dictionary of warming results by table
    """
    if operations is None:
        operations = ["data", "config", "stats", "schema"]

    results = {}

    try:
        info(f"Warming cache for user '{user_id}' with {len(table_names)} tables")

        for table_name in table_names:
            table_results = []

            for operation in operations:
                cache_key = generate_cache_key(table_name, user_id, operation)

                # Check if already cached
                cached_data = smart_cache_get(cache_key, operation)
                if cached_data is not None:
                    table_results.append(True)
                    continue

                # Schedule warming
                warming_scheduled = schedule_cache_refresh(cache_key, operation, "high")
                table_results.append(warming_scheduled)

            results[table_name] = all(table_results)

        success(f"Cache warming completed for user '{user_id}'")
        return results

    except Exception as e:
        error(f"Error warming cache for user: {str(e)}")
        return {table: False for table in table_names}


def intelligent_cache_invalidation(
    table_name: str, user_id: str, operation_type: str = "data_change", affected_columns: List[str] = None
) -> bool:
    """Intelligent cache invalidation based on operation type.

    Args:
        table_name: Name of the table
        user_id: User ID
        operation_type: Type of operation (data_change, config_change, schema_change)
        affected_columns: List of affected columns (for targeted invalidation)

    Returns:
        True if invalidation successful, False otherwise
    """
    try:
        info(f"Intelligent cache invalidation for table '{table_name}', user '{user_id}', operation: {operation_type}")

        # Define invalidation patterns based on operation type
        invalidation_patterns = []

        if operation_type == "data_change":
            # Data changes affect data caches and aggregations
            invalidation_patterns.extend(
                [
                    f"table_data:*{table_name}*{user_id}*",
                    f"query_results:*{table_name}*{user_id}*",
                    f"aggregations:*{table_name}*{user_id}*",
                ]
            )

        elif operation_type == "config_change":
            # Config changes affect configs and potentially data display
            invalidation_patterns.extend(
                [
                    f"table_config:*{table_name}*{user_id}*",
                    f"table_data:*{table_name}*{user_id}*",  # May affect formatting
                ]
            )

        elif operation_type == "schema_change":
            # Schema changes affect everything
            invalidation_patterns.extend(
                [
                    f"table_data:*{table_name}*{user_id}*",
                    f"table_config:*{table_name}*{user_id}*",
                    f"query_results:*{table_name}*{user_id}*",
                    f"aggregations:*{table_name}*{user_id}*",
                ]
            )

        elif operation_type == "user_preference_change":
            # User preference changes affect display but not data
            invalidation_patterns.extend(
                [
                    f"user_preferences:*{table_name}*{user_id}*",
                    f"table_data:*{table_name}*{user_id}*",  # May affect display
                ]
            )

        # Execute invalidation
        total_invalidated = 0
        for pattern in invalidation_patterns:
            count = invalidate_cache_pattern(pattern)
            total_invalidated += count

        _cache_metrics["invalidation_operations"] += 1

        # Schedule re-warming for critical data
        if operation_type in ["data_change", "config_change"]:
            schedule_cache_refresh(generate_cache_key(table_name, user_id, "data"), "table_data", "high")

        success(f"Intelligent invalidation completed: {total_invalidated} entries invalidated")
        return True

    except Exception as e:
        error(f"Error in intelligent cache invalidation: {str(e)}")
        return False


def get_cache_health_metrics() -> Dict[str, Any]:
    """Get comprehensive cache health and performance metrics.

    Returns:
        Dictionary with cache health metrics
    """
    try:
        info("Retrieving cache health metrics")

        # Get base Redis stats
        redis_stats = get_cache_stats()

        # Calculate performance metrics
        total_operations = _cache_metrics["operations"]
        hit_rate = (_cache_metrics["hits"] / total_operations * 100) if total_operations > 0 else 0
        miss_rate = (_cache_metrics["misses"] / total_operations * 100) if total_operations > 0 else 0
        avg_response_time = (_cache_metrics["total_time"] / total_operations * 1000) if total_operations > 0 else 0

        # Get warming queue status
        client = get_redis_client()
        warming_queue_size = 0
        if client:
            warming_queue_size = client.llen("cache_warming_queue")

        # Calculate uptime
        uptime = datetime.utcnow() - _cache_metrics["last_reset"]

        health_metrics = {
            "performance": {
                "hit_rate_percent": round(hit_rate, 2),
                "miss_rate_percent": round(miss_rate, 2),
                "total_operations": total_operations,
                "avg_response_time_ms": round(avg_response_time, 2),
                "warming_operations": _cache_metrics["warming_operations"],
                "invalidation_operations": _cache_metrics["invalidation_operations"],
            },
            "redis_stats": redis_stats,
            "warming": {
                "queue_size": warming_queue_size,
                "status": "active" if warming_queue_size > 0 else "idle",
            },
            "uptime": {
                "seconds": uptime.total_seconds(),
                "human_readable": str(uptime),
            },
            "health_status": "healthy" if hit_rate > 50 and avg_response_time < 10 else "degraded",
        }

        success("Cache health metrics retrieved successfully")
        return health_metrics

    except Exception as e:
        error(f"Error retrieving cache health metrics: {str(e)}")
        return {"error": str(e), "health_status": "unhealthy"}


def optimize_cache_performance() -> Dict[str, Any]:
    """Optimize cache performance by analyzing usage patterns.

    Returns:
        Dictionary with optimization results
    """
    try:
        info("Optimizing cache performance")

        client = get_redis_client()
        if not client:
            return {"error": "Redis not connected"}

        optimizations = []

        # Check for keys with very short TTL
        all_keys = client.keys("*")
        short_ttl_keys = []

        for key in all_keys[:100]:  # Sample first 100 keys
            ttl = client.ttl(key)
            if 0 < ttl < 60:  # Less than 1 minute
                short_ttl_keys.append(key.decode("utf-8"))

        if short_ttl_keys:
            optimizations.append(
                {
                    "type": "short_ttl_keys",
                    "count": len(short_ttl_keys),
                    "recommendation": "Consider increasing TTL for frequently accessed keys",
                    "keys": short_ttl_keys[:10],  # Show first 10 as examples
                }
            )

        # Check hit rate and recommend warming
        metrics = get_cache_health_metrics()
        hit_rate = metrics.get("performance", {}).get("hit_rate_percent", 0)

        if hit_rate < 50:
            optimizations.append(
                {
                    "type": "low_hit_rate",
                    "current_rate": hit_rate,
                    "recommendation": "Enable cache warming for frequently accessed data",
                    "action": "increase_warming",
                }
            )

        # Check memory usage
        if "redis_stats" in metrics:
            redis_info = metrics["redis_stats"]
            if "memory" in redis_info:
                optimizations.append(
                    {
                        "type": "memory_usage",
                        "current_usage": redis_info["memory"],
                        "recommendation": "Monitor memory usage and consider eviction policies",
                        "action": "monitor_memory",
                    }
                )

        optimization_result = {
            "optimizations_found": len(optimizations),
            "optimizations": optimizations,
            "current_performance": metrics.get("performance", {}),
            "timestamp": datetime.utcnow().isoformat(),
        }

        success(f"Cache performance optimization completed: {len(optimizations)} recommendations")
        return optimization_result

    except Exception as e:
        error(f"Error optimizing cache performance: {str(e)}")
        return {"error": str(e)}


def reset_cache_metrics() -> bool:
    """Reset cache performance metrics.

    Returns:
        True if reset successful, False otherwise
    """
    try:
        global _cache_metrics

        _cache_metrics = {
            "hits": 0,
            "misses": 0,
            "operations": 0,
            "total_time": 0.0,
            "warming_operations": 0,
            "invalidation_operations": 0,
            "last_reset": datetime.utcnow(),
        }

        info("Cache metrics reset successfully")
        return True

    except Exception as e:
        error(f"Error resetting cache metrics: {str(e)}")
        return False


def generate_cache_report() -> Dict[str, Any]:
    """Generate comprehensive cache performance report.

    Returns:
        Dictionary with cache report data
    """
    try:
        info("Generating cache performance report")

        health_metrics = get_cache_health_metrics()
        optimization_results = optimize_cache_performance()

        report = {
            "report_timestamp": datetime.utcnow().isoformat(),
            "health_metrics": health_metrics,
            "optimization_analysis": optimization_results,
            "recommendations": [],
            "summary": {
                "status": health_metrics.get("health_status", "unknown"),
                "total_operations": _cache_metrics["operations"],
                "uptime_hours": round(_cache_metrics["total_time"] / 3600, 2),
            },
        }

        # Generate recommendations based on metrics
        hit_rate = health_metrics.get("performance", {}).get("hit_rate_percent", 0)
        avg_response_time = health_metrics.get("performance", {}).get("avg_response_time_ms", 0)

        if hit_rate < 60:
            report["recommendations"].append(
                {
                    "priority": "high",
                    "category": "performance",
                    "recommendation": "Implement cache warming for frequently accessed data",
                    "impact": "Improve hit rate by 20-30%",
                }
            )

        if avg_response_time > 5:
            report["recommendations"].append(
                {
                    "priority": "medium",
                    "category": "performance",
                    "recommendation": "Optimize cache key structure and Redis configuration",
                    "impact": "Reduce average response time by 50%",
                }
            )

        warming_queue_size = health_metrics.get("warming", {}).get("queue_size", 0)
        if warming_queue_size > 100:
            report["recommendations"].append(
                {
                    "priority": "medium",
                    "category": "warming",
                    "recommendation": "Increase cache warming worker capacity",
                    "impact": "Reduce warming queue backlog",
                }
            )

        success("Cache performance report generated successfully")
        return report

    except Exception as e:
        error(f"Error generating cache report: {str(e)}")
        return {"error": str(e)}


# FastAPI Dependency Injection Functions
def get_cache_service_for_dependency() -> Dict[str, callable]:
    """FastAPI dependency for cache service operations."""
    return {
        "smart_cache_get": smart_cache_get,
        "smart_cache_set": smart_cache_set,
        "schedule_cache_refresh": schedule_cache_refresh,
        "warm_cache_for_user": warm_cache_for_user,
        "intelligent_cache_invalidation": intelligent_cache_invalidation,
        "get_cache_health_metrics": get_cache_health_metrics,
        "optimize_cache_performance": optimize_cache_performance,
        "generate_cache_report": generate_cache_report,
        "reset_cache_metrics": reset_cache_metrics,
    }


def get_smart_cache_for_dependency() -> callable:
    """FastAPI dependency for smart cache operations."""
    return smart_cache_get


def get_cache_health_for_dependency() -> callable:
    """FastAPI dependency for cache health monitoring."""
    return get_cache_health_metrics


def get_cache_warming_for_dependency() -> callable:
    """FastAPI dependency for cache warming operations."""
    return warm_cache_for_user
