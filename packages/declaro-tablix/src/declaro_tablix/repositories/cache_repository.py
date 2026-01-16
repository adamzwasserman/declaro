"""Cache repository implementation for Table Module V2.

This module provides function-based Redis caching operations,
following the repository pattern with FastAPI dependency injection.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import redis
from redis.exceptions import ConnectionError, RedisError

from declaro_advise import error, info, success, warning
from declaro_tablix.domain.models import TableConfig, TableData

# Global Redis connection pool
_redis_client: Optional[redis.Redis] = None
_connection_pool: Optional[redis.ConnectionPool] = None


def initialize_cache_connection(
    host: Optional[str] = None, port: int = 6379, db: int = 0, password: Optional[str] = None, max_connections: int = 20
) -> bool:
    """Initialize Redis connection pool.

    Args:
        host: Redis server host
        port: Redis server port
        db: Redis database number
        password: Redis password (optional)
        max_connections: Maximum connection pool size

    Returns:
        True if connection successful, False otherwise
    """
    global _redis_client, _connection_pool

    # Auto-detect host if not provided
    if host is None:
        host = "redis" if os.getenv("RUNNING_IN_DOCKER") else "localhost"

    try:
        info(f"Initializing Redis connection to {host}:{port}/{db}")

        # Create connection pool
        _connection_pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
            max_connections=max_connections,
            decode_responses=False,  # We'll handle encoding ourselves
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )

        # Create Redis client with connection pool
        _redis_client = redis.Redis(connection_pool=_connection_pool)

        # Test connection
        _redis_client.ping()

        success(f"Redis connection established to {host}:{port}/{db}")
        return True

    except ConnectionError as e:
        error(f"Failed to connect to Redis: {str(e)}")
        return False
    except Exception as e:
        error(f"Error initializing Redis connection: {str(e)}")
        return False


def get_redis_client() -> Optional[redis.Redis]:
    """Get Redis client instance.

    Returns:
        Redis client or None if not connected
    """
    global _redis_client

    if _redis_client is None:
        warning("Redis client not initialized, attempting default connection")
        if not initialize_cache_connection():
            return None

    try:
        # Test connection
        _redis_client.ping()
        return _redis_client
    except (ConnectionError, RedisError) as e:
        error(f"Redis connection lost: {str(e)}")
        return None


def cache_table_data(key: str, data: TableData, ttl_seconds: int = 300) -> bool:
    """Cache table data with TTL.

    Args:
        key: Cache key for the data
        data: TableData object to cache
        ttl_seconds: Time to live in seconds (default: 5 minutes)

    Returns:
        True if cache successful, False otherwise
    """
    try:
        client = get_redis_client()
        if not client:
            return False

        info(f"Caching table data with key '{key}', TTL: {ttl_seconds}s")

        # Serialize TableData to JSON
        data_json = data.model_dump_json()

        # Cache with TTL
        result = client.setex(name=f"table_data:{key}", time=ttl_seconds, value=data_json)

        if result:
            success(f"Cached table data with key '{key}'")
            return True
        else:
            warning(f"Failed to cache table data with key '{key}'")
            return False

    except RedisError as e:
        error(f"Redis error caching table data: {str(e)}")
        return False
    except Exception as e:
        error(f"Error caching table data: {str(e)}")
        return False


def get_cached_table_data(key: str) -> Optional[TableData]:
    """Retrieve cached table data.

    Args:
        key: Cache key for the data

    Returns:
        TableData object if found, None otherwise
    """
    try:
        client = get_redis_client()
        if not client:
            return None

        info(f"Retrieving cached table data with key '{key}'")

        # Get from cache
        cached_data = client.get(f"table_data:{key}")

        if cached_data:
            # Deserialize from JSON
            data_dict = json.loads(cached_data.decode("utf-8"))
            table_data = TableData(**data_dict)

            success(f"Retrieved cached table data with key '{key}'")
            return table_data
        else:
            info(f"No cached table data found for key '{key}'")
            return None

    except (RedisError, json.JSONDecodeError) as e:
        error(f"Error retrieving cached table data: {str(e)}")
        return None
    except Exception as e:
        error(f"Error retrieving cached table data: {str(e)}")
        return None


def cache_table_config(key: str, config: TableConfig, ttl_seconds: int = 3600) -> bool:
    """Cache table configuration with TTL.

    Args:
        key: Cache key for the configuration
        config: TableConfig object to cache
        ttl_seconds: Time to live in seconds (default: 1 hour)

    Returns:
        True if cache successful, False otherwise
    """
    try:
        client = get_redis_client()
        if not client:
            return False

        info(f"Caching table config with key '{key}', TTL: {ttl_seconds}s")

        # Serialize TableConfig to JSON
        config_json = config.model_dump_json()

        # Cache with TTL
        result = client.setex(name=f"table_config:{key}", time=ttl_seconds, value=config_json)

        if result:
            success(f"Cached table config with key '{key}'")
            return True
        else:
            warning(f"Failed to cache table config with key '{key}'")
            return False

    except RedisError as e:
        error(f"Redis error caching table config: {str(e)}")
        return False
    except Exception as e:
        error(f"Error caching table config: {str(e)}")
        return False


def get_cached_table_config(key: str) -> Optional[TableConfig]:
    """Retrieve cached table configuration.

    Args:
        key: Cache key for the configuration

    Returns:
        TableConfig object if found, None otherwise
    """
    try:
        client = get_redis_client()
        if not client:
            return None

        info(f"Retrieving cached table config with key '{key}'")

        # Get from cache
        cached_config = client.get(f"table_config:{key}")

        if cached_config:
            # Deserialize from JSON
            config_dict = json.loads(cached_config.decode("utf-8"))
            table_config = TableConfig(**config_dict)

            success(f"Retrieved cached table config with key '{key}'")
            return table_config
        else:
            info(f"No cached table config found for key '{key}'")
            return None

    except (RedisError, json.JSONDecodeError) as e:
        error(f"Error retrieving cached table config: {str(e)}")
        return None
    except Exception as e:
        error(f"Error retrieving cached table config: {str(e)}")
        return None


def cache_query_result(key: str, result: List[Dict[str, Any]], ttl_seconds: int = 300) -> bool:
    """Cache arbitrary query results.

    Args:
        key: Cache key for the result
        result: Query result to cache
        ttl_seconds: Time to live in seconds (default: 5 minutes)

    Returns:
        True if cache successful, False otherwise
    """
    try:
        client = get_redis_client()
        if not client:
            return False

        info(f"Caching query result with key '{key}', TTL: {ttl_seconds}s")

        # Serialize result to JSON
        result_json = json.dumps(result, default=str)  # default=str handles datetime objects

        # Cache with TTL
        result = client.setex(name=f"query_result:{key}", time=ttl_seconds, value=result_json)

        if result:
            success(f"Cached query result with key '{key}'")
            return True
        else:
            warning(f"Failed to cache query result with key '{key}'")
            return False

    except RedisError as e:
        error(f"Redis error caching query result: {str(e)}")
        return False
    except Exception as e:
        error(f"Error caching query result: {str(e)}")
        return False


def get_cached_query_result(key: str) -> Optional[List[Dict[str, Any]]]:
    """Retrieve cached query results.

    Args:
        key: Cache key for the result

    Returns:
        Query result list if found, None otherwise
    """
    try:
        client = get_redis_client()
        if not client:
            return None

        info(f"Retrieving cached query result with key '{key}'")

        # Get from cache
        cached_result = client.get(f"query_result:{key}")

        if cached_result:
            # Deserialize from JSON
            result = json.loads(cached_result.decode("utf-8"))

            success(f"Retrieved cached query result with key '{key}'")
            return result
        else:
            info(f"No cached query result found for key '{key}'")
            return None

    except (RedisError, json.JSONDecodeError) as e:
        error(f"Error retrieving cached query result: {str(e)}")
        return None
    except Exception as e:
        error(f"Error retrieving cached query result: {str(e)}")
        return None


def invalidate_table_cache(table_name: str, user_id: str) -> bool:
    """Invalidate all cache entries for table and user.

    Args:
        table_name: Name of the table
        user_id: User ID to invalidate cache for

    Returns:
        True if invalidation successful, False otherwise
    """
    try:
        client = get_redis_client()
        if not client:
            return False

        info(f"Invalidating cache for table '{table_name}', user '{user_id}'")

        # Build patterns to match
        patterns = [
            f"table_data:*{table_name}*{user_id}*",
            f"table_config:*{table_name}*{user_id}*",
            f"query_result:*{table_name}*{user_id}*",
        ]

        total_deleted = 0
        for pattern in patterns:
            deleted = invalidate_cache_pattern(pattern)
            total_deleted += deleted

        success(f"Invalidated {total_deleted} cache entries for table '{table_name}', user '{user_id}'")
        return True

    except RedisError as e:
        error(f"Redis error invalidating table cache: {str(e)}")
        return False
    except Exception as e:
        error(f"Error invalidating table cache: {str(e)}")
        return False


def invalidate_cache_pattern(pattern: str) -> int:
    """Invalidate cache entries matching pattern.

    Args:
        pattern: Redis pattern to match (supports * wildcards)

    Returns:
        Number of keys deleted
    """
    try:
        client = get_redis_client()
        if not client:
            return 0

        info(f"Invalidating cache entries matching pattern '{pattern}'")

        # Find keys matching pattern
        keys = client.keys(pattern)

        if keys:
            # Delete all matching keys
            deleted = client.delete(*keys)
            success(f"Deleted {deleted} cache entries matching pattern '{pattern}'")
            return deleted
        else:
            info(f"No cache entries found matching pattern '{pattern}'")
            return 0

    except RedisError as e:
        error(f"Redis error invalidating cache pattern: {str(e)}")
        return 0
    except Exception as e:
        error(f"Error invalidating cache pattern: {str(e)}")
        return 0


def get_cache_stats() -> Dict[str, Any]:
    """Get cache performance statistics.

    Returns:
        Dictionary with cache statistics
    """
    try:
        client = get_redis_client()
        if not client:
            return {"error": "Redis not connected"}

        info("Retrieving cache statistics")

        # Get Redis info
        redis_info = client.info()

        # Get key counts by type
        table_data_keys = len(client.keys("table_data:*"))
        table_config_keys = len(client.keys("table_config:*"))
        query_result_keys = len(client.keys("query_result:*"))

        # Get memory usage
        memory_used = redis_info.get("used_memory_human", "Unknown")
        memory_peak = redis_info.get("used_memory_peak_human", "Unknown")

        # Get connection stats
        connected_clients = redis_info.get("connected_clients", 0)
        total_connections = redis_info.get("total_connections_received", 0)

        # Get command stats
        total_commands = redis_info.get("total_commands_processed", 0)
        ops_per_sec = redis_info.get("instantaneous_ops_per_sec", 0)

        stats = {
            "cache_keys": {
                "table_data": table_data_keys,
                "table_config": table_config_keys,
                "query_results": query_result_keys,
                "total": table_data_keys + table_config_keys + query_result_keys,
            },
            "memory": {"used": memory_used, "peak": memory_peak},
            "connections": {"current": connected_clients, "total": total_connections},
            "performance": {"total_commands": total_commands, "ops_per_second": ops_per_sec},
            "uptime_seconds": redis_info.get("uptime_in_seconds", 0),
        }

        success("Retrieved cache statistics")
        return stats

    except RedisError as e:
        error(f"Redis error getting cache stats: {str(e)}")
        return {"error": str(e)}
    except Exception as e:
        error(f"Error getting cache stats: {str(e)}")
        return {"error": str(e)}


def clear_cache() -> bool:
    """Clear all cache entries.

    Returns:
        True if successful, False otherwise
    """
    try:
        client = get_redis_client()
        if not client:
            return False

        warning("Clearing all cache entries")

        # Flush current database
        client.flushdb()

        success("Cleared all cache entries")
        return True

    except RedisError as e:
        error(f"Redis error clearing cache: {str(e)}")
        return False
    except Exception as e:
        error(f"Error clearing cache: {str(e)}")
        return False


def cleanup_cache_connection() -> bool:
    """Cleanup Redis connection and pool.

    Returns:
        True if cleanup successful, False otherwise
    """
    global _redis_client, _connection_pool

    try:
        info("Cleaning up Redis connections")

        if _redis_client:
            _redis_client.close()
            _redis_client = None

        if _connection_pool:
            _connection_pool.disconnect()
            _connection_pool = None

        success("Redis connections cleaned up")
        return True

    except Exception as e:
        error(f"Error cleaning up Redis connections: {str(e)}")
        return False


def generate_cache_key(table_name: str, user_id: str, operation: str, **kwargs) -> str:
    """Generate standardized cache key.

    Args:
        table_name: Name of the table
        user_id: User ID
        operation: Type of operation (data, config, query)
        **kwargs: Additional parameters for key generation

    Returns:
        Standardized cache key
    """
    # Build base key
    key_parts = [table_name, user_id, operation]

    # Add sorted kwargs to ensure consistent key generation
    if kwargs:
        sorted_kwargs = sorted(kwargs.items())
        for key, value in sorted_kwargs:
            key_parts.append(f"{key}:{value}")

    # Join with separator
    cache_key = ":".join(str(part) for part in key_parts)

    info(f"Generated cache key: {cache_key}")
    return cache_key


def set_cache_ttl(key: str, ttl_seconds: int) -> bool:
    """Set TTL for existing cache key.

    Args:
        key: Cache key to update
        ttl_seconds: New TTL in seconds

    Returns:
        True if successful, False otherwise
    """
    try:
        client = get_redis_client()
        if not client:
            return False

        info(f"Setting TTL for key '{key}' to {ttl_seconds}s")

        result = client.expire(key, ttl_seconds)

        if result:
            success(f"Set TTL for key '{key}' to {ttl_seconds}s")
            return True
        else:
            warning(f"Key '{key}' not found or TTL not set")
            return False

    except RedisError as e:
        error(f"Redis error setting TTL: {str(e)}")
        return False
    except Exception as e:
        error(f"Error setting TTL: {str(e)}")
        return False


def get_cache_key_ttl(key: str) -> int:
    """Get remaining TTL for cache key.

    Args:
        key: Cache key to check

    Returns:
        Remaining TTL in seconds, -1 if no TTL, -2 if key doesn't exist
    """
    try:
        client = get_redis_client()
        if not client:
            return -2

        ttl = client.ttl(key)

        if ttl >= 0:
            info(f"Key '{key}' has TTL: {ttl}s")
        elif ttl == -1:
            info(f"Key '{key}' has no TTL (persistent)")
        else:
            info(f"Key '{key}' does not exist")

        return ttl

    except RedisError as e:
        error(f"Redis error getting TTL: {str(e)}")
        return -2
    except Exception as e:
        error(f"Error getting TTL: {str(e)}")
        return -2


# FastAPI Dependency Injection Functions
def get_cache_repository_for_dependency() -> callable:
    """FastAPI dependency for cache repository operations."""
    return {
        "cache_table_data": cache_table_data,
        "get_cached_table_data": get_cached_table_data,
        "cache_table_config": cache_table_config,
        "get_cached_table_config": get_cached_table_config,
        "cache_query_result": cache_query_result,
        "get_cached_query_result": get_cached_query_result,
        "invalidate_table_cache": invalidate_table_cache,
        "invalidate_cache_pattern": invalidate_cache_pattern,
        "get_cache_stats": get_cache_stats,
        "clear_cache": clear_cache,
        "generate_cache_key": generate_cache_key,
    }


def get_cache_data_for_dependency() -> callable:
    """FastAPI dependency for cache data operations."""
    return cache_table_data


def get_cached_data_for_dependency() -> callable:
    """FastAPI dependency for cached data retrieval."""
    return get_cached_table_data


def get_cache_stats_for_dependency() -> callable:
    """FastAPI dependency for cache statistics."""
    return get_cache_stats


def get_redis_client_for_dependency() -> redis.Redis:
    """FastAPI dependency for Redis client."""
    return get_redis_client()
