"""
Cache middleware for automatic caching of TableV2 operations.

This middleware provides transparent caching for FastAPI routes and
function calls, with intelligent cache key generation and invalidation.
"""

import hashlib
import json
import time
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, List, Optional

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from declaro_advise import error, info, warning
from declaro_tablix.caching.cache_invalidation import (
    invalidate_smart,
)
from declaro_tablix.caching.cache_service import (
    smart_cache_get,
    smart_cache_set,
)


class CacheConfig:
    """Configuration for cache middleware."""

    def __init__(self):
        self.enabled: bool = True
        self.default_ttl: int = 300  # 5 minutes
        self.cache_headers: bool = True
        self.cache_get_requests: bool = True
        self.cache_post_requests: bool = False
        self.ignore_query_params: List[str] = ["timestamp", "_", "cache_bust"]
        self.cache_control_header: str = "public, max-age=300"
        self.etag_enabled: bool = True
        self.vary_headers: List[str] = ["Accept", "Accept-Encoding", "Authorization"]

        # Route-specific caching rules
        self.route_rules: Dict[str, Dict[str, Any]] = {
            "/tables/data": {
                "ttl": 180,  # 3 minutes
                "cache_post": True,
                "invalidate_on": ["data_change"],
                "key_includes": ["table_name", "user_id", "page", "per_page"],
            },
            "/tables/{table_name}/schema": {
                "ttl": 3600,  # 1 hour
                "cache_post": False,
                "invalidate_on": ["schema_change"],
                "key_includes": ["table_name", "user_id"],
            },
            "/tables/{table_name}/statistics": {
                "ttl": 600,  # 10 minutes
                "cache_post": False,
                "invalidate_on": ["data_change"],
                "key_includes": ["table_name", "user_id"],
            },
            "/tables/config": {
                "ttl": 1800,  # 30 minutes
                "cache_post": True,
                "invalidate_on": ["config_change"],
                "key_includes": ["table_name", "user_id", "config_name"],
            },
            "/tables/format": {
                "ttl": 120,  # 2 minutes
                "cache_post": True,
                "invalidate_on": ["config_change"],
                "key_includes": ["table_name", "user_id", "strategy"],
            },
        }


class TableCacheMiddleware(BaseHTTPMiddleware):
    """Middleware for automatic caching of table operations."""

    def __init__(self, app: ASGIApp, config: Optional[CacheConfig] = None):
        super().__init__(app)
        self.config = config or CacheConfig()
        self.cache_stats = {"hits": 0, "misses": 0, "bypassed": 0, "errors": 0, "total_requests": 0}

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process request through cache middleware."""
        start_time = time.time()
        self.cache_stats["total_requests"] += 1

        # Check if caching is enabled
        if not self.config.enabled:
            self.cache_stats["bypassed"] += 1
            return await call_next(request)

        # Check if route should be cached
        route_path = request.url.path
        if not self._should_cache_route(route_path, request.method):
            self.cache_stats["bypassed"] += 1
            return await call_next(request)

        try:
            # Generate cache key
            cache_key = self._generate_cache_key(request)

            # Try to get cached response
            cached_response = await self._get_cached_response(cache_key)
            if cached_response:
                self.cache_stats["hits"] += 1
                info(f"Cache hit for key: {cache_key}")
                return self._create_response_from_cache(cached_response, start_time)

            # Cache miss - proceed with request
            self.cache_stats["misses"] += 1
            info(f"Cache miss for key: {cache_key}")

            # Execute request
            response = await call_next(request)

            # Cache successful responses
            if response.status_code < 400:
                await self._cache_response(cache_key, response, route_path)

            # Add cache headers
            self._add_cache_headers(response, route_path)

            return response

        except Exception as e:
            self.cache_stats["errors"] += 1
            error(f"Cache middleware error: {str(e)}")
            # Fall back to normal request processing
            return await call_next(request)

    def _should_cache_route(self, route_path: str, method: str) -> bool:
        """Determine if route should be cached."""
        # Check if route is in table API
        if not route_path.startswith("/tables"):
            return False

        # Check method-specific rules
        if method == "GET" and not self.config.cache_get_requests:
            return False
        if method == "POST" and not self.config.cache_post_requests:
            return False

        # Check route-specific rules
        route_rule = self._get_route_rule(route_path)
        if route_rule:
            if method == "POST" and not route_rule.get("cache_post", False):
                return False

        return True

    def _get_route_rule(self, route_path: str) -> Optional[Dict[str, Any]]:
        """Get caching rule for specific route."""
        # Direct match
        if route_path in self.config.route_rules:
            return self.config.route_rules[route_path]

        # Pattern matching for parameterized routes
        for pattern, rule in self.config.route_rules.items():
            if self._match_route_pattern(route_path, pattern):
                return rule

        return None

    def _match_route_pattern(self, route_path: str, pattern: str) -> bool:
        """Match route path against pattern with parameters."""
        import re

        # Convert pattern to regex
        regex_pattern = pattern.replace("{", "(?P<").replace("}", ">[^/]+)")
        regex_pattern = f"^{regex_pattern}$"

        return bool(re.match(regex_pattern, route_path))

    def _generate_cache_key(self, request: Request) -> str:
        """Generate cache key for request."""
        # Base components
        key_components = [
            request.method,
            request.url.path,
        ]

        # Add user ID if available
        if hasattr(request.state, "uid") and request.state.uid:
            key_components.append(f"user:{request.state.uid}")

        # Add query parameters (excluding ignored ones)
        query_params = dict(request.query_params)
        for ignored_param in self.config.ignore_query_params:
            query_params.pop(ignored_param, None)

        if query_params:
            sorted_params = sorted(query_params.items())
            key_components.append(f"query:{json.dumps(sorted_params)}")

        # Add route-specific parameters
        route_rule = self._get_route_rule(request.url.path)
        if route_rule and "key_includes" in route_rule:
            for include_key in route_rule["key_includes"]:
                if include_key in query_params:
                    key_components.append(f"{include_key}:{query_params[include_key]}")

        # Add body hash for POST requests
        if request.method == "POST":
            try:
                # This would need to be implemented based on your request body handling
                # For now, we'll use a placeholder
                key_components.append("body:post_request")
            except Exception:
                pass

        # Create hash
        key_string = "|".join(key_components)
        cache_key = hashlib.md5(key_string.encode()).hexdigest()

        info(f"Generated cache key: {cache_key} for {request.method} {request.url.path}")
        return cache_key

    async def _get_cached_response(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Get cached response data."""
        try:
            cached_data = smart_cache_get(cache_key, "http_response")
            return cached_data
        except Exception as e:
            error(f"Error getting cached response: {str(e)}")
            return None

    async def _cache_response(self, cache_key: str, response: Response, route_path: str):
        """Cache response data."""
        try:
            # Don't cache if response has errors
            if response.status_code >= 400:
                return

            # Get TTL from route rule
            route_rule = self._get_route_rule(route_path)
            ttl = route_rule.get("ttl", self.config.default_ttl) if route_rule else self.config.default_ttl

            # Prepare cache data
            cache_data = {
                "status_code": response.status_code,
                "headers": dict(response.headers),
                "body": response.body,
                "timestamp": datetime.utcnow().isoformat(),
                "ttl": ttl,
            }

            # Remove problematic headers
            cache_data["headers"].pop("content-length", None)
            cache_data["headers"].pop("transfer-encoding", None)

            # Cache the response
            success_cached = smart_cache_set(cache_key, cache_data, "http_response", ttl)

            if success_cached:
                info(f"Cached response for key: {cache_key}, TTL: {ttl}s")
            else:
                warning(f"Failed to cache response for key: {cache_key}")

        except Exception as e:
            error(f"Error caching response: {str(e)}")

    def _create_response_from_cache(self, cached_data: Dict[str, Any], start_time: float) -> Response:
        """Create response from cached data."""
        # Create response
        response = Response(
            content=cached_data["body"], status_code=cached_data["status_code"], headers=cached_data["headers"]
        )

        # Add cache-specific headers
        response.headers["X-Cache"] = "HIT"
        response.headers["X-Cache-Key"] = hashlib.md5(str(cached_data).encode()).hexdigest()[:8]
        response.headers["X-Cache-Timestamp"] = cached_data["timestamp"]
        response.headers["X-Response-Time"] = f"{(time.time() - start_time) * 1000:.2f}ms"

        return response

    def _add_cache_headers(self, response: Response, route_path: str):
        """Add cache control headers to response."""
        if not self.config.cache_headers:
            return

        # Add cache control
        response.headers["Cache-Control"] = self.config.cache_control_header

        # Add ETag if enabled
        if self.config.etag_enabled:
            etag = hashlib.md5(str(response.body).encode()).hexdigest()[:16]
            response.headers["ETag"] = f'"{etag}"'

        # Add Vary headers
        if self.config.vary_headers:
            response.headers["Vary"] = ", ".join(self.config.vary_headers)

        # Add cache miss indicator
        if "X-Cache" not in response.headers:
            response.headers["X-Cache"] = "MISS"

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache middleware statistics."""
        total = self.cache_stats["total_requests"]
        hit_rate = (self.cache_stats["hits"] / total * 100) if total > 0 else 0

        return {
            "total_requests": total,
            "cache_hits": self.cache_stats["hits"],
            "cache_misses": self.cache_stats["misses"],
            "bypassed": self.cache_stats["bypassed"],
            "errors": self.cache_stats["errors"],
            "hit_rate_percent": round(hit_rate, 2),
            "middleware_enabled": self.config.enabled,
        }

    def reset_stats(self):
        """Reset cache statistics."""
        self.cache_stats = {"hits": 0, "misses": 0, "bypassed": 0, "errors": 0, "total_requests": 0}


def cache_function(
    cache_key: Optional[str] = None,
    ttl: int = 300,
    cache_type: str = "function_result",
    use_args: bool = True,
    use_kwargs: bool = True,
    exclude_args: List[str] = None,
):
    """Decorator for caching function results."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            # Generate cache key if not provided
            if cache_key is None:
                key_parts = [func.__name__]

                if use_args:
                    key_parts.extend(str(arg) for arg in args)

                if use_kwargs:
                    filtered_kwargs = kwargs.copy()
                    if exclude_args:
                        for excluded in exclude_args:
                            filtered_kwargs.pop(excluded, None)

                    if filtered_kwargs:
                        sorted_kwargs = sorted(filtered_kwargs.items())
                        key_parts.append(json.dumps(sorted_kwargs))

                key = hashlib.md5("|".join(key_parts).encode()).hexdigest()
            else:
                key = cache_key

            # Try to get cached result
            cached_result = smart_cache_get(key, cache_type)
            if cached_result is not None:
                info(f"Function cache hit for {func.__name__}")
                return cached_result

            # Execute function
            result = await func(*args, **kwargs)

            # Cache result
            smart_cache_set(key, result, cache_type, ttl)
            info(f"Function result cached for {func.__name__}")

            return result

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            # Generate cache key if not provided
            if cache_key is None:
                key_parts = [func.__name__]

                if use_args:
                    key_parts.extend(str(arg) for arg in args)

                if use_kwargs:
                    filtered_kwargs = kwargs.copy()
                    if exclude_args:
                        for excluded in exclude_args:
                            filtered_kwargs.pop(excluded, None)

                    if filtered_kwargs:
                        sorted_kwargs = sorted(filtered_kwargs.items())
                        key_parts.append(json.dumps(sorted_kwargs))

                key = hashlib.md5("|".join(key_parts).encode()).hexdigest()
            else:
                key = cache_key

            # Try to get cached result
            cached_result = smart_cache_get(key, cache_type)
            if cached_result is not None:
                info(f"Function cache hit for {func.__name__}")
                return cached_result

            # Execute function
            result = func(*args, **kwargs)

            # Cache result
            smart_cache_set(key, result, cache_type, ttl)
            info(f"Function result cached for {func.__name__}")

            return result

        # Return appropriate wrapper based on function type
        import asyncio

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


def invalidate_function_cache(func: Callable, *args, cache_key: Optional[str] = None, **kwargs):
    """Invalidate cached function result."""
    if cache_key is None:
        key_parts = [func.__name__]
        key_parts.extend(str(arg) for arg in args)

        if kwargs:
            sorted_kwargs = sorted(kwargs.items())
            key_parts.append(json.dumps(sorted_kwargs))

        key = hashlib.md5("|".join(key_parts).encode()).hexdigest()
    else:
        key = cache_key

    # Use invalidation system
    invalidate_smart("function_cache", "system", "manual", {"cache_key": key})


def create_cache_middleware(config: Optional[CacheConfig] = None) -> TableCacheMiddleware:
    """Create cache middleware instance."""
    return TableCacheMiddleware(None, config)


def get_default_cache_config() -> CacheConfig:
    """Get default cache configuration."""
    return CacheConfig()


# FastAPI Dependency Injection Functions
def get_cache_middleware_for_dependency() -> Dict[str, callable]:
    """FastAPI dependency for cache middleware functions."""
    return {
        "cache_function": cache_function,
        "invalidate_function_cache": invalidate_function_cache,
        "create_cache_middleware": create_cache_middleware,
        "get_default_cache_config": get_default_cache_config,
    }


def get_function_cache_for_dependency() -> callable:
    """FastAPI dependency for function caching."""
    return cache_function


def get_cache_invalidation_for_dependency() -> callable:
    """FastAPI dependency for cache invalidation."""
    return invalidate_function_cache
