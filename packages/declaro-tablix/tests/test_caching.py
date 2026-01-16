"""Tests for Redis caching layer.

BDD Scenarios based on Gherkin specification:
  Feature: Redis caching for sorted table data
    As a tablix user
    I want sorted data cached per user
    So that repeated requests are fast

Tests updated to match tableV2 function-based API.
"""

from unittest.mock import MagicMock, patch

import pytest

from declaro_tablix.caching import (
    DEFAULT_TTL,
    generate_cache_key,
    smart_cache_get,
    smart_cache_set,
    intelligent_cache_invalidation,
)


class TestCacheSortedDataWithTTL:
    """Scenario: Cache sorted data with TTL."""

    def test_cache_key_includes_user_and_table(self) -> None:
        """Result is cached with key containing table and user."""
        key = generate_cache_key(
            table_name="holdings",
            user_id="user123",
            operation="data",
            sort_column="name",
            sort_direction="asc",
        )
        assert "holdings" in key
        assert "user123" in key

    def test_cache_key_includes_operation(self) -> None:
        """Operation type is included in key."""
        key = generate_cache_key("holdings", "user123", "data")
        assert "data" in key

    def test_cache_key_different_for_different_sort(self) -> None:
        """Sort parameters create different keys."""
        key1 = generate_cache_key("t", "u", "data", sort_column="name", sort_direction="asc")
        key2 = generate_cache_key("t", "u", "data", sort_column="name", sort_direction="desc")
        assert key1 != key2

    def test_default_ttl_for_table_data(self) -> None:
        """Default TTL for table_data is 300 seconds."""
        assert DEFAULT_TTL["table_data"] == 300


class TestCacheOperations:
    """Scenario: Cache operations work with Redis."""

    @patch("declaro_tablix.caching.cache_service.get_redis_client")
    def test_smart_cache_get_returns_none_without_redis(self, mock_get_client: MagicMock) -> None:
        """Returns None when Redis not connected."""
        mock_get_client.return_value = None
        result = smart_cache_get("test_key")
        assert result is None

    @patch("declaro_tablix.caching.cache_service.get_redis_client")
    def test_smart_cache_set_returns_false_without_redis(self, mock_get_client: MagicMock) -> None:
        """Returns False when trying to cache without Redis."""
        mock_get_client.return_value = None
        result = smart_cache_set("key", {"data": "test"})
        assert result is False


class TestPerUserCacheIsolation:
    """Scenario: Per-user cache isolation."""

    def test_different_users_have_separate_keys(self) -> None:
        """Each user has separate cache entries."""
        key_a = generate_cache_key("holdings", "userA", "data")
        key_b = generate_cache_key("holdings", "userB", "data")

        assert key_a != key_b
        assert "userA" in key_a
        assert "userB" in key_b


class TestCacheInvalidation:
    """Scenario: Cache invalidation on data change."""

    @patch("declaro_tablix.caching.cache_service.invalidate_cache_pattern")
    @patch("declaro_tablix.caching.cache_service.schedule_cache_refresh")
    @patch("declaro_tablix.caching.cache_service.generate_cache_key")
    def test_intelligent_invalidation_data_change(
        self,
        mock_generate_key: MagicMock,
        mock_schedule: MagicMock,
        mock_invalidate: MagicMock,
    ) -> None:
        """Data changes invalidate data caches and schedule refresh."""
        mock_invalidate.return_value = 3
        mock_generate_key.return_value = "test_key"
        mock_schedule.return_value = True

        result = intelligent_cache_invalidation(
            table_name="holdings",
            user_id="user123",
            operation_type="data_change",
        )

        assert result is True
        assert mock_invalidate.called

    @patch("declaro_tablix.caching.cache_service.invalidate_cache_pattern")
    @patch("declaro_tablix.caching.cache_service.schedule_cache_refresh")
    @patch("declaro_tablix.caching.cache_service.generate_cache_key")
    def test_intelligent_invalidation_config_change(
        self,
        mock_generate_key: MagicMock,
        mock_schedule: MagicMock,
        mock_invalidate: MagicMock,
    ) -> None:
        """Config changes invalidate config and data caches."""
        mock_invalidate.return_value = 2
        mock_generate_key.return_value = "test_key"
        mock_schedule.return_value = True

        result = intelligent_cache_invalidation(
            table_name="holdings",
            user_id="user123",
            operation_type="config_change",
        )

        assert result is True


class TestCacheKeyGeneration:
    """Test cache key generation with various parameters."""

    def test_cache_key_with_kwargs(self) -> None:
        """Additional kwargs are included in key."""
        key = generate_cache_key(
            "table",
            "user",
            "data",
            page=1,
            page_size=25,
        )
        assert "page" in key
        assert "page_size" in key

    def test_cache_key_consistent(self) -> None:
        """Same parameters produce same key."""
        key1 = generate_cache_key("table", "user", "data", page=1)
        key2 = generate_cache_key("table", "user", "data", page=1)
        assert key1 == key2

    def test_cache_key_kwarg_order_independent(self) -> None:
        """Kwargs order doesn't affect key."""
        key1 = generate_cache_key("t", "u", "data", a=1, b=2)
        key2 = generate_cache_key("t", "u", "data", b=2, a=1)
        assert key1 == key2
