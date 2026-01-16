"""
Cache invalidation strategies for TableV2 caching system.

This module provides sophisticated cache invalidation patterns based on
data relationships, user actions, and time-based expiration policies.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from declaro_advise import error, info, success
from declaro_tablix.repositories.cache_repository import (
    get_redis_client,
    invalidate_cache_pattern,
)


class InvalidationTrigger(Enum):
    """Types of events that trigger cache invalidation."""

    DATA_CHANGE = "data_change"
    SCHEMA_CHANGE = "schema_change"
    CONFIG_CHANGE = "config_change"
    USER_ACTION = "user_action"
    TIME_BASED = "time_based"
    DEPENDENCY_CHANGE = "dependency_change"
    MANUAL = "manual"


class InvalidationScope(Enum):
    """Scope of cache invalidation."""

    SINGLE_USER = "single_user"
    ALL_USERS = "all_users"
    TABLE_SPECIFIC = "table_specific"
    GLOBAL = "global"
    RELATED_TABLES = "related_tables"


@dataclass
class InvalidationRule:
    """Configuration for cache invalidation rules."""

    trigger: InvalidationTrigger
    scope: InvalidationScope
    pattern: str
    priority: int = 3  # 1=highest, 5=lowest
    delay_seconds: int = 0  # Delayed invalidation
    cascade: bool = True  # Whether to cascade to related caches
    condition: Optional[str] = None  # Condition for invalidation
    ttl_threshold: Optional[int] = None  # Only invalidate if TTL below threshold


class CacheInvalidationManager:
    """Manages cache invalidation strategies and rules."""

    def __init__(self):
        self.rules: List[InvalidationRule] = []
        self.dependency_graph: Dict[str, Set[str]] = {}
        self.invalidation_history: List[Dict[str, Any]] = []
        self._setup_default_rules()

    def _setup_default_rules(self):
        """Set up default invalidation rules."""
        # Data change rules
        self.add_rule(
            InvalidationRule(
                trigger=InvalidationTrigger.DATA_CHANGE,
                scope=InvalidationScope.SINGLE_USER,
                pattern="table_data:*{table_name}*{user_id}*",
                priority=1,
                cascade=True,
            )
        )

        self.add_rule(
            InvalidationRule(
                trigger=InvalidationTrigger.DATA_CHANGE,
                scope=InvalidationScope.SINGLE_USER,
                pattern="query_results:*{table_name}*{user_id}*",
                priority=1,
                cascade=True,
            )
        )

        self.add_rule(
            InvalidationRule(
                trigger=InvalidationTrigger.DATA_CHANGE,
                scope=InvalidationScope.SINGLE_USER,
                pattern="aggregations:*{table_name}*{user_id}*",
                priority=2,
                cascade=True,
            )
        )

        # Schema change rules
        self.add_rule(
            InvalidationRule(
                trigger=InvalidationTrigger.SCHEMA_CHANGE,
                scope=InvalidationScope.ALL_USERS,
                pattern="*{table_name}*",
                priority=1,
                cascade=True,
            )
        )

        # Config change rules
        self.add_rule(
            InvalidationRule(
                trigger=InvalidationTrigger.CONFIG_CHANGE,
                scope=InvalidationScope.SINGLE_USER,
                pattern="table_config:*{table_name}*{user_id}*",
                priority=2,
                cascade=True,
            )
        )

        self.add_rule(
            InvalidationRule(
                trigger=InvalidationTrigger.CONFIG_CHANGE,
                scope=InvalidationScope.SINGLE_USER,
                pattern="table_data:*{table_name}*{user_id}*",
                priority=3,
                cascade=False,
                condition="affects_display",
            )
        )

        # User action rules
        self.add_rule(
            InvalidationRule(
                trigger=InvalidationTrigger.USER_ACTION,
                scope=InvalidationScope.SINGLE_USER,
                pattern="user_preferences:*{table_name}*{user_id}*",
                priority=2,
                cascade=True,
            )
        )

        # Time-based rules
        self.add_rule(
            InvalidationRule(
                trigger=InvalidationTrigger.TIME_BASED,
                scope=InvalidationScope.GLOBAL,
                pattern="*",
                priority=4,
                condition="expired_ttl",
                ttl_threshold=60,  # Invalidate if TTL < 60 seconds
            )
        )

    def add_rule(self, rule: InvalidationRule):
        """Add a new invalidation rule."""
        self.rules.append(rule)
        self.rules.sort(key=lambda r: r.priority)

    def remove_rule(self, trigger: InvalidationTrigger, pattern: str) -> bool:
        """Remove an invalidation rule."""
        initial_count = len(self.rules)
        self.rules = [r for r in self.rules if not (r.trigger == trigger and r.pattern == pattern)]
        return len(self.rules) < initial_count

    def add_dependency(self, parent_key: str, child_key: str):
        """Add a dependency relationship between cache keys."""
        if parent_key not in self.dependency_graph:
            self.dependency_graph[parent_key] = set()
        self.dependency_graph[parent_key].add(child_key)

    def remove_dependency(self, parent_key: str, child_key: str):
        """Remove a dependency relationship."""
        if parent_key in self.dependency_graph:
            self.dependency_graph[parent_key].discard(child_key)

    def get_dependent_keys(self, key: str) -> Set[str]:
        """Get all keys that depend on the given key."""
        dependencies = set()

        def collect_dependencies(current_key: str):
            if current_key in self.dependency_graph:
                for dep_key in self.dependency_graph[current_key]:
                    dependencies.add(dep_key)
                    collect_dependencies(dep_key)  # Recursive dependency collection

        collect_dependencies(key)
        return dependencies

    def invalidate_by_trigger(
        self, trigger: InvalidationTrigger, context: Dict[str, Any], dry_run: bool = False
    ) -> Dict[str, Any]:
        """Invalidate cache based on trigger and context."""
        try:
            info(f"Processing invalidation trigger: {trigger.value}")

            # Find matching rules
            matching_rules = [r for r in self.rules if r.trigger == trigger]

            if not matching_rules:
                info(f"No rules found for trigger: {trigger.value}")
                return {"rules_processed": 0, "keys_invalidated": 0}

            total_invalidated = 0
            rules_processed = 0
            invalidation_details = []

            for rule in matching_rules:
                # Check condition if specified
                if rule.condition and not self._evaluate_condition(rule.condition, context):
                    continue

                # Format pattern with context
                pattern = self._format_pattern(rule.pattern, context)

                if dry_run:
                    info(f"DRY RUN: Would invalidate pattern '{pattern}'")
                    invalidation_details.append({"rule": rule, "pattern": pattern, "action": "would_invalidate"})
                    continue

                # Apply delay if specified
                if rule.delay_seconds > 0:
                    info(f"Delaying invalidation by {rule.delay_seconds} seconds")
                    # In a real implementation, you might use a task queue here
                    # For now, we'll proceed immediately

                # Perform invalidation
                invalidated_count = invalidate_cache_pattern(pattern)
                total_invalidated += invalidated_count
                rules_processed += 1

                invalidation_details.append(
                    {"rule": rule, "pattern": pattern, "keys_invalidated": invalidated_count, "action": "invalidated"}
                )

                # Handle cascading invalidation
                if rule.cascade:
                    cascade_count = self._cascade_invalidation(pattern, context)
                    total_invalidated += cascade_count

                    invalidation_details.append(
                        {
                            "rule": rule,
                            "pattern": f"cascade:{pattern}",
                            "keys_invalidated": cascade_count,
                            "action": "cascade_invalidated",
                        }
                    )

            # Record invalidation history
            self.invalidation_history.append(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "trigger": trigger.value,
                    "context": context,
                    "rules_processed": rules_processed,
                    "total_invalidated": total_invalidated,
                    "details": invalidation_details,
                }
            )

            # Keep only last 100 entries
            if len(self.invalidation_history) > 100:
                self.invalidation_history = self.invalidation_history[-100:]

            result = {
                "rules_processed": rules_processed,
                "keys_invalidated": total_invalidated,
                "details": invalidation_details,
            }

            if dry_run:
                info(f"DRY RUN: Would process {rules_processed} rules, invalidate {total_invalidated} keys")
            else:
                success(f"Invalidation completed: {rules_processed} rules processed, {total_invalidated} keys invalidated")

            return result

        except Exception as e:
            error(f"Error in invalidation by trigger: {str(e)}")
            return {"error": str(e), "rules_processed": 0, "keys_invalidated": 0}

    def _evaluate_condition(self, condition: str, context: Dict[str, Any]) -> bool:
        """Evaluate invalidation condition."""
        if condition == "affects_display":
            return context.get("display_affected", False)
        elif condition == "expired_ttl":
            return context.get("ttl_expired", False)
        elif condition == "high_priority":
            return context.get("priority", 3) <= 2
        else:
            # Simple string condition evaluation
            return condition in context

    def _format_pattern(self, pattern: str, context: Dict[str, Any]) -> str:
        """Format invalidation pattern with context values."""
        formatted_pattern = pattern

        # Replace context variables
        for key, value in context.items():
            placeholder = f"{{{key}}}"
            if placeholder in formatted_pattern:
                formatted_pattern = formatted_pattern.replace(placeholder, str(value))

        return formatted_pattern

    def _cascade_invalidation(self, pattern: str, context: Dict[str, Any]) -> int:
        """Handle cascading invalidation for dependent keys."""
        try:
            client = get_redis_client()
            if not client:
                return 0

            # Find keys matching the pattern
            keys = client.keys(pattern)
            total_cascade_invalidated = 0

            for key in keys:
                key_str = key.decode("utf-8") if isinstance(key, bytes) else key

                # Get dependent keys
                dependent_keys = self.get_dependent_keys(key_str)

                if dependent_keys:
                    # Invalidate dependent keys
                    for dep_key in dependent_keys:
                        if client.exists(dep_key):
                            client.delete(dep_key)
                            total_cascade_invalidated += 1

            return total_cascade_invalidated

        except Exception as e:
            error(f"Error in cascade invalidation: {str(e)}")
            return 0

    def smart_invalidate(
        self, table_name: str, user_id: str, operation: str, affected_data: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Smart invalidation based on operation and affected data."""
        try:
            info(f"Smart invalidation for table '{table_name}', user '{user_id}', operation '{operation}'")

            # Determine context based on operation
            context = {
                "table_name": table_name,
                "user_id": user_id,
                "operation": operation,
                "timestamp": datetime.utcnow().isoformat(),
            }

            # Add affected data to context
            if affected_data:
                context.update(affected_data)

            # Determine trigger based on operation
            trigger_mapping = {
                "insert": InvalidationTrigger.DATA_CHANGE,
                "update": InvalidationTrigger.DATA_CHANGE,
                "delete": InvalidationTrigger.DATA_CHANGE,
                "alter_table": InvalidationTrigger.SCHEMA_CHANGE,
                "create_table": InvalidationTrigger.SCHEMA_CHANGE,
                "drop_table": InvalidationTrigger.SCHEMA_CHANGE,
                "update_config": InvalidationTrigger.CONFIG_CHANGE,
                "save_preferences": InvalidationTrigger.USER_ACTION,
                "manual_refresh": InvalidationTrigger.MANUAL,
            }

            trigger = trigger_mapping.get(operation, InvalidationTrigger.MANUAL)

            # Add operation-specific context
            if operation in ["insert", "update", "delete"]:
                context["display_affected"] = True
                context["priority"] = 1
            elif operation in ["alter_table", "create_table", "drop_table"]:
                context["display_affected"] = True
                context["priority"] = 1
                context["affects_all_users"] = True
            elif operation == "update_config":
                context["display_affected"] = affected_data.get("affects_display", False)
                context["priority"] = 2

            # Perform invalidation
            result = self.invalidate_by_trigger(trigger, context)

            success(f"Smart invalidation completed for operation '{operation}'")
            return result

        except Exception as e:
            error(f"Error in smart invalidation: {str(e)}")
            return {"error": str(e)}

    def invalidate_related_tables(
        self, table_name: str, user_id: str, relationship_type: str = "foreign_key"
    ) -> Dict[str, Any]:
        """Invalidate cache for related tables."""
        try:
            info(f"Invalidating related tables for '{table_name}', relationship: {relationship_type}")

            # In a real implementation, you would query the database schema
            # to find related tables. For now, we'll use a simple pattern
            related_patterns = []

            if relationship_type == "foreign_key":
                # Tables that reference this table
                related_patterns.extend(
                    [
                        f"table_data:*{table_name}_*{user_id}*",
                        f"query_results:*{table_name}*{user_id}*",
                    ]
                )
            elif relationship_type == "view":
                # Views that depend on this table
                related_patterns.extend(
                    [
                        f"table_data:*view_*{table_name}*{user_id}*",
                    ]
                )
            elif relationship_type == "aggregation":
                # Aggregation tables
                related_patterns.extend(
                    [
                        f"aggregations:*{table_name}*{user_id}*",
                    ]
                )

            total_invalidated = 0
            for pattern in related_patterns:
                count = invalidate_cache_pattern(pattern)
                total_invalidated += count

            result = {
                "table_name": table_name,
                "relationship_type": relationship_type,
                "patterns_processed": len(related_patterns),
                "keys_invalidated": total_invalidated,
            }

            success(f"Related table invalidation completed: {total_invalidated} keys invalidated")
            return result

        except Exception as e:
            error(f"Error invalidating related tables: {str(e)}")
            return {"error": str(e)}

    def get_invalidation_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent invalidation history."""
        return self.invalidation_history[-limit:]

    def get_invalidation_stats(self) -> Dict[str, Any]:
        """Get invalidation statistics."""
        try:
            if not self.invalidation_history:
                return {"total_operations": 0, "stats": {}}

            # Calculate statistics
            total_operations = len(self.invalidation_history)
            total_keys_invalidated = sum(h.get("total_invalidated", 0) for h in self.invalidation_history)

            # Group by trigger
            trigger_stats = {}
            for history in self.invalidation_history:
                trigger = history["trigger"]
                if trigger not in trigger_stats:
                    trigger_stats[trigger] = {"count": 0, "keys_invalidated": 0}
                trigger_stats[trigger]["count"] += 1
                trigger_stats[trigger]["keys_invalidated"] += history.get("total_invalidated", 0)

            # Calculate average
            avg_keys_per_operation = total_keys_invalidated / total_operations if total_operations > 0 else 0

            stats = {
                "total_operations": total_operations,
                "total_keys_invalidated": total_keys_invalidated,
                "avg_keys_per_operation": round(avg_keys_per_operation, 2),
                "trigger_breakdown": trigger_stats,
                "active_rules": len(self.rules),
                "dependency_relationships": len(self.dependency_graph),
            }

            return stats

        except Exception as e:
            error(f"Error getting invalidation stats: {str(e)}")
            return {"error": str(e)}


# Global invalidation manager instance
_invalidation_manager = CacheInvalidationManager()


def get_invalidation_manager() -> CacheInvalidationManager:
    """Get the global invalidation manager instance."""
    return _invalidation_manager


def invalidate_smart(table_name: str, user_id: str, operation: str, affected_data: Dict[str, Any] = None) -> Dict[str, Any]:
    """Smart invalidation function for use in application code."""
    return _invalidation_manager.smart_invalidate(table_name, user_id, operation, affected_data)


def invalidate_by_trigger(trigger: InvalidationTrigger, context: Dict[str, Any], dry_run: bool = False) -> Dict[str, Any]:
    """Invalidate cache by trigger for use in application code."""
    return _invalidation_manager.invalidate_by_trigger(trigger, context, dry_run)


def add_cache_dependency(parent_key: str, child_key: str):
    """Add cache dependency relationship."""
    _invalidation_manager.add_dependency(parent_key, child_key)


def get_invalidation_stats() -> Dict[str, Any]:
    """Get invalidation statistics."""
    return _invalidation_manager.get_invalidation_stats()


def get_invalidation_history(limit: int = 50) -> List[Dict[str, Any]]:
    """Get recent invalidation history."""
    return _invalidation_manager.get_invalidation_history(limit)


# FastAPI Dependency Injection Functions
def get_invalidation_service_for_dependency() -> Dict[str, callable]:
    """FastAPI dependency for invalidation service."""
    return {
        "invalidate_smart": invalidate_smart,
        "invalidate_by_trigger": invalidate_by_trigger,
        "add_cache_dependency": add_cache_dependency,
        "get_invalidation_stats": get_invalidation_stats,
        "get_invalidation_history": get_invalidation_history,
        "get_invalidation_manager": get_invalidation_manager,
    }


def get_smart_invalidation_for_dependency() -> callable:
    """FastAPI dependency for smart invalidation."""
    return invalidate_smart


def get_invalidation_stats_for_dependency() -> callable:
    """FastAPI dependency for invalidation statistics."""
    return get_invalidation_stats
