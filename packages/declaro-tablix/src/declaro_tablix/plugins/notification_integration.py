"""Notification integration for Table Module V2 Plugin System.

This module provides integration between the plugin system and the
existing notification service for comprehensive user feedback.
"""

from typing import Any, Dict, List

from declaro_advise import error, info, success, warning
from declaro_tablix.plugins.protocols import get_global_plugin_registry


def initialize_plugin_notifications() -> bool:
    """Initialize notification integration for plugin system."""
    try:
        info("Initializing plugin notification integration")

        # Register notification handlers for plugin events
        registry = get_global_plugin_registry()

        # Hook into plugin registration events
        _setup_plugin_event_notifications()

        success("Plugin notification integration initialized")
        return True

    except Exception as e:
        error(f"Failed to initialize plugin notifications: {e}")
        return False


def notify_plugin_registered(plugin_name: str, plugin_version: str, plugin_type: str) -> None:
    """Notify when a plugin is successfully registered."""
    success(f"Plugin '{plugin_name}' v{plugin_version} ({plugin_type}) registered successfully")


def notify_plugin_unregistered(plugin_name: str) -> None:
    """Notify when a plugin is unregistered."""
    info(f"Plugin '{plugin_name}' unregistered")


def notify_plugin_error(plugin_name: str, operation: str, error_message: str) -> None:
    """Notify when a plugin encounters an error."""
    error(f"Plugin '{plugin_name}' error during {operation}: {error_message}")


def notify_plugin_warning(plugin_name: str, operation: str, warning_message: str) -> None:
    """Notify when a plugin encounters a warning."""
    warning(f"Plugin '{plugin_name}' warning during {operation}: {warning_message}")


def notify_hook_execution_start(hook_type: str, plugin_count: int) -> None:
    """Notify when hook execution starts."""
    if plugin_count > 0:
        info(f"Executing {plugin_count} plugin hook(s) for '{hook_type}'")


def notify_hook_execution_complete(hook_type: str, plugin_count: int, execution_time: float) -> None:
    """Notify when hook execution completes."""
    if plugin_count > 0:
        success(f"Completed {plugin_count} plugin hook(s) for '{hook_type}' in {execution_time:.2f}ms")


def notify_hook_execution_error(hook_type: str, plugin_name: str, error_message: str) -> None:
    """Notify when hook execution fails."""
    error(f"Hook '{hook_type}' failed for plugin '{plugin_name}': {error_message}")


def notify_plugin_discovery_start(search_paths: List[str]) -> None:
    """Notify when plugin discovery starts."""
    info(f"Starting plugin discovery in {len(search_paths)} path(s)")


def notify_plugin_discovery_complete(discovered_count: int, loaded_count: int) -> None:
    """Notify when plugin discovery completes."""
    if discovered_count > 0:
        success(f"Plugin discovery completed: {discovered_count} discovered, {loaded_count} loaded")
    else:
        warning("No plugins discovered in search paths")


def notify_plugin_load_error(plugin_name: str, error_message: str) -> None:
    """Notify when plugin loading fails."""
    error(f"Failed to load plugin '{plugin_name}': {error_message}")


def notify_plugin_validation_error(plugin_name: str, validation_errors: List[str]) -> None:
    """Notify when plugin validation fails."""
    error_list = "; ".join(validation_errors)
    error(f"Plugin '{plugin_name}' validation failed: {error_list}")


def notify_plugin_dependency_error(plugin_name: str, missing_dependencies: List[str]) -> None:
    """Notify when plugin dependencies are missing."""
    deps = ", ".join(missing_dependencies)
    error(f"Plugin '{plugin_name}' missing dependencies: {deps}")


def notify_plugin_performance_warning(plugin_name: str, operation: str, execution_time: float, threshold: float) -> None:
    """Notify when plugin performance is slow."""
    warning(
        f"Plugin '{plugin_name}' performance warning: {operation} took {execution_time:.2f}ms (threshold: {threshold:.2f}ms)"
    )


def notify_plugin_system_status(total_plugins: int, active_plugins: int, error_plugins: int) -> None:
    """Notify about overall plugin system status."""
    if error_plugins > 0:
        warning(f"Plugin system status: {active_plugins}/{total_plugins} active, {error_plugins} with errors")
    else:
        info(f"Plugin system status: {active_plugins}/{total_plugins} plugins active")


def notify_plugin_configuration_update(plugin_name: str, config_changes: Dict[str, Any]) -> None:
    """Notify when plugin configuration is updated."""
    change_count = len(config_changes)
    info(f"Plugin '{plugin_name}' configuration updated: {change_count} setting(s) changed")


def notify_plugin_hook_registration(plugin_name: str, hook_type: str, priority: int) -> None:
    """Notify when a plugin hook is registered."""
    info(f"Registered hook '{hook_type}' for plugin '{plugin_name}' (priority: {priority})")


def notify_plugin_hook_unregistration(plugin_name: str, hook_type: str) -> None:
    """Notify when a plugin hook is unregistered."""
    info(f"Unregistered hook '{hook_type}' for plugin '{plugin_name}'")


def notify_plugin_cleanup_start() -> None:
    """Notify when plugin cleanup starts."""
    info("Starting plugin system cleanup")


def notify_plugin_cleanup_complete(cleaned_count: int) -> None:
    """Notify when plugin cleanup completes."""
    success(f"Plugin system cleanup completed: {cleaned_count} plugins cleaned up")


def notify_plugin_stats_update(plugin_name: str, stats: Dict[str, Any]) -> None:
    """Notify about plugin statistics update."""
    executions = stats.get("execution_count", 0)
    errors = stats.get("error_count", 0)
    if errors > 0:
        warning(f"Plugin '{plugin_name}' stats: {executions} executions, {errors} errors")
    else:
        info(f"Plugin '{plugin_name}' stats: {executions} executions, no errors")


def get_notification_preferences() -> Dict[str, bool]:
    """Get current notification preferences for plugin system."""
    return {
        "plugin_registration": True,
        "plugin_unregistration": True,
        "plugin_errors": True,
        "plugin_warnings": True,
        "hook_execution": False,  # Too verbose for normal use
        "hook_errors": True,
        "discovery_events": True,
        "load_errors": True,
        "validation_errors": True,
        "dependency_errors": True,
        "performance_warnings": True,
        "system_status": True,
        "configuration_updates": True,
        "hook_registration": False,  # Too verbose for normal use
        "cleanup_events": True,
        "stats_updates": False,  # Too verbose for normal use
    }


def set_notification_preferences(preferences: Dict[str, bool]) -> None:
    """Set notification preferences for plugin system."""
    # Store preferences in global configuration
    global _notification_preferences
    _notification_preferences = preferences
    info(f"Plugin notification preferences updated: {len(preferences)} settings configured")


def _setup_plugin_event_notifications() -> None:
    """Setup internal notification handlers for plugin events."""
    # This would register with plugin registry events in a real implementation
    info("Plugin event notification handlers configured")


# Global notification preferences
_notification_preferences = get_notification_preferences()


def should_notify(notification_type: str) -> bool:
    """Check if a notification type should be sent."""
    return _notification_preferences.get(notification_type, True)


# Enhanced notification functions that respect preferences
def notify_if_enabled(notification_type: str, notification_func, *args, **kwargs) -> None:
    """Send notification only if enabled in preferences."""
    if should_notify(notification_type):
        notification_func(*args, **kwargs)


def get_plugin_notification_stats() -> Dict[str, Any]:
    """Get statistics about plugin notification usage."""
    return {
        "total_preferences": len(_notification_preferences),
        "enabled_notifications": sum(1 for enabled in _notification_preferences.values() if enabled),
        "disabled_notifications": sum(1 for enabled in _notification_preferences.values() if not enabled),
        "preferences": _notification_preferences.copy(),
    }


def reset_plugin_notifications() -> None:
    """Reset plugin notification system to defaults."""
    global _notification_preferences
    _notification_preferences = get_notification_preferences()
    info("Plugin notification system reset to defaults")
