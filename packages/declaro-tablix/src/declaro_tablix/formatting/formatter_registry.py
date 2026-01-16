"""
Formatter registry with plugin support for TableV2.

This module provides a function-based registry system for formatter plugins,
following the function-based architecture pattern.
"""

import importlib
import inspect
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from declaro_advise import error, info, warning

from .cell_formatters import COLUMN_TYPE_FORMATTERS


class PluginStatus(Enum):
    """Plugin status enumeration."""

    REGISTERED = "registered"
    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"


@dataclass
class FormatterPlugin:
    """Formatter plugin metadata."""

    name: str
    description: str
    version: str
    author: str
    column_types: List[str]
    formatter_func: Callable
    priority: int = 0
    status: PluginStatus = PluginStatus.REGISTERED
    dependencies: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    last_used: Optional[float] = None
    usage_count: int = 0
    error_count: int = 0

    def __post_init__(self):
        """Validate plugin after initialization."""
        if not callable(self.formatter_func):
            raise ValueError(f"formatter_func must be callable for plugin {self.name}")

        # Validate formatter function signature
        sig = inspect.signature(self.formatter_func)
        required_params = ["value", "options", "context"]
        if list(sig.parameters.keys()) != required_params:
            raise ValueError(f"formatter_func must have signature (value, options, context) for plugin {self.name}")


@dataclass
class RegistryStats:
    """Registry statistics."""

    total_plugins: int = 0
    active_plugins: int = 0
    inactive_plugins: int = 0
    error_plugins: int = 0
    total_formatters: int = 0
    custom_formatters: int = 0
    builtin_formatters: int = 0
    most_used_plugin: Optional[str] = None
    least_used_plugin: Optional[str] = None

    def update_from_registry(self, registry: Dict[str, FormatterPlugin]):
        """Update stats from registry."""
        self.total_plugins = len(registry)
        self.active_plugins = len([p for p in registry.values() if p.status == PluginStatus.ACTIVE])
        self.inactive_plugins = len([p for p in registry.values() if p.status == PluginStatus.INACTIVE])
        self.error_plugins = len([p for p in registry.values() if p.status == PluginStatus.ERROR])
        self.total_formatters = len(COLUMN_TYPE_FORMATTERS) + len(registry)
        self.custom_formatters = len(registry)
        self.builtin_formatters = len(COLUMN_TYPE_FORMATTERS)

        # Find most and least used plugins
        if registry:
            most_used = max(registry.values(), key=lambda p: p.usage_count)
            least_used = min(registry.values(), key=lambda p: p.usage_count)
            self.most_used_plugin = most_used.name if most_used.usage_count > 0 else None
            self.least_used_plugin = least_used.name if least_used.usage_count > 0 else None


# Global formatter plugin registry
_FORMATTER_PLUGINS: Dict[str, FormatterPlugin] = {}
_PLUGIN_DISCOVERY_PATHS: List[str] = []
_REGISTRY_STATS = RegistryStats()


def register_formatter_plugin(
    name: str,
    description: str,
    version: str,
    author: str,
    column_types: List[str],
    formatter_func: Callable,
    priority: int = 0,
    dependencies: Optional[List[str]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Register a formatter plugin.

    Args:
        name: Plugin name (must be unique)
        description: Plugin description
        version: Plugin version
        author: Plugin author
        column_types: List of column types this plugin handles
        formatter_func: Formatting function
        priority: Plugin priority (higher = higher priority)
        dependencies: List of required plugin names
        config: Plugin configuration

    Returns:
        True if registered successfully, False otherwise
    """
    try:
        # Check if plugin already exists
        if name in _FORMATTER_PLUGINS:
            warning(f"Plugin '{name}' already registered, updating...")

        # Create plugin instance
        plugin = FormatterPlugin(
            name=name,
            description=description,
            version=version,
            author=author,
            column_types=column_types,
            formatter_func=formatter_func,
            priority=priority,
            dependencies=dependencies or [],
            config=config or {},
        )

        # Validate dependencies
        if not _validate_plugin_dependencies(plugin):
            error(f"Plugin '{name}' has unmet dependencies")
            return False

        # Register plugin
        _FORMATTER_PLUGINS[name] = plugin
        plugin.status = PluginStatus.ACTIVE

        # Update registry for each column type
        for column_type in column_types:
            _register_column_type_formatter(column_type, plugin)

        # Update stats
        _REGISTRY_STATS.update_from_registry(_FORMATTER_PLUGINS)

        info(f"Successfully registered formatter plugin '{name}' v{version}")
        return True

    except Exception as e:
        error(f"Failed to register plugin '{name}': {str(e)}")
        return False


def unregister_formatter_plugin(name: str) -> bool:
    """
    Unregister a formatter plugin.

    Args:
        name: Plugin name to unregister

    Returns:
        True if unregistered successfully, False otherwise
    """
    try:
        if name not in _FORMATTER_PLUGINS:
            warning(f"Plugin '{name}' not found in registry")
            return False

        plugin = _FORMATTER_PLUGINS[name]

        # Remove from column type formatters
        for column_type in plugin.column_types:
            _unregister_column_type_formatter(column_type, plugin)

        # Remove from registry
        del _FORMATTER_PLUGINS[name]

        # Update stats
        _REGISTRY_STATS.update_from_registry(_FORMATTER_PLUGINS)

        info(f"Successfully unregistered formatter plugin '{name}'")
        return True

    except Exception as e:
        error(f"Failed to unregister plugin '{name}': {str(e)}")
        return False


def get_formatter_registry() -> Dict[str, FormatterPlugin]:
    """
    Get the complete formatter plugin registry.

    Returns:
        Dictionary of registered plugins
    """
    return _FORMATTER_PLUGINS.copy()


def clear_formatter_registry() -> None:
    """Clear all registered formatter plugins."""
    _FORMATTER_PLUGINS.clear()
    _REGISTRY_STATS.update_from_registry(_FORMATTER_PLUGINS)
    info("Cleared formatter plugin registry")


def validate_formatter_plugin(plugin: FormatterPlugin) -> List[str]:
    """
    Validate a formatter plugin.

    Args:
        plugin: Plugin to validate

    Returns:
        List of validation errors (empty if valid)
    """
    errors = []

    # Validate name
    if not plugin.name or not isinstance(plugin.name, str):
        errors.append("Plugin name must be a non-empty string")

    # Validate column types
    if not plugin.column_types or not isinstance(plugin.column_types, list):
        errors.append("Plugin must specify at least one column type")

    # Validate formatter function
    if not callable(plugin.formatter_func):
        errors.append("Plugin formatter_func must be callable")
    else:
        # Check function signature
        try:
            sig = inspect.signature(plugin.formatter_func)
            required_params = ["value", "options", "context"]
            if list(sig.parameters.keys()) != required_params:
                errors.append(f"formatter_func must have signature (value, options, context)")
        except Exception as e:
            errors.append(f"Cannot inspect formatter_func signature: {str(e)}")

    # Validate dependencies
    for dep in plugin.dependencies:
        if dep not in _FORMATTER_PLUGINS:
            errors.append(f"Dependency '{dep}' not found in registry")

    return errors


def get_formatter_plugin_stats() -> RegistryStats:
    """
    Get formatter plugin registry statistics.

    Returns:
        Registry statistics
    """
    _REGISTRY_STATS.update_from_registry(_FORMATTER_PLUGINS)
    return _REGISTRY_STATS


def discover_formatter_plugins(search_paths: Optional[List[str]] = None) -> List[str]:
    """
    Discover formatter plugins in specified paths.

    Args:
        search_paths: List of paths to search for plugins

    Returns:
        List of discovered plugin names
    """
    if search_paths is None:
        search_paths = _PLUGIN_DISCOVERY_PATHS

    discovered_plugins = []

    for search_path in search_paths:
        try:
            path = Path(search_path)
            if not path.exists():
                continue

            # Search for Python files
            for plugin_file in path.glob("*_plugin.py"):
                plugin_name = _discover_plugin_from_file(plugin_file)
                if plugin_name:
                    discovered_plugins.append(plugin_name)

        except Exception as e:
            warning(f"Error discovering plugins in {search_path}: {str(e)}")

    info(f"Discovered {len(discovered_plugins)} formatter plugins")
    return discovered_plugins


def get_formatter_for_column_type(column_type: str) -> Optional[Callable]:
    """
    Get the best formatter for a column type (considering plugins).

    Args:
        column_type: Column type to get formatter for

    Returns:
        Formatter function or None if not found
    """
    # First check custom plugins (higher priority)
    best_plugin = None
    best_priority = -1

    for plugin in _FORMATTER_PLUGINS.values():
        if plugin.status == PluginStatus.ACTIVE and column_type in plugin.column_types and plugin.priority > best_priority:
            best_plugin = plugin
            best_priority = plugin.priority

    if best_plugin:
        # Update usage stats
        best_plugin.last_used = time.time()
        best_plugin.usage_count += 1
        return best_plugin.formatter_func

    # Fallback to built-in formatters
    return COLUMN_TYPE_FORMATTERS.get(column_type)


def activate_formatter_plugin(name: str) -> bool:
    """
    Activate a formatter plugin.

    Args:
        name: Plugin name to activate

    Returns:
        True if activated successfully, False otherwise
    """
    if name not in _FORMATTER_PLUGINS:
        warning(f"Plugin '{name}' not found in registry")
        return False

    plugin = _FORMATTER_PLUGINS[name]

    # Validate plugin before activation
    errors = validate_formatter_plugin(plugin)
    if errors:
        plugin.status = PluginStatus.ERROR
        error(f"Cannot activate plugin '{name}': {'; '.join(errors)}")
        return False

    # Check dependencies
    if not _validate_plugin_dependencies(plugin):
        plugin.status = PluginStatus.ERROR
        error(f"Plugin '{name}' has unmet dependencies")
        return False

    plugin.status = PluginStatus.ACTIVE
    _REGISTRY_STATS.update_from_registry(_FORMATTER_PLUGINS)

    info(f"Activated formatter plugin '{name}'")
    return True


def deactivate_formatter_plugin(name: str) -> bool:
    """
    Deactivate a formatter plugin.

    Args:
        name: Plugin name to deactivate

    Returns:
        True if deactivated successfully, False otherwise
    """
    if name not in _FORMATTER_PLUGINS:
        warning(f"Plugin '{name}' not found in registry")
        return False

    plugin = _FORMATTER_PLUGINS[name]
    plugin.status = PluginStatus.INACTIVE
    _REGISTRY_STATS.update_from_registry(_FORMATTER_PLUGINS)

    info(f"Deactivated formatter plugin '{name}'")
    return True


def list_formatter_plugins(status_filter: Optional[PluginStatus] = None) -> List[FormatterPlugin]:
    """
    List formatter plugins, optionally filtered by status.

    Args:
        status_filter: Optional status to filter by

    Returns:
        List of formatter plugins
    """
    plugins = list(_FORMATTER_PLUGINS.values())

    if status_filter:
        plugins = [p for p in plugins if p.status == status_filter]

    # Sort by priority (descending), then by name
    plugins.sort(key=lambda p: (-p.priority, p.name))

    return plugins


def get_plugin_info(name: str) -> Optional[Dict[str, Any]]:
    """
    Get detailed information about a plugin.

    Args:
        name: Plugin name

    Returns:
        Plugin information dictionary or None if not found
    """
    if name not in _FORMATTER_PLUGINS:
        return None

    plugin = _FORMATTER_PLUGINS[name]

    return {
        "name": plugin.name,
        "description": plugin.description,
        "version": plugin.version,
        "author": plugin.author,
        "column_types": plugin.column_types,
        "priority": plugin.priority,
        "status": plugin.status.value,
        "dependencies": plugin.dependencies,
        "config": plugin.config,
        "created_at": plugin.created_at,
        "last_used": plugin.last_used,
        "usage_count": plugin.usage_count,
        "error_count": plugin.error_count,
    }


def update_plugin_config(name: str, config: Dict[str, Any]) -> bool:
    """
    Update plugin configuration.

    Args:
        name: Plugin name
        config: New configuration

    Returns:
        True if updated successfully, False otherwise
    """
    if name not in _FORMATTER_PLUGINS:
        warning(f"Plugin '{name}' not found in registry")
        return False

    plugin = _FORMATTER_PLUGINS[name]
    plugin.config.update(config)

    info(f"Updated configuration for plugin '{name}'")
    return True


def add_plugin_discovery_path(path: str) -> None:
    """
    Add a path for plugin discovery.

    Args:
        path: Path to add to discovery paths
    """
    if path not in _PLUGIN_DISCOVERY_PATHS:
        _PLUGIN_DISCOVERY_PATHS.append(path)
        info(f"Added plugin discovery path: {path}")


def remove_plugin_discovery_path(path: str) -> bool:
    """
    Remove a path from plugin discovery.

    Args:
        path: Path to remove from discovery paths

    Returns:
        True if removed successfully, False otherwise
    """
    if path in _PLUGIN_DISCOVERY_PATHS:
        _PLUGIN_DISCOVERY_PATHS.remove(path)
        info(f"Removed plugin discovery path: {path}")
        return True
    return False


def get_plugin_discovery_paths() -> List[str]:
    """
    Get current plugin discovery paths.

    Returns:
        List of discovery paths
    """
    return _PLUGIN_DISCOVERY_PATHS.copy()


# Private helper functions


def _validate_plugin_dependencies(plugin: FormatterPlugin) -> bool:
    """Validate plugin dependencies."""
    for dep in plugin.dependencies:
        if dep not in _FORMATTER_PLUGINS:
            return False
        dep_plugin = _FORMATTER_PLUGINS[dep]
        if dep_plugin.status != PluginStatus.ACTIVE:
            return False
    return True


def _register_column_type_formatter(column_type: str, plugin: FormatterPlugin) -> None:
    """Register plugin formatter for a column type."""
    # This would integrate with the existing COLUMN_TYPE_FORMATTERS
    # For now, we rely on get_formatter_for_column_type to handle priority
    pass


def _unregister_column_type_formatter(column_type: str, plugin: FormatterPlugin) -> None:
    """Unregister plugin formatter for a column type."""
    # This would remove from COLUMN_TYPE_FORMATTERS if needed
    pass


def _discover_plugin_from_file(plugin_file: Path) -> Optional[str]:
    """Discover plugin from a Python file."""
    try:
        spec = importlib.util.spec_from_file_location("plugin", plugin_file)
        if spec is None or spec.loader is None:
            return None

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Look for plugin registration function
        if hasattr(module, "register_plugin"):
            plugin_name = module.register_plugin()
            return plugin_name

    except Exception as e:
        warning(f"Error loading plugin from {plugin_file}: {str(e)}")

    return None


def reset_plugin_stats() -> None:
    """Reset all plugin usage statistics."""
    for plugin in _FORMATTER_PLUGINS.values():
        plugin.usage_count = 0
        plugin.error_count = 0
        plugin.last_used = None

    _REGISTRY_STATS.update_from_registry(_FORMATTER_PLUGINS)
    info("Reset plugin statistics")


def get_plugin_performance_metrics() -> Dict[str, Any]:
    """
    Get performance metrics for all plugins.

    Returns:
        Dictionary of performance metrics
    """
    metrics = {
        "total_plugins": len(_FORMATTER_PLUGINS),
        "active_plugins": len([p for p in _FORMATTER_PLUGINS.values() if p.status == PluginStatus.ACTIVE]),
        "total_usage": sum(p.usage_count for p in _FORMATTER_PLUGINS.values()),
        "total_errors": sum(p.error_count for p in _FORMATTER_PLUGINS.values()),
        "plugins": [],
    }

    for plugin in _FORMATTER_PLUGINS.values():
        metrics["plugins"].append(
            {
                "name": plugin.name,
                "usage_count": plugin.usage_count,
                "error_count": plugin.error_count,
                "last_used": plugin.last_used,
                "status": plugin.status.value,
            }
        )

    return metrics
