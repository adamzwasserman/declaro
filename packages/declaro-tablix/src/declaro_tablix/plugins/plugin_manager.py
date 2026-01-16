"""Plugin manager functions for Table Module V2.

This module provides function-based plugin management including registration,
discovery, loading, and lifecycle management using pure functions.
"""

import importlib
import importlib.util
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union

from declaro_advise import error, info, success, warning
from declaro_tablix.plugins.protocols import (
    DataProcessingPlugin,
    NotificationPlugin,
    PluginHookType,
    PluginManifest,
    PluginPriority,
    RenderingPlugin,
    SystemPlugin,
    TablePlugin,
    get_global_plugin_registry,
    reset_global_plugin_registry,
)

# Global plugin system configuration
_plugin_system_config: Dict[str, Any] = {
    "enabled": True,
    "plugin_directories": [],
    "auto_discovery": True,
    "load_timeout": 30,
    "max_plugins": 100,
    "enable_notifications": True,
}


def initialize_plugin_system(config: Optional[Dict[str, Any]] = None) -> bool:
    """Initialize the plugin system with configuration.

    Args:
        config: Optional configuration dictionary

    Returns:
        True if initialization successful
    """
    global _plugin_system_config

    try:
        info("Initializing plugin system")

        # Update configuration
        if config:
            _plugin_system_config.update(config)

        # Reset registry to clean state
        reset_global_plugin_registry()

        # Auto-discover plugins if enabled
        if _plugin_system_config.get("auto_discovery", True):
            plugin_count = discover_plugins()
            info(f"Auto-discovered {plugin_count} plugins")

        success("Plugin system initialized successfully")
        return True

    except Exception as e:
        error(f"Failed to initialize plugin system: {str(e)}")
        return False


def register_plugin(plugin: TablePlugin, manifest: Optional[PluginManifest] = None) -> bool:
    """Register a plugin with the plugin system.

    Args:
        plugin: Plugin instance to register
        manifest: Optional plugin manifest

    Returns:
        True if registration successful
    """
    try:
        registry = get_global_plugin_registry()

        # Create manifest if not provided
        if manifest is None:
            manifest = PluginManifest(
                name=plugin.name,
                version=plugin.version,
                description=plugin.description,
                plugin_type=_determine_plugin_type(plugin),
            )

        # Check if plugin already registered
        if registry.get_plugin(plugin.name):
            warning(f"Plugin '{plugin.name}' is already registered")
            return False

        # Register the plugin
        if registry.register_plugin(plugin, manifest):
            # Initialize the plugin
            try:
                plugin.initialize(manifest.configuration)

                # Register hooks if the plugin supports them
                _register_plugin_hooks(plugin, manifest)

                success(f"Plugin '{plugin.name}' registered successfully")
                return True
            except Exception as e:
                # Remove from registry if initialization failed
                registry.unregister_plugin(plugin.name)
                error(f"Failed to initialize plugin '{plugin.name}': {str(e)}")
                return False
        else:
            error(f"Failed to register plugin '{plugin.name}'")
            return False

    except Exception as e:
        error(f"Error registering plugin: {str(e)}")
        return False


def unregister_plugin(plugin_name: str) -> bool:
    """Unregister a plugin from the system.

    Args:
        plugin_name: Name of plugin to unregister

    Returns:
        True if unregistration successful
    """
    try:
        registry = get_global_plugin_registry()
        plugin = registry.get_plugin(plugin_name)

        if not plugin:
            warning(f"Plugin '{plugin_name}' not found")
            return False

        # Cleanup the plugin
        try:
            plugin.cleanup()
        except Exception as e:
            warning(f"Plugin '{plugin_name}' cleanup failed: {str(e)}")

        # Unregister from registry
        if registry.unregister_plugin(plugin_name):
            success(f"Plugin '{plugin_name}' unregistered successfully")
            return True
        else:
            error(f"Failed to unregister plugin '{plugin_name}'")
            return False

    except Exception as e:
        error(f"Error unregistering plugin: {str(e)}")
        return False


def get_plugins(plugin_type: Optional[Type[TablePlugin]] = None) -> List[TablePlugin]:
    """Get registered plugins, optionally filtered by type.

    Args:
        plugin_type: Optional plugin type to filter by

    Returns:
        List of matching plugins
    """
    try:
        registry = get_global_plugin_registry()

        if plugin_type:
            return registry.get_plugins_by_type(plugin_type)
        else:
            return list(registry.get_all_plugins().values())

    except Exception as e:
        error(f"Error getting plugins: {str(e)}")
        return []


def get_plugin(plugin_name: str) -> Optional[TablePlugin]:
    """Get a specific plugin by name.

    Args:
        plugin_name: Name of plugin to retrieve

    Returns:
        Plugin instance or None if not found
    """
    try:
        registry = get_global_plugin_registry()
        return registry.get_plugin(plugin_name)

    except Exception as e:
        error(f"Error getting plugin '{plugin_name}': {str(e)}")
        return None


def list_plugin_types() -> List[str]:
    """Get list of all plugin types in the system.

    Returns:
        List of plugin type names
    """
    try:
        registry = get_global_plugin_registry()
        plugin_types = set()

        for manifest in registry.manifests.values():
            plugin_types.add(manifest.plugin_type)

        return sorted(list(plugin_types))

    except Exception as e:
        error(f"Error listing plugin types: {str(e)}")
        return []


def is_plugin_registered(plugin_name: str) -> bool:
    """Check if a plugin is registered.

    Args:
        plugin_name: Name of plugin to check

    Returns:
        True if plugin is registered
    """
    try:
        registry = get_global_plugin_registry()
        return registry.get_plugin(plugin_name) is not None

    except Exception as e:
        error(f"Error checking plugin registration: {str(e)}")
        return False


def get_plugin_stats(plugin_name: Optional[str] = None) -> Dict[str, Any]:
    """Get plugin statistics.

    Args:
        plugin_name: Optional specific plugin name

    Returns:
        Dictionary of plugin statistics
    """
    try:
        registry = get_global_plugin_registry()

        if plugin_name:
            stats = registry.get_plugin_stats(plugin_name)
            return stats if stats else {}
        else:
            # Return stats for all plugins
            all_stats = {}
            for name in registry.plugins.keys():
                stats = registry.get_plugin_stats(name)
                if stats:
                    all_stats[name] = stats
            return all_stats

    except Exception as e:
        error(f"Error getting plugin stats: {str(e)}")
        return {}


def discover_plugins(directories: Optional[List[str]] = None) -> int:
    """Discover and load plugins from specified directories.

    Args:
        directories: Optional list of directories to search

    Returns:
        Number of plugins discovered
    """
    try:
        if not directories:
            directories = _plugin_system_config.get("plugin_directories", [])

        plugin_count = 0

        for directory in directories:
            count = _discover_plugins_in_directory(directory)
            plugin_count += count
            info(f"Discovered {count} plugins in {directory}")

        return plugin_count

    except Exception as e:
        error(f"Error discovering plugins: {str(e)}")
        return 0


def _discover_plugins_in_directory(directory: str) -> int:
    """Discover plugins in a specific directory.

    Args:
        directory: Directory path to search

    Returns:
        Number of plugins discovered
    """
    try:
        if not os.path.exists(directory):
            warning(f"Plugin directory does not exist: {directory}")
            return 0

        plugin_count = 0

        for root, dirs, files in os.walk(directory):
            for file in files:
                if file.endswith(".py") and not file.startswith("__"):
                    file_path = os.path.join(root, file)
                    plugin = _load_plugin_from_file(file_path)
                    if plugin:
                        plugin_count += 1

        return plugin_count

    except Exception as e:
        error(f"Error discovering plugins in directory '{directory}': {str(e)}")
        return 0


def _load_plugin_from_file(file_path: str) -> Optional[TablePlugin]:
    """Load a plugin from a Python file.

    Args:
        file_path: Path to plugin file

    Returns:
        Plugin instance or None if loading failed
    """
    try:
        # Load the module
        module_name = Path(file_path).stem
        spec = importlib.util.spec_from_file_location(module_name, file_path)

        if not spec or not spec.loader:
            return None

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Look for plugin classes
        for attr_name in dir(module):
            attr = getattr(module, attr_name)

            # Check if it's a plugin class
            if isinstance(attr, type) and issubclass(attr, TablePlugin) and attr != TablePlugin:

                # Instantiate the plugin
                plugin_instance = attr()

                # Try to load manifest if it exists
                manifest_path = file_path.replace(".py", ".json")
                manifest = None

                if os.path.exists(manifest_path):
                    manifest = _load_plugin_manifest(manifest_path)

                # Register the plugin
                if register_plugin(plugin_instance, manifest):
                    return plugin_instance

        return None

    except Exception as e:
        error(f"Error loading plugin from '{file_path}': {str(e)}")
        return None


def _load_plugin_manifest(manifest_path: str) -> Optional[PluginManifest]:
    """Load plugin manifest from JSON file.

    Args:
        manifest_path: Path to manifest file

    Returns:
        Plugin manifest or None if loading failed
    """
    try:
        with open(manifest_path, "r") as f:
            data = json.load(f)

        return PluginManifest.from_dict(data)

    except Exception as e:
        error(f"Error loading plugin manifest from '{manifest_path}': {str(e)}")
        return None


def _determine_plugin_type(plugin: TablePlugin) -> str:
    """Determine the type of a plugin.

    Args:
        plugin: Plugin instance

    Returns:
        Plugin type string
    """
    if isinstance(plugin, DataProcessingPlugin):
        return "data_processing"
    elif isinstance(plugin, RenderingPlugin):
        return "rendering"
    elif isinstance(plugin, NotificationPlugin):
        return "notification"
    elif isinstance(plugin, SystemPlugin):
        return "system"
    else:
        return "generic"


def _register_plugin_hooks(plugin: TablePlugin, manifest: PluginManifest) -> None:
    """Register hooks for a plugin.

    Args:
        plugin: Plugin instance
        manifest: Plugin manifest
    """
    try:
        registry = get_global_plugin_registry()

        # Register hooks based on plugin type and supported hooks
        supported_hooks = manifest.supported_hooks

        if not supported_hooks:
            # Default hooks based on plugin type
            plugin_type = manifest.plugin_type

            if plugin_type == "data_processing":
                supported_hooks = [
                    PluginHookType.BEFORE_DATA_PROCESS,
                    PluginHookType.AFTER_DATA_PROCESS,
                    PluginHookType.BEFORE_DATA_FILTER,
                    PluginHookType.AFTER_DATA_FILTER,
                ]
            elif plugin_type == "rendering":
                supported_hooks = [
                    PluginHookType.BEFORE_RENDER,
                    PluginHookType.AFTER_RENDER,
                    PluginHookType.BEFORE_CELL_RENDER,
                    PluginHookType.AFTER_CELL_RENDER,
                ]
            elif plugin_type == "notification":
                supported_hooks = [PluginHookType.ERROR_OCCURRED, PluginHookType.PERFORMANCE_WARNING]
            else:
                supported_hooks = []

        # Register each hook
        for hook_type in supported_hooks:
            registry.register_hook(plugin.name, hook_type, PluginPriority.NORMAL)

    except Exception as e:
        warning(f"Error registering hooks for plugin '{plugin.name}': {str(e)}")


def _initialize_registered_plugins() -> None:
    """Initialize all registered plugins."""
    try:
        registry = get_global_plugin_registry()

        for plugin_name, plugin in registry.get_all_plugins().items():
            try:
                manifest = registry.manifests.get(plugin_name)
                config = manifest.configuration if manifest else {}
                plugin.initialize(config)
                info(f"Initialized plugin '{plugin_name}'")
            except Exception as e:
                error(f"Failed to initialize plugin '{plugin_name}': {str(e)}")

    except Exception as e:
        error(f"Error initializing plugins: {str(e)}")


def cleanup_plugin_system() -> None:
    """Cleanup the plugin system and all registered plugins."""
    try:
        info("Cleaning up plugin system")

        registry = get_global_plugin_registry()

        # Cleanup all plugins
        for plugin_name, plugin in registry.get_all_plugins().items():
            try:
                plugin.cleanup()
                info(f"Cleaned up plugin '{plugin_name}'")
            except Exception as e:
                warning(f"Error cleaning up plugin '{plugin_name}': {str(e)}")

        # Reset the registry
        reset_global_plugin_registry()

        success("Plugin system cleanup completed")

    except Exception as e:
        error(f"Error during plugin system cleanup: {str(e)}")


def get_plugin_registry() -> Dict[str, Any]:
    """Get the current plugin registry state.

    Returns:
        Dictionary representation of plugin registry
    """
    try:
        registry = get_global_plugin_registry()

        return {
            "plugins": {
                name: {"name": plugin.name, "version": plugin.version, "description": plugin.description}
                for name, plugin in registry.get_all_plugins().items()
            },
            "manifests": {name: manifest.to_dict() for name, manifest in registry.manifests.items()},
            "hook_counts": {hook_type: len(handlers) for hook_type, handlers in registry.hooks.items()},
            "stats": registry.plugin_stats,
        }

    except Exception as e:
        error(f"Error getting plugin registry: {str(e)}")
        return {}


def get_plugin_config(plugin_name: str) -> Dict[str, Any]:
    """Get configuration for a specific plugin.

    Args:
        plugin_name: Name of plugin

    Returns:
        Plugin configuration dictionary
    """
    try:
        registry = get_global_plugin_registry()
        manifest = registry.manifests.get(plugin_name)

        if manifest:
            return manifest.configuration
        else:
            warning(f"Plugin '{plugin_name}' manifest not found")
            return {}

    except Exception as e:
        error(f"Error getting plugin config: {str(e)}")
        return {}


def get_plugin_system_status() -> Dict[str, Any]:
    """Get overall plugin system status.

    Returns:
        Dictionary with system status information
    """
    try:
        registry = get_global_plugin_registry()

        total_plugins = len(registry.get_all_plugins())
        total_hooks = sum(len(handlers) for handlers in registry.hooks.values())

        plugin_types = {}
        for manifest in registry.manifests.values():
            plugin_type = manifest.plugin_type
            plugin_types[plugin_type] = plugin_types.get(plugin_type, 0) + 1

        return {
            "enabled": _plugin_system_config.get("enabled", True),
            "total_plugins": total_plugins,
            "total_hooks": total_hooks,
            "plugin_types": plugin_types,
            "configuration": _plugin_system_config.copy(),
            "registry_status": "healthy" if total_plugins > 0 else "empty",
        }

    except Exception as e:
        error(f"Error getting plugin system status: {str(e)}")
        return {"status": "error", "error": str(e)}


def execute_hook(hook_type: str, *args, **kwargs) -> List[Any]:
    """Execute all registered hooks for a specific hook type.

    Args:
        hook_type: Type of hook to execute
        *args: Positional arguments to pass to hooks
        **kwargs: Keyword arguments to pass to hooks

    Returns:
        List of results from hook executions
    """
    try:
        registry = get_global_plugin_registry()
        handlers = registry.get_hook_handlers(hook_type)

        results = []

        for handler in handlers:
            try:
                start_time = time.time()
                result = handler(*args, **kwargs)
                execution_time = time.time() - start_time

                # Update plugin stats
                registry.update_plugin_stats(handler.plugin.name, execution_time, True)

                results.append(result)

            except Exception as e:
                # Update error stats
                registry.update_plugin_stats(handler.plugin.name, 0, False)
                error(f"Hook execution failed for plugin '{handler.plugin.name}': {str(e)}")
                results.append(None)

        return results

    except Exception as e:
        error(f"Error executing hooks for '{hook_type}': {str(e)}")
        return []


def configure_plugin_system(config: Dict[str, Any]) -> bool:
    """Update plugin system configuration.

    Args:
        config: Configuration dictionary

    Returns:
        True if configuration successful
    """
    try:
        global _plugin_system_config
        _plugin_system_config.update(config)

        info("Plugin system configuration updated")
        return True

    except Exception as e:
        error(f"Error configuring plugin system: {str(e)}")
        return False
