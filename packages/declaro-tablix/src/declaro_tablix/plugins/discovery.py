"""Plugin discovery functions for Table Module V2.

This module provides function-based plugin discovery and loading
capabilities for the plugin architecture system.
"""

import importlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Type

from declaro_advise import error, info, success, warning
from declaro_tablix.plugins.protocols import (
    DataProcessingPlugin,
    NotificationPlugin,
    PluginManifest,
    RenderingPlugin,
    SystemPlugin,
    TablePlugin,
)

# Global plugin discovery state
_search_paths: Set[str] = set()
_discovered_plugins: Dict[str, Dict[str, Any]] = {}
_loaded_plugins: Dict[str, TablePlugin] = {}
_plugin_manifests: Dict[str, PluginManifest] = {}


def initialize_plugin_discovery(search_paths: Optional[List[str]] = None) -> bool:
    """Initialize plugin discovery system.

    Args:
        search_paths: Optional list of paths to search for plugins

    Returns:
        True if initialization successful
    """
    try:
        info("Initializing plugin discovery system")

        # Reset state
        _search_paths.clear()
        _discovered_plugins.clear()
        _loaded_plugins.clear()
        _plugin_manifests.clear()

        # Add default search paths
        default_paths = [
            "backend/services/tableV2/plugins/examples",
            "plugins",  # User plugins directory
            "custom_plugins",  # Custom plugins directory
        ]

        # Add search paths
        for path in default_paths:
            add_search_path(path)

        if search_paths:
            for path in search_paths:
                add_search_path(path)

        success("Plugin discovery system initialized")
        return True

    except Exception as e:
        error(f"Failed to initialize plugin discovery: {str(e)}")
        return False


def add_search_path(path: str) -> bool:
    """Add a directory to the plugin search paths.

    Args:
        path: Directory path to add

    Returns:
        True if path added successfully
    """
    try:
        # Convert to absolute path
        abs_path = os.path.abspath(path)

        if os.path.exists(abs_path) and os.path.isdir(abs_path):
            _search_paths.add(abs_path)
            info(f"Added plugin search path: {abs_path}")
            return True
        else:
            warning(f"Plugin search path does not exist: {abs_path}")
            return False

    except Exception as e:
        error(f"Error adding search path '{path}': {str(e)}")
        return False


def discover_plugins() -> Dict[str, Dict[str, Any]]:
    """Discover all plugins in search paths.

    Returns:
        Dictionary of discovered plugins with metadata
    """
    try:
        info("Starting plugin discovery")

        _discovered_plugins.clear()

        total_discovered = 0

        for search_path in _search_paths:
            count = _discover_in_path(search_path)
            total_discovered += count
            info(f"Discovered {count} plugins in {search_path}")

        success(f"Plugin discovery completed. Found {total_discovered} plugins total")
        return _discovered_plugins.copy()

    except Exception as e:
        error(f"Error during plugin discovery: {str(e)}")
        return {}


def _discover_in_path(path: str) -> int:
    """Discover plugins in a specific path.

    Args:
        path: Directory path to search

    Returns:
        Number of plugins discovered
    """
    try:
        if not os.path.exists(path):
            return 0

        plugin_count = 0

        # Walk through directory recursively
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith(".py") and not file.startswith("__"):
                    file_path = os.path.join(root, file)

                    # Discover plugin in file
                    plugin_info = _discover_plugin_in_file(file_path)
                    if plugin_info:
                        plugin_name = plugin_info["name"]
                        _discovered_plugins[plugin_name] = plugin_info
                        plugin_count += 1

        return plugin_count

    except Exception as e:
        error(f"Error discovering plugins in path '{path}': {str(e)}")
        return 0


def _discover_plugin_in_file(file_path: str) -> Optional[Dict[str, Any]]:
    """Discover plugin information from a Python file.

    Args:
        file_path: Path to Python file

    Returns:
        Plugin information dictionary or None
    """
    try:
        # Load the module to inspect it
        module_name = Path(file_path).stem
        spec = importlib.util.spec_from_file_location(module_name, file_path)

        if not spec or not spec.loader:
            return None

        module = importlib.util.module_from_spec(spec)

        # Execute module to get classes
        try:
            spec.loader.exec_module(module)
        except Exception as e:
            warning(f"Failed to execute plugin module '{file_path}': {str(e)}")
            return None

        # Look for plugin classes
        for name, obj in inspect.getmembers(module, inspect.isclass):
            if _is_plugin_class(obj):
                # Found a plugin class
                try:
                    # Try to instantiate to get plugin info
                    instance = obj()

                    # Look for manifest file
                    manifest_path = file_path.replace(".py", ".json")
                    manifest = None

                    if os.path.exists(manifest_path):
                        manifest = _load_plugin_manifest(manifest_path)

                    plugin_info = {
                        "name": instance.name,
                        "version": instance.version,
                        "description": instance.description,
                        "class_name": name,
                        "module_name": module_name,
                        "file_path": file_path,
                        "manifest_path": manifest_path if os.path.exists(manifest_path) else None,
                        "manifest": manifest.to_dict() if manifest else None,
                        "plugin_type": _determine_plugin_type(instance),
                        "dependencies": manifest.dependencies if manifest else [],
                        "supported_hooks": manifest.supported_hooks if manifest else [],
                        "loaded": False,
                        "error": None,
                    }

                    return plugin_info

                except Exception as e:
                    warning(f"Failed to instantiate plugin class '{name}' in '{file_path}': {str(e)}")
                    return None

        return None

    except Exception as e:
        warning(f"Error discovering plugin in file '{file_path}': {str(e)}")
        return None


def _load_plugin_manifest(manifest_path: str) -> Optional[PluginManifest]:
    """Load plugin manifest from JSON file.

    Args:
        manifest_path: Path to manifest file

    Returns:
        Plugin manifest or None
    """
    try:
        with open(manifest_path, "r") as f:
            data = json.load(f)

        return PluginManifest.from_dict(data)

    except Exception as e:
        error(f"Error loading plugin manifest from '{manifest_path}': {str(e)}")
        return None


def _is_plugin_class(cls: Type) -> bool:
    """Check if a class is a valid plugin class.

    Args:
        cls: Class to check

    Returns:
        True if class is a valid plugin
    """
    try:
        # Must be a class and subclass of TablePlugin
        if not (inspect.isclass(cls) and issubclass(cls, TablePlugin)):
            return False

        # Must not be the base TablePlugin class itself
        if cls == TablePlugin:
            return False

        # Must not be an abstract class
        if inspect.isabstract(cls):
            return False

        # Check if it has required methods
        required_properties = ["name", "version", "description"]
        required_methods = ["initialize", "cleanup"]

        for prop in required_properties:
            if not hasattr(cls, prop):
                return False

        for method in required_methods:
            if not hasattr(cls, method):
                return False

        return True

    except Exception:
        return False


def _determine_plugin_type(plugin: TablePlugin) -> str:
    """Determine the type of a plugin instance.

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


def load_plugin(plugin_name: str) -> Optional[TablePlugin]:
    """Load a discovered plugin by name.

    Args:
        plugin_name: Name of plugin to load

    Returns:
        Loaded plugin instance or None
    """
    try:
        if plugin_name in _loaded_plugins:
            info(f"Plugin '{plugin_name}' already loaded")
            return _loaded_plugins[plugin_name]

        plugin_info = _discovered_plugins.get(plugin_name)
        if not plugin_info:
            error(f"Plugin '{plugin_name}' not found in discovered plugins")
            return None

        # Load dependencies first
        dependencies = plugin_info.get("dependencies", [])
        for dep in dependencies:
            if dep not in _loaded_plugins:
                dep_plugin = load_plugin(dep)
                if not dep_plugin:
                    error(f"Failed to load dependency '{dep}' for plugin '{plugin_name}'")
                    return None

        # Load the plugin module
        file_path = plugin_info["file_path"]
        module_name = plugin_info["module_name"]
        class_name = plugin_info["class_name"]

        spec = importlib.util.spec_from_file_location(module_name, file_path)
        if not spec or not spec.loader:
            error(f"Failed to load module spec for plugin '{plugin_name}'")
            return None

        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)

        # Get the plugin class and instantiate
        plugin_class = getattr(module, class_name)
        plugin_instance = plugin_class()

        # Load manifest if available
        manifest_path = plugin_info.get("manifest_path")
        if manifest_path and os.path.exists(manifest_path):
            manifest = _load_plugin_manifest(manifest_path)
            if manifest:
                _plugin_manifests[plugin_name] = manifest

        # Store loaded plugin
        _loaded_plugins[plugin_name] = plugin_instance
        plugin_info["loaded"] = True

        success(f"Plugin '{plugin_name}' loaded successfully")
        return plugin_instance

    except Exception as e:
        error(f"Error loading plugin '{plugin_name}': {str(e)}")
        if plugin_name in _discovered_plugins:
            _discovered_plugins[plugin_name]["error"] = str(e)
        return None


def unload_plugin(plugin_name: str) -> bool:
    """Unload a loaded plugin.

    Args:
        plugin_name: Name of plugin to unload

    Returns:
        True if unload successful
    """
    try:
        if plugin_name not in _loaded_plugins:
            warning(f"Plugin '{plugin_name}' is not loaded")
            return False

        plugin = _loaded_plugins[plugin_name]

        # Cleanup the plugin
        try:
            plugin.cleanup()
        except Exception as e:
            warning(f"Plugin '{plugin_name}' cleanup failed: {str(e)}")

        # Remove from loaded plugins
        del _loaded_plugins[plugin_name]

        # Update discovery info
        if plugin_name in _discovered_plugins:
            _discovered_plugins[plugin_name]["loaded"] = False

        # Remove manifest
        if plugin_name in _plugin_manifests:
            del _plugin_manifests[plugin_name]

        success(f"Plugin '{plugin_name}' unloaded successfully")
        return True

    except Exception as e:
        error(f"Error unloading plugin '{plugin_name}': {str(e)}")
        return False


def load_plugin_with_dependencies(plugin_name: str) -> Optional[TablePlugin]:
    """Load a plugin and all its dependencies.

    Args:
        plugin_name: Name of plugin to load

    Returns:
        Loaded plugin instance or None
    """
    try:
        info(f"Loading plugin '{plugin_name}' with dependencies")

        # Check if plugin exists
        if plugin_name not in _discovered_plugins:
            error(f"Plugin '{plugin_name}' not found")
            return None

        # Get dependencies
        dependencies = get_plugin_dependencies(plugin_name)

        # Load dependencies in order
        for dep in dependencies:
            if dep not in _loaded_plugins:
                dep_plugin = load_plugin(dep)
                if not dep_plugin:
                    error(f"Failed to load dependency '{dep}' for plugin '{plugin_name}'")
                    return None

        # Load the main plugin
        plugin = load_plugin(plugin_name)

        if plugin:
            success(f"Plugin '{plugin_name}' and dependencies loaded successfully")

        return plugin

    except Exception as e:
        error(f"Error loading plugin with dependencies '{plugin_name}': {str(e)}")
        return None


def get_plugin_info(plugin_name: str) -> Optional[Dict[str, Any]]:
    """Get information about a discovered plugin.

    Args:
        plugin_name: Name of plugin

    Returns:
        Plugin information dictionary or None
    """
    return _discovered_plugins.get(plugin_name)


def list_discovered_plugins() -> List[str]:
    """Get list of all discovered plugin names.

    Returns:
        List of plugin names
    """
    return list(_discovered_plugins.keys())


def get_plugins_by_type(plugin_type: str) -> List[str]:
    """Get discovered plugins filtered by type.

    Args:
        plugin_type: Type of plugins to get

    Returns:
        List of plugin names of the specified type
    """
    return [name for name, info in _discovered_plugins.items() if info.get("plugin_type") == plugin_type]


def get_loaded_plugins() -> Dict[str, TablePlugin]:
    """Get all currently loaded plugins.

    Returns:
        Dictionary of loaded plugins
    """
    return _loaded_plugins.copy()


def get_discovered_plugins() -> Dict[str, Dict[str, Any]]:
    """Get all discovered plugins with their information.

    Returns:
        Dictionary of discovered plugins
    """
    return _discovered_plugins.copy()


def get_search_paths() -> List[str]:
    """Get current plugin search paths.

    Returns:
        List of search paths
    """
    return list(_search_paths)


def get_plugin_manifests() -> Dict[str, PluginManifest]:
    """Get all loaded plugin manifests.

    Returns:
        Dictionary of plugin manifests
    """
    return _plugin_manifests.copy()


def validate_plugin(plugin_name: str) -> Dict[str, Any]:
    """Validate a discovered plugin.

    Args:
        plugin_name: Name of plugin to validate

    Returns:
        Validation results dictionary
    """
    try:
        plugin_info = _discovered_plugins.get(plugin_name)
        if not plugin_info:
            return {"valid": False, "error": "Plugin not found"}

        validation_results = {"valid": True, "warnings": [], "errors": [], "info": []}

        # Check if plugin can be loaded
        try:
            file_path = plugin_info["file_path"]
            module_name = plugin_info["module_name"]
            class_name = plugin_info["class_name"]

            spec = importlib.util.spec_from_file_location(module_name, file_path)
            if not spec or not spec.loader:
                validation_results["valid"] = False
                validation_results["errors"].append("Cannot load module spec")
                return validation_results

            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            plugin_class = getattr(module, class_name)
            plugin_instance = plugin_class()

            # Validate required attributes
            required_attrs = ["name", "version", "description"]
            for attr in required_attrs:
                if not hasattr(plugin_instance, attr):
                    validation_results["errors"].append(f"Missing required attribute: {attr}")
                    validation_results["valid"] = False
                elif not getattr(plugin_instance, attr):
                    validation_results["warnings"].append(f"Empty required attribute: {attr}")

            # Validate required methods
            required_methods = ["initialize", "cleanup"]
            for method in required_methods:
                if not hasattr(plugin_instance, method):
                    validation_results["errors"].append(f"Missing required method: {method}")
                    validation_results["valid"] = False
                elif not callable(getattr(plugin_instance, method)):
                    validation_results["errors"].append(f"Attribute '{method}' is not callable")
                    validation_results["valid"] = False

            # Check dependencies
            dependencies = plugin_info.get("dependencies", [])
            for dep in dependencies:
                if dep not in _discovered_plugins:
                    validation_results["warnings"].append(f"Dependency '{dep}' not found")

            # Validate manifest if present
            manifest_path = plugin_info.get("manifest_path")
            if manifest_path and os.path.exists(manifest_path):
                try:
                    with open(manifest_path, "r") as f:
                        json.load(f)
                    validation_results["info"].append("Valid manifest file found")
                except json.JSONDecodeError:
                    validation_results["warnings"].append("Invalid JSON in manifest file")

        except Exception as e:
            validation_results["valid"] = False
            validation_results["errors"].append(f"Plugin validation failed: {str(e)}")

        return validation_results

    except Exception as e:
        return {"valid": False, "error": f"Validation error: {str(e)}"}


def get_plugin_dependencies(plugin_name: str) -> List[str]:
    """Get dependencies for a plugin.

    Args:
        plugin_name: Name of plugin

    Returns:
        List of dependency plugin names
    """
    plugin_info = _discovered_plugins.get(plugin_name)
    if not plugin_info:
        return []

    return plugin_info.get("dependencies", [])


def cleanup_plugin_discovery() -> None:
    """Cleanup plugin discovery system."""
    try:
        info("Cleaning up plugin discovery system")

        # Unload all loaded plugins
        for plugin_name in list(_loaded_plugins.keys()):
            unload_plugin(plugin_name)

        # Clear all state
        _search_paths.clear()
        _discovered_plugins.clear()
        _loaded_plugins.clear()
        _plugin_manifests.clear()

        success("Plugin discovery system cleanup completed")

    except Exception as e:
        error(f"Error during plugin discovery cleanup: {str(e)}")


def get_discovery_stats() -> Dict[str, Any]:
    """Get plugin discovery statistics.

    Returns:
        Dictionary with discovery statistics
    """
    try:
        total_discovered = len(_discovered_plugins)
        total_loaded = len(_loaded_plugins)

        plugin_types = {}
        for info in _discovered_plugins.values():
            plugin_type = info.get("plugin_type", "unknown")
            plugin_types[plugin_type] = plugin_types.get(plugin_type, 0) + 1

        return {
            "search_paths": len(_search_paths),
            "total_discovered": total_discovered,
            "total_loaded": total_loaded,
            "plugin_types": plugin_types,
            "load_success_rate": (total_loaded / total_discovered * 100) if total_discovered > 0 else 0,
            "paths": list(_search_paths),
        }

    except Exception as e:
        error(f"Error getting discovery stats: {str(e)}")
        return {"error": str(e)}
