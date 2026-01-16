"""Plugin protocols for Table Module V2.

This module defines the plugin interfaces using Python's typing.Protocol
for the plugin architecture system using function-based design.
"""

from abc import abstractmethod
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

from declaro_tablix.domain.models import ColumnDefinition, TableConfig, TableData


@runtime_checkable
class TablePlugin(Protocol):
    """Base protocol for all table plugins."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Plugin name identifier."""
        ...

    @property
    @abstractmethod
    def version(self) -> str:
        """Plugin version string."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """Plugin description."""
        ...

    @abstractmethod
    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the plugin with configuration."""
        ...

    @abstractmethod
    def cleanup(self) -> None:
        """Cleanup resources when plugin is unloaded."""
        ...


@runtime_checkable
class DataProcessingPlugin(TablePlugin, Protocol):
    """Protocol for plugins that process table data."""

    @abstractmethod
    def process_data(self, data: TableData, config: TableConfig, context: Dict[str, Any]) -> TableData:
        """Process table data and return modified data."""
        ...

    @abstractmethod
    def validate_data(self, data: TableData, config: TableConfig, context: Dict[str, Any]) -> List[str]:
        """Validate table data and return list of error messages."""
        ...

    @abstractmethod
    def transform_column(self, column: ColumnDefinition, data: List[Any], context: Dict[str, Any]) -> List[Any]:
        """Transform data for a specific column."""
        ...


@runtime_checkable
class RenderingPlugin(TablePlugin, Protocol):
    """Protocol for plugins that customize table rendering."""

    @abstractmethod
    def render_cell(self, value: Any, column: ColumnDefinition, row_data: Dict[str, Any], context: Dict[str, Any]) -> str:
        """Render a custom cell value."""
        ...

    @abstractmethod
    def render_header(self, column: ColumnDefinition, context: Dict[str, Any]) -> str:
        """Render a custom column header."""
        ...

    @abstractmethod
    def render_controls(self, config: TableConfig, context: Dict[str, Any]) -> str:
        """Render custom table controls."""
        ...

    @abstractmethod
    def get_css_classes(self, value: Any, column: ColumnDefinition, context: Dict[str, Any]) -> List[str]:
        """Get CSS classes for styling."""
        ...


@runtime_checkable
class NotificationPlugin(TablePlugin, Protocol):
    """Protocol for plugins that handle notifications."""

    @abstractmethod
    def send_notification(self, message: str, notification_type: str, context: Dict[str, Any]) -> bool:
        """Send a notification through the plugin."""
        ...

    @abstractmethod
    def get_notification_template(self, notification_type: str, context: Dict[str, Any]) -> str:
        """Get custom notification template."""
        ...


@runtime_checkable
class SystemPlugin(TablePlugin, Protocol):
    """Protocol for plugins that extend system functionality."""

    @abstractmethod
    def extend_functionality(self, operation: str, data: Any, context: Dict[str, Any]) -> Any:
        """Extend system functionality with custom operations."""
        ...

    @abstractmethod
    def get_supported_operations(self) -> List[str]:
        """Get list of operations this plugin supports."""
        ...


class PluginHookType:
    """Enum-like class for plugin hook types."""

    # Data processing hooks
    BEFORE_DATA_LOAD = "before_data_load"
    AFTER_DATA_LOAD = "after_data_load"
    BEFORE_DATA_PROCESS = "before_data_process"
    AFTER_DATA_PROCESS = "after_data_process"
    BEFORE_DATA_SAVE = "before_data_save"
    AFTER_DATA_SAVE = "after_data_save"
    BEFORE_DATA_FILTER = "before_data_filter"
    AFTER_DATA_FILTER = "after_data_filter"

    # Rendering hooks
    BEFORE_RENDER = "before_render"
    AFTER_RENDER = "after_render"
    BEFORE_CELL_RENDER = "before_cell_render"
    AFTER_CELL_RENDER = "after_cell_render"
    BEFORE_HEADER_RENDER = "before_header_render"
    AFTER_HEADER_RENDER = "after_header_render"

    # Table lifecycle hooks
    BEFORE_TABLE_CREATE = "before_table_create"
    AFTER_TABLE_CREATE = "after_table_create"
    BEFORE_TABLE_UPDATE = "before_table_update"
    AFTER_TABLE_UPDATE = "after_table_update"
    BEFORE_TABLE_DELETE = "before_table_delete"
    AFTER_TABLE_DELETE = "after_table_delete"

    # User action hooks
    BEFORE_USER_ACTION = "before_user_action"
    AFTER_USER_ACTION = "after_user_action"
    BEFORE_SEARCH = "before_search"
    AFTER_SEARCH = "after_search"
    BEFORE_EXPORT = "before_export"
    AFTER_EXPORT = "after_export"

    # System hooks
    PLUGIN_LOADED = "plugin_loaded"
    PLUGIN_UNLOADED = "plugin_unloaded"
    ERROR_OCCURRED = "error_occurred"
    PERFORMANCE_WARNING = "performance_warning"


class PluginPriority:
    """Plugin execution priority levels."""

    CRITICAL = 0  # System-critical plugins
    HIGH = 1  # High priority plugins
    NORMAL = 2  # Normal priority plugins
    LOW = 3  # Low priority plugins
    BACKGROUND = 4  # Background/optional plugins


class PluginHookHandler:
    """Hook handler function signature."""

    def __init__(self, plugin: TablePlugin, hook_type: str, priority: int = PluginPriority.NORMAL):
        self.plugin = plugin
        self.hook_type = hook_type
        self.priority = priority
        self.enabled = True

    def __call__(self, *args, **kwargs) -> Any:
        """Execute the hook handler."""
        if not self.enabled:
            return None

        try:
            # Call the appropriate plugin method based on hook type
            if hasattr(self.plugin, "handle_hook"):
                return self.plugin.handle_hook(self.hook_type, *args, **kwargs)
            else:
                # Default hook handling
                return None
        except Exception as e:
            # Log error but don't break the hook chain
            print(f"Plugin {self.plugin.name} hook {self.hook_type} failed: {e}")
            return None


class PluginManifest:
    """Plugin manifest for metadata and configuration."""

    def __init__(
        self,
        name: str,
        version: str,
        description: str,
        author: str = "",
        dependencies: Optional[List[str]] = None,
        configuration: Optional[Dict[str, Any]] = None,
        supported_hooks: Optional[List[str]] = None,
        plugin_type: str = "generic",
    ):
        self.name = name
        self.version = version
        self.description = description
        self.author = author
        self.dependencies = dependencies or []
        self.configuration = configuration or {}
        self.supported_hooks = supported_hooks or []
        self.plugin_type = plugin_type

    def to_dict(self) -> Dict[str, Any]:
        """Convert manifest to dictionary."""
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "author": self.author,
            "dependencies": self.dependencies,
            "configuration": self.configuration,
            "supported_hooks": self.supported_hooks,
            "plugin_type": self.plugin_type,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PluginManifest":
        """Create manifest from dictionary."""
        return cls(
            name=data["name"],
            version=data["version"],
            description=data["description"],
            author=data.get("author", ""),
            dependencies=data.get("dependencies", []),
            configuration=data.get("configuration", {}),
            supported_hooks=data.get("supported_hooks", []),
            plugin_type=data.get("plugin_type", "generic"),
        )


class PluginRegistry:
    """Plugin registry for managing registered plugins."""

    def __init__(self):
        self.plugins: Dict[str, TablePlugin] = {}
        self.manifests: Dict[str, PluginManifest] = {}
        self.hooks: Dict[str, List[PluginHookHandler]] = {}
        self.plugin_stats: Dict[str, Dict[str, Any]] = {}

    def register_plugin(self, plugin: TablePlugin, manifest: PluginManifest) -> bool:
        """Register a plugin in the registry."""
        if plugin.name in self.plugins:
            return False

        self.plugins[plugin.name] = plugin
        self.manifests[plugin.name] = manifest
        self.plugin_stats[plugin.name] = {"load_time": 0, "execution_count": 0, "error_count": 0, "last_execution": None}
        return True

    def unregister_plugin(self, plugin_name: str) -> bool:
        """Unregister a plugin from the registry."""
        if plugin_name not in self.plugins:
            return False

        # Cleanup plugin hooks
        for hook_type, handlers in self.hooks.items():
            self.hooks[hook_type] = [h for h in handlers if h.plugin.name != plugin_name]

        # Remove plugin data
        del self.plugins[plugin_name]
        del self.manifests[plugin_name]
        del self.plugin_stats[plugin_name]

        return True

    def get_plugin(self, plugin_name: str) -> Optional[TablePlugin]:
        """Get a plugin by name."""
        return self.plugins.get(plugin_name)

    def get_plugins_by_type(self, plugin_type: type) -> List[TablePlugin]:
        """Get all plugins of a specific type."""
        return [plugin for plugin in self.plugins.values() if isinstance(plugin, plugin_type)]

    def get_hook_handlers(self, hook_type: str) -> List[PluginHookHandler]:
        """Get all hook handlers for a specific hook type."""
        handlers = self.hooks.get(hook_type, [])
        # Sort by priority (lower number = higher priority)
        return sorted(handlers, key=lambda h: h.priority)

    def register_hook(self, plugin_name: str, hook_type: str, priority: int = PluginPriority.NORMAL) -> bool:
        """Register a hook handler for a plugin."""
        plugin = self.get_plugin(plugin_name)
        if not plugin:
            return False

        if hook_type not in self.hooks:
            self.hooks[hook_type] = []

        handler = PluginHookHandler(plugin, hook_type, priority)
        self.hooks[hook_type].append(handler)
        return True

    def get_all_plugins(self) -> Dict[str, TablePlugin]:
        """Get all registered plugins."""
        return self.plugins.copy()

    def get_plugin_stats(self, plugin_name: str) -> Optional[Dict[str, Any]]:
        """Get statistics for a specific plugin."""
        return self.plugin_stats.get(plugin_name)

    def update_plugin_stats(self, plugin_name: str, execution_time: float, success: bool = True) -> None:
        """Update plugin execution statistics."""
        if plugin_name in self.plugin_stats:
            stats = self.plugin_stats[plugin_name]
            stats["execution_count"] += 1
            stats["last_execution"] = execution_time
            if not success:
                stats["error_count"] += 1


# Global plugin registry instance (singleton pattern for shared state)
_global_plugin_registry: Optional[PluginRegistry] = None


def get_global_plugin_registry() -> PluginRegistry:
    """Get or create the global plugin registry."""
    global _global_plugin_registry
    if _global_plugin_registry is None:
        _global_plugin_registry = PluginRegistry()
    return _global_plugin_registry


def reset_global_plugin_registry() -> None:
    """Reset the global plugin registry (useful for testing)."""
    global _global_plugin_registry
    _global_plugin_registry = None
