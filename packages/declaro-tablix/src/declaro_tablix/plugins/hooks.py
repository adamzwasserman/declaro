"""Plugin hook system for Table Module V2.

This module provides function-based plugin hook execution system
for data processing, rendering, and notification hooks.
"""

import time
from typing import Any, Callable, Dict, List, Optional, Union

from declaro_advise import error, info, success, warning
from declaro_tablix.domain.models import ColumnDefinition, TableConfig, TableData
from declaro_tablix.plugins.protocols import (
    PluginHookType,
    PluginPriority,
    get_global_plugin_registry,
)

# Global hook performance tracking
_hook_performance_stats: Dict[str, Dict[str, Any]] = {}
_hook_execution_count: Dict[str, int] = {}


def register_hook(plugin_name: str, hook_type: str, priority: int = PluginPriority.NORMAL) -> bool:
    """Register a hook for a plugin.

    Args:
        plugin_name: Name of plugin
        hook_type: Type of hook to register
        priority: Hook execution priority

    Returns:
        True if registration successful
    """
    try:
        registry = get_global_plugin_registry()

        if registry.register_hook(plugin_name, hook_type, priority):
            info(f"Registered hook '{hook_type}' for plugin '{plugin_name}'")
            return True
        else:
            error(f"Failed to register hook '{hook_type}' for plugin '{plugin_name}'")
            return False

    except Exception as e:
        error(f"Error registering hook: {str(e)}")
        return False


def unregister_hook(plugin_name: str, hook_type: str) -> bool:
    """Unregister a hook for a plugin.

    Args:
        plugin_name: Name of plugin
        hook_type: Type of hook to unregister

    Returns:
        True if unregistration successful
    """
    try:
        registry = get_global_plugin_registry()

        # Find and remove the hook handler
        handlers = registry.hooks.get(hook_type, [])
        original_count = len(handlers)

        registry.hooks[hook_type] = [h for h in handlers if h.plugin.name != plugin_name]

        removed_count = original_count - len(registry.hooks[hook_type])

        if removed_count > 0:
            info(f"Unregistered {removed_count} hook(s) '{hook_type}' for plugin '{plugin_name}'")
            return True
        else:
            warning(f"No hooks '{hook_type}' found for plugin '{plugin_name}'")
            return False

    except Exception as e:
        error(f"Error unregistering hook: {str(e)}")
        return False


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

        if not handlers:
            return []

        info(f"Executing {len(handlers)} hook(s) for '{hook_type}'")

        results = []

        for handler in handlers:
            try:
                start_time = time.time()

                # Execute the hook
                result = _execute_single_hook(handler, hook_type, *args, **kwargs)

                execution_time = time.time() - start_time

                # Update performance stats
                _update_hook_performance(handler.plugin.name, hook_type, execution_time, True)

                results.append(result)

            except Exception as e:
                # Update error stats
                _update_hook_performance(handler.plugin.name, hook_type, 0, False)
                error(f"Hook execution failed for plugin '{handler.plugin.name}' on '{hook_type}': {str(e)}")
                results.append(None)

        success(f"Executed {len(handlers)} hook(s) for '{hook_type}'")
        return results

    except Exception as e:
        error(f"Error executing hooks for '{hook_type}': {str(e)}")
        return []


def _execute_single_hook(handler, hook_type: str, *args, **kwargs) -> Any:
    """Execute a single hook handler.

    Args:
        handler: Hook handler to execute
        hook_type: Type of hook
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        Hook execution result
    """
    try:
        plugin = handler.plugin

        # Check if plugin has a handle_hook method
        if hasattr(plugin, "handle_hook"):
            return plugin.handle_hook(hook_type, *args, **kwargs)

        # Otherwise, call specific plugin methods based on hook type
        if hook_type == PluginHookType.BEFORE_DATA_PROCESS:
            if hasattr(plugin, "process_data"):
                return plugin.process_data(*args, **kwargs)
        elif hook_type == PluginHookType.BEFORE_RENDER:
            if hasattr(plugin, "render_cell"):
                return plugin.render_cell(*args, **kwargs)
        elif hook_type == PluginHookType.ERROR_OCCURRED:
            if hasattr(plugin, "send_notification"):
                return plugin.send_notification(*args, **kwargs)

        # Default: return None if no specific handler found
        return None

    except Exception as e:
        error(f"Single hook execution failed: {str(e)}")
        raise


def list_hooks() -> Dict[str, List[str]]:
    """Get list of all registered hooks grouped by type.

    Returns:
        Dictionary mapping hook types to plugin names
    """
    try:
        registry = get_global_plugin_registry()

        hook_list = {}

        for hook_type, handlers in registry.hooks.items():
            hook_list[hook_type] = [handler.plugin.name for handler in handlers]

        return hook_list

    except Exception as e:
        error(f"Error listing hooks: {str(e)}")
        return {}


def get_hook_count(hook_type: Optional[str] = None) -> Union[int, Dict[str, int]]:
    """Get count of registered hooks.

    Args:
        hook_type: Optional specific hook type

    Returns:
        Count of hooks or dictionary of counts by type
    """
    try:
        registry = get_global_plugin_registry()

        if hook_type:
            handlers = registry.hooks.get(hook_type, [])
            return len(handlers)
        else:
            return {hook_type: len(handlers) for hook_type, handlers in registry.hooks.items()}

    except Exception as e:
        error(f"Error getting hook count: {str(e)}")
        return 0 if hook_type else {}


def clear_hooks(hook_type: Optional[str] = None) -> bool:
    """Clear registered hooks.

    Args:
        hook_type: Optional specific hook type to clear

    Returns:
        True if clearing successful
    """
    try:
        registry = get_global_plugin_registry()

        if hook_type:
            if hook_type in registry.hooks:
                count = len(registry.hooks[hook_type])
                registry.hooks[hook_type] = []
                info(f"Cleared {count} hook(s) for '{hook_type}'")
            else:
                warning(f"Hook type '{hook_type}' not found")
        else:
            total_count = sum(len(handlers) for handlers in registry.hooks.values())
            registry.hooks.clear()
            info(f"Cleared all {total_count} hooks")

        return True

    except Exception as e:
        error(f"Error clearing hooks: {str(e)}")
        return False


def get_hook_registry() -> Dict[str, Any]:
    """Get the current hook registry state.

    Returns:
        Dictionary representation of hook registry
    """
    try:
        registry = get_global_plugin_registry()

        hook_registry = {}

        for hook_type, handlers in registry.hooks.items():
            hook_registry[hook_type] = [
                {
                    "plugin_name": handler.plugin.name,
                    "plugin_version": handler.plugin.version,
                    "priority": handler.priority,
                    "enabled": handler.enabled,
                }
                for handler in handlers
            ]

        return hook_registry

    except Exception as e:
        error(f"Error getting hook registry: {str(e)}")
        return {}


def get_hook_metadata(hook_type: str) -> Dict[str, Any]:
    """Get metadata about a specific hook type.

    Args:
        hook_type: Hook type to get metadata for

    Returns:
        Hook metadata dictionary
    """
    try:
        registry = get_global_plugin_registry()
        handlers = registry.hooks.get(hook_type, [])

        metadata = {
            "hook_type": hook_type,
            "handler_count": len(handlers),
            "handlers": [],
            "execution_stats": _hook_performance_stats.get(hook_type, {}),
            "total_executions": _hook_execution_count.get(hook_type, 0),
        }

        for handler in handlers:
            handler_info = {
                "plugin_name": handler.plugin.name,
                "plugin_type": getattr(handler.plugin, "_plugin_type", "unknown"),
                "priority": handler.priority,
                "enabled": handler.enabled,
            }
            metadata["handlers"].append(handler_info)

        return metadata

    except Exception as e:
        error(f"Error getting hook metadata: {str(e)}")
        return {"error": str(e)}


def hook_with_error_handling(hook_type: str, error_handler: Optional[Callable] = None):
    """Decorator for executing hooks with error handling.

    Args:
        hook_type: Type of hook to execute
        error_handler: Optional custom error handler function

    Returns:
        Decorator function
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                # Execute before hooks
                before_hook = f"before_{hook_type}"
                execute_hook(before_hook, *args, **kwargs)

                # Execute main function
                result = func(*args, **kwargs)

                # Execute after hooks
                after_hook = f"after_{hook_type}"
                execute_hook(after_hook, result, *args, **kwargs)

                return result

            except Exception as e:
                # Execute error hooks
                error_hook = PluginHookType.ERROR_OCCURRED
                execute_hook(error_hook, e, hook_type, *args, **kwargs)

                if error_handler:
                    return error_handler(e, *args, **kwargs)
                else:
                    raise

        return wrapper

    return decorator


def create_hook_context(operation: str, user_id: str, table_name: str, **extra_context) -> Dict[str, Any]:
    """Create context dictionary for hook execution.

    Args:
        operation: Operation being performed
        user_id: User ID
        table_name: Table name
        **extra_context: Additional context variables

    Returns:
        Hook context dictionary
    """
    context = {
        "operation": operation,
        "user_id": user_id,
        "table_name": table_name,
        "timestamp": time.time(),
        "hook_execution_id": f"{operation}_{user_id}_{table_name}_{int(time.time())}",
    }

    context.update(extra_context)
    return context


def process_data_with_hooks(data: TableData, config: TableConfig, context: Dict[str, Any]) -> TableData:
    """Process table data with plugin hooks.

    Args:
        data: Table data to process
        config: Table configuration
        context: Processing context

    Returns:
        Processed table data
    """
    try:
        info("Processing table data with hooks")

        # Execute before data process hooks
        execute_hook(PluginHookType.BEFORE_DATA_PROCESS, data, config, context)

        # Process data (this would call actual data processing functions)
        processed_data = data  # Placeholder - real implementation would process data

        # Execute after data process hooks
        execute_hook(PluginHookType.AFTER_DATA_PROCESS, processed_data, config, context)

        return processed_data

    except Exception as e:
        error(f"Error processing data with hooks: {str(e)}")
        execute_hook(PluginHookType.ERROR_OCCURRED, e, "data_processing", context)
        raise


def render_table_with_hooks(data: TableData, config: TableConfig, context: Dict[str, Any]) -> str:
    """Render table with plugin hooks.

    Args:
        data: Table data to render
        config: Table configuration
        context: Rendering context

    Returns:
        Rendered HTML string
    """
    try:
        info("Rendering table with hooks")

        # Execute before render hooks
        execute_hook(PluginHookType.BEFORE_RENDER, data, config, context)

        # Render table (this would call actual rendering functions)
        rendered_html = "<table></table>"  # Placeholder - real implementation would render

        # Execute after render hooks
        execute_hook(PluginHookType.AFTER_RENDER, rendered_html, data, config, context)

        return rendered_html

    except Exception as e:
        error(f"Error rendering table with hooks: {str(e)}")
        execute_hook(PluginHookType.ERROR_OCCURRED, e, "table_rendering", context)
        raise


def handle_user_action_with_hooks(action: str, data: Any, context: Dict[str, Any]) -> Any:
    """Handle user action with plugin hooks.

    Args:
        action: User action type
        data: Action data
        context: Action context

    Returns:
        Action result
    """
    try:
        info(f"Handling user action '{action}' with hooks")

        # Execute before user action hooks
        execute_hook(PluginHookType.BEFORE_USER_ACTION, action, data, context)

        # Handle action (this would call actual action handlers)
        result = {"action": action, "data": data, "status": "completed"}  # Placeholder

        # Execute after user action hooks
        execute_hook(PluginHookType.AFTER_USER_ACTION, action, result, context)

        return result

    except Exception as e:
        error(f"Error handling user action with hooks: {str(e)}")
        execute_hook(PluginHookType.ERROR_OCCURRED, e, f"user_action_{action}", context)
        raise


def measure_hook_performance(hook_type: str) -> Dict[str, Any]:
    """Get performance measurements for a hook type.

    Args:
        hook_type: Hook type to measure

    Returns:
        Performance measurement dictionary
    """
    try:
        stats = _hook_performance_stats.get(hook_type, {})
        execution_count = _hook_execution_count.get(hook_type, 0)

        if not stats:
            return {
                "hook_type": hook_type,
                "total_executions": 0,
                "average_time": 0,
                "total_time": 0,
                "success_rate": 100,
                "error_count": 0,
            }

        total_time = sum(plugin_stats.get("total_time", 0) for plugin_stats in stats.values())
        success_count = sum(plugin_stats.get("success_count", 0) for plugin_stats in stats.values())
        error_count = sum(plugin_stats.get("error_count", 0) for plugin_stats in stats.values())

        total_executions = success_count + error_count

        return {
            "hook_type": hook_type,
            "total_executions": total_executions,
            "average_time": total_time / total_executions if total_executions > 0 else 0,
            "total_time": total_time,
            "success_rate": (success_count / total_executions * 100) if total_executions > 0 else 100,
            "success_count": success_count,
            "error_count": error_count,
            "plugin_stats": stats,
        }

    except Exception as e:
        error(f"Error measuring hook performance: {str(e)}")
        return {"error": str(e)}


def conditional_hook(condition: Callable[..., bool], hook_type: str):
    """Decorator for conditional hook execution.

    Args:
        condition: Function that returns True if hook should execute
        hook_type: Type of hook to execute conditionally

    Returns:
        Decorator function
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            if condition(*args, **kwargs):
                execute_hook(hook_type, *args, **kwargs)
            return func(*args, **kwargs)

        return wrapper

    return decorator


def _update_hook_performance(plugin_name: str, hook_type: str, execution_time: float, success: bool) -> None:
    """Update hook performance statistics.

    Args:
        plugin_name: Name of plugin
        hook_type: Type of hook
        execution_time: Execution time in seconds
        success: Whether execution was successful
    """
    try:
        # Initialize stats if needed
        if hook_type not in _hook_performance_stats:
            _hook_performance_stats[hook_type] = {}

        if plugin_name not in _hook_performance_stats[hook_type]:
            _hook_performance_stats[hook_type][plugin_name] = {
                "total_time": 0,
                "success_count": 0,
                "error_count": 0,
                "average_time": 0,
            }

        # Update execution count
        _hook_execution_count[hook_type] = _hook_execution_count.get(hook_type, 0) + 1

        # Update plugin stats
        plugin_stats = _hook_performance_stats[hook_type][plugin_name]
        plugin_stats["total_time"] += execution_time

        if success:
            plugin_stats["success_count"] += 1
        else:
            plugin_stats["error_count"] += 1

        # Update average time
        total_executions = plugin_stats["success_count"] + plugin_stats["error_count"]
        if total_executions > 0:
            plugin_stats["average_time"] = plugin_stats["total_time"] / total_executions

    except Exception as e:
        warning(f"Error updating hook performance stats: {str(e)}")


def get_hook_performance_stats() -> Dict[str, Any]:
    """Get all hook performance statistics.

    Returns:
        Dictionary of performance statistics
    """
    try:
        return {
            "performance_stats": _hook_performance_stats.copy(),
            "execution_counts": _hook_execution_count.copy(),
            "summary": {
                "total_hook_types": len(_hook_performance_stats),
                "total_executions": sum(_hook_execution_count.values()),
                "avg_execution_time": (
                    sum(
                        sum(plugin_stats.get("total_time", 0) for plugin_stats in hook_stats.values())
                        for hook_stats in _hook_performance_stats.values()
                    )
                    / sum(_hook_execution_count.values())
                    if sum(_hook_execution_count.values()) > 0
                    else 0
                ),
            },
        }

    except Exception as e:
        error(f"Error getting hook performance stats: {str(e)}")
        return {"error": str(e)}


def reset_hook_performance_stats() -> None:
    """Reset all hook performance statistics."""
    try:
        global _hook_performance_stats, _hook_execution_count
        _hook_performance_stats.clear()
        _hook_execution_count.clear()
        info("Hook performance statistics reset")

    except Exception as e:
        error(f"Error resetting hook performance stats: {str(e)}")


def enable_hook(plugin_name: str, hook_type: str) -> bool:
    """Enable a specific hook for a plugin.

    Args:
        plugin_name: Name of plugin
        hook_type: Type of hook

    Returns:
        True if hook was enabled
    """
    try:
        registry = get_global_plugin_registry()
        handlers = registry.hooks.get(hook_type, [])

        for handler in handlers:
            if handler.plugin.name == plugin_name:
                handler.enabled = True
                info(f"Enabled hook '{hook_type}' for plugin '{plugin_name}'")
                return True

        warning(f"Hook '{hook_type}' not found for plugin '{plugin_name}'")
        return False

    except Exception as e:
        error(f"Error enabling hook: {str(e)}")
        return False


def disable_hook(plugin_name: str, hook_type: str) -> bool:
    """Disable a specific hook for a plugin.

    Args:
        plugin_name: Name of plugin
        hook_type: Type of hook

    Returns:
        True if hook was disabled
    """
    try:
        registry = get_global_plugin_registry()
        handlers = registry.hooks.get(hook_type, [])

        for handler in handlers:
            if handler.plugin.name == plugin_name:
                handler.enabled = False
                info(f"Disabled hook '{hook_type}' for plugin '{plugin_name}'")
                return True

        warning(f"Hook '{hook_type}' not found for plugin '{plugin_name}'")
        return False

    except Exception as e:
        error(f"Error disabling hook: {str(e)}")
        return False
