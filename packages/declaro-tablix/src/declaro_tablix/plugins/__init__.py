"""TableV2 Plugin System

This package provides a comprehensive plugin system for extending TableV2 functionality
with custom rendering, data processing, and notification capabilities.
"""

from .plugin_manager import get_plugin, get_plugins, register_plugin
from .protocols import DataProcessingPlugin, NotificationPlugin, RenderingPlugin, SystemPlugin

__all__ = [
    "RenderingPlugin",
    "DataProcessingPlugin",
    "NotificationPlugin",
    "SystemPlugin",
    "register_plugin",
    "get_plugin",
    "get_plugins",
]
