"""Plugin API Routes

This package contains FastAPI routes for plugin management,
preview, and configuration.
"""

from .plugin_preview_routes import router

__all__ = ["router"]
