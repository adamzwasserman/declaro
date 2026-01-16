"""
UI components for TableV2 customization system.

This module provides HTMX-based UI components for column customization,
user preferences, and layout management.
"""

# UI Component configuration
UI_CONFIG = {
    "modal_animation_duration": 300,
    "form_validation_delay": 500,
    "auto_save_delay": 2000,
    "max_component_depth": 5,
    "component_cache_ttl": 3600,
}

# UI Error messages
UI_ERRORS = {
    "component_not_found": "UI component not found",
    "invalid_component_data": "Invalid component data provided",
    "render_error": "Failed to render component",
    "validation_failed": "Component validation failed",
    "user_session_expired": "User session expired",
    "permission_denied": "Permission denied for this action",
}

# UI Success messages
UI_SUCCESS = {
    "component_rendered": "Component rendered successfully",
    "customization_saved": "Customization saved successfully",
    "layout_applied": "Layout applied successfully",
    "preferences_updated": "Preferences updated successfully",
}

# Export configuration
__all__ = [
    "UI_CONFIG",
    "UI_ERRORS",
    "UI_SUCCESS",
]
