"""
TableV2 Column Customization Module.

This module provides comprehensive column customization functionality including
column aliases, visibility settings, ordering, and user preference persistence.
"""

# Configuration constants

# Module version
__version__ = "1.0.0"

# Module exports
__all__ = [
    "CustomizationPersistence",
    "ColumnCustomization",
    "UserPreferences",
    "CustomizationValidator",
    "CustomizationRepository",
    "CustomizationService",
]

# Configuration constants
CUSTOMIZATION_CONFIG = {
    "max_column_alias_length": 255,
    "max_user_preferences_size": 1000000,  # 1MB JSON
    "default_column_visibility": True,
    "max_column_order": 9999,
    "supported_customization_types": ["alias", "visibility", "order", "width", "format", "filter", "sort"],
    "cache_ttl_seconds": 300,
    "validation_enabled": True,
}

# Error messages
CUSTOMIZATION_ERRORS = {
    "invalid_column_id": "Invalid column ID provided",
    "invalid_user_id": "Invalid user ID provided",
    "invalid_table_name": "Invalid table name provided",
    "customization_not_found": "Customization not found",
    "invalid_customization_data": "Invalid customization data",
    "persistence_failed": "Failed to persist customization",
    "validation_failed": "Customization validation failed",
    "duplicate_customization": "Duplicate customization exists",
    "max_size_exceeded": "Customization data exceeds maximum size",
}

# Success messages
CUSTOMIZATION_SUCCESS = {
    "customization_saved": "Customization saved successfully",
    "customization_updated": "Customization updated successfully",
    "customization_deleted": "Customization deleted successfully",
    "preferences_saved": "User preferences saved successfully",
    "preferences_loaded": "User preferences loaded successfully",
    "validation_passed": "Customization validation passed",
}
