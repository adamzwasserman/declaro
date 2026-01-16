"""Default preferences service functions for Table Module V2.

This module provides pure functions for managing user default preferences with
validation and consistency enforcement. All dependencies are explicit parameters
for clean library design.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from declaro_advise import error, info, success, warning
from declaro_tablix.domain.models import (
    UserDefaultPreferences,
    UserFilterSet,
    UserSavedLayout,
    UserSavedSearch,
)
from declaro_tablix.domain.protocols import DefaultPreferencesService as DefaultPreferencesServiceProtocol
from declaro_tablix.domain.protocols import (
    UserLayoutRepository,
    create_noop_layout_repository,
    create_noop_preferences_service,
)
from declaro_tablix.domain.validators import (
    validate_default_uniqueness,
)

# Default Search Functions


async def set_default_search(
    user_id: str,
    table_id: str,
    search_name: str,
    layout_repo: UserLayoutRepository | None = None,
    prefs_service: DefaultPreferencesServiceProtocol | None = None,
) -> bool:
    """Set a search as the default for a user and table.

    Args:
        user_id: User ID
        table_id: Table ID
        search_name: Name of search to set as default
        layout_repo: Repository for layout operations (required)
        prefs_service: Optional default preferences service

    Returns:
        True if successful, False otherwise
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    if prefs_service is None:
        prefs_service = create_noop_preferences_service()
    try:
        info(f"Setting default search '{search_name}' for user: {user_id}, table: {table_id}")

        # Get all user items for validation
        searches = await layout_repo.get_user_saved_searches(user_id, table_id)
        filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)
        layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

        # Find the target search
        target_search = next((s for s in searches if s.name == search_name), None)
        if not target_search:
            error(f"Search '{search_name}' not found")
            return False

        # Clear all existing defaults
        for search in searches:
            search.is_default = False

        # Set new default
        target_search.is_default = True

        # Validate uniqueness
        is_valid, errors = validate_default_uniqueness(
            saved_searches=searches,
            filter_sets=filter_sets,
            layouts=layouts,
            user_id=user_id,
            table_id=table_id,
        )

        if not is_valid:
            error(f"Default validation failed: {', '.join(errors)}")
            return False

        # Save all searches
        for search in searches:
            await layout_repo.save_user_search(search)

        # Update preferences cache
        await prefs_service.invalidate_preferences_cache(user_id, table_id)

        success(f"Default search '{search_name}' set successfully")
        return True

    except Exception as e:
        error(f"Failed to set default search: {str(e)}")
        return False


async def clear_default_search(
    user_id: str,
    table_id: str,
    layout_repo: UserLayoutRepository | None = None,
    prefs_service: DefaultPreferencesServiceProtocol | None = None,
) -> bool:
    """Clear the default search for a user and table.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_repo: Repository for layout operations (required)
        prefs_service: Optional default preferences service

    Returns:
        True if successful, False otherwise
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    if prefs_service is None:
        prefs_service = create_noop_preferences_service()
    try:
        info(f"Clearing default search for user: {user_id}, table: {table_id}")

        # Get all searches
        searches = await layout_repo.get_user_saved_searches(user_id, table_id)

        # Clear all defaults
        for search in searches:
            if search.is_default:
                search.is_default = False
                await layout_repo.save_user_search(search)

        # Update preferences cache
        await prefs_service.invalidate_preferences_cache(user_id, table_id)

        success(f"Default search cleared successfully")
        return True

    except Exception as e:
        error(f"Failed to clear default search: {str(e)}")
        return False


# Default Filter Set Functions


async def set_default_filter_set(
    user_id: str,
    table_id: str,
    filter_set_name: str,
    layout_repo: UserLayoutRepository | None = None,
    prefs_service: DefaultPreferencesServiceProtocol | None = None,
) -> bool:
    """Set a filter set as the default for a user and table.

    Args:
        user_id: User ID
        table_id: Table ID
        filter_set_name: Name of filter set to set as default
        layout_repo: Repository for layout operations (required)
        prefs_service: Optional default preferences service

    Returns:
        True if successful, False otherwise
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    if prefs_service is None:
        prefs_service = create_noop_preferences_service()
    try:
        info(f"Setting default filter set '{filter_set_name}' for user: {user_id}, table: {table_id}")

        # Get all user items for validation
        searches = await layout_repo.get_user_saved_searches(user_id, table_id)
        filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)
        layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

        # Find the target filter set
        target_filter_set = next((f for f in filter_sets if f.name == filter_set_name), None)
        if not target_filter_set:
            error(f"Filter set '{filter_set_name}' not found")
            return False

        # Clear all existing defaults
        for filter_set in filter_sets:
            filter_set.is_default = False

        # Set new default
        target_filter_set.is_default = True

        # Validate uniqueness
        is_valid, errors = validate_default_uniqueness(
            saved_searches=searches,
            filter_sets=filter_sets,
            layouts=layouts,
            user_id=user_id,
            table_id=table_id,
        )

        if not is_valid:
            error(f"Default validation failed: {', '.join(errors)}")
            return False

        # Save all filter sets
        for filter_set in filter_sets:
            await layout_repo.save_user_filter_set(filter_set)

        # Update preferences cache
        await prefs_service.invalidate_preferences_cache(user_id, table_id)

        success(f"Default filter set '{filter_set_name}' set successfully")
        return True

    except Exception as e:
        error(f"Failed to set default filter set: {str(e)}")
        return False


async def clear_default_filter_set(
    user_id: str,
    table_id: str,
    layout_repo: UserLayoutRepository | None = None,
    prefs_service: DefaultPreferencesServiceProtocol | None = None,
) -> bool:
    """Clear the default filter set for a user and table.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_repo: Repository for layout operations (required)
        prefs_service: Optional default preferences service

    Returns:
        True if successful, False otherwise
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    if prefs_service is None:
        prefs_service = create_noop_preferences_service()
    try:
        info(f"Clearing default filter set for user: {user_id}, table: {table_id}")

        # Get all filter sets
        filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)

        # Clear all defaults
        for filter_set in filter_sets:
            if filter_set.is_default:
                filter_set.is_default = False
                await layout_repo.save_user_filter_set(filter_set)

        # Update preferences cache
        await prefs_service.invalidate_preferences_cache(user_id, table_id)

        success(f"Default filter set cleared successfully")
        return True

    except Exception as e:
        error(f"Failed to clear default filter set: {str(e)}")
        return False


# Default Layout Functions


async def set_default_layout(
    user_id: str,
    table_id: str,
    layout_name: str,
    layout_repo: UserLayoutRepository | None = None,
    prefs_service: DefaultPreferencesServiceProtocol | None = None,
) -> bool:
    """Set a layout as the default for a user and table.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_name: Name of layout to set as default
        layout_repo: Repository for layout operations (required)
        prefs_service: Optional default preferences service

    Returns:
        True if successful, False otherwise
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    if prefs_service is None:
        prefs_service = create_noop_preferences_service()
    try:
        info(f"Setting default layout '{layout_name}' for user: {user_id}, table: {table_id}")

        # Get all user items for validation
        searches = await layout_repo.get_user_saved_searches(user_id, table_id)
        filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)
        layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

        # Find the target layout
        target_layout = next((l for l in layouts if l.name == layout_name), None)
        if not target_layout:
            error(f"Layout '{layout_name}' not found")
            return False

        # Clear all existing defaults
        for layout in layouts:
            layout.is_default = False

        # Set new default
        target_layout.is_default = True

        # Validate uniqueness
        is_valid, errors = validate_default_uniqueness(
            saved_searches=searches,
            filter_sets=filter_sets,
            layouts=layouts,
            user_id=user_id,
            table_id=table_id,
        )

        if not is_valid:
            error(f"Default validation failed: {', '.join(errors)}")
            return False

        # Save all layouts
        for layout in layouts:
            await layout_repo.save_user_layout(layout)

        # Update preferences cache
        await prefs_service.invalidate_preferences_cache(user_id, table_id)

        success(f"Default layout '{layout_name}' set successfully")
        return True

    except Exception as e:
        error(f"Failed to set default layout: {str(e)}")
        return False


async def clear_default_layout(
    user_id: str,
    table_id: str,
    layout_repo: UserLayoutRepository | None = None,
    prefs_service: DefaultPreferencesServiceProtocol | None = None,
) -> bool:
    """Clear the default layout for a user and table.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_repo: Repository for layout operations (required)
        prefs_service: Optional default preferences service

    Returns:
        True if successful, False otherwise
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    if prefs_service is None:
        prefs_service = create_noop_preferences_service()
    try:
        info(f"Clearing default layout for user: {user_id}, table: {table_id}")

        # Get all layouts
        layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

        # Clear all defaults
        for layout in layouts:
            if layout.is_default:
                layout.is_default = False
                await layout_repo.save_user_layout(layout)

        # Update preferences cache
        await prefs_service.invalidate_preferences_cache(user_id, table_id)

        success(f"Default layout cleared successfully")
        return True

    except Exception as e:
        error(f"Failed to clear default layout: {str(e)}")
        return False


# Preference Management Functions


async def get_default_preferences(
    user_id: str,
    table_id: str,
    layout_repo: UserLayoutRepository | None = None,
    prefs_service: DefaultPreferencesServiceProtocol | None = None,
) -> UserDefaultPreferences:
    """Get comprehensive default preferences for a user and table.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_repo: Repository for layout operations (required)
        prefs_service: Optional default preferences service

    Returns:
        UserDefaultPreferences object
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    if prefs_service is None:
        prefs_service = create_noop_preferences_service()
    try:
        info(f"Getting default preferences for user: {user_id}, table: {table_id}")

        # Try cache first
        cached_prefs = await prefs_service.get_cached_preferences(user_id, table_id)
        if cached_prefs:
            return cached_prefs

        # Load all user items
        searches = await layout_repo.get_user_saved_searches(user_id, table_id)
        filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)
        layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

        # Find defaults
        default_search = next((s for s in searches if s.is_default), None)
        default_filter_set = next((f for f in filter_sets if f.is_default), None)
        default_layout = next((l for l in layouts if l.is_default), None)

        # Create preferences
        preferences = UserDefaultPreferences(
            user_id=user_id,
            table_id=table_id,
            default_search=default_search,
            default_filter_set=default_filter_set,
            default_layout=default_layout,
        )

        # Cache preferences
        await prefs_service.cache_preferences(preferences, ttl_seconds=1800)  # 30 minutes

        success(f"Default preferences retrieved successfully")
        return preferences

    except Exception as e:
        error(f"Failed to get default preferences: {str(e)}")
        return UserDefaultPreferences(user_id=user_id, table_id=table_id)


async def validate_default_consistency(
    user_id: str,
    table_id: str,
    layout_repo: UserLayoutRepository | None = None,
) -> Dict[str, Any]:
    """Validate consistency of default preferences.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_repo: Repository for layout operations (required)

    Returns:
        Dictionary with validation results
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Validating default consistency for user: {user_id}, table: {table_id}")

        # Load all user items
        searches = await layout_repo.get_user_saved_searches(user_id, table_id)
        filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)
        layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

        # Count defaults
        default_search_count = sum(1 for s in searches if s.is_default)
        default_filter_set_count = sum(1 for f in filter_sets if f.is_default)
        default_layout_count = sum(1 for l in layouts if l.is_default)

        # Validate using domain validator
        is_valid, errors = validate_default_uniqueness(
            saved_searches=searches,
            filter_sets=filter_sets,
            layouts=layouts,
            user_id=user_id,
            table_id=table_id,
        )

        # Build validation result
        validation_result = {
            "is_consistent": is_valid,
            "errors": errors,
            "default_counts": {
                "searches": default_search_count,
                "filter_sets": default_filter_set_count,
                "layouts": default_layout_count,
            },
            "total_items": {
                "searches": len(searches),
                "filter_sets": len(filter_sets),
                "layouts": len(layouts),
            },
            "recommendations": [],
        }

        # Add recommendations
        if default_search_count > 1:
            validation_result["recommendations"].append("Remove duplicate default searches")
        if default_filter_set_count > 1:
            validation_result["recommendations"].append("Remove duplicate default filter sets")
        if default_layout_count > 1:
            validation_result["recommendations"].append("Remove duplicate default layouts")

        if is_valid:
            success(f"Default consistency validation passed")
        else:
            warning(f"Default consistency issues found: {len(errors)} errors")

        return validation_result

    except Exception as e:
        error(f"Failed to validate default consistency: {str(e)}")
        return {
            "is_consistent": False,
            "errors": [str(e)],
            "default_counts": {},
            "total_items": {},
            "recommendations": ["Unable to validate - check system logs"],
        }


async def repair_default_consistency(
    user_id: str,
    table_id: str,
    layout_repo: UserLayoutRepository | None = None,
    prefs_service: DefaultPreferencesServiceProtocol | None = None,
) -> Dict[str, Any]:
    """Repair default preference consistency issues.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_repo: Repository for layout operations (required)
        prefs_service: Optional default preferences service

    Returns:
        Dictionary with repair results
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    if prefs_service is None:
        prefs_service = create_noop_preferences_service()
    try:
        info(f"Repairing default consistency for user: {user_id}, table: {table_id}")

        # Validate first
        validation_result = await validate_default_consistency(user_id, table_id, layout_repo)

        if validation_result["is_consistent"]:
            return {
                "repairs_needed": False,
                "repairs_performed": [],
                "final_state": validation_result,
            }

        repairs_performed = []

        # Load all user items
        searches = await layout_repo.get_user_saved_searches(user_id, table_id)
        filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)
        layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

        # Repair searches (keep first default, clear others)
        default_searches = [s for s in searches if s.is_default]
        if len(default_searches) > 1:
            for i, search in enumerate(default_searches[1:], 1):
                search.is_default = False
                await layout_repo.save_user_search(search)
                repairs_performed.append(f"Cleared default from search: {search.name}")

        # Repair filter sets (keep first default, clear others)
        default_filter_sets = [f for f in filter_sets if f.is_default]
        if len(default_filter_sets) > 1:
            for i, filter_set in enumerate(default_filter_sets[1:], 1):
                filter_set.is_default = False
                await layout_repo.save_user_filter_set(filter_set)
                repairs_performed.append(f"Cleared default from filter set: {filter_set.name}")

        # Repair layouts (keep first default, clear others)
        default_layouts = [l for l in layouts if l.is_default]
        if len(default_layouts) > 1:
            for i, layout in enumerate(default_layouts[1:], 1):
                layout.is_default = False
                await layout_repo.save_user_layout(layout)
                repairs_performed.append(f"Cleared default from layout: {layout.name}")

        # Clear cache
        await prefs_service.invalidate_preferences_cache(user_id, table_id)

        # Re-validate
        final_validation = await validate_default_consistency(user_id, table_id, layout_repo)

        repair_result = {
            "repairs_needed": True,
            "repairs_performed": repairs_performed,
            "final_state": final_validation,
        }

        if final_validation["is_consistent"]:
            success(f"Default consistency repaired successfully ({len(repairs_performed)} repairs)")
        else:
            warning(f"Default consistency repair incomplete")

        return repair_result

    except Exception as e:
        error(f"Failed to repair default consistency: {str(e)}")
        return {
            "repairs_needed": True,
            "repairs_performed": [],
            "final_state": {"is_consistent": False, "errors": [str(e)]},
        }


# Utility Functions


async def get_default_summary(
    user_id: str,
    table_id: str,
    layout_repo: UserLayoutRepository | None = None,
) -> Dict[str, Any]:
    """Get a summary of default preferences for a user and table.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_repo: Repository for layout operations (required)

    Returns:
        Dictionary with default preference summary
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Getting default summary for user: {user_id}, table: {table_id}")

        # Load all user items
        searches = await layout_repo.get_user_saved_searches(user_id, table_id)
        filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)
        layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

        # Find defaults
        default_search = next((s for s in searches if s.is_default), None)
        default_filter_set = next((f for f in filter_sets if f.is_default), None)
        default_layout = next((l for l in layouts if l.is_default), None)

        summary = {
            "user_id": user_id,
            "table_id": table_id,
            "has_defaults": {
                "search": default_search is not None,
                "filter_set": default_filter_set is not None,
                "layout": default_layout is not None,
            },
            "default_names": {
                "search": default_search.name if default_search else None,
                "filter_set": default_filter_set.name if default_filter_set else None,
                "layout": default_layout.name if default_layout else None,
            },
            "total_items": {
                "searches": len(searches),
                "filter_sets": len(filter_sets),
                "layouts": len(layouts),
            },
            "completion_percentage": 0,
        }

        # Calculate completion percentage
        total_possible = 3  # search, filter_set, layout
        total_configured = sum(summary["has_defaults"].values())
        summary["completion_percentage"] = round((total_configured / total_possible) * 100, 1)

        success(f"Default summary generated successfully")
        return summary

    except Exception as e:
        error(f"Failed to get default summary: {str(e)}")
        return {
            "user_id": user_id,
            "table_id": table_id,
            "error": str(e),
        }


async def reset_all_defaults(
    user_id: str,
    table_id: str,
    layout_repo: UserLayoutRepository | None = None,
    prefs_service: DefaultPreferencesServiceProtocol | None = None,
) -> bool:
    """Reset all default preferences for a user and table.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_repo: Repository for layout operations (required)
        prefs_service: Optional default preferences service

    Returns:
        True if successful, False otherwise
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    if prefs_service is None:
        prefs_service = create_noop_preferences_service()
    try:
        info(f"Resetting all defaults for user: {user_id}, table: {table_id}")

        # Clear all defaults
        await clear_default_search(user_id, table_id, layout_repo, prefs_service)
        await clear_default_filter_set(user_id, table_id, layout_repo, prefs_service)
        await clear_default_layout(user_id, table_id, layout_repo, prefs_service)

        success(f"All defaults reset successfully")
        return True

    except Exception as e:
        error(f"Failed to reset all defaults: {str(e)}")
        return False
