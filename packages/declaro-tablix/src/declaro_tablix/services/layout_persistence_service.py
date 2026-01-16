"""Layout persistence service functions for Table Module V2.

This module provides pure functions for saving and loading user searches, filter sets,
layouts, and default preferences. All dependencies are explicit parameters for clean
library design.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from declaro_advise import error, info, success
from declaro_tablix.domain.models import (
    UserDefaultPreferences,
    UserFilterSet,
    UserSavedLayout,
    UserSavedSearch,
)
from declaro_tablix.domain.protocols import (
    UserLayoutRepository,
    create_noop_layout_repository,
)
from declaro_tablix.domain.validators import (
    validate_default_uniqueness,
    validate_saved_layout_completeness,
)

# User Search Functions


async def save_user_search(
    user_id: str,
    table_id: str,
    name: str,
    search_term: str,
    layout_repo: UserLayoutRepository | None = None,
    is_default: bool = False,
) -> Optional[UserSavedSearch]:
    """Save a user search configuration.

    Args:
        user_id: User ID
        table_id: Table ID
        name: Search name
        search_term: Search term to save
        layout_repo: Repository for layout operations (optional, uses no-op if not provided)
        is_default: Whether this should be the default search

    Returns:
        UserSavedSearch object or None if error
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Saving user search '{name}' for user: {user_id}, table: {table_id}")

        # Create search object
        search = UserSavedSearch(
            user_id=user_id,
            table_id=table_id,
            name=name,
            search_term=search_term,
            is_default=is_default,
        )

        # If setting as default, validate uniqueness
        if is_default:
            existing_searches = await layout_repo.get_user_saved_searches(user_id, table_id)
            existing_filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)
            existing_layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

            # Add new search to list for validation
            all_searches = existing_searches + [search]

            is_valid, errors = validate_default_uniqueness(
                saved_searches=all_searches,
                filter_sets=existing_filter_sets,
                layouts=existing_layouts,
                user_id=user_id,
                table_id=table_id,
            )

            if not is_valid:
                error(f"Default validation failed: {', '.join(errors)}")
                return None

            # Clear other defaults
            await _clear_default_searches(user_id, table_id, layout_repo)

        # Save search
        saved_search = await layout_repo.save_user_search(search)

        success(f"User search '{name}' saved successfully")
        return saved_search

    except Exception as e:
        error(f"Failed to save user search: {str(e)}")
        return None


async def load_user_searches(
    user_id: str,
    table_id: str,
    layout_repo: UserLayoutRepository | None = None,
) -> List[UserSavedSearch]:
    """Load all saved searches for a user and table.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_repo: Repository for layout operations (required)

    Returns:
        List of UserSavedSearch objects
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Loading user searches for user: {user_id}, table: {table_id}")

        searches = await layout_repo.get_user_saved_searches(user_id, table_id)

        success(f"Loaded {len(searches)} user searches")
        return searches

    except Exception as e:
        error(f"Failed to load user searches: {str(e)}")
        return []


async def delete_user_search(
    user_id: str,
    table_id: str,
    search_name: str,
    layout_repo: UserLayoutRepository | None = None,
) -> bool:
    """Delete a user search.

    Args:
        user_id: User ID
        table_id: Table ID
        search_name: Name of search to delete
        layout_repo: Repository for layout operations (required)

    Returns:
        True if successful, False otherwise
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Deleting user search '{search_name}' for user: {user_id}, table: {table_id}")

        result = await layout_repo.delete_user_search(user_id, table_id, search_name)

        if result:
            success(f"User search '{search_name}' deleted successfully")
        else:
            error(f"User search '{search_name}' not found")

        return result

    except Exception as e:
        error(f"Failed to delete user search: {str(e)}")
        return False


# User Filter Set Functions


async def save_user_filter_set(
    user_id: str,
    table_id: str,
    name: str,
    filters: List[Dict[str, Any]],
    layout_repo: UserLayoutRepository | None = None,
    is_default: bool = False,
) -> Optional[UserFilterSet]:
    """Save a user filter set configuration.

    Args:
        user_id: User ID
        table_id: Table ID
        name: Filter set name
        filters: List of filter definitions
        layout_repo: Repository for layout operations (optional, uses no-op if not provided)
        is_default: Whether this should be the default filter set

    Returns:
        UserFilterSet object or None if error
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Saving user filter set '{name}' for user: {user_id}, table: {table_id}")

        # Create filter set object
        filter_set = UserFilterSet(
            user_id=user_id,
            table_id=table_id,
            name=name,
            filters=filters,
            is_default=is_default,
        )

        # If setting as default, validate uniqueness
        if is_default:
            existing_searches = await layout_repo.get_user_saved_searches(user_id, table_id)
            existing_filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)
            existing_layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

            # Add new filter set to list for validation
            all_filter_sets = existing_filter_sets + [filter_set]

            is_valid, errors = validate_default_uniqueness(
                saved_searches=existing_searches,
                filter_sets=all_filter_sets,
                layouts=existing_layouts,
                user_id=user_id,
                table_id=table_id,
            )

            if not is_valid:
                error(f"Default validation failed: {', '.join(errors)}")
                return None

            # Clear other defaults
            await _clear_default_filter_sets(user_id, table_id, layout_repo)

        # Save filter set
        saved_filter_set = await layout_repo.save_user_filter_set(filter_set)

        success(f"User filter set '{name}' saved successfully")
        return saved_filter_set

    except Exception as e:
        error(f"Failed to save user filter set: {str(e)}")
        return None


async def load_user_filter_sets(
    user_id: str,
    table_id: str,
    layout_repo: UserLayoutRepository | None = None,
) -> List[UserFilterSet]:
    """Load all saved filter sets for a user and table.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_repo: Repository for layout operations (required)

    Returns:
        List of UserFilterSet objects
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Loading user filter sets for user: {user_id}, table: {table_id}")

        filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)

        success(f"Loaded {len(filter_sets)} user filter sets")
        return filter_sets

    except Exception as e:
        error(f"Failed to load user filter sets: {str(e)}")
        return []


async def delete_user_filter_set(
    user_id: str,
    table_id: str,
    filter_set_name: str,
    layout_repo: UserLayoutRepository | None = None,
) -> bool:
    """Delete a user filter set.

    Args:
        user_id: User ID
        table_id: Table ID
        filter_set_name: Name of filter set to delete
        layout_repo: Repository for layout operations (required)

    Returns:
        True if successful, False otherwise
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Deleting user filter set '{filter_set_name}' for user: {user_id}, table: {table_id}")

        result = await layout_repo.delete_user_filter_set(user_id, table_id, filter_set_name)

        if result:
            success(f"User filter set '{filter_set_name}' deleted successfully")
        else:
            error(f"User filter set '{filter_set_name}' not found")

        return result

    except Exception as e:
        error(f"Failed to delete user filter set: {str(e)}")
        return False


# User Layout Functions


async def save_complete_layout(
    user_id: str,
    table_id: str,
    name: str,
    column_configuration: Dict[str, Any],
    filter_configuration: Dict[str, Any],
    search_configuration: Dict[str, Any],
    sort_configuration: Dict[str, Any],
    pagination_configuration: Dict[str, Any],
    customization_configuration: Dict[str, Any],
    layout_repo: UserLayoutRepository | None = None,
    is_default: bool = False,
) -> Optional[UserSavedLayout]:
    """Save a complete layout configuration.

    Args:
        user_id: User ID
        table_id: Table ID
        name: Layout name
        column_configuration: Column layout configuration
        filter_configuration: Filter configuration
        search_configuration: Search configuration
        sort_configuration: Sort configuration
        pagination_configuration: Pagination configuration
        customization_configuration: Customization configuration
        layout_repo: Repository for layout operations (optional, uses no-op if not provided)
        is_default: Whether this should be the default layout

    Returns:
        UserSavedLayout object or None if error
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Saving complete layout '{name}' for user: {user_id}, table: {table_id}")

        # Create layout object
        layout = UserSavedLayout(
            user_id=user_id,
            table_id=table_id,
            name=name,
            column_configuration=column_configuration,
            filter_configuration=filter_configuration,
            search_configuration=search_configuration,
            sort_configuration=sort_configuration,
            pagination_configuration=pagination_configuration,
            customization_configuration=customization_configuration,
            is_default=is_default,
        )

        # Validate layout completeness
        is_valid, errors = validate_saved_layout_completeness(layout)
        if not is_valid:
            error(f"Layout validation failed: {', '.join(errors)}")
            return None

        # If setting as default, validate uniqueness
        if is_default:
            existing_searches = await layout_repo.get_user_saved_searches(user_id, table_id)
            existing_filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)
            existing_layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

            # Add new layout to list for validation
            all_layouts = existing_layouts + [layout]

            is_valid, errors = validate_default_uniqueness(
                saved_searches=existing_searches,
                filter_sets=existing_filter_sets,
                layouts=all_layouts,
                user_id=user_id,
                table_id=table_id,
            )

            if not is_valid:
                error(f"Default validation failed: {', '.join(errors)}")
                return None

            # Clear other defaults
            await _clear_default_layouts(user_id, table_id, layout_repo)

        # Save layout
        saved_layout = await layout_repo.save_user_layout(layout)

        success(f"Complete layout '{name}' saved successfully")
        return saved_layout

    except Exception as e:
        error(f"Failed to save complete layout: {str(e)}")
        return None


async def load_saved_layouts(
    user_id: str,
    table_id: str,
    layout_repo: UserLayoutRepository | None = None,
) -> List[UserSavedLayout]:
    """Load all saved layouts for a user and table.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_repo: Repository for layout operations (required)

    Returns:
        List of UserSavedLayout objects
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Loading saved layouts for user: {user_id}, table: {table_id}")

        layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

        success(f"Loaded {len(layouts)} saved layouts")
        return layouts

    except Exception as e:
        error(f"Failed to load saved layouts: {str(e)}")
        return []


async def delete_saved_layout(
    user_id: str,
    table_id: str,
    layout_name: str,
    layout_repo: UserLayoutRepository | None = None,
) -> bool:
    """Delete a saved layout.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_name: Name of layout to delete
        layout_repo: Repository for layout operations (required)

    Returns:
        True if successful, False otherwise
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Deleting saved layout '{layout_name}' for user: {user_id}, table: {table_id}")

        result = await layout_repo.delete_user_layout(user_id, table_id, layout_name)

        if result:
            success(f"Saved layout '{layout_name}' deleted successfully")
        else:
            error(f"Saved layout '{layout_name}' not found")

        return result

    except Exception as e:
        error(f"Failed to delete saved layout: {str(e)}")
        return False


# Default Management Functions


async def get_user_defaults(
    user_id: str,
    table_id: str,
    layout_repo: UserLayoutRepository | None = None,
) -> UserDefaultPreferences:
    """Get all user default preferences for a table.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_repo: Repository for layout operations (required)

    Returns:
        UserDefaultPreferences object with all defaults
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Loading user defaults for user: {user_id}, table: {table_id}")

        # Load all user preferences
        searches = await layout_repo.get_user_saved_searches(user_id, table_id)
        filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)
        layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)

        # Find defaults
        default_search = next((s for s in searches if s.is_default), None)
        default_filter_set = next((f for f in filter_sets if f.is_default), None)
        default_layout = next((l for l in layouts if l.is_default), None)

        # Create preferences object
        preferences = UserDefaultPreferences(
            user_id=user_id,
            table_id=table_id,
            default_search=default_search,
            default_filter_set=default_filter_set,
            default_layout=default_layout,
        )

        success(f"Loaded user defaults successfully")
        return preferences

    except Exception as e:
        error(f"Failed to load user defaults: {str(e)}")
        return UserDefaultPreferences(user_id=user_id, table_id=table_id)


async def apply_default_layout(
    user_id: str,
    table_id: str,
    base_configuration: Dict[str, Any],
    layout_repo: UserLayoutRepository | None = None,
) -> Dict[str, Any]:
    """Apply default layout preferences to a base configuration.

    Args:
        user_id: User ID
        table_id: Table ID
        base_configuration: Base table configuration to apply defaults to
        layout_repo: Repository for layout operations (required)

    Returns:
        Configuration with defaults applied
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Applying default layout for user: {user_id}, table: {table_id}")

        # Get user defaults
        defaults = await get_user_defaults(user_id, table_id, layout_repo)

        # Start with base configuration
        applied_config = base_configuration.copy()

        # Apply default search
        if defaults.default_search:
            applied_config["search_term"] = defaults.default_search.search_term

        # Apply default filter set
        if defaults.default_filter_set:
            applied_config["filters"] = defaults.default_filter_set.filters

        # Apply default layout
        if defaults.default_layout:
            if "columns" not in applied_config:
                applied_config["columns"] = {}
            applied_config["columns"].update(defaults.default_layout.column_configuration)

            if "pagination" not in applied_config:
                applied_config["pagination"] = {}
            applied_config["pagination"].update(defaults.default_layout.pagination_configuration)

            if "sorts" not in applied_config:
                applied_config["sorts"] = []
            applied_config["sorts"] = defaults.default_layout.sort_configuration.get("sort_definitions", [])

            if "customizations" not in applied_config:
                applied_config["customizations"] = {}
            applied_config["customizations"].update(defaults.default_layout.customization_configuration)

        success(f"Default layout applied successfully")
        return applied_config

    except Exception as e:
        error(f"Failed to apply default layout: {str(e)}")
        return base_configuration


# Layout Management Functions


async def duplicate_layout(
    user_id: str,
    table_id: str,
    source_layout_name: str,
    new_layout_name: str,
    layout_repo: UserLayoutRepository | None = None,
) -> Optional[UserSavedLayout]:
    """Duplicate an existing layout with a new name.

    Args:
        user_id: User ID
        table_id: Table ID
        source_layout_name: Name of layout to duplicate
        new_layout_name: Name for the new layout
        layout_repo: Repository for layout operations (required)

    Returns:
        New UserSavedLayout object or None if error
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Duplicating layout '{source_layout_name}' to '{new_layout_name}' for user: {user_id}")

        # Get source layout
        layouts = await load_saved_layouts(user_id, table_id, layout_repo)
        source_layout = next((l for l in layouts if l.name == source_layout_name), None)

        if not source_layout:
            error(f"Source layout '{source_layout_name}' not found")
            return None

        # Create new layout with same configuration
        new_layout = await save_complete_layout(
            user_id=user_id,
            table_id=table_id,
            name=new_layout_name,
            column_configuration=source_layout.column_configuration,
            filter_configuration=source_layout.filter_configuration,
            search_configuration=source_layout.search_configuration,
            sort_configuration=source_layout.sort_configuration,
            pagination_configuration=source_layout.pagination_configuration,
            customization_configuration=source_layout.customization_configuration,
            is_default=False,  # Duplicates are never default
            layout_repo=layout_repo,
        )

        success(f"Layout duplicated successfully")
        return new_layout

    except Exception as e:
        error(f"Failed to duplicate layout: {str(e)}")
        return None


async def export_user_layout(
    user_id: str,
    table_id: str,
    layout_name: str,
    layout_repo: UserLayoutRepository | None = None,
) -> Optional[str]:
    """Export a user layout as JSON string.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_name: Name of layout to export
        layout_repo: Repository for layout operations (required)

    Returns:
        JSON string of layout or None if error
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Exporting layout '{layout_name}' for user: {user_id}")

        # Get layout
        layouts = await load_saved_layouts(user_id, table_id, layout_repo)
        layout = next((l for l in layouts if l.name == layout_name), None)

        if not layout:
            error(f"Layout '{layout_name}' not found")
            return None

        # Convert to exportable format
        export_data = {
            "name": layout.name,
            "table_id": layout.table_id,
            "column_configuration": layout.column_configuration,
            "filter_configuration": layout.filter_configuration,
            "search_configuration": layout.search_configuration,
            "sort_configuration": layout.sort_configuration,
            "pagination_configuration": layout.pagination_configuration,
            "customization_configuration": layout.customization_configuration,
            "exported_at": datetime.now().isoformat(),
            "export_version": "1.0",
        }

        json_export = json.dumps(export_data, indent=2)

        success(f"Layout exported successfully")
        return json_export

    except Exception as e:
        error(f"Failed to export layout: {str(e)}")
        return None


async def import_user_layout(
    user_id: str,
    table_id: str,
    layout_json: str,
    layout_repo: UserLayoutRepository | None = None,
    new_name: Optional[str] = None,
) -> Optional[UserSavedLayout]:
    """Import a user layout from JSON string.

    Args:
        user_id: User ID
        table_id: Table ID
        layout_json: JSON string of layout to import
        layout_repo: Repository for layout operations (optional, uses no-op if not provided)
        new_name: Optional new name for imported layout

    Returns:
        Imported UserSavedLayout object or None if error
    """
    if layout_repo is None:
        layout_repo = create_noop_layout_repository()
    try:
        info(f"Importing layout for user: {user_id}, table: {table_id}")

        # Parse JSON
        import_data = json.loads(layout_json)

        # Validate required fields
        required_fields = [
            "column_configuration",
            "filter_configuration",
            "search_configuration",
            "sort_configuration",
            "pagination_configuration",
            "customization_configuration",
        ]

        for field in required_fields:
            if field not in import_data:
                raise ValueError(f"Missing required field: {field}")

        # Use provided name or imported name
        layout_name = new_name or import_data.get("name", f"Imported Layout {datetime.now().strftime('%Y%m%d_%H%M%S')}")

        # Create layout
        imported_layout = await save_complete_layout(
            user_id=user_id,
            table_id=table_id,
            name=layout_name,
            column_configuration=import_data["column_configuration"],
            filter_configuration=import_data["filter_configuration"],
            search_configuration=import_data["search_configuration"],
            sort_configuration=import_data["sort_configuration"],
            pagination_configuration=import_data["pagination_configuration"],
            customization_configuration=import_data["customization_configuration"],
            is_default=False,  # Imports are never default
            layout_repo=layout_repo,
        )

        success(f"Layout imported successfully")
        return imported_layout

    except Exception as e:
        error(f"Failed to import layout: {str(e)}")
        return None


# Helper functions for clearing defaults


async def _clear_default_searches(user_id: str, table_id: str, layout_repo: UserLayoutRepository) -> None:
    """Clear all default searches for a user/table."""
    searches = await layout_repo.get_user_saved_searches(user_id, table_id)
    for search in searches:
        if search.is_default:
            search.is_default = False
            await layout_repo.save_user_search(search)


async def _clear_default_filter_sets(user_id: str, table_id: str, layout_repo: UserLayoutRepository) -> None:
    """Clear all default filter sets for a user/table."""
    filter_sets = await layout_repo.get_user_filter_sets(user_id, table_id)
    for filter_set in filter_sets:
        if filter_set.is_default:
            filter_set.is_default = False
            await layout_repo.save_user_filter_set(filter_set)


async def _clear_default_layouts(user_id: str, table_id: str, layout_repo: UserLayoutRepository) -> None:
    """Clear all default layouts for a user/table."""
    layouts = await layout_repo.get_user_saved_layouts(user_id, table_id)
    for layout in layouts:
        if layout.is_default:
            layout.is_default = False
            await layout_repo.save_user_layout(layout)
