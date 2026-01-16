"""Customization repository implementation for Table Module V2.

This module provides function-based user customization persistence operations,
following the repository pattern with FastAPI dependency injection.
"""

import json
from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from declaro_persistum.compat import get_db
from declaro_advise import error, info, success, warning
from declaro_tablix.domain.models import (
    UserColumnRename,
    UserFilterSet,
    UserSavedLayout,
    UserSavedSearch,
)


def save_column_customization(
    user_id: str, table_name: str, customization: UserColumnRename, db_session: Session = None
) -> bool:
    """Save column customization.

    Args:
        user_id: User ID who owns the customization
        table_name: Name of the table
        customization: UserColumnRename object to save
        db_session: Database session (injected)

    Returns:
        True if save successful, False otherwise
    """
    try:
        info(f"Saving column customization for user '{user_id}', table '{table_name}', column '{customization.column_id}'")

        if not db_session:
            db_session = next(get_db())

        # Check if customization already exists
        check_query = text(
            """
            SELECT id FROM user_column_customizations
            WHERE user_id = :user_id AND table_name = :table_name AND column_id = :column_id
        """
        )

        existing = db_session.execute(
            check_query, {"user_id": user_id, "table_name": table_name, "column_id": customization.column_id}
        ).fetchone()

        if existing:
            # Update existing customization
            update_query = text(
                """
                UPDATE user_column_customizations
                SET custom_name = :custom_name, is_visible = :is_visible, updated_at = :updated_at
                WHERE user_id = :user_id AND table_name = :table_name AND column_id = :column_id
            """
            )

            db_session.execute(
                update_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "column_id": customization.column_id,
                    "custom_name": customization.custom_name,
                    "is_visible": customization.is_visible,
                    "updated_at": datetime.utcnow(),
                },
            )

            success(f"Updated column customization for column '{customization.column_id}'")
        else:
            # Create new customization
            insert_query = text(
                """
                INSERT INTO user_column_customizations
                (user_id, table_name, column_id, custom_name, is_visible, created_at, updated_at)
                VALUES (:user_id, :table_name, :column_id, :custom_name, :is_visible, :created_at, :updated_at)
            """
            )

            now = datetime.utcnow()
            db_session.execute(
                insert_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "column_id": customization.column_id,
                    "custom_name": customization.custom_name,
                    "is_visible": customization.is_visible,
                    "created_at": now,
                    "updated_at": now,
                },
            )

            success(f"Created new column customization for column '{customization.column_id}'")

        db_session.commit()
        return True

    except SQLAlchemyError as e:
        error(f"Database error saving column customization: {str(e)}")
        if db_session:
            db_session.rollback()
        return False
    except Exception as e:
        error(f"Error saving column customization: {str(e)}")
        if db_session:
            db_session.rollback()
        return False


def load_column_customizations(user_id: str, table_name: str, db_session: Session = None) -> List[UserColumnRename]:
    """Load user column customizations.

    Args:
        user_id: User ID who owns the customizations
        table_name: Name of the table
        db_session: Database session (injected)

    Returns:
        List of UserColumnRename objects
    """
    try:
        info(f"Loading column customizations for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        query = text(
            """
            SELECT column_id, custom_name, is_visible
            FROM user_column_customizations
            WHERE user_id = :user_id AND table_name = :table_name
            ORDER BY column_id
        """
        )

        result = db_session.execute(query, {"user_id": user_id, "table_name": table_name})

        customizations = []
        for row in result:
            customization = UserColumnRename(column_id=row.column_id, custom_name=row.custom_name, is_visible=row.is_visible)
            customizations.append(customization)

        success(f"Loaded {len(customizations)} column customizations for user '{user_id}'")
        return customizations

    except SQLAlchemyError as e:
        error(f"Database error loading column customizations: {str(e)}")
        return []
    except Exception as e:
        error(f"Error loading column customizations: {str(e)}")
        return []


def save_saved_search(user_id: str, table_name: str, search: UserSavedSearch, db_session: Session = None) -> bool:
    """Save named search configuration.

    Args:
        user_id: User ID who owns the search
        table_name: Name of the table
        search: UserSavedSearch object to save
        db_session: Database session (injected)

    Returns:
        True if save successful, False otherwise
    """
    try:
        info(f"Saving search '{search.name}' for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        # If this is being set as default, clear other defaults first
        if search.is_default:
            clear_query = text(
                """
                UPDATE user_saved_searches
                SET is_default = false
                WHERE user_id = :user_id AND table_name = :table_name AND is_default = true
            """
            )
            db_session.execute(clear_query, {"user_id": user_id, "table_name": table_name})

        # Check if search already exists
        check_query = text(
            """
            SELECT id FROM user_saved_searches
            WHERE user_id = :user_id AND table_name = :table_name AND search_name = :search_name
        """
        )

        existing = db_session.execute(
            check_query, {"user_id": user_id, "table_name": table_name, "search_name": search.name}
        ).fetchone()

        if existing:
            # Update existing search
            update_query = text(
                """
                UPDATE user_saved_searches
                SET search_term = :search_term, is_default = :is_default, updated_at = :updated_at
                WHERE user_id = :user_id AND table_name = :table_name AND search_name = :search_name
            """
            )

            db_session.execute(
                update_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "search_name": search.name,
                    "search_term": search.search_term,
                    "is_default": search.is_default,
                    "updated_at": datetime.utcnow(),
                },
            )

            success(f"Updated saved search '{search.name}'")
        else:
            # Create new search
            insert_query = text(
                """
                INSERT INTO user_saved_searches
                (user_id, table_name, search_name, search_term, is_default, created_at, updated_at)
                VALUES (:user_id, :table_name, :search_name, :search_term, :is_default, :created_at, :updated_at)
            """
            )

            now = datetime.utcnow()
            db_session.execute(
                insert_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "search_name": search.name,
                    "search_term": search.search_term,
                    "is_default": search.is_default,
                    "created_at": now,
                    "updated_at": now,
                },
            )

            success(f"Created new saved search '{search.name}'")

        db_session.commit()
        return True

    except SQLAlchemyError as e:
        error(f"Database error saving search: {str(e)}")
        if db_session:
            db_session.rollback()
        return False
    except Exception as e:
        error(f"Error saving search: {str(e)}")
        if db_session:
            db_session.rollback()
        return False


def load_saved_searches(user_id: str, table_name: str, db_session: Session = None) -> List[UserSavedSearch]:
    """Load user saved searches.

    Args:
        user_id: User ID who owns the searches
        table_name: Name of the table
        db_session: Database session (injected)

    Returns:
        List of UserSavedSearch objects
    """
    try:
        info(f"Loading saved searches for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        query = text(
            """
            SELECT search_name, search_term, is_default
            FROM user_saved_searches
            WHERE user_id = :user_id AND table_name = :table_name
            ORDER BY is_default DESC, search_name
        """
        )

        result = db_session.execute(query, {"user_id": user_id, "table_name": table_name})

        searches = []
        for row in result:
            search = UserSavedSearch(search_name=row.search_name, search_term=row.search_term, is_default=row.is_default)
            searches.append(search)

        success(f"Loaded {len(searches)} saved searches for user '{user_id}'")
        return searches

    except SQLAlchemyError as e:
        error(f"Database error loading saved searches: {str(e)}")
        return []
    except Exception as e:
        error(f"Error loading saved searches: {str(e)}")
        return []


def save_filter_set(user_id: str, table_name: str, filter_set: UserFilterSet, db_session: Session = None) -> bool:
    """Save filter set configuration.

    Args:
        user_id: User ID who owns the filter set
        table_name: Name of the table
        filter_set: UserFilterSet object to save
        db_session: Database session (injected)

    Returns:
        True if save successful, False otherwise
    """
    try:
        info(f"Saving filter set '{filter_set.name}' for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        # If this is being set as default, clear other defaults first
        if filter_set.is_default:
            clear_query = text(
                """
                UPDATE user_filter_sets
                SET is_default = false
                WHERE user_id = :user_id AND table_name = :table_name AND is_default = true
            """
            )
            db_session.execute(clear_query, {"user_id": user_id, "table_name": table_name})

        # Serialize filters to JSON
        filters_json = json.dumps([f.model_dump() for f in filter_set.filters])

        # Check if filter set already exists
        check_query = text(
            """
            SELECT id FROM user_filter_sets
            WHERE user_id = :user_id AND table_name = :table_name AND filter_set_name = :filter_set_name
        """
        )

        existing = db_session.execute(
            check_query, {"user_id": user_id, "table_name": table_name, "filter_set_name": filter_set.name}
        ).fetchone()

        if existing:
            # Update existing filter set
            update_query = text(
                """
                UPDATE user_filter_sets
                SET filters_json = :filters_json, is_default = :is_default, updated_at = :updated_at
                WHERE user_id = :user_id AND table_name = :table_name AND filter_set_name = :filter_set_name
            """
            )

            db_session.execute(
                update_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "filter_set_name": filter_set.name,
                    "filters_json": filters_json,
                    "is_default": filter_set.is_default,
                    "updated_at": datetime.utcnow(),
                },
            )

            success(f"Updated filter set '{filter_set.name}'")
        else:
            # Create new filter set
            insert_query = text(
                """
                INSERT INTO user_filter_sets
                (user_id, table_name, filter_set_name, filters_json, is_default, created_at, updated_at)
                VALUES (:user_id, :table_name, :filter_set_name, :filters_json, :is_default, :created_at, :updated_at)
            """
            )

            now = datetime.utcnow()
            db_session.execute(
                insert_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "filter_set_name": filter_set.name,
                    "filters_json": filters_json,
                    "is_default": filter_set.is_default,
                    "created_at": now,
                    "updated_at": now,
                },
            )

            success(f"Created new filter set '{filter_set.name}'")

        db_session.commit()
        return True

    except SQLAlchemyError as e:
        error(f"Database error saving filter set: {str(e)}")
        if db_session:
            db_session.rollback()
        return False
    except Exception as e:
        error(f"Error saving filter set: {str(e)}")
        if db_session:
            db_session.rollback()
        return False


def load_filter_sets(user_id: str, table_name: str, db_session: Session = None) -> List[UserFilterSet]:
    """Load user filter sets.

    Args:
        user_id: User ID who owns the filter sets
        table_name: Name of the table
        db_session: Database session (injected)

    Returns:
        List of UserFilterSet objects
    """
    try:
        info(f"Loading filter sets for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        query = text(
            """
            SELECT filter_set_name, filters_json, is_default
            FROM user_filter_sets
            WHERE user_id = :user_id AND table_name = :table_name
            ORDER BY is_default DESC, filter_set_name
        """
        )

        result = db_session.execute(query, {"user_id": user_id, "table_name": table_name})

        filter_sets = []
        for row in result:
            # Deserialize filters from JSON
            try:
                filters_data = json.loads(row.filters_json)
                from declaro_tablix.domain.models import FilterDefinition

                filters = [FilterDefinition(**f) for f in filters_data]

                filter_set = UserFilterSet(filter_set_name=row.filter_set_name, filters=filters, is_default=row.is_default)
                filter_sets.append(filter_set)
            except (json.JSONDecodeError, ValueError) as e:
                warning(f"Failed to deserialize filter set '{row.filter_set_name}': {str(e)}")
                continue

        success(f"Loaded {len(filter_sets)} filter sets for user '{user_id}'")
        return filter_sets

    except SQLAlchemyError as e:
        error(f"Database error loading filter sets: {str(e)}")
        return []
    except Exception as e:
        error(f"Error loading filter sets: {str(e)}")
        return []


def save_layout_preset(user_id: str, table_name: str, layout: UserSavedLayout, db_session: Session = None) -> bool:
    """Save complete layout preset.

    Args:
        user_id: User ID who owns the layout
        table_name: Name of the table
        layout: UserSavedLayout object to save
        db_session: Database session (injected)

    Returns:
        True if save successful, False otherwise
    """
    try:
        info(f"Saving layout '{layout.name}' for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        # If this is being set as default, clear other defaults first
        if layout.is_default:
            clear_query = text(
                """
                UPDATE user_saved_layouts
                SET is_default = false
                WHERE user_id = :user_id AND table_name = :table_name AND is_default = true
            """
            )
            db_session.execute(clear_query, {"user_id": user_id, "table_name": table_name})

        # Serialize layout configuration to JSON
        layout_json = layout.model_dump_json()

        # Check if layout already exists
        check_query = text(
            """
            SELECT id FROM user_saved_layouts
            WHERE user_id = :user_id AND table_name = :table_name AND layout_name = :layout_name
        """
        )

        existing = db_session.execute(
            check_query, {"user_id": user_id, "table_name": table_name, "layout_name": layout.name}
        ).fetchone()

        if existing:
            # Update existing layout
            update_query = text(
                """
                UPDATE user_saved_layouts
                SET layout_config = :layout_config, is_default = :is_default, updated_at = :updated_at
                WHERE user_id = :user_id AND table_name = :table_name AND layout_name = :layout_name
            """
            )

            db_session.execute(
                update_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "layout_name": layout.name,
                    "layout_config": layout_json,
                    "is_default": layout.is_default,
                    "updated_at": datetime.utcnow(),
                },
            )

            success(f"Updated layout '{layout.name}'")
        else:
            # Create new layout
            insert_query = text(
                """
                INSERT INTO user_saved_layouts
                (user_id, table_name, layout_name, layout_config, is_default, created_at, updated_at)
                VALUES (:user_id, :table_name, :layout_name, :layout_config, :is_default, :created_at, :updated_at)
            """
            )

            now = datetime.utcnow()
            db_session.execute(
                insert_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "layout_name": layout.name,
                    "layout_config": layout_json,
                    "is_default": layout.is_default,
                    "created_at": now,
                    "updated_at": now,
                },
            )

            success(f"Created new layout '{layout.name}'")

        db_session.commit()
        return True

    except SQLAlchemyError as e:
        error(f"Database error saving layout: {str(e)}")
        if db_session:
            db_session.rollback()
        return False
    except Exception as e:
        error(f"Error saving layout: {str(e)}")
        if db_session:
            db_session.rollback()
        return False


def load_layout_presets(user_id: str, table_name: str, db_session: Session = None) -> List[UserSavedLayout]:
    """Load user layout presets.

    Args:
        user_id: User ID who owns the layouts
        table_name: Name of the table
        db_session: Database session (injected)

    Returns:
        List of UserSavedLayout objects
    """
    try:
        info(f"Loading layout presets for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        query = text(
            """
            SELECT layout_name, layout_config, is_default
            FROM user_saved_layouts
            WHERE user_id = :user_id AND table_name = :table_name
            ORDER BY is_default DESC, layout_name
        """
        )

        result = db_session.execute(query, {"user_id": user_id, "table_name": table_name})

        layouts = []
        for row in result:
            try:
                # Deserialize layout from JSON
                layout_data = json.loads(row.layout_config)
                layout = UserSavedLayout(**layout_data)
                layouts.append(layout)
            except (json.JSONDecodeError, ValueError) as e:
                warning(f"Failed to deserialize layout '{row.layout_name}': {str(e)}")
                continue

        success(f"Loaded {len(layouts)} layout presets for user '{user_id}'")
        return layouts

    except SQLAlchemyError as e:
        error(f"Database error loading layout presets: {str(e)}")
        return []
    except Exception as e:
        error(f"Error loading layout presets: {str(e)}")
        return []


def delete_customization_item(
    user_id: str, table_name: str, item_type: str, item_name: str, db_session: Session = None
) -> bool:
    """Delete a customization item (search, filter set, or layout).

    Args:
        user_id: User ID who owns the item
        table_name: Name of the table
        item_type: Type of item ('search', 'filter_set', 'layout')
        item_name: Name of the item to delete
        db_session: Database session (injected)

    Returns:
        True if delete successful, False otherwise
    """
    try:
        info(f"Deleting {item_type} '{item_name}' for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        # Map item types to table names and column names
        type_mapping = {
            "search": ("user_saved_searches", "search_name"),
            "filter_set": ("user_filter_sets", "filter_set_name"),
            "layout": ("user_saved_layouts", "layout_name"),
        }

        if item_type not in type_mapping:
            error(f"Unknown item type: {item_type}")
            return False

        table_name_db, name_column = type_mapping[item_type]

        delete_query = text(
            f"""
            DELETE FROM {table_name_db}
            WHERE user_id = :user_id AND table_name = :table_name AND {name_column} = :item_name
        """
        )

        result = db_session.execute(delete_query, {"user_id": user_id, "table_name": table_name, "item_name": item_name})

        if result.rowcount > 0:
            db_session.commit()
            success(f"Deleted {item_type} '{item_name}'")
            return True
        else:
            warning(f"No {item_type} '{item_name}' found to delete")
            return False

    except SQLAlchemyError as e:
        error(f"Database error deleting {item_type}: {str(e)}")
        if db_session:
            db_session.rollback()
        return False
    except Exception as e:
        error(f"Error deleting {item_type}: {str(e)}")
        if db_session:
            db_session.rollback()
        return False


def get_customization_stats(user_id: str, db_session: Session = None) -> Dict[str, Any]:
    """Get statistics about user's customizations.

    Args:
        user_id: User ID to get stats for
        db_session: Database session (injected)

    Returns:
        Dictionary with customization statistics
    """
    try:
        info(f"Getting customization statistics for user '{user_id}'")

        if not db_session:
            db_session = next(get_db())

        stats = {}

        # Column customizations count
        col_query = text(
            """
            SELECT COUNT(*) as count FROM user_column_customizations WHERE user_id = :user_id
        """
        )
        col_result = db_session.execute(col_query, {"user_id": user_id}).fetchone()
        stats["column_customizations"] = col_result.count if col_result else 0

        # Saved searches count
        search_query = text(
            """
            SELECT COUNT(*) as count FROM user_saved_searches WHERE user_id = :user_id
        """
        )
        search_result = db_session.execute(search_query, {"user_id": user_id}).fetchone()
        stats["saved_searches"] = search_result.count if search_result else 0

        # Filter sets count
        filter_query = text(
            """
            SELECT COUNT(*) as count FROM user_filter_sets WHERE user_id = :user_id
        """
        )
        filter_result = db_session.execute(filter_query, {"user_id": user_id}).fetchone()
        stats["filter_sets"] = filter_result.count if filter_result else 0

        # Layouts count
        layout_query = text(
            """
            SELECT COUNT(*) as count FROM user_saved_layouts WHERE user_id = :user_id
        """
        )
        layout_result = db_session.execute(layout_query, {"user_id": user_id}).fetchone()
        stats["saved_layouts"] = layout_result.count if layout_result else 0

        # Total customizations
        stats["total_customizations"] = (
            stats["column_customizations"] + stats["saved_searches"] + stats["filter_sets"] + stats["saved_layouts"]
        )

        success(f"Retrieved customization statistics for user '{user_id}'")
        return stats

    except SQLAlchemyError as e:
        error(f"Database error getting customization statistics: {str(e)}")
        return {"error": str(e)}
    except Exception as e:
        error(f"Error getting customization statistics: {str(e)}")
        return {"error": str(e)}


# FastAPI Dependency Injection Functions
def get_customization_repository_for_dependency() -> callable:
    """FastAPI dependency for customization repository operations."""
    return {
        "save_column": save_column_customization,
        "load_columns": load_column_customizations,
        "save_search": save_saved_search,
        "load_searches": load_saved_searches,
        "save_filter_set": save_filter_set,
        "load_filter_sets": load_filter_sets,
        "save_layout": save_layout_preset,
        "load_layouts": load_layout_presets,
        "delete_item": delete_customization_item,
        "stats": get_customization_stats,
    }


def get_save_customization_for_dependency() -> callable:
    """FastAPI dependency for save customization operations."""
    return save_column_customization


def get_load_customizations_for_dependency() -> callable:
    """FastAPI dependency for load customization operations."""
    return load_column_customizations
