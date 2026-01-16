"""User preference repository implementation for Table Module V2.

This module provides function-based user preference persistence operations,
following the repository pattern with FastAPI dependency injection.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from declaro_persistum.compat import get_db
from declaro_advise import error, info, success, warning
from declaro_tablix.domain.models import UserDefaultPreferences


def save_user_preferences(
    user_id: str, table_name: str, preferences: UserDefaultPreferences, db_session: Session = None
) -> bool:
    """Save user default preferences.

    Args:
        user_id: User ID who owns the preferences
        table_name: Name of the table
        preferences: UserDefaultPreferences object to save
        db_session: Database session (injected)

    Returns:
        True if save successful, False otherwise
    """
    try:
        info(f"Saving user preferences for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        # Serialize preferences to JSON
        preferences_json = preferences.model_dump_json()

        # Check if preferences already exist
        check_query = text(
            """
            SELECT id FROM user_default_preferences
            WHERE user_id = :user_id AND table_name = :table_name
        """
        )

        existing = db_session.execute(check_query, {"user_id": user_id, "table_name": table_name}).fetchone()

        if existing:
            # Update existing preferences
            update_query = text(
                """
                UPDATE user_default_preferences
                SET preferences_config = :preferences_json, updated_at = :updated_at
                WHERE user_id = :user_id AND table_name = :table_name
            """
            )

            db_session.execute(
                update_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "preferences_json": preferences_json,
                    "updated_at": datetime.utcnow(),
                },
            )

            success(f"Updated user preferences for user '{user_id}', table '{table_name}'")
        else:
            # Create new preferences
            insert_query = text(
                """
                INSERT INTO user_default_preferences
                (user_id, table_name, preferences_config, created_at, updated_at)
                VALUES (:user_id, :table_name, :preferences_json, :created_at, :updated_at)
            """
            )

            now = datetime.utcnow()
            db_session.execute(
                insert_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "preferences_json": preferences_json,
                    "created_at": now,
                    "updated_at": now,
                },
            )

            success(f"Created new user preferences for user '{user_id}', table '{table_name}'")

        db_session.commit()
        return True

    except SQLAlchemyError as e:
        error(f"Database error saving user preferences: {str(e)}")
        if db_session:
            db_session.rollback()
        return False
    except Exception as e:
        error(f"Error saving user preferences: {str(e)}")
        if db_session:
            db_session.rollback()
        return False


def load_user_preferences(user_id: str, table_name: str, db_session: Session = None) -> Optional[UserDefaultPreferences]:
    """Load user default preferences.

    Args:
        user_id: User ID who owns the preferences
        table_name: Name of the table
        db_session: Database session (injected)

    Returns:
        UserDefaultPreferences object if found, None otherwise
    """
    try:
        info(f"Loading user preferences for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        query = text(
            """
            SELECT preferences_config FROM user_default_preferences
            WHERE user_id = :user_id AND table_name = :table_name
        """
        )

        result = db_session.execute(query, {"user_id": user_id, "table_name": table_name}).fetchone()

        if result:
            # Deserialize preferences from JSON
            preferences_data = json.loads(result.preferences_config)
            preferences = UserDefaultPreferences(**preferences_data)

            success(f"Loaded user preferences for user '{user_id}', table '{table_name}'")
            return preferences
        else:
            info(f"No preferences found for user '{user_id}', table '{table_name}'")
            return None

    except SQLAlchemyError as e:
        error(f"Database error loading user preferences: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        error(f"JSON decode error loading user preferences: {str(e)}")
        return None
    except Exception as e:
        error(f"Error loading user preferences: {str(e)}")
        return None


def clear_user_preferences(user_id: str, table_name: str, db_session: Session = None) -> bool:
    """Clear user preferences for table.

    Args:
        user_id: User ID who owns the preferences
        table_name: Name of the table
        db_session: Database session (injected)

    Returns:
        True if clear successful, False otherwise
    """
    try:
        info(f"Clearing user preferences for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        delete_query = text(
            """
            DELETE FROM user_default_preferences
            WHERE user_id = :user_id AND table_name = :table_name
        """
        )

        result = db_session.execute(delete_query, {"user_id": user_id, "table_name": table_name})

        if result.rowcount > 0:
            db_session.commit()
            success(f"Cleared user preferences for user '{user_id}', table '{table_name}'")
            return True
        else:
            warning(f"No preferences found to clear for user '{user_id}', table '{table_name}'")
            return False

    except SQLAlchemyError as e:
        error(f"Database error clearing user preferences: {str(e)}")
        if db_session:
            db_session.rollback()
        return False
    except Exception as e:
        error(f"Error clearing user preferences: {str(e)}")
        if db_session:
            db_session.rollback()
        return False


def get_user_preference_summary(user_id: str, db_session: Session = None) -> Dict[str, Any]:
    """Get summary of user preferences across all tables.

    Args:
        user_id: User ID to get summary for
        db_session: Database session (injected)

    Returns:
        Dictionary with preference summary
    """
    try:
        info(f"Getting preference summary for user '{user_id}'")

        if not db_session:
            db_session = next(get_db())

        # Get preferences count by table
        summary_query = text(
            """
            SELECT
                table_name,
                COUNT(*) as preference_count,
                MAX(updated_at) as last_updated
            FROM user_default_preferences
            WHERE user_id = :user_id
            GROUP BY table_name
            ORDER BY last_updated DESC
        """
        )

        result = db_session.execute(summary_query, {"user_id": user_id})

        tables = []
        total_preferences = 0

        for row in result:
            table_info = {
                "table_name": row.table_name,
                "preference_count": row.preference_count,
                "last_updated": row.last_updated.isoformat() if row.last_updated else None,
            }
            tables.append(table_info)
            total_preferences += row.preference_count

        # Get overall stats
        stats_query = text(
            """
            SELECT
                COUNT(*) as total_count,
                MIN(created_at) as oldest_preference,
                MAX(updated_at) as newest_preference
            FROM user_default_preferences
            WHERE user_id = :user_id
        """
        )

        stats_result = db_session.execute(stats_query, {"user_id": user_id}).fetchone()

        summary = {
            "user_id": user_id,
            "total_preferences": total_preferences,
            "table_count": len(tables),
            "tables": tables,
            "oldest_preference": stats_result.oldest_preference.isoformat() if stats_result.oldest_preference else None,
            "newest_preference": stats_result.newest_preference.isoformat() if stats_result.newest_preference else None,
        }

        success(
            f"Retrieved preference summary for user '{user_id}': {total_preferences} preferences across {len(tables)} tables"
        )
        return summary

    except SQLAlchemyError as e:
        error(f"Database error getting preference summary: {str(e)}")
        return {"error": str(e)}
    except Exception as e:
        error(f"Error getting preference summary: {str(e)}")
        return {"error": str(e)}


def save_table_view_preferences(
    user_id: str, table_name: str, view_preferences: Dict[str, Any], db_session: Session = None
) -> bool:
    """Save table view-specific preferences.

    Args:
        user_id: User ID who owns the preferences
        table_name: Name of the table
        view_preferences: Dictionary of view preferences
        db_session: Database session (injected)

    Returns:
        True if save successful, False otherwise
    """
    try:
        info(f"Saving table view preferences for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        # Check if table view preferences already exist
        check_query = text(
            """
            SELECT id FROM user_table_view_preferences
            WHERE user_id = :user_id AND table_name = :table_name
        """
        )

        existing = db_session.execute(check_query, {"user_id": user_id, "table_name": table_name}).fetchone()

        view_preferences_json = json.dumps(view_preferences, default=str)

        if existing:
            # Update existing preferences
            update_query = text(
                """
                UPDATE user_table_view_preferences
                SET view_config = :view_config, updated_at = :updated_at
                WHERE user_id = :user_id AND table_name = :table_name
            """
            )

            db_session.execute(
                update_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "view_config": view_preferences_json,
                    "updated_at": datetime.utcnow(),
                },
            )

            success(f"Updated table view preferences for user '{user_id}', table '{table_name}'")
        else:
            # Create new preferences
            insert_query = text(
                """
                INSERT INTO user_table_view_preferences
                (user_id, table_name, view_config, created_at, updated_at)
                VALUES (:user_id, :table_name, :view_config, :created_at, :updated_at)
            """
            )

            now = datetime.utcnow()
            db_session.execute(
                insert_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "view_config": view_preferences_json,
                    "created_at": now,
                    "updated_at": now,
                },
            )

            success(f"Created new table view preferences for user '{user_id}', table '{table_name}'")

        db_session.commit()
        return True

    except SQLAlchemyError as e:
        error(f"Database error saving table view preferences: {str(e)}")
        if db_session:
            db_session.rollback()
        return False
    except Exception as e:
        error(f"Error saving table view preferences: {str(e)}")
        if db_session:
            db_session.rollback()
        return False


def load_table_view_preferences(user_id: str, table_name: str, db_session: Session = None) -> Optional[Dict[str, Any]]:
    """Load table view-specific preferences.

    Args:
        user_id: User ID who owns the preferences
        table_name: Name of the table
        db_session: Database session (injected)

    Returns:
        Dictionary of view preferences if found, None otherwise
    """
    try:
        info(f"Loading table view preferences for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        query = text(
            """
            SELECT view_config FROM user_table_view_preferences
            WHERE user_id = :user_id AND table_name = :table_name
        """
        )

        result = db_session.execute(query, {"user_id": user_id, "table_name": table_name}).fetchone()

        if result:
            # Deserialize preferences from JSON
            view_preferences = json.loads(result.view_config)

            success(f"Loaded table view preferences for user '{user_id}', table '{table_name}'")
            return view_preferences
        else:
            info(f"No table view preferences found for user '{user_id}', table '{table_name}'")
            return None

    except SQLAlchemyError as e:
        error(f"Database error loading table view preferences: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        error(f"JSON decode error loading table view preferences: {str(e)}")
        return None
    except Exception as e:
        error(f"Error loading table view preferences: {str(e)}")
        return None


def save_dashboard_preferences(user_id: str, dashboard_preferences: Dict[str, Any], db_session: Session = None) -> bool:
    """Save dashboard-wide preferences.

    Args:
        user_id: User ID who owns the preferences
        dashboard_preferences: Dictionary of dashboard preferences
        db_session: Database session (injected)

    Returns:
        True if save successful, False otherwise
    """
    try:
        info(f"Saving dashboard preferences for user '{user_id}'")

        if not db_session:
            db_session = next(get_db())

        # Check if dashboard preferences already exist
        check_query = text(
            """
            SELECT id FROM user_dashboard_preferences
            WHERE user_id = :user_id
        """
        )

        existing = db_session.execute(check_query, {"user_id": user_id}).fetchone()

        dashboard_preferences_json = json.dumps(dashboard_preferences, default=str)

        if existing:
            # Update existing preferences
            update_query = text(
                """
                UPDATE user_dashboard_preferences
                SET dashboard_config = :dashboard_config, updated_at = :updated_at
                WHERE user_id = :user_id
            """
            )

            db_session.execute(
                update_query,
                {"user_id": user_id, "dashboard_config": dashboard_preferences_json, "updated_at": datetime.utcnow()},
            )

            success(f"Updated dashboard preferences for user '{user_id}'")
        else:
            # Create new preferences
            insert_query = text(
                """
                INSERT INTO user_dashboard_preferences
                (user_id, dashboard_config, created_at, updated_at)
                VALUES (:user_id, :dashboard_config, :created_at, :updated_at)
            """
            )

            now = datetime.utcnow()
            db_session.execute(
                insert_query,
                {"user_id": user_id, "dashboard_config": dashboard_preferences_json, "created_at": now, "updated_at": now},
            )

            success(f"Created new dashboard preferences for user '{user_id}'")

        db_session.commit()
        return True

    except SQLAlchemyError as e:
        error(f"Database error saving dashboard preferences: {str(e)}")
        if db_session:
            db_session.rollback()
        return False
    except Exception as e:
        error(f"Error saving dashboard preferences: {str(e)}")
        if db_session:
            db_session.rollback()
        return False


def load_dashboard_preferences(user_id: str, db_session: Session = None) -> Optional[Dict[str, Any]]:
    """Load dashboard-wide preferences.

    Args:
        user_id: User ID who owns the preferences
        db_session: Database session (injected)

    Returns:
        Dictionary of dashboard preferences if found, None otherwise
    """
    try:
        info(f"Loading dashboard preferences for user '{user_id}'")

        if not db_session:
            db_session = next(get_db())

        query = text(
            """
            SELECT dashboard_config FROM user_dashboard_preferences
            WHERE user_id = :user_id
        """
        )

        result = db_session.execute(query, {"user_id": user_id}).fetchone()

        if result:
            # Deserialize preferences from JSON
            dashboard_preferences = json.loads(result.dashboard_config)

            success(f"Loaded dashboard preferences for user '{user_id}'")
            return dashboard_preferences
        else:
            info(f"No dashboard preferences found for user '{user_id}'")
            return None

    except SQLAlchemyError as e:
        error(f"Database error loading dashboard preferences: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        error(f"JSON decode error loading dashboard preferences: {str(e)}")
        return None
    except Exception as e:
        error(f"Error loading dashboard preferences: {str(e)}")
        return None


def export_user_preferences(user_id: str, db_session: Session = None) -> Optional[Dict[str, Any]]:
    """Export all user preferences for backup/migration.

    Args:
        user_id: User ID to export preferences for
        db_session: Database session (injected)

    Returns:
        Dictionary with all user preferences or None if error
    """
    try:
        info(f"Exporting all preferences for user '{user_id}'")

        if not db_session:
            db_session = next(get_db())

        export_data = {
            "user_id": user_id,
            "export_timestamp": datetime.utcnow().isoformat(),
            "default_preferences": [],
            "table_view_preferences": [],
            "dashboard_preferences": None,
        }

        # Export default preferences
        default_query = text(
            """
            SELECT table_name, preferences_config, created_at, updated_at
            FROM user_default_preferences
            WHERE user_id = :user_id
            ORDER BY table_name
        """
        )

        default_result = db_session.execute(default_query, {"user_id": user_id})
        for row in default_result:
            export_data["default_preferences"].append(
                {
                    "table_name": row.table_name,
                    "preferences_config": json.loads(row.preferences_config),
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                }
            )

        # Export table view preferences
        view_query = text(
            """
            SELECT table_name, view_config, created_at, updated_at
            FROM user_table_view_preferences
            WHERE user_id = :user_id
            ORDER BY table_name
        """
        )

        view_result = db_session.execute(view_query, {"user_id": user_id})
        for row in view_result:
            export_data["table_view_preferences"].append(
                {
                    "table_name": row.table_name,
                    "view_config": json.loads(row.view_config),
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                }
            )

        # Export dashboard preferences
        dashboard_query = text(
            """
            SELECT dashboard_config, created_at, updated_at
            FROM user_dashboard_preferences
            WHERE user_id = :user_id
        """
        )

        dashboard_result = db_session.execute(dashboard_query, {"user_id": user_id}).fetchone()
        if dashboard_result:
            export_data["dashboard_preferences"] = {
                "dashboard_config": json.loads(dashboard_result.dashboard_config),
                "created_at": dashboard_result.created_at.isoformat() if dashboard_result.created_at else None,
                "updated_at": dashboard_result.updated_at.isoformat() if dashboard_result.updated_at else None,
            }

        success(
            f"Exported preferences for user '{user_id}': {len(export_data['default_preferences'])} default, {len(export_data['table_view_preferences'])} view, dashboard: {export_data['dashboard_preferences'] is not None}"
        )
        return export_data

    except SQLAlchemyError as e:
        error(f"Database error exporting user preferences: {str(e)}")
        return None
    except Exception as e:
        error(f"Error exporting user preferences: {str(e)}")
        return None


def import_user_preferences(
    user_id: str, import_data: Dict[str, Any], overwrite: bool = False, db_session: Session = None
) -> bool:
    """Import user preferences from backup data.

    Args:
        user_id: User ID to import preferences for
        import_data: Dictionary with preference data to import
        overwrite: Whether to overwrite existing preferences
        db_session: Database session (injected)

    Returns:
        True if import successful, False otherwise
    """
    try:
        info(f"Importing preferences for user '{user_id}', overwrite: {overwrite}")

        if not db_session:
            db_session = next(get_db())

        imported_count = 0

        # Import default preferences
        for pref_data in import_data.get("default_preferences", []):
            table_name = pref_data["table_name"]
            preferences_config = pref_data["preferences_config"]

            try:
                preferences = UserDefaultPreferences(**preferences_config)
                if save_user_preferences(user_id, table_name, preferences, db_session):
                    imported_count += 1
            except Exception as e:
                warning(f"Failed to import default preferences for table '{table_name}': {str(e)}")

        # Import table view preferences
        for view_data in import_data.get("table_view_preferences", []):
            table_name = view_data["table_name"]
            view_config = view_data["view_config"]

            if save_table_view_preferences(user_id, table_name, view_config, db_session):
                imported_count += 1

        # Import dashboard preferences
        dashboard_data = import_data.get("dashboard_preferences")
        if dashboard_data:
            dashboard_config = dashboard_data["dashboard_config"]
            if save_dashboard_preferences(user_id, dashboard_config, db_session):
                imported_count += 1

        success(f"Imported {imported_count} preference sets for user '{user_id}'")
        return True

    except Exception as e:
        error(f"Error importing user preferences: {str(e)}")
        if db_session:
            db_session.rollback()
        return False


def generate_localstorage_commands(user_id: str, db_session: Session = None) -> List[str]:
    """Generate JavaScript commands to set preferences in localStorage.

    Args:
        user_id: User ID to generate commands for
        db_session: Database session (injected)

    Returns:
        List of JavaScript localStorage commands
    """
    try:
        info(f"Generating localStorage commands for user '{user_id}'")

        export_data = export_user_preferences(user_id, db_session)
        if not export_data:
            return []

        commands = []

        # Generate commands for default preferences
        for pref_data in export_data.get("default_preferences", []):
            table_name = pref_data["table_name"]
            config = pref_data["preferences_config"]
            key = f"tableV2_default_prefs_{table_name}"
            value = json.dumps(config)
            commands.append(f'localStorage.setItem("{key}", {json.dumps(value)});')

        # Generate commands for table view preferences
        for view_data in export_data.get("table_view_preferences", []):
            table_name = view_data["table_name"]
            config = view_data["view_config"]
            key = f"tableV2_view_prefs_{table_name}"
            value = json.dumps(config)
            commands.append(f'localStorage.setItem("{key}", {json.dumps(value)});')

        # Generate commands for dashboard preferences
        dashboard_data = export_data.get("dashboard_preferences")
        if dashboard_data:
            config = dashboard_data["dashboard_config"]
            key = "tableV2_dashboard_prefs"
            value = json.dumps(config)
            commands.append(f'localStorage.setItem("{key}", {json.dumps(value)});')

        success(f"Generated {len(commands)} localStorage commands for user '{user_id}'")
        return commands

    except Exception as e:
        error(f"Error generating localStorage commands: {str(e)}")
        return []


# FastAPI Dependency Injection Functions
def get_preference_repository_for_dependency() -> callable:
    """FastAPI dependency for preference repository operations."""
    return {
        "save_user_preferences": save_user_preferences,
        "load_user_preferences": load_user_preferences,
        "clear_user_preferences": clear_user_preferences,
        "get_user_preference_summary": get_user_preference_summary,
        "save_table_view_preferences": save_table_view_preferences,
        "load_table_view_preferences": load_table_view_preferences,
        "save_dashboard_preferences": save_dashboard_preferences,
        "load_dashboard_preferences": load_dashboard_preferences,
        "export_user_preferences": export_user_preferences,
        "import_user_preferences": import_user_preferences,
        "generate_localstorage_commands": generate_localstorage_commands,
    }


def get_save_preference_for_dependency() -> callable:
    """FastAPI dependency for save preference operations."""
    return save_user_preferences


def get_load_preference_for_dependency() -> callable:
    """FastAPI dependency for load preference operations."""
    return load_user_preferences


def get_save_table_preferences_for_dependency() -> callable:
    """FastAPI dependency for save table view preference operations."""
    return save_table_view_preferences


def get_load_table_preferences_for_dependency() -> callable:
    """FastAPI dependency for load table view preference operations."""
    return load_table_view_preferences
