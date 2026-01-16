"""Table configuration repository implementation for Table Module V2.

This module provides function-based configuration persistence operations,
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
from declaro_tablix.domain.models import TableConfig


def save_table_configuration(
    user_id: str, table_name: str, config: TableConfig, config_name: str = "default", db_session: Session = None
) -> bool:
    """Save table configuration for user.

    Args:
        user_id: User ID who owns the configuration
        table_name: Name of the table
        config: TableConfig object to save
        config_name: Name for this configuration (default: "default")
        db_session: Database session (injected)

    Returns:
        True if save successful, False otherwise
    """
    try:
        info(f"Saving table configuration '{config_name}' for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        # Serialize the configuration to JSON
        config_json = config.model_dump_json()

        # Check if configuration already exists
        check_query = text(
            """
            SELECT id FROM table_configurations
            WHERE user_id = :user_id AND table_name = :table_name AND config_name = :config_name
        """
        )

        existing = db_session.execute(
            check_query, {"user_id": user_id, "table_name": table_name, "config_name": config_name}
        ).fetchone()

        if existing:
            # Update existing configuration
            update_query = text(
                """
                UPDATE table_configurations
                SET configuration = :config_json, updated_at = :updated_at
                WHERE user_id = :user_id AND table_name = :table_name AND config_name = :config_name
            """
            )

            db_session.execute(
                update_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "config_name": config_name,
                    "config_json": config_json,
                    "updated_at": datetime.utcnow(),
                },
            )

            success(f"Updated table configuration '{config_name}' for user '{user_id}'")
        else:
            # Create new configuration
            insert_query = text(
                """
                INSERT INTO table_configurations (user_id, table_name, config_name, configuration, created_at, updated_at)
                VALUES (:user_id, :table_name, :config_name, :config_json, :created_at, :updated_at)
            """
            )

            now = datetime.utcnow()
            db_session.execute(
                insert_query,
                {
                    "user_id": user_id,
                    "table_name": table_name,
                    "config_name": config_name,
                    "config_json": config_json,
                    "created_at": now,
                    "updated_at": now,
                },
            )

            success(f"Created new table configuration '{config_name}' for user '{user_id}'")

        db_session.commit()
        return True

    except SQLAlchemyError as e:
        error(f"Database error saving table configuration: {str(e)}")
        if db_session:
            db_session.rollback()
        return False
    except Exception as e:
        error(f"Error saving table configuration: {str(e)}")
        if db_session:
            db_session.rollback()
        return False


def load_table_configuration(
    user_id: str, table_name: str, config_name: str = "default", db_session: Session = None
) -> Optional[TableConfig]:
    """Load saved table configuration.

    Args:
        user_id: User ID who owns the configuration
        table_name: Name of the table
        config_name: Name of the configuration to load (default: "default")
        db_session: Database session (injected)

    Returns:
        TableConfig object if found, None otherwise
    """
    try:
        info(f"Loading table configuration '{config_name}' for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        query = text(
            """
            SELECT configuration FROM table_configurations
            WHERE user_id = :user_id AND table_name = :table_name AND config_name = :config_name
        """
        )

        result = db_session.execute(
            query, {"user_id": user_id, "table_name": table_name, "config_name": config_name}
        ).fetchone()

        if result:
            # Deserialize JSON to TableConfig object
            config_data = json.loads(result.configuration)
            config = TableConfig(**config_data)

            success(f"Loaded table configuration '{config_name}' for user '{user_id}'")
            return config
        else:
            info(f"No configuration '{config_name}' found for user '{user_id}', table '{table_name}'")
            return None

    except SQLAlchemyError as e:
        error(f"Database error loading table configuration: {str(e)}")
        return None
    except json.JSONDecodeError as e:
        error(f"JSON decode error loading table configuration: {str(e)}")
        return None
    except Exception as e:
        error(f"Error loading table configuration: {str(e)}")
        return None


def list_table_configurations(user_id: str, table_name: str = None, db_session: Session = None) -> List[Dict[str, Any]]:
    """List all user table configurations.

    Args:
        user_id: User ID who owns the configurations
        table_name: Optional table name to filter by
        db_session: Database session (injected)

    Returns:
        List of configuration metadata dictionaries
    """
    try:
        info(f"Listing table configurations for user '{user_id}'" + (f", table '{table_name}'" if table_name else ""))

        if not db_session:
            db_session = next(get_db())

        # Build query with optional table filter
        base_query = """
            SELECT
                id,
                table_name,
                config_name,
                created_at,
                updated_at,
                CASE WHEN LENGTH(configuration) > 100
                     THEN SUBSTRING(configuration, 1, 100) || '...'
                     ELSE configuration
                END as config_preview
            FROM table_configurations
            WHERE user_id = :user_id
        """

        parameters = {"user_id": user_id}

        if table_name:
            base_query += " AND table_name = :table_name"
            parameters["table_name"] = table_name

        base_query += " ORDER BY table_name, config_name"

        query = text(base_query)
        result = db_session.execute(query, parameters)

        configurations = []
        for row in result:
            config_info = {
                "id": row.id,
                "table_name": row.table_name,
                "config_name": row.config_name,
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "updated_at": row.updated_at.isoformat() if row.updated_at else None,
                "config_preview": row.config_preview,
            }
            configurations.append(config_info)

        success(f"Found {len(configurations)} configurations for user '{user_id}'")
        return configurations

    except SQLAlchemyError as e:
        error(f"Database error listing table configurations: {str(e)}")
        return []
    except Exception as e:
        error(f"Error listing table configurations: {str(e)}")
        return []


def delete_table_configuration(user_id: str, table_name: str, config_name: str, db_session: Session = None) -> bool:
    """Delete table configuration.

    Args:
        user_id: User ID who owns the configuration
        table_name: Name of the table
        config_name: Name of the configuration to delete
        db_session: Database session (injected)

    Returns:
        True if delete successful, False otherwise
    """
    try:
        info(f"Deleting table configuration '{config_name}' for user '{user_id}', table '{table_name}'")

        if not db_session:
            db_session = next(get_db())

        # Prevent deletion of default configuration
        if config_name == "default":
            warning("Cannot delete default configuration")
            return False

        delete_query = text(
            """
            DELETE FROM table_configurations
            WHERE user_id = :user_id AND table_name = :table_name AND config_name = :config_name
        """
        )

        result = db_session.execute(delete_query, {"user_id": user_id, "table_name": table_name, "config_name": config_name})

        if result.rowcount > 0:
            db_session.commit()
            success(f"Deleted table configuration '{config_name}' for user '{user_id}'")
            return True
        else:
            warning(f"No configuration '{config_name}' found to delete for user '{user_id}'")
            return False

    except SQLAlchemyError as e:
        error(f"Database error deleting table configuration: {str(e)}")
        if db_session:
            db_session.rollback()
        return False
    except Exception as e:
        error(f"Error deleting table configuration: {str(e)}")
        if db_session:
            db_session.rollback()
        return False


def copy_table_configuration(
    user_id: str, table_name: str, source_config_name: str, target_config_name: str, db_session: Session = None
) -> bool:
    """Copy table configuration to a new name.

    Args:
        user_id: User ID who owns the configurations
        table_name: Name of the table
        source_config_name: Name of the configuration to copy
        target_config_name: Name for the new configuration
        db_session: Database session (injected)

    Returns:
        True if copy successful, False otherwise
    """
    try:
        info(f"Copying configuration '{source_config_name}' to '{target_config_name}' for user '{user_id}'")

        # Load source configuration
        source_config = load_table_configuration(user_id, table_name, source_config_name, db_session)

        if not source_config:
            error(f"Source configuration '{source_config_name}' not found")
            return False

        # Save as new configuration
        return save_table_configuration(user_id, table_name, source_config, target_config_name, db_session)

    except Exception as e:
        error(f"Error copying table configuration: {str(e)}")
        return False


def get_table_configuration_stats(user_id: str, db_session: Session = None) -> Dict[str, Any]:
    """Get statistics about user's table configurations.

    Args:
        user_id: User ID to get stats for
        db_session: Database session (injected)

    Returns:
        Dictionary with configuration statistics
    """
    try:
        info(f"Getting configuration statistics for user '{user_id}'")

        if not db_session:
            db_session = next(get_db())

        stats_query = text(
            """
            SELECT
                COUNT(*) as total_configs,
                COUNT(DISTINCT table_name) as unique_tables,
                MIN(created_at) as oldest_config,
                MAX(updated_at) as newest_config,
                AVG(LENGTH(configuration)) as avg_config_size
            FROM table_configurations
            WHERE user_id = :user_id
        """
        )

        result = db_session.execute(stats_query, {"user_id": user_id}).fetchone()

        if result:
            stats = {
                "total_configurations": result.total_configs,
                "unique_tables": result.unique_tables,
                "oldest_configuration": result.oldest_config.isoformat() if result.oldest_config else None,
                "newest_configuration": result.newest_config.isoformat() if result.newest_config else None,
                "average_config_size": round(result.avg_config_size, 2) if result.avg_config_size else 0,
            }
        else:
            stats = {
                "total_configurations": 0,
                "unique_tables": 0,
                "oldest_configuration": None,
                "newest_configuration": None,
                "average_config_size": 0,
            }

        # Get per-table breakdown
        table_query = text(
            """
            SELECT
                table_name,
                COUNT(*) as config_count,
                MAX(updated_at) as last_updated
            FROM table_configurations
            WHERE user_id = :user_id
            GROUP BY table_name
            ORDER BY config_count DESC
        """
        )

        table_result = db_session.execute(table_query, {"user_id": user_id})
        tables = []

        for row in table_result:
            table_info = {
                "table_name": row.table_name,
                "configuration_count": row.config_count,
                "last_updated": row.last_updated.isoformat() if row.last_updated else None,
            }
            tables.append(table_info)

        stats["tables"] = tables

        success(f"Retrieved configuration statistics for user '{user_id}'")
        return stats

    except SQLAlchemyError as e:
        error(f"Database error getting configuration statistics: {str(e)}")
        return {"error": str(e)}
    except Exception as e:
        error(f"Error getting configuration statistics: {str(e)}")
        return {"error": str(e)}


# FastAPI Dependency Injection Functions
def get_table_config_repository_for_dependency() -> callable:
    """FastAPI dependency for table configuration operations."""
    return {
        "save": save_table_configuration,
        "load": load_table_configuration,
        "list": list_table_configurations,
        "delete": delete_table_configuration,
        "copy": copy_table_configuration,
        "stats": get_table_configuration_stats,
    }


def get_save_table_config_for_dependency() -> callable:
    """FastAPI dependency for save table configuration operation."""
    return save_table_configuration


def get_load_table_config_for_dependency() -> callable:
    """FastAPI dependency for load table configuration operation."""
    return load_table_configuration
