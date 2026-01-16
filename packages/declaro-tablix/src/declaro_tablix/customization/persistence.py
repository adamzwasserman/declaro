"""
Customization persistence functions for TableV2.

This module provides function-based persistence operations for column customizations,
user preferences, and customization history without using classes.
"""

import json
from datetime import datetime
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy import and_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from declaro_persistum.compat import SessionLocal
from declaro_advise import error, info, success, warning
from declaro_tablix.customization import (
    CUSTOMIZATION_CONFIG,
    CUSTOMIZATION_ERRORS,
    CUSTOMIZATION_SUCCESS,
)
from declaro_tablix.customization.models import (
    ColumnCustomization,
    CustomizationHistory,
    UserTablePreferences,
)


def create_column_customization(
    user_id: str,
    table_name: str,
    column_id: str,
    customization_data: Dict[str, Any],
    db_session: Optional[Session] = None,
) -> Dict[str, Any]:
    """
    Create a new column customization record.

    Args:
        user_id: User identifier
        table_name: Name of the table
        column_id: Column identifier
        customization_data: Customization settings
        db_session: Optional database session

    Returns:
        Result dictionary with success status and data
    """
    try:
        # Validate input data
        if not user_id or not table_name or not column_id:
            error(CUSTOMIZATION_ERRORS["invalid_column_id"])
            return {"success": False, "error": "Invalid input parameters"}

        # Use provided session or create new one
        if db_session:
            session = db_session
            should_close = False
        else:
            session = SessionLocal()
            should_close = True

        try:
            # Check if customization already exists
            existing = (
                session.query(ColumnCustomization)
                .filter(
                    and_(
                        ColumnCustomization.user_id == user_id,
                        ColumnCustomization.table_name == table_name,
                        ColumnCustomization.column_id == column_id,
                        ColumnCustomization.is_active.is_(True),
                    )
                )
                .first()
            )

            if existing:
                warning(CUSTOMIZATION_ERRORS["duplicate_customization"])
                return {"success": False, "error": "Customization already exists"}

            # Create new customization
            customization = ColumnCustomization(
                user_id=user_id,
                table_name=table_name,
                column_id=column_id,
                alias=customization_data.get("alias"),
                is_visible=customization_data.get("is_visible", True),
                display_order=customization_data.get("display_order"),
                column_width=customization_data.get("column_width"),
                format_options=customization_data.get("format_options"),
                filter_options=customization_data.get("filter_options"),
                sort_options=customization_data.get("sort_options"),
            )

            session.add(customization)
            session.commit()

            # Record history
            record_customization_history(
                user_id=user_id,
                table_name=table_name,
                customization_id=customization.id,
                action="create",
                new_data=customization.to_dict(),
                db_session=session,
            )

            success(CUSTOMIZATION_SUCCESS["customization_saved"])
            return {"success": True, "data": customization.to_dict(), "message": "Customization created successfully"}

        finally:
            if should_close:
                session.close()

    except IntegrityError as e:
        error(f"Database integrity error: {str(e)}")
        return {"success": False, "error": "Database integrity violation"}
    except Exception as e:
        error(f"Failed to create customization: {str(e)}")
        return {"success": False, "error": str(e)}


def update_column_customization(
    user_id: str,
    table_name: str,
    column_id: str,
    customization_data: Dict[str, Any],
    db_session: Optional[Session] = None,
) -> Dict[str, Any]:
    """
    Update an existing column customization.

    Args:
        user_id: User identifier
        table_name: Name of the table
        column_id: Column identifier
        customization_data: Updated customization settings
        db_session: Optional database session

    Returns:
        Result dictionary with success status and data
    """
    try:
        # Use provided session or create new one
        if db_session:
            session = db_session
            should_close = False
        else:
            session = SessionLocal()
            should_close = True

        try:
            # Find existing customization
            customization = (
                session.query(ColumnCustomization)
                .filter(
                    and_(
                        ColumnCustomization.user_id == user_id,
                        ColumnCustomization.table_name == table_name,
                        ColumnCustomization.column_id == column_id,
                        ColumnCustomization.is_active.is_(True),
                    )
                )
                .first()
            )

            if not customization:
                warning(CUSTOMIZATION_ERRORS["customization_not_found"])
                return {"success": False, "error": "Customization not found"}

            # Store old data for history
            old_data = customization.to_dict()

            # Update customization
            if "alias" in customization_data:
                customization.alias = customization_data["alias"]
            if "is_visible" in customization_data:
                customization.is_visible = customization_data["is_visible"]
            if "display_order" in customization_data:
                customization.display_order = customization_data["display_order"]
            if "column_width" in customization_data:
                customization.column_width = customization_data["column_width"]
            if "format_options" in customization_data:
                customization.format_options = customization_data["format_options"]
            if "filter_options" in customization_data:
                customization.filter_options = customization_data["filter_options"]
            if "sort_options" in customization_data:
                customization.sort_options = customization_data["sort_options"]

            customization.updated_at = datetime.utcnow()

            session.commit()

            # Record history
            record_customization_history(
                user_id=user_id,
                table_name=table_name,
                customization_id=customization.id,
                action="update",
                old_data=old_data,
                new_data=customization.to_dict(),
                db_session=session,
            )

            success(CUSTOMIZATION_SUCCESS["customization_updated"])
            return {"success": True, "data": customization.to_dict(), "message": "Customization updated successfully"}

        finally:
            if should_close:
                session.close()

    except Exception as e:
        error(f"Failed to update customization: {str(e)}")
        return {"success": False, "error": str(e)}


def get_column_customizations(
    user_id: str,
    table_name: str,
    column_id: Optional[str] = None,
    db_session: Optional[Session] = None,
) -> Dict[str, Any]:
    """
    Get column customizations for a user and table.

    Args:
        user_id: User identifier
        table_name: Name of the table
        column_id: Optional specific column identifier
        db_session: Optional database session

    Returns:
        Result dictionary with customizations data
    """
    try:
        # Use provided session or create new one
        if db_session:
            session = db_session
            should_close = False
        else:
            session = SessionLocal()
            should_close = True

        try:
            # Build query
            query = session.query(ColumnCustomization).filter(
                and_(
                    ColumnCustomization.user_id == user_id,
                    ColumnCustomization.table_name == table_name,
                    ColumnCustomization.is_active.is_(True),
                )
            )

            if column_id:
                query = query.filter(ColumnCustomization.column_id == column_id)

            # Order by display_order and column_id
            query = query.order_by(ColumnCustomization.display_order.asc().nullslast(), ColumnCustomization.column_id.asc())

            customizations = query.all()

            # Convert to dictionary format
            customizations_data = [c.to_dict() for c in customizations]

            return {"success": True, "data": customizations_data, "count": len(customizations_data)}

        finally:
            if should_close:
                session.close()

    except Exception as e:
        error(f"Failed to get customizations: {str(e)}")
        return {"success": False, "error": str(e)}


def delete_column_customization(
    user_id: str,
    table_name: str,
    column_id: str,
    db_session: Optional[Session] = None,
) -> Dict[str, Any]:
    """
    Delete a column customization (soft delete).

    Args:
        user_id: User identifier
        table_name: Name of the table
        column_id: Column identifier
        db_session: Optional database session

    Returns:
        Result dictionary with success status
    """
    try:
        # Use provided session or create new one
        if db_session:
            session = db_session
            should_close = False
        else:
            session = SessionLocal()
            should_close = True

        try:
            # Find existing customization
            customization = (
                session.query(ColumnCustomization)
                .filter(
                    and_(
                        ColumnCustomization.user_id == user_id,
                        ColumnCustomization.table_name == table_name,
                        ColumnCustomization.column_id == column_id,
                        ColumnCustomization.is_active.is_(True),
                    )
                )
                .first()
            )

            if not customization:
                warning(CUSTOMIZATION_ERRORS["customization_not_found"])
                return {"success": False, "error": "Customization not found"}

            # Store old data for history
            old_data = customization.to_dict()

            # Soft delete
            customization.is_active = False
            customization.updated_at = datetime.utcnow()

            session.commit()

            # Record history
            record_customization_history(
                user_id=user_id,
                table_name=table_name,
                customization_id=customization.id,
                action="delete",
                old_data=old_data,
                db_session=session,
            )

            success(CUSTOMIZATION_SUCCESS["customization_deleted"])
            return {"success": True, "message": "Customization deleted successfully"}

        finally:
            if should_close:
                session.close()

    except Exception as e:
        error(f"Failed to delete customization: {str(e)}")
        return {"success": False, "error": str(e)}


def save_user_preferences(
    user_id: str,
    table_name: str,
    preference_name: str,
    preference_data: Dict[str, Any],
    is_default: bool = False,
    is_shared: bool = False,
    db_session: Optional[Session] = None,
) -> Dict[str, Any]:
    """
    Save user table preferences.

    Args:
        user_id: User identifier
        table_name: Name of the table
        preference_name: Name of the preference set
        preference_data: Preference configuration data
        is_default: Whether this is the default preference
        is_shared: Whether this preference is shared
        db_session: Optional database session

    Returns:
        Result dictionary with success status and data
    """
    try:
        # Validate preference data size
        preference_json = json.dumps(preference_data)
        if len(preference_json) > CUSTOMIZATION_CONFIG["max_user_preferences_size"]:
            error(CUSTOMIZATION_ERRORS["max_size_exceeded"])
            return {"success": False, "error": "Preference data too large"}

        # Use provided session or create new one
        if db_session:
            session = db_session
            should_close = False
        else:
            session = SessionLocal()
            should_close = True

        try:
            # Check if preference already exists
            existing = (
                session.query(UserTablePreferences)
                .filter(
                    and_(
                        UserTablePreferences.user_id == user_id,
                        UserTablePreferences.table_name == table_name,
                        UserTablePreferences.preference_name == preference_name,
                    )
                )
                .first()
            )

            if existing:
                # Update existing preference
                existing.preference_data = preference_data
                existing.is_default = is_default
                existing.is_shared = is_shared
                existing.updated_at = datetime.utcnow()
                existing.usage_count += 1
                existing.last_used_at = datetime.utcnow()

                session.commit()

                success(CUSTOMIZATION_SUCCESS["preferences_saved"])
                return {"success": True, "data": existing.to_dict(), "message": "Preferences updated successfully"}
            else:
                # Create new preference
                preference = UserTablePreferences(
                    user_id=user_id,
                    table_name=table_name,
                    preference_name=preference_name,
                    preference_data=preference_data,
                    is_default=is_default,
                    is_shared=is_shared,
                    usage_count=1,
                    last_used_at=datetime.utcnow(),
                )

                session.add(preference)
                session.commit()

                success(CUSTOMIZATION_SUCCESS["preferences_saved"])
                return {"success": True, "data": preference.to_dict(), "message": "Preferences saved successfully"}

        finally:
            if should_close:
                session.close()

    except Exception as e:
        error(f"Failed to save preferences: {str(e)}")
        return {"success": False, "error": str(e)}


def get_user_preferences(
    user_id: str,
    table_name: str,
    preference_name: Optional[str] = None,
    db_session: Optional[Session] = None,
) -> Dict[str, Any]:
    """
    Get user table preferences.

    Args:
        user_id: User identifier
        table_name: Name of the table
        preference_name: Optional specific preference name
        db_session: Optional database session

    Returns:
        Result dictionary with preferences data
    """
    try:
        # Use provided session or create new one
        if db_session:
            session = db_session
            should_close = False
        else:
            session = SessionLocal()
            should_close = True

        try:
            # Build query
            query = session.query(UserTablePreferences).filter(
                and_(UserTablePreferences.user_id == user_id, UserTablePreferences.table_name == table_name)
            )

            if preference_name:
                query = query.filter(UserTablePreferences.preference_name == preference_name)

            # Order by usage and creation date
            query = query.order_by(
                UserTablePreferences.is_default.desc(),
                UserTablePreferences.usage_count.desc(),
                UserTablePreferences.created_at.desc(),
            )

            preferences = query.all()

            # Convert to dictionary format
            preferences_data = [p.to_dict() for p in preferences]

            success(CUSTOMIZATION_SUCCESS["preferences_loaded"])
            return {"success": True, "data": preferences_data, "count": len(preferences_data)}

        finally:
            if should_close:
                session.close()

    except Exception as e:
        error(f"Failed to get preferences: {str(e)}")
        return {"success": False, "error": str(e)}


def record_customization_history(
    user_id: str,
    table_name: str,
    action: str,
    customization_id: Optional[UUID] = None,
    old_data: Optional[Dict[str, Any]] = None,
    new_data: Optional[Dict[str, Any]] = None,
    change_description: Optional[str] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    db_session: Optional[Session] = None,
) -> Dict[str, Any]:
    """
    Record customization history entry.

    Args:
        user_id: User identifier
        table_name: Name of the table
        action: Action performed (create, update, delete)
        customization_id: Optional customization ID
        old_data: Optional old data
        new_data: Optional new data
        change_description: Optional description
        ip_address: Optional IP address
        user_agent: Optional user agent
        db_session: Optional database session

    Returns:
        Result dictionary with success status
    """
    try:
        # Use provided session or create new one
        if db_session:
            session = db_session
            should_close = False
        else:
            session = SessionLocal()
            should_close = True

        try:
            # Create history record
            history = CustomizationHistory(
                user_id=user_id,
                table_name=table_name,
                customization_id=customization_id,
                action=action,
                old_data=old_data,
                new_data=new_data,
                change_description=change_description,
                ip_address=ip_address,
                user_agent=user_agent,
            )

            session.add(history)
            session.commit()

            return {"success": True, "data": history.to_dict(), "message": "History recorded successfully"}

        finally:
            if should_close:
                session.close()

    except Exception as e:
        error(f"Failed to record history: {str(e)}")
        return {"success": False, "error": str(e)}


def get_customization_history(
    user_id: str,
    table_name: str,
    limit: int = 50,
    offset: int = 0,
    db_session: Optional[Session] = None,
) -> Dict[str, Any]:
    """
    Get customization history for a user and table.

    Args:
        user_id: User identifier
        table_name: Name of the table
        limit: Maximum number of records to return
        offset: Number of records to skip
        db_session: Optional database session

    Returns:
        Result dictionary with history data
    """
    try:
        # Use provided session or create new one
        if db_session:
            session = db_session
            should_close = False
        else:
            session = SessionLocal()
            should_close = True

        try:
            # Build query
            query = (
                session.query(CustomizationHistory)
                .filter(and_(CustomizationHistory.user_id == user_id, CustomizationHistory.table_name == table_name))
                .order_by(CustomizationHistory.created_at.desc())
            )

            # Apply pagination
            history_records = query.limit(limit).offset(offset).all()

            # Get total count
            total_count = query.count()

            # Convert to dictionary format
            history_data = [h.to_dict() for h in history_records]

            return {"success": True, "data": history_data, "total_count": total_count, "limit": limit, "offset": offset}

        finally:
            if should_close:
                session.close()

    except Exception as e:
        error(f"Failed to get history: {str(e)}")
        return {"success": False, "error": str(e)}


# Export all functions
__all__ = [
    "create_column_customization",
    "update_column_customization",
    "get_column_customizations",
    "delete_column_customization",
    "save_user_preferences",
    "get_user_preferences",
    "record_customization_history",
    "get_customization_history",
]
