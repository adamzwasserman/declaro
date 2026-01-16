"""
Column customization database models for TableV2.

This module defines SQLAlchemy models for storing column customization data,
user preferences, and customization history.
"""

from datetime import datetime
from typing import Any, Dict
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID as PostgreSQLUUID

from declaro_persistum.compat import Base


class ColumnCustomization(Base):
    """
    Model for storing individual column customizations.

    Stores customization data for specific columns including aliases,
    visibility settings, ordering, and formatting options.
    """

    __tablename__ = "table_column_customizations"

    # Primary key
    id = Column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)

    # Foreign keys
    user_id = Column(String(128), nullable=False, index=True)
    table_name = Column(String(255), nullable=False, index=True)
    column_id = Column(String(255), nullable=False, index=True)

    # Customization data
    alias = Column(String(255), nullable=True)
    is_visible = Column(Boolean, nullable=False, default=True)
    display_order = Column(Integer, nullable=True)
    column_width = Column(Integer, nullable=True)
    format_options = Column(JSON, nullable=True)
    filter_options = Column(JSON, nullable=True)
    sort_options = Column(JSON, nullable=True)

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)

    # Constraints
    __table_args__ = (
        UniqueConstraint("user_id", "table_name", "column_id", name="uq_user_table_column"),
        Index("idx_user_table", "user_id", "table_name"),
        Index("idx_table_column", "table_name", "column_id"),
        Index("idx_active_customizations", "is_active", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<ColumnCustomization(user={self.user_id}, table={self.table_name}, column={self.column_id})>"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": str(self.id),
            "user_id": self.user_id,
            "table_name": self.table_name,
            "column_id": self.column_id,
            "alias": self.alias,
            "is_visible": self.is_visible,
            "display_order": self.display_order,
            "column_width": self.column_width,
            "format_options": self.format_options,
            "filter_options": self.filter_options,
            "sort_options": self.sort_options,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "is_active": self.is_active,
        }


class UserTablePreferences(Base):
    """
    Model for storing user-specific table preferences.

    Stores comprehensive user preferences for entire tables including
    default settings, view configurations, and personalization options.
    """

    __tablename__ = "table_user_preferences"

    # Primary key
    id = Column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)

    # Foreign keys
    user_id = Column(String(128), nullable=False, index=True)
    table_name = Column(String(255), nullable=False, index=True)

    # Preference data
    preference_name = Column(String(255), nullable=False)
    preference_data = Column(JSON, nullable=False)
    is_default = Column(Boolean, default=False, nullable=False)
    is_shared = Column(Boolean, default=False, nullable=False)

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    last_used_at = Column(DateTime, nullable=True)
    usage_count = Column(Integer, default=0, nullable=False)

    # Constraints
    __table_args__ = (
        UniqueConstraint("user_id", "table_name", "preference_name", name="uq_user_table_preference"),
        Index("idx_user_preferences", "user_id", "table_name"),
        Index("idx_default_preferences", "user_id", "is_default"),
        Index("idx_shared_preferences", "is_shared", "created_at"),
        Index("idx_preference_usage", "usage_count", "last_used_at"),
    )

    def __repr__(self) -> str:
        return f"<UserTablePreferences(user={self.user_id}, table={self.table_name}, name={self.preference_name})>"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": str(self.id),
            "user_id": self.user_id,
            "table_name": self.table_name,
            "preference_name": self.preference_name,
            "preference_data": self.preference_data,
            "is_default": self.is_default,
            "is_shared": self.is_shared,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "last_used_at": self.last_used_at.isoformat() if self.last_used_at else None,
            "usage_count": self.usage_count,
        }


class CustomizationHistory(Base):
    """
    Model for storing customization change history.

    Maintains an audit trail of all customization changes for
    troubleshooting, analytics, and potential rollback functionality.
    """

    __tablename__ = "table_customization_history"

    # Primary key
    id = Column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)

    # Foreign keys
    user_id = Column(String(128), nullable=False, index=True)
    table_name = Column(String(255), nullable=False, index=True)
    customization_id = Column(PostgreSQLUUID(as_uuid=True), nullable=True)

    # History data
    action = Column(String(50), nullable=False)  # create, update, delete
    old_data = Column(JSON, nullable=True)
    new_data = Column(JSON, nullable=True)
    change_description = Column(Text, nullable=True)

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    ip_address = Column(String(45), nullable=True)
    user_agent = Column(String(500), nullable=True)

    # Constraints
    __table_args__ = (
        Index("idx_history_user_table", "user_id", "table_name"),
        Index("idx_history_customization", "customization_id"),
        Index("idx_history_action", "action", "created_at"),
        Index("idx_history_created", "created_at"),
    )

    def __repr__(self) -> str:
        return f"<CustomizationHistory(user={self.user_id}, table={self.table_name}, action={self.action})>"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": str(self.id),
            "user_id": self.user_id,
            "table_name": self.table_name,
            "customization_id": str(self.customization_id) if self.customization_id else None,
            "action": self.action,
            "old_data": self.old_data,
            "new_data": self.new_data,
            "change_description": self.change_description,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
        }


class CustomizationTemplate(Base):
    """
    Model for storing customization templates.

    Allows users to save and share customization templates that can be
    applied to multiple tables or shared with other users.
    """

    __tablename__ = "table_customization_templates"

    # Primary key
    id = Column(PostgreSQLUUID(as_uuid=True), primary_key=True, default=uuid4)

    # Template data
    template_name = Column(String(255), nullable=False)
    template_description = Column(Text, nullable=True)
    template_data = Column(JSON, nullable=False)

    # Ownership and sharing
    created_by = Column(String(128), nullable=False, index=True)
    is_public = Column(Boolean, default=False, nullable=False)
    is_system_template = Column(Boolean, default=False, nullable=False)

    # Usage tracking
    usage_count = Column(Integer, default=0, nullable=False)
    last_used_at = Column(DateTime, nullable=True)

    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)

    # Constraints
    __table_args__ = (
        UniqueConstraint("created_by", "template_name", name="uq_user_template_name"),
        Index("idx_template_creator", "created_by", "is_active"),
        Index("idx_public_templates", "is_public", "created_at"),
        Index("idx_system_templates", "is_system_template", "is_active"),
        Index("idx_template_usage", "usage_count", "last_used_at"),
    )

    def __repr__(self) -> str:
        return f"<CustomizationTemplate(name={self.template_name}, creator={self.created_by})>"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "id": str(self.id),
            "template_name": self.template_name,
            "template_description": self.template_description,
            "template_data": self.template_data,
            "created_by": self.created_by,
            "is_public": self.is_public,
            "is_system_template": self.is_system_template,
            "usage_count": self.usage_count,
            "last_used_at": self.last_used_at.isoformat() if self.last_used_at else None,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "is_active": self.is_active,
        }


# Model registry for easy access
CUSTOMIZATION_MODELS = {
    "ColumnCustomization": ColumnCustomization,
    "UserTablePreferences": UserTablePreferences,
    "CustomizationHistory": CustomizationHistory,
    "CustomizationTemplate": CustomizationTemplate,
}

# Export all models
__all__ = [
    "ColumnCustomization",
    "UserTablePreferences",
    "CustomizationHistory",
    "CustomizationTemplate",
    "CUSTOMIZATION_MODELS",
]
