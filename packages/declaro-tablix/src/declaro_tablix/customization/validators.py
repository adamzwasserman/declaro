"""
Validation functions for TableV2 customization data.

This module provides comprehensive validation for customization data including
column configurations, user preferences, and data integrity checks.
"""

import json
import re
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator

from declaro_advise import error
from declaro_tablix.customization import CUSTOMIZATION_CONFIG

# Validation constants
VALID_COLUMN_ID_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
VALID_TABLE_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
VALID_USER_ID_PATTERN = re.compile(r"^[a-zA-Z0-9_.-]+$")
VALID_PREFERENCE_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9_\s.-]+$")


class ColumnCustomizationValidator(BaseModel):
    """Pydantic model for validating column customization data."""

    user_id: str = Field(..., min_length=1, max_length=128)
    table_name: str = Field(..., min_length=1, max_length=255)
    column_id: str = Field(..., min_length=1, max_length=255)
    alias: Optional[str] = Field(None, max_length=255)
    is_visible: bool = Field(default=True)
    display_order: Optional[int] = Field(None, ge=0, le=9999)
    column_width: Optional[int] = Field(None, ge=10, le=2000)
    format_options: Optional[Dict[str, Any]] = Field(None)
    filter_options: Optional[Dict[str, Any]] = Field(None)
    sort_options: Optional[Dict[str, Any]] = Field(None)

    @validator("user_id")
    def validate_user_id(cls, v):
        if not VALID_USER_ID_PATTERN.match(v):
            raise ValueError("Invalid user ID format")
        return v

    @validator("table_name")
    def validate_table_name(cls, v):
        if not VALID_TABLE_NAME_PATTERN.match(v):
            raise ValueError("Invalid table name format")
        return v

    @validator("column_id")
    def validate_column_id(cls, v):
        if not VALID_COLUMN_ID_PATTERN.match(v):
            raise ValueError("Invalid column ID format")
        return v

    @validator("alias")
    def validate_alias(cls, v):
        if v is not None and len(v.strip()) == 0:
            raise ValueError("Alias cannot be empty")
        return v

    @validator("format_options")
    def validate_format_options(cls, v):
        if v is not None:
            validate_json_size(v, "format_options")
        return v

    @validator("filter_options")
    def validate_filter_options(cls, v):
        if v is not None:
            validate_json_size(v, "filter_options")
        return v

    @validator("sort_options")
    def validate_sort_options(cls, v):
        if v is not None:
            validate_json_size(v, "sort_options")
        return v


class UserPreferencesValidator(BaseModel):
    """Pydantic model for validating user preferences data."""

    user_id: str = Field(..., min_length=1, max_length=128)
    table_name: str = Field(..., min_length=1, max_length=255)
    preference_name: str = Field(..., min_length=1, max_length=255)
    preference_data: Dict[str, Any] = Field(...)
    is_default: bool = Field(default=False)
    is_shared: bool = Field(default=False)

    @validator("user_id")
    def validate_user_id(cls, v):
        if not VALID_USER_ID_PATTERN.match(v):
            raise ValueError("Invalid user ID format")
        return v

    @validator("table_name")
    def validate_table_name(cls, v):
        if not VALID_TABLE_NAME_PATTERN.match(v):
            raise ValueError("Invalid table name format")
        return v

    @validator("preference_name")
    def validate_preference_name(cls, v):
        if not VALID_PREFERENCE_NAME_PATTERN.match(v):
            raise ValueError("Invalid preference name format")
        return v

    @validator("preference_data")
    def validate_preference_data(cls, v):
        # Check size
        json_str = json.dumps(v)
        if len(json_str) > CUSTOMIZATION_CONFIG["max_user_preferences_size"]:
            raise ValueError("Preference data exceeds maximum size")

        # Validate structure
        if not isinstance(v, dict):
            raise ValueError("Preference data must be a dictionary")

        return v


class CustomizationTemplateValidator(BaseModel):
    """Pydantic model for validating customization templates."""

    template_name: str = Field(..., min_length=1, max_length=255)
    template_description: Optional[str] = Field(None, max_length=1000)
    template_data: Dict[str, Any] = Field(...)
    created_by: str = Field(..., min_length=1, max_length=128)
    is_public: bool = Field(default=False)
    is_system_template: bool = Field(default=False)

    @validator("template_name")
    def validate_template_name(cls, v):
        if not VALID_PREFERENCE_NAME_PATTERN.match(v):
            raise ValueError("Invalid template name format")
        return v

    @validator("template_data")
    def validate_template_data(cls, v):
        # Check size
        json_str = json.dumps(v)
        if len(json_str) > CUSTOMIZATION_CONFIG["max_user_preferences_size"]:
            raise ValueError("Template data exceeds maximum size")

        # Validate structure
        if not isinstance(v, dict):
            raise ValueError("Template data must be a dictionary")

        return v

    @validator("created_by")
    def validate_created_by(cls, v):
        if not VALID_USER_ID_PATTERN.match(v):
            raise ValueError("Invalid creator user ID format")
        return v


def validate_json_size(data: Any, field_name: str, max_size: int = 10000) -> None:
    """
    Validate JSON data size.

    Args:
        data: Data to validate
        field_name: Field name for error messages
        max_size: Maximum size in bytes

    Raises:
        ValueError: If data exceeds maximum size
    """
    json_str = json.dumps(data)
    if len(json_str) > max_size:
        raise ValueError(f"{field_name} exceeds maximum size of {max_size} bytes")


def validate_column_customization_data(customization_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate column customization data.

    Args:
        customization_data: Data to validate

    Returns:
        Validation result with success status and errors
    """
    try:
        # Validate using Pydantic model
        validator = ColumnCustomizationValidator(**customization_data)

        # Additional business logic validation
        errors = []

        # Check supported customization types
        for key in customization_data.keys():
            if key not in [
                "user_id",
                "table_name",
                "column_id",
                "alias",
                "is_visible",
                "display_order",
                "column_width",
                "format_options",
                "filter_options",
                "sort_options",
            ]:
                if key not in CUSTOMIZATION_CONFIG["supported_customization_types"]:
                    errors.append(f"Unsupported customization type: {key}")

        # Validate display order uniqueness (would need database check)
        if customization_data.get("display_order") is not None:
            if customization_data["display_order"] < 0:
                errors.append("Display order must be non-negative")

        # Validate column width
        if customization_data.get("column_width") is not None:
            width = customization_data["column_width"]
            if width < 10 or width > 2000:
                errors.append("Column width must be between 10 and 2000 pixels")

        # Validate format options
        if customization_data.get("format_options"):
            format_validation = validate_format_options(customization_data["format_options"])
            if not format_validation["success"]:
                errors.extend(format_validation["errors"])

        if errors:
            return {"success": False, "errors": errors}

        return {"success": True, "validated_data": validator.dict()}

    except Exception as e:
        error(f"Validation failed: {str(e)}")
        return {"success": False, "errors": [str(e)]}


def validate_user_preferences_data(preferences_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate user preferences data.

    Args:
        preferences_data: Data to validate

    Returns:
        Validation result with success status and errors
    """
    try:
        # Validate using Pydantic model
        validator = UserPreferencesValidator(**preferences_data)

        # Additional business logic validation
        errors = []

        # Validate preference data structure
        pref_data = preferences_data.get("preference_data", {})

        # Check for required preference fields
        if "columns" not in pref_data and "filters" not in pref_data and "sorts" not in pref_data:
            errors.append("Preference data must contain at least one of: columns, filters, sorts")

        # Validate columns configuration
        if "columns" in pref_data:
            columns_validation = validate_columns_configuration(pref_data["columns"])
            if not columns_validation["success"]:
                errors.extend(columns_validation["errors"])

        # Validate filters configuration
        if "filters" in pref_data:
            filters_validation = validate_filters_configuration(pref_data["filters"])
            if not filters_validation["success"]:
                errors.extend(filters_validation["errors"])

        # Validate sorts configuration
        if "sorts" in pref_data:
            sorts_validation = validate_sorts_configuration(pref_data["sorts"])
            if not sorts_validation["success"]:
                errors.extend(sorts_validation["errors"])

        if errors:
            return {"success": False, "errors": errors}

        return {"success": True, "validated_data": validator.dict()}

    except Exception as e:
        error(f"Preferences validation failed: {str(e)}")
        return {"success": False, "errors": [str(e)]}


def validate_format_options(format_options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate format options data.

    Args:
        format_options: Format options to validate

    Returns:
        Validation result with success status and errors
    """
    errors = []

    # Validate date format
    if "date_format" in format_options:
        date_format = format_options["date_format"]
        if not isinstance(date_format, str) or len(date_format) == 0:
            errors.append("Date format must be a non-empty string")

    # Validate number format
    if "number_format" in format_options:
        number_format = format_options["number_format"]
        if not isinstance(number_format, dict):
            errors.append("Number format must be a dictionary")
        else:
            if "decimal_places" in number_format:
                decimal_places = number_format["decimal_places"]
                if not isinstance(decimal_places, int) or decimal_places < 0 or decimal_places > 10:
                    errors.append("Decimal places must be an integer between 0 and 10")

    # Validate currency format
    if "currency_format" in format_options:
        currency_format = format_options["currency_format"]
        if not isinstance(currency_format, dict):
            errors.append("Currency format must be a dictionary")
        else:
            if "currency_code" in currency_format:
                currency_code = currency_format["currency_code"]
                if not isinstance(currency_code, str) or len(currency_code) != 3:
                    errors.append("Currency code must be a 3-character string")

    if errors:
        return {"success": False, "errors": errors}

    return {"success": True}


def validate_columns_configuration(columns_config: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate columns configuration data.

    Args:
        columns_config: Columns configuration to validate

    Returns:
        Validation result with success status and errors
    """
    errors = []

    if not isinstance(columns_config, list):
        errors.append("Columns configuration must be a list")
        return {"success": False, "errors": errors}

    column_ids = set()
    display_orders = set()

    for i, column in enumerate(columns_config):
        if not isinstance(column, dict):
            errors.append(f"Column {i} must be a dictionary")
            continue

        # Check required fields
        if "id" not in column:
            errors.append(f"Column {i} missing required field 'id'")
            continue

        column_id = column["id"]
        if not isinstance(column_id, str) or len(column_id) == 0:
            errors.append(f"Column {i} ID must be a non-empty string")
            continue

        # Check for duplicate column IDs
        if column_id in column_ids:
            errors.append(f"Duplicate column ID: {column_id}")
        column_ids.add(column_id)

        # Check display order
        if "display_order" in column:
            display_order = column["display_order"]
            if not isinstance(display_order, int) or display_order < 0:
                errors.append(f"Column {column_id} display order must be a non-negative integer")
            elif display_order in display_orders:
                errors.append(f"Duplicate display order: {display_order}")
            else:
                display_orders.add(display_order)

        # Check visibility
        if "is_visible" in column:
            is_visible = column["is_visible"]
            if not isinstance(is_visible, bool):
                errors.append(f"Column {column_id} is_visible must be a boolean")

        # Check width
        if "width" in column:
            width = column["width"]
            if not isinstance(width, int) or width < 10 or width > 2000:
                errors.append(f"Column {column_id} width must be between 10 and 2000 pixels")

    if errors:
        return {"success": False, "errors": errors}

    return {"success": True}


def validate_filters_configuration(filters_config: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate filters configuration data.

    Args:
        filters_config: Filters configuration to validate

    Returns:
        Validation result with success status and errors
    """
    errors = []

    if not isinstance(filters_config, list):
        errors.append("Filters configuration must be a list")
        return {"success": False, "errors": errors}

    valid_operators = [
        "equals",
        "not_equals",
        "contains",
        "not_contains",
        "starts_with",
        "ends_with",
        "greater_than",
        "less_than",
        "between",
        "in",
        "not_in",
    ]

    for i, filter_item in enumerate(filters_config):
        if not isinstance(filter_item, dict):
            errors.append(f"Filter {i} must be a dictionary")
            continue

        # Check required fields
        required_fields = ["column_id", "operator", "value"]
        for field in required_fields:
            if field not in filter_item:
                errors.append(f"Filter {i} missing required field '{field}'")

        # Check operator
        if "operator" in filter_item:
            operator = filter_item["operator"]
            if operator not in valid_operators:
                errors.append(f"Filter {i} invalid operator: {operator}")

        # Check column_id
        if "column_id" in filter_item:
            column_id = filter_item["column_id"]
            if not isinstance(column_id, str) or len(column_id) == 0:
                errors.append(f"Filter {i} column_id must be a non-empty string")

    if errors:
        return {"success": False, "errors": errors}

    return {"success": True}


def validate_sorts_configuration(sorts_config: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate sorts configuration data.

    Args:
        sorts_config: Sorts configuration to validate

    Returns:
        Validation result with success status and errors
    """
    errors = []

    if not isinstance(sorts_config, list):
        errors.append("Sorts configuration must be a list")
        return {"success": False, "errors": errors}

    valid_directions = ["asc", "desc"]
    column_ids = set()

    for i, sort_item in enumerate(sorts_config):
        if not isinstance(sort_item, dict):
            errors.append(f"Sort {i} must be a dictionary")
            continue

        # Check required fields
        required_fields = ["column_id", "direction"]
        for field in required_fields:
            if field not in sort_item:
                errors.append(f"Sort {i} missing required field '{field}'")

        # Check column_id
        if "column_id" in sort_item:
            column_id = sort_item["column_id"]
            if not isinstance(column_id, str) or len(column_id) == 0:
                errors.append(f"Sort {i} column_id must be a non-empty string")
            elif column_id in column_ids:
                errors.append(f"Duplicate sort column_id: {column_id}")
            else:
                column_ids.add(column_id)

        # Check direction
        if "direction" in sort_item:
            direction = sort_item["direction"]
            if direction not in valid_directions:
                errors.append(f"Sort {i} invalid direction: {direction}")

        # Check priority
        if "priority" in sort_item:
            priority = sort_item["priority"]
            if not isinstance(priority, int) or priority < 0:
                errors.append(f"Sort {i} priority must be a non-negative integer")

    if errors:
        return {"success": False, "errors": errors}

    return {"success": True}


def validate_customization_template_data(template_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate customization template data.

    Args:
        template_data: Template data to validate

    Returns:
        Validation result with success status and errors
    """
    try:
        # Validate using Pydantic model
        validator = CustomizationTemplateValidator(**template_data)

        # Additional business logic validation
        errors = []

        # Validate template data structure
        template_content = template_data.get("template_data", {})

        # Template should contain customization rules
        if not isinstance(template_content, dict):
            errors.append("Template data must be a dictionary")
        elif len(template_content) == 0:
            errors.append("Template data cannot be empty")

        # Validate template content structure
        if "columns" in template_content:
            columns_validation = validate_columns_configuration(template_content["columns"])
            if not columns_validation["success"]:
                errors.extend(columns_validation["errors"])

        if errors:
            return {"success": False, "errors": errors}

        return {"success": True, "validated_data": validator.dict()}

    except Exception as e:
        error(f"Template validation failed: {str(e)}")
        return {"success": False, "errors": [str(e)]}


# Export all validation functions
__all__ = [
    "ColumnCustomizationValidator",
    "UserPreferencesValidator",
    "CustomizationTemplateValidator",
    "validate_column_customization_data",
    "validate_user_preferences_data",
    "validate_format_options",
    "validate_columns_configuration",
    "validate_filters_configuration",
    "validate_sorts_configuration",
    "validate_customization_template_data",
]
