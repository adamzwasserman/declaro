"""Data validation plugin example for Table Module V2.

This plugin demonstrates comprehensive data validation including
email validation, phone number formatting, URL checking, and custom
business rule validation.
"""

import re
from typing import Any, Dict, List

from declaro_advise import error, info, warning
from declaro_tablix.domain.models import ColumnDefinition, TableConfig, TableData
from declaro_tablix.plugins.protocols import DataProcessingPlugin


class DataValidationPlugin:
    """Example plugin for comprehensive data validation and cleaning."""

    def __init__(self):
        self._name = "data_validation_plugin"
        self._version = "1.0.0"
        self._description = "Provides comprehensive data validation and cleaning capabilities"
        self._initialized = False
        self._validation_rules = {}
        self._error_count = 0
        self._warning_count = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def version(self) -> str:
        return self._version

    @property
    def description(self) -> str:
        return self._description

    def initialize(self, config: Dict[str, Any]) -> None:
        """Initialize the data validation plugin."""
        try:
            self._config = config
            self._validation_rules = config.get("validation_rules", {})
            self._strict_mode = config.get("strict_mode", False)
            self._auto_fix = config.get("auto_fix", True)
            self._initialized = True
            self._error_count = 0
            self._warning_count = 0
            info(f"Data validation plugin initialized with {len(self._validation_rules)} custom rules")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize data validation plugin: {e}")

    def cleanup(self) -> None:
        """Cleanup plugin resources."""
        if self._error_count > 0 or self._warning_count > 0:
            info(f"Data validation completed: {self._error_count} errors, {self._warning_count} warnings")
        self._config = {}
        self._validation_rules = {}
        self._error_count = 0
        self._warning_count = 0
        self._initialized = False
        info("Data validation plugin cleaned up")

    def process_data(self, data: TableData, config: TableConfig, context: Dict[str, Any]) -> TableData:
        """Process and validate table data."""
        if not self._initialized:
            return data

        try:
            info(f"Starting data validation for {len(data.rows)} rows")
            validated_data = TableData(
                columns=data.columns,
                rows=[],
                total_count=data.total_count,
                metadata=data.metadata.copy() if data.metadata else {},
            )

            # Process each row
            for row_index, row in enumerate(data.rows):
                validated_row = self._validate_row(row, data.columns, row_index)
                validated_data.rows.append(validated_row)

            # Add validation metadata
            if not validated_data.metadata:
                validated_data.metadata = {}
            validated_data.metadata.update(
                {
                    "validation_applied": True,
                    "validation_errors": self._error_count,
                    "validation_warnings": self._warning_count,
                    "plugin_version": self._version,
                }
            )

            if self._error_count > 0:
                warning(f"Data validation found {self._error_count} errors and {self._warning_count} warnings")
            else:
                info(f"Data validation completed successfully with {self._warning_count} warnings")

            return validated_data

        except Exception as e:
            error(f"Data validation processing failed: {e}")
            return data

    def validate_data(self, data: TableData, config: TableConfig, context: Dict[str, Any]) -> List[str]:
        """Validate table data and return list of error messages."""
        if not self._initialized:
            return []

        try:
            errors = []

            # Validate overall data structure
            if not data.rows:
                errors.append("No data rows found")
                return errors

            if not data.columns:
                errors.append("No column definitions found")
                return errors

            # Validate each row
            for row_index, row in enumerate(data.rows):
                row_errors = self._validate_row_errors(row, data.columns, row_index)
                errors.extend(row_errors)

            return errors

        except Exception as e:
            return [f"Validation error: {str(e)}"]

    def transform_column(self, column: ColumnDefinition, data: List[Any], context: Dict[str, Any]) -> List[Any]:
        """Transform and clean data for a specific column."""
        if not self._initialized:
            return data

        try:
            transformed_data = []
            column_type = column.type
            column_id = column.id

            for i, value in enumerate(data):
                try:
                    cleaned_value = self._clean_value(value, column)
                    validated_value = self._validate_value(cleaned_value, column, i)
                    transformed_data.append(validated_value)
                except Exception as e:
                    warning(f"Failed to transform value in column {column_id}, row {i}: {e}")
                    transformed_data.append(value)  # Keep original on error

            return transformed_data

        except Exception as e:
            error(f"Column transformation failed for {column.id}: {e}")
            return data

    def _validate_row(self, row: Dict[str, Any], columns: List[ColumnDefinition], row_index: int) -> Dict[str, Any]:
        """Validate and clean a single row."""
        validated_row = {}

        for column in columns:
            column_id = column.id
            value = row.get(column_id)

            try:
                # Clean the value first
                cleaned_value = self._clean_value(value, column)
                # Then validate it
                validated_value = self._validate_value(cleaned_value, column, row_index)
                validated_row[column_id] = validated_value

            except Exception as e:
                self._error_count += 1
                warning(f"Validation failed for {column_id} in row {row_index}: {e}")
                validated_row[column_id] = value  # Keep original on error

        return validated_row

    def _validate_row_errors(self, row: Dict[str, Any], columns: List[ColumnDefinition], row_index: int) -> List[str]:
        """Get validation errors for a single row."""
        errors = []

        for column in columns:
            column_id = column.id
            value = row.get(column_id)

            # Check required fields
            if column.required and (value is None or value == ""):
                errors.append(f"Row {row_index + 1}: {column_id} is required")

            # Validate value format
            if value is not None and value != "":
                column_errors = self._validate_value_errors(value, column, row_index)
                errors.extend(column_errors)

        return errors

    def _clean_value(self, value: Any, column: ColumnDefinition) -> Any:
        """Clean and normalize a value based on column type."""
        if value is None:
            return None

        column_type = column.type

        try:
            # Convert to string for processing
            str_value = str(value).strip()

            if column_type == "EMAIL":
                return self._clean_email(str_value)
            elif column_type == "PHONE":
                return self._clean_phone(str_value)
            elif column_type == "URL":
                return self._clean_url(str_value)
            elif column_type == "TEXT":
                return self._clean_text(str_value)
            elif column_type == "NUMBER":
                return self._clean_number(str_value)
            elif column_type == "CURRENCY":
                return self._clean_currency(str_value)
            elif column_type == "PERCENTAGE":
                return self._clean_percentage(str_value)
            elif column_type == "DATE":
                return self._clean_date(str_value)
            elif column_type == "BOOLEAN":
                return self._clean_boolean(str_value)
            else:
                return str_value

        except Exception:
            return value  # Return original if cleaning fails

    def _validate_value(self, value: Any, column: ColumnDefinition, row_index: int) -> Any:
        """Validate a cleaned value."""
        if value is None or value == "":
            return value

        column_type = column.type
        column_id = column.id

        try:
            if column_type == "EMAIL":
                return self._validate_email(value, column_id, row_index)
            elif column_type == "PHONE":
                return self._validate_phone(value, column_id, row_index)
            elif column_type == "URL":
                return self._validate_url(value, column_id, row_index)
            elif column_type == "NUMBER":
                return self._validate_number(value, column, row_index)
            elif column_type == "CURRENCY":
                return self._validate_currency(value, column, row_index)
            elif column_type == "PERCENTAGE":
                return self._validate_percentage(value, column, row_index)
            elif column_type == "DATE":
                return self._validate_date(value, column_id, row_index)
            elif column_type == "BOOLEAN":
                return self._validate_boolean(value, column_id, row_index)
            else:
                return value

        except Exception as e:
            self._error_count += 1
            raise ValueError(f"Validation failed: {e}")

    def _validate_value_errors(self, value: Any, column: ColumnDefinition, row_index: int) -> List[str]:
        """Get validation errors for a single value."""
        errors = []
        column_type = column.type
        column_id = column.id

        try:
            if column_type == "EMAIL" and not self._is_valid_email(str(value)):
                errors.append(f"Row {row_index + 1}: {column_id} is not a valid email address")
            elif column_type == "PHONE" and not self._is_valid_phone(str(value)):
                errors.append(f"Row {row_index + 1}: {column_id} is not a valid phone number")
            elif column_type == "URL" and not self._is_valid_url(str(value)):
                errors.append(f"Row {row_index + 1}: {column_id} is not a valid URL")
            elif column_type == "NUMBER" and not self._is_valid_number(str(value)):
                errors.append(f"Row {row_index + 1}: {column_id} is not a valid number")

        except Exception:
            pass  # Ignore validation errors in error checking

        return errors

    # Cleaning functions
    def _clean_email(self, value: str) -> str:
        """Clean email address."""
        return value.lower().strip()

    def _clean_phone(self, value: str) -> str:
        """Clean phone number."""
        # Remove all non-digits except + for international numbers
        cleaned = re.sub(r"[^\d+]", "", value)
        if cleaned.startswith("1") and len(cleaned) == 11:
            # US number with country code
            return f"+1-{cleaned[1:4]}-{cleaned[4:7]}-{cleaned[7:]}"
        elif len(cleaned) == 10:
            # US number without country code
            return f"{cleaned[:3]}-{cleaned[3:6]}-{cleaned[6:]}"
        else:
            return cleaned

    def _clean_url(self, value: str) -> str:
        """Clean URL."""
        url = value.strip()
        if url and not url.startswith(("http://", "https://")):
            return f"https://{url}"
        return url

    def _clean_text(self, value: str) -> str:
        """Clean text value."""
        return value.strip()

    def _clean_number(self, value: str) -> float:
        """Clean numeric value."""
        # Remove currency symbols and commas
        cleaned = re.sub(r"[$,]", "", value)
        return float(cleaned)

    def _clean_currency(self, value: str) -> float:
        """Clean currency value."""
        # Remove currency symbols and commas
        cleaned = re.sub(r"[$,]", "", value)
        return float(cleaned)

    def _clean_percentage(self, value: str) -> float:
        """Clean percentage value."""
        cleaned = value.replace("%", "").strip()
        percent = float(cleaned)
        # Convert to decimal if it looks like a percentage
        if percent > 1:
            return percent / 100
        return percent

    def _clean_date(self, value: str) -> str:
        """Clean date value."""
        return value.strip()

    def _clean_boolean(self, value: str) -> bool:
        """Clean boolean value."""
        value_lower = value.lower()
        if value_lower in ["true", "yes", "1", "on", "enabled"]:
            return True
        elif value_lower in ["false", "no", "0", "off", "disabled"]:
            return False
        else:
            raise ValueError(f"Cannot convert '{value}' to boolean")

    # Validation functions
    def _validate_email(self, value: str, column_id: str, row_index: int) -> str:
        """Validate email address."""
        if not self._is_valid_email(value):
            self._error_count += 1
            raise ValueError(f"Invalid email format: {value}")
        return value

    def _validate_phone(self, value: str, column_id: str, row_index: int) -> str:
        """Validate phone number."""
        if not self._is_valid_phone(value):
            self._warning_count += 1
            # Don't raise error, just warn
        return value

    def _validate_url(self, value: str, column_id: str, row_index: int) -> str:
        """Validate URL."""
        if not self._is_valid_url(value):
            self._warning_count += 1
            # Don't raise error, just warn
        return value

    def _validate_number(self, value: float, column: ColumnDefinition, row_index: int) -> float:
        """Validate numeric value."""
        # Check min/max constraints if specified
        min_val = column.format_options.get("min_value")
        max_val = column.format_options.get("max_value")

        if min_val is not None and value < min_val:
            self._error_count += 1
            raise ValueError(f"Value {value} is below minimum {min_val}")

        if max_val is not None and value > max_val:
            self._error_count += 1
            raise ValueError(f"Value {value} is above maximum {max_val}")

        return value

    def _validate_currency(self, value: float, column: ColumnDefinition, row_index: int) -> float:
        """Validate currency value."""
        # Same as number validation
        return self._validate_number(value, column, row_index)

    def _validate_percentage(self, value: float, column: ColumnDefinition, row_index: int) -> float:
        """Validate percentage value."""
        if value < 0 or value > 1:
            self._warning_count += 1
            # Don't raise error for percentage out of 0-1 range
        return value

    def _validate_date(self, value: str, column_id: str, row_index: int) -> str:
        """Validate date value."""
        # Try to parse common date formats
        from datetime import datetime

        formats = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S"]

        for fmt in formats:
            try:
                datetime.strptime(value, fmt)
                return value
            except ValueError:
                continue

        self._error_count += 1
        raise ValueError(f"Invalid date format: {value}")

    def _validate_boolean(self, value: bool, column_id: str, row_index: int) -> bool:
        """Validate boolean value."""
        return value

    # Validation check functions
    def _is_valid_email(self, email: str) -> bool:
        """Check if email format is valid."""
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return re.match(pattern, email) is not None

    def _is_valid_phone(self, phone: str) -> bool:
        """Check if phone number format is valid."""
        # Remove all non-digits
        digits = re.sub(r"\D", "", phone)
        # Valid if 10 or 11 digits (US format)
        return len(digits) in [10, 11]

    def _is_valid_url(self, url: str) -> bool:
        """Check if URL format is valid."""
        pattern = r"^https?://[^\s/$.?#].[^\s]*$"
        return re.match(pattern, url) is not None

    def _is_valid_number(self, value: str) -> bool:
        """Check if value can be converted to number."""
        try:
            float(value)
            return True
        except ValueError:
            return False
