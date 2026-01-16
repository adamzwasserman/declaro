"""
Input validation and sanitization for TableV2 security.

This module provides comprehensive input validation and sanitization for all
user-created content including column names, filter values, and customizations.
"""

import html
import re
import unicodedata
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union

import bleach

from declaro_advise import error, info, success, warning


class InputSecurityLevel(Enum):
    """Security level for input validation."""

    PERMISSIVE = "permissive"
    STANDARD = "standard"
    STRICT = "strict"
    PARANOID = "paranoid"


@dataclass
class ValidationError:
    """Input validation error."""

    field_name: str
    error_type: str
    message: str
    severity: InputSecurityLevel
    suggested_fix: Optional[str] = None


@dataclass
class InputValidationResult:
    """Result of input validation."""

    is_valid: bool
    sanitized_value: Any
    errors: List[ValidationError] = field(default_factory=list)
    warnings: List[ValidationError] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


# Input validation configuration
VALIDATION_CONFIG = {
    "max_column_name_length": 100,
    "max_filter_value_length": 1000,
    "max_customization_name_length": 100,
    "max_formula_length": 1000,
    "allowed_column_name_chars": r"[a-zA-Z0-9_\-\s\.]",
    "allowed_filter_chars": r"[a-zA-Z0-9_\-\s\.,@\+\(\)%]",
    "forbidden_sql_keywords": {
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "UNION",
        "EXEC",
        "EXECUTE",
        "DECLARE",
        "GRANT",
        "REVOKE",
        "SHUTDOWN",
        "BACKUP",
        "RESTORE",
        "KILL",
        "WAITFOR",
        "OPENROWSET",
        "OPENQUERY",
        "OPENDATASOURCE",
        "BULK",
        "RECONFIGURE",
        "DBCC",
    },
    "dangerous_patterns": [
        r"<script[^>]*>.*?</script>",  # Script tags
        r"javascript:",  # JavaScript protocol
        r"vbscript:",  # VBScript protocol
        r"on\w+\s*=",  # Event handlers
        r"expression\s*\(",  # CSS expressions
        r"url\s*\(",  # URL function
        r"@import",  # CSS imports
        r"<!--.*?-->",  # HTML comments
        r"<\?.*?\?>",  # PHP tags
        r"<%.*?%>",  # ASP tags
        r"\${.*?}",  # Template literals
        r"{{.*?}}",  # Template literals
        r"&\w+;",  # HTML entities (some)
        r"&#\d+;",  # Numeric entities
        r"&#x[a-fA-F0-9]+;",  # Hex entities
    ],
    "html_tags_whitelist": ["b", "i", "u", "strong", "em", "p", "br", "span", "div"],
    "html_attributes_whitelist": {
        "*": ["class", "id"],
        "span": ["style"],
        "div": ["style"],
    },
}


def validate_user_input(
    value: Any,
    field_name: str,
    input_type: str = "text",
    security_level: InputSecurityLevel = InputSecurityLevel.STANDARD,
    max_length: Optional[int] = None,
    allow_html: bool = False,
    custom_patterns: Optional[List[str]] = None,
) -> InputValidationResult:
    """
    Validate and sanitize user input.

    Args:
        value: Input value to validate
        field_name: Name of the field being validated
        input_type: Type of input (text, number, email, etc.)
        security_level: Security level for validation
        max_length: Maximum allowed length
        allow_html: Whether to allow HTML content
        custom_patterns: Additional patterns to check

    Returns:
        InputValidationResult with validation results
    """
    errors = []
    warnings = []

    # Convert to string for processing
    str_value = str(value) if value is not None else ""
    original_value = str_value

    # Basic validation
    if not isinstance(value, (str, int, float, bool, type(None))):
        errors.append(
            ValidationError(
                field_name=field_name,
                error_type="invalid_type",
                message=f"Invalid input type: {type(value).__name__}",
                severity=InputSecurityLevel.STRICT,
            )
        )
        return InputValidationResult(is_valid=False, sanitized_value=None, errors=errors)

    # Length validation
    if max_length and len(str_value) > max_length:
        errors.append(
            ValidationError(
                field_name=field_name,
                error_type="length_exceeded",
                message=f"Input length {len(str_value)} exceeds maximum {max_length}",
                severity=InputSecurityLevel.STANDARD,
                suggested_fix=f"Truncate to {max_length} characters",
            )
        )
        str_value = str_value[:max_length]

    # Unicode normalization
    str_value = unicodedata.normalize("NFKC", str_value)

    # Dangerous pattern detection
    dangerous_patterns = VALIDATION_CONFIG["dangerous_patterns"]
    if custom_patterns:
        dangerous_patterns.extend(custom_patterns)

    for pattern in dangerous_patterns:
        if re.search(pattern, str_value, re.IGNORECASE | re.DOTALL):
            errors.append(
                ValidationError(
                    field_name=field_name,
                    error_type="dangerous_pattern",
                    message=f"Dangerous pattern detected",
                    severity=InputSecurityLevel.STRICT,
                )
            )

    # Path traversal detection
    if "../" in str_value or "..\\" in str_value:
        errors.append(
            ValidationError(
                field_name=field_name,
                error_type="path_traversal",
                message="Path traversal attempt detected",
                severity=InputSecurityLevel.STRICT,
            )
        )

    # SQL injection detection
    sql_keywords = VALIDATION_CONFIG["forbidden_sql_keywords"]
    for keyword in sql_keywords:
        if re.search(rf"\b{keyword}\b", str_value, re.IGNORECASE):
            errors.append(
                ValidationError(
                    field_name=field_name,
                    error_type="sql_injection",
                    message=f"Potential SQL injection detected: {keyword}",
                    severity=InputSecurityLevel.STRICT,
                )
            )

    # Type-specific validation
    if input_type == "email":
        if not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", str_value):
            errors.append(
                ValidationError(
                    field_name=field_name,
                    error_type="invalid_email",
                    message="Invalid email format",
                    severity=InputSecurityLevel.STANDARD,
                )
            )

    elif input_type == "url":
        if not re.match(r"^https?://[^\s/$.?#].[^\s]*$", str_value):
            errors.append(
                ValidationError(
                    field_name=field_name,
                    error_type="invalid_url",
                    message="Invalid URL format",
                    severity=InputSecurityLevel.STANDARD,
                )
            )

    elif input_type == "number":
        try:
            float(str_value)
        except ValueError:
            errors.append(
                ValidationError(
                    field_name=field_name,
                    error_type="invalid_number",
                    message="Invalid number format",
                    severity=InputSecurityLevel.STANDARD,
                )
            )

    # HTML sanitization
    if allow_html:
        str_value = bleach.clean(
            str_value,
            tags=VALIDATION_CONFIG["html_tags_whitelist"],
            attributes=VALIDATION_CONFIG["html_attributes_whitelist"],
            strip=True,
        )
    else:
        str_value = html.escape(str_value)

    # Security level specific validation
    if security_level == InputSecurityLevel.PARANOID:
        # Only allow alphanumeric and basic punctuation
        if not re.match(r"^[a-zA-Z0-9\s\.,\-_@\(\)]*$", str_value):
            errors.append(
                ValidationError(
                    field_name=field_name,
                    error_type="invalid_characters",
                    message="Contains invalid characters for paranoid security level",
                    severity=InputSecurityLevel.PARANOID,
                )
            )

    # Determine if input is valid
    critical_errors = [e for e in errors if e.severity in [InputSecurityLevel.STRICT, InputSecurityLevel.PARANOID]]
    is_valid = len(critical_errors) == 0

    return InputValidationResult(
        is_valid=is_valid,
        sanitized_value=str_value if is_valid else None,
        errors=errors,
        warnings=warnings,
        metadata={
            "original_value": original_value,
            "original_length": len(original_value),
            "sanitized_length": len(str_value),
            "security_level": security_level.value,
            "input_type": input_type,
        },
    )


def sanitize_column_name(column_name: str) -> str:
    """
    Sanitize column name for safe use.

    Args:
        column_name: Column name to sanitize

    Returns:
        Sanitized column name
    """
    if not column_name:
        return ""

    # Convert to string and normalize
    sanitized = str(column_name)
    sanitized = unicodedata.normalize("NFKC", sanitized)

    # Remove dangerous characters
    sanitized = re.sub(r"[^a-zA-Z0-9_\-\s\.]", "", sanitized)

    # Limit length
    if len(sanitized) > VALIDATION_CONFIG["max_column_name_length"]:
        sanitized = sanitized[: VALIDATION_CONFIG["max_column_name_length"]]

    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"

    # Replace spaces with underscores
    sanitized = re.sub(r"\s+", "_", sanitized)

    # Remove multiple underscores
    sanitized = re.sub(r"_+", "_", sanitized)

    # Strip leading/trailing underscores
    sanitized = sanitized.strip("_")

    return sanitized or "column"


def sanitize_filter_value(filter_value: Any) -> str:
    """
    Sanitize filter value for safe use.

    Args:
        filter_value: Filter value to sanitize

    Returns:
        Sanitized filter value
    """
    if filter_value is None:
        return ""

    # Convert to string
    sanitized = str(filter_value)

    # Normalize unicode
    sanitized = unicodedata.normalize("NFKC", sanitized)

    # Remove dangerous patterns
    for pattern in VALIDATION_CONFIG["dangerous_patterns"]:
        sanitized = re.sub(pattern, "", sanitized, flags=re.IGNORECASE | re.DOTALL)

    # Remove SQL keywords
    for keyword in VALIDATION_CONFIG["forbidden_sql_keywords"]:
        sanitized = re.sub(rf"\b{keyword}\b", "", sanitized, flags=re.IGNORECASE)

    # Limit length
    if len(sanitized) > VALIDATION_CONFIG["max_filter_value_length"]:
        sanitized = sanitized[: VALIDATION_CONFIG["max_filter_value_length"]]

    # HTML escape
    sanitized = html.escape(sanitized)

    return sanitized


def validate_customization_input(
    customization_data: Dict[str, Any], security_level: InputSecurityLevel = InputSecurityLevel.STANDARD
) -> InputValidationResult:
    """
    Validate customization input data.

    Args:
        customization_data: Customization data to validate
        security_level: Security level for validation

    Returns:
        InputValidationResult with validation results
    """
    errors = []
    warnings = []
    sanitized_data = {}

    if not isinstance(customization_data, dict):
        errors.append(
            ValidationError(
                field_name="customization_data",
                error_type="invalid_type",
                message="Customization data must be a dictionary",
                severity=InputSecurityLevel.STRICT,
            )
        )
        return InputValidationResult(is_valid=False, sanitized_value=None, errors=errors)

    # Validate each field
    for field_name, field_value in customization_data.items():
        # Validate field name
        field_name_result = validate_user_input(
            field_name,
            "field_name",
            security_level=security_level,
            max_length=VALIDATION_CONFIG["max_customization_name_length"],
        )

        if not field_name_result.is_valid:
            errors.extend(field_name_result.errors)
            continue

        sanitized_field_name = field_name_result.sanitized_value

        # Validate field value
        if isinstance(field_value, dict):
            # Recursive validation for nested objects
            nested_result = validate_customization_input(field_value, security_level)
            if not nested_result.is_valid:
                errors.extend(nested_result.errors)
            else:
                sanitized_data[sanitized_field_name] = nested_result.sanitized_value

        elif isinstance(field_value, list):
            # Validate list items
            sanitized_list = []
            for i, item in enumerate(field_value):
                item_result = validate_user_input(item, f"{field_name}[{i}]", security_level=security_level)
                if not item_result.is_valid:
                    errors.extend(item_result.errors)
                else:
                    sanitized_list.append(item_result.sanitized_value)

            if not errors:
                sanitized_data[sanitized_field_name] = sanitized_list

        else:
            # Validate single value
            value_result = validate_user_input(field_value, field_name, security_level=security_level)
            if not value_result.is_valid:
                errors.extend(value_result.errors)
            else:
                sanitized_data[sanitized_field_name] = value_result.sanitized_value

    # Check for required fields
    required_fields = ["user_id", "table_name"]
    for required_field in required_fields:
        if required_field not in customization_data:
            warnings.append(
                ValidationError(
                    field_name=required_field,
                    error_type="missing_field",
                    message=f"Required field '{required_field}' is missing",
                    severity=InputSecurityLevel.STANDARD,
                )
            )

    is_valid = len([e for e in errors if e.severity in [InputSecurityLevel.STRICT, InputSecurityLevel.PARANOID]]) == 0

    return InputValidationResult(
        is_valid=is_valid,
        sanitized_value=sanitized_data if is_valid else None,
        errors=errors,
        warnings=warnings,
        metadata={
            "field_count": len(customization_data),
            "security_level": security_level.value,
        },
    )


def validate_user_customizations(
    customizations: Dict[str, Any], user_id: str, security_level: InputSecurityLevel = InputSecurityLevel.STANDARD
) -> InputValidationResult:
    """
    Validate all user customizations.

    Args:
        customizations: User customizations to validate
        user_id: User ID for context
        security_level: Security level for validation

    Returns:
        InputValidationResult with validation results
    """
    errors = []
    warnings = []
    sanitized_customizations = {}

    # Validate user_id
    user_id_result = validate_user_input(user_id, "user_id", security_level=security_level, max_length=100)

    if not user_id_result.is_valid:
        errors.extend(user_id_result.errors)
        return InputValidationResult(is_valid=False, sanitized_value=None, errors=errors)

    # Validate customizations structure
    if not isinstance(customizations, dict):
        errors.append(
            ValidationError(
                field_name="customizations",
                error_type="invalid_type",
                message="Customizations must be a dictionary",
                severity=InputSecurityLevel.STRICT,
            )
        )
        return InputValidationResult(is_valid=False, sanitized_value=None, errors=errors)

    # Validate each customization type
    for customization_type, customization_data in customizations.items():
        # Validate customization type
        type_result = validate_user_input(customization_type, "customization_type", security_level=security_level)

        if not type_result.is_valid:
            errors.extend(type_result.errors)
            continue

        # Validate customization data
        data_result = validate_customization_input(customization_data, security_level)
        if not data_result.is_valid:
            errors.extend(data_result.errors)
        else:
            sanitized_customizations[type_result.sanitized_value] = data_result.sanitized_value

    is_valid = len([e for e in errors if e.severity in [InputSecurityLevel.STRICT, InputSecurityLevel.PARANOID]]) == 0

    return InputValidationResult(
        is_valid=is_valid,
        sanitized_value=sanitized_customizations if is_valid else None,
        errors=errors,
        warnings=warnings,
        metadata={
            "user_id": user_id_result.sanitized_value,
            "customization_count": len(customizations),
            "security_level": security_level.value,
        },
    )


def is_safe_input(value: Any, field_name: str = "input") -> bool:
    """
    Quick safety check for input.

    Args:
        value: Input value to check
        field_name: Field name for context

    Returns:
        True if input is safe, False otherwise
    """
    result = validate_user_input(value, field_name, security_level=InputSecurityLevel.STANDARD)
    return result.is_valid


def clean_input(value: Any, max_length: Optional[int] = None) -> str:
    """
    Clean input value for safe use.

    Args:
        value: Input value to clean
        max_length: Maximum length to enforce

    Returns:
        Cleaned input value
    """
    if value is None:
        return ""

    # Convert to string
    cleaned = str(value)

    # Normalize unicode
    cleaned = unicodedata.normalize("NFKC", cleaned)

    # Remove dangerous patterns
    for pattern in VALIDATION_CONFIG["dangerous_patterns"]:
        cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE | re.DOTALL)

    # HTML escape
    cleaned = html.escape(cleaned)

    # Limit length
    if max_length and len(cleaned) > max_length:
        cleaned = cleaned[:max_length]

    return cleaned


# Export all functions
__all__ = [
    "validate_user_input",
    "sanitize_column_name",
    "sanitize_filter_value",
    "validate_customization_input",
    "validate_user_customizations",
    "is_safe_input",
    "clean_input",
    "InputValidationResult",
    "ValidationError",
    "InputSecurityLevel",
]
