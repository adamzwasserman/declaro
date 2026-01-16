"""Domain validation functions for Table Module V2.

This module provides pure validation functions following clean architecture principles.
All validation functions are pure functions with no side effects.
"""

import re
from typing import Any, Dict, List, Set, Tuple
from urllib.parse import urlparse

from .models import (
    ColumnDefinition,
    ColumnType,
    FilterDefinition,
    FilterOperator,
    PaginationSettings,
    SortDefinition,
    TableConfig,
    TableData,
    UserFilterSet,
    UserSavedLayout,
    UserSavedSearch,
)


def validate_table_config(config: TableConfig) -> Tuple[bool, List[str]]:
    """Validate complete table configuration.

    Args:
        config: Table configuration to validate

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Validate table name
    if not config.table_name or not config.table_name.strip():
        errors.append("Table name cannot be empty")

    # Validate columns
    if not config.columns:
        errors.append("Table must have at least one column")
    else:
        column_ids = set()
        for i, column in enumerate(config.columns):
            is_valid, column_errors = validate_column_definition(column)
            if not is_valid:
                errors.extend([f"Column {i+1}: {error}" for error in column_errors])

            # Check for duplicate column IDs
            if column.id in column_ids:
                errors.append(f"Duplicate column ID: {column.id}")
            column_ids.add(column.id)

    # Validate filters
    for i, filter_def in enumerate(config.filters):
        is_valid, filter_errors = validate_filter_definition(filter_def)
        if not is_valid:
            errors.extend([f"Filter {i+1}: {error}" for error in filter_errors])

    # Validate sorts
    for i, sort_def in enumerate(config.sorts):
        is_valid, sort_errors = validate_sort_definition(sort_def)
        if not is_valid:
            errors.extend([f"Sort {i+1}: {error}" for error in sort_errors])

    # Validate pagination
    is_valid, pagination_errors = validate_pagination_settings(config.pagination)
    if not is_valid:
        errors.extend([f"Pagination: {error}" for error in pagination_errors])

    # Validate group_by references valid column
    if config.group_by:
        column_ids = {col.id for col in config.columns}
        if config.group_by not in column_ids:
            errors.append(f"Group by column '{config.group_by}' not found in table columns")

    return len(errors) == 0, errors


def validate_column_definition(column: ColumnDefinition) -> Tuple[bool, List[str]]:
    """Validate individual column definition.

    Args:
        column: Column definition to validate

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Validate column ID
    if not is_valid_column_id(column.id):
        errors.append("Column ID must be non-empty string")

    # Validate column name
    if not column.name or not column.name.strip():
        errors.append("Column name cannot be empty")

    # Validate width constraints
    if column.width is not None:
        if column.width < 50:
            errors.append("Column width must be at least 50 pixels")
        elif column.width > 2000:
            errors.append("Column width cannot exceed 2000 pixels")

    # Validate format options for specific column types
    if column.type == ColumnType.CURRENCY:
        if not column.format_options or "currency_symbol" not in column.format_options:
            errors.append("Currency columns must have currency_symbol in format_options")

    if column.type == ColumnType.DATE or column.type == ColumnType.DATETIME:
        if column.format_options and "date_format" in column.format_options:
            if not is_valid_date_format(column.format_options["date_format"]):
                errors.append("Invalid date format specified")

    # Validate aggregation functions
    if column.aggregation_functions:
        valid_functions = {"sum", "avg", "min", "max", "count", "distinct_count"}
        invalid_functions = set(column.aggregation_functions) - valid_functions
        if invalid_functions:
            errors.append(f"Invalid aggregation functions: {', '.join(invalid_functions)}")

        # Check if aggregation is compatible with column type
        if not validate_aggregation_compatibility(column.type, column.aggregation_functions):
            errors.append(f"Aggregation functions {column.aggregation_functions} not compatible with {column.type} column")

    return len(errors) == 0, errors


def validate_table_data(data: TableData, config: TableConfig) -> Tuple[bool, List[str]]:
    """Validate table data against configuration.

    Args:
        data: Table data to validate
        config: Table configuration for validation

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Validate basic data structure
    if not isinstance(data.rows, list):
        errors.append("Table data rows must be a list")
        return False, errors

    if data.total_count < 0:
        errors.append("Total count cannot be negative")

    if len(data.rows) > data.total_count:
        errors.append("Row count cannot exceed total count")

    # Validate each row against column definitions
    required_columns = {col.id for col in config.columns if col.required}
    unique_columns = {col.id for col in config.columns if col.unique}
    unique_values = {col_id: set() for col_id in unique_columns}

    for row_idx, row in enumerate(data.rows):
        # Check required columns
        for col_id in required_columns:
            if col_id not in row or row[col_id] is None or row[col_id] == "":
                errors.append(f"Row {row_idx + 1}: Missing required value for column '{col_id}'")

        # Check unique constraints
        for col_id in unique_columns:
            if col_id in row and row[col_id] is not None:
                value = row[col_id]
                if value in unique_values[col_id]:
                    errors.append(f"Row {row_idx + 1}: Duplicate value '{value}' for unique column '{col_id}'")
                unique_values[col_id].add(value)

        # Validate column values
        for column in config.columns:
            if column.id in row:
                value = row[column.id]
                if value is not None:
                    if not is_valid_column_value(value, column.type):
                        errors.append(f"Row {row_idx + 1}: Invalid value '{value}' for {column.type} column '{column.id}'")

    return len(errors) == 0, errors


def validate_filter_definition(filter_def: FilterDefinition) -> Tuple[bool, List[str]]:
    """Validate filter definition.

    Args:
        filter_def: Filter definition to validate

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Validate column ID
    if not is_valid_column_id(filter_def.column_id):
        errors.append("Filter column ID must be non-empty string")

    # Validate operator
    if not is_valid_filter_operator(filter_def.operator):
        errors.append(f"Invalid filter operator: {filter_def.operator}")

    # Validate value requirements based on operator
    if filter_def.operator in [FilterOperator.IS_NULL, FilterOperator.IS_NOT_NULL]:
        if filter_def.value is not None:
            errors.append(f"Operator {filter_def.operator} should not have a value")
    elif filter_def.operator in [FilterOperator.IN, FilterOperator.NOT_IN]:
        if not filter_def.values or len(filter_def.values) == 0:
            errors.append(f"Operator {filter_def.operator} requires a non-empty values list")
    else:
        if filter_def.value is None:
            errors.append(f"Operator {filter_def.operator} requires a value")

    return len(errors) == 0, errors


def validate_sort_definition(sort_def: SortDefinition) -> Tuple[bool, List[str]]:
    """Validate sort definition.

    Args:
        sort_def: Sort definition to validate

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Validate column ID
    if not is_valid_column_id(sort_def.column_id):
        errors.append("Sort column ID must be non-empty string")

    # Validate priority
    if sort_def.priority < 0:
        errors.append("Sort priority cannot be negative")

    return len(errors) == 0, errors


def validate_pagination_settings(pagination: PaginationSettings) -> Tuple[bool, List[str]]:
    """Validate pagination settings.

    Args:
        pagination: Pagination settings to validate

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Validate page number
    if pagination.page < 1:
        errors.append("Page number must be at least 1")

    # Validate page size
    if pagination.page_size < 1:
        errors.append("Page size must be at least 1")
    elif pagination.page_size > 1000:
        errors.append("Page size cannot exceed 1000")

    # Validate total count
    if pagination.total_count is not None and pagination.total_count < 0:
        errors.append("Total count cannot be negative")

    return len(errors) == 0, errors


def is_valid_column_id(column_id: str) -> bool:
    """Validate column ID format.

    Args:
        column_id: Column ID to validate

    Returns:
        True if valid, False otherwise
    """
    return bool(column_id and isinstance(column_id, str) and column_id.strip())


def is_valid_column_value(value: Any, column_type: ColumnType) -> bool:
    """Validate column value against type.

    Args:
        value: Value to validate
        column_type: Expected column type

    Returns:
        True if valid, False otherwise
    """
    if value is None:
        return True

    try:
        if column_type == ColumnType.TEXT:
            return isinstance(value, str)
        elif column_type == ColumnType.NUMBER:
            return isinstance(value, (int, float))
        elif column_type == ColumnType.BOOLEAN:
            return isinstance(value, bool)
        elif column_type == ColumnType.EMAIL:
            return isinstance(value, str) and is_valid_email(value)
        elif column_type == ColumnType.URL:
            return isinstance(value, str) and is_valid_url(value)
        elif column_type == ColumnType.PHONE:
            return isinstance(value, str) and is_valid_phone(value)
        elif column_type == ColumnType.DATE or column_type == ColumnType.DATETIME:
            return isinstance(value, str) and is_valid_date_format(value)
        elif column_type == ColumnType.CURRENCY or column_type == ColumnType.PERCENTAGE:
            return isinstance(value, (int, float))
        elif column_type == ColumnType.JSON:
            return True  # JSON can be any serializable type
        else:
            return True
    except Exception:
        return False


def is_valid_filter_operator(operator: FilterOperator) -> bool:
    """Validate filter operator.

    Args:
        operator: Filter operator to validate

    Returns:
        True if valid, False otherwise
    """
    return operator in FilterOperator


def is_valid_date_format(date_string: str) -> bool:
    """Validate date format.

    Args:
        date_string: Date string to validate

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(date_string, str):
        return False

    # Common date formats
    date_patterns = [
        r"^\d{4}-\d{2}-\d{2}$",  # YYYY-MM-DD
        r"^\d{2}/\d{2}/\d{4}$",  # MM/DD/YYYY
        r"^\d{2}-\d{2}-\d{4}$",  # MM-DD-YYYY
        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$",  # ISO datetime
        r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",  # SQL datetime
    ]

    return any(re.match(pattern, date_string) for pattern in date_patterns)


def is_valid_email(email: str) -> bool:
    """Validate email format.

    Args:
        email: Email to validate

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(email, str):
        return False

    # Basic email regex pattern
    email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return re.match(email_pattern, email) is not None


def is_valid_url(url: str) -> bool:
    """Validate URL format.

    Args:
        url: URL to validate

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(url, str):
        return False

    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False


def is_valid_phone(phone: str) -> bool:
    """Validate phone number format.

    Args:
        phone: Phone number to validate

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(phone, str):
        return False

    # Remove common phone formatting characters
    clean_phone = re.sub(r"[^\d+]", "", phone)

    # Basic phone validation (10-15 digits, optional + prefix)
    phone_pattern = r"^\+?[1-9]\d{9,14}$"
    return re.match(phone_pattern, clean_phone) is not None


def validate_aggregation_compatibility(column_type: ColumnType, functions: List[str]) -> bool:
    """Validate that aggregation functions are compatible with column type.

    Args:
        column_type: Column type
        functions: List of aggregation functions

    Returns:
        True if compatible, False otherwise
    """
    numeric_functions = {"sum", "avg", "min", "max"}
    all_functions = {"sum", "avg", "min", "max", "count", "distinct_count"}

    # Check if all functions are valid
    if not all(func in all_functions for func in functions):
        return False

    # Numeric aggregations only work with numeric types
    if any(func in numeric_functions for func in functions):
        return column_type in [ColumnType.NUMBER, ColumnType.CURRENCY, ColumnType.PERCENTAGE]

    # count and distinct_count work with all types
    return True


def validate_formula_syntax(formula: str) -> Tuple[bool, List[str]]:
    """Validate Excel-like formula syntax.

    Args:
        formula: Formula to validate

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    if not formula or not formula.strip():
        errors.append("Formula cannot be empty")
        return False, errors

    formula = formula.strip()

    # Check if formula starts with =
    if not formula.startswith("="):
        errors.append("Formula must start with '='")

    # Check for balanced parentheses
    if not _check_balanced_parentheses(formula):
        errors.append("Formula has unbalanced parentheses")

    # Check for invalid functions
    invalid_functions = _check_invalid_functions(formula)
    if invalid_functions:
        errors.append(f"Formula contains invalid functions: {', '.join(invalid_functions)}")

    # Check for security issues
    security_issues = _check_formula_security(formula)
    if security_issues:
        errors.extend(security_issues)

    return len(errors) == 0, errors


def detect_circular_references(formulas: Dict[str, str]) -> List[List[str]]:
    """Detect circular references in formulas.

    Args:
        formulas: Dictionary of column_id -> formula

    Returns:
        List of circular reference chains
    """
    circular_refs = []

    def find_references(formula: str) -> Set[str]:
        """Find column references in formula."""
        references = set()
        # Find cell references like A1, B2, etc.
        cell_refs = re.findall(r"[A-Z]+\d+", formula)
        references.update(cell_refs)

        # Find column references like [column_name]
        column_refs = re.findall(r"\[([^\]]+)\]", formula)
        references.update(column_refs)

        return references

    # Build dependency graph
    dependencies = {}
    for col_id, formula in formulas.items():
        dependencies[col_id] = find_references(formula)

    # Detect cycles using DFS
    visited = set()
    rec_stack = set()

    def has_cycle(node: str, path: List[str]) -> bool:
        if node in rec_stack:
            # Found cycle, extract the cycle
            cycle_start = path.index(node)
            cycle = path[cycle_start:] + [node]
            circular_refs.append(cycle)
            return True

        if node in visited:
            return False

        visited.add(node)
        rec_stack.add(node)

        for neighbor in dependencies.get(node, []):
            if neighbor in dependencies:  # Only check if neighbor is also a formula
                if has_cycle(neighbor, path + [node]):
                    return True

        rec_stack.remove(node)
        return False

    for col_id in dependencies:
        if col_id not in visited:
            has_cycle(col_id, [])

    return circular_refs


def validate_value_translations(translations: Dict[str, str]) -> Tuple[bool, List[str]]:
    """Validate value translation mappings.

    Args:
        translations: Dictionary of value translations

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    if not translations:
        return True, []

    # Check for empty keys
    if any(not key or not str(key).strip() for key in translations.keys()):
        errors.append("Translation keys cannot be empty")

    # Check for None values
    if any(value is None for value in translations.values()):
        errors.append("Translation values cannot be None")

    # Check for non-string values
    if any(not isinstance(value, str) for value in translations.values()):
        errors.append("Translation values must be strings")

    # Check for excessively long keys or values
    for key, value in translations.items():
        if len(str(key)) > 200:
            errors.append(f"Translation key '{key}' is too long (max 200 characters)")
        if len(str(value)) > 200:
            errors.append(f"Translation value '{value}' is too long (max 200 characters)")

    # Check for circular translations
    if _has_circular_translations(translations):
        errors.append("Circular translations detected")

    return len(errors) == 0, errors


def validate_default_uniqueness(
    saved_searches: List[UserSavedSearch],
    filter_sets: List[UserFilterSet],
    layouts: List[UserSavedLayout],
    user_id: str,
    table_id: str,
) -> Tuple[bool, List[str]]:
    """Validate that only one item of each type is marked as default.

    Args:
        saved_searches: List of saved searches
        filter_sets: List of filter sets
        layouts: List of saved layouts
        user_id: User ID
        table_id: Table ID

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    # Check saved searches
    user_searches = [s for s in saved_searches if s.user_id == user_id and s.table_id == table_id]
    default_searches = [s for s in user_searches if s.is_default]
    if len(default_searches) > 1:
        errors.append("Only one saved search can be marked as default per user/table")

    # Check filter sets
    user_filter_sets = [f for f in filter_sets if f.user_id == user_id and f.table_id == table_id]
    default_filter_sets = [f for f in user_filter_sets if f.is_default]
    if len(default_filter_sets) > 1:
        errors.append("Only one filter set can be marked as default per user/table")

    # Check layouts
    user_layouts = [layout for layout in layouts if layout.user_id == user_id and layout.table_id == table_id]
    default_layouts = [layout for layout in user_layouts if layout.is_default]
    if len(default_layouts) > 1:
        errors.append("Only one layout can be marked as default per user/table")

    return len(errors) == 0, errors


def validate_saved_layout_completeness(layout: UserSavedLayout) -> Tuple[bool, List[str]]:
    """Validate that saved layout has all required configuration components.

    Args:
        layout: Saved layout to validate

    Returns:
        Tuple of (is_valid, list_of_errors)
    """
    errors = []

    required_configs = [
        "column_configuration",
        "filter_configuration",
        "search_configuration",
        "sort_configuration",
        "pagination_configuration",
        "customization_configuration",
    ]

    for config_name in required_configs:
        config_value = getattr(layout, config_name, None)
        if config_value is None:
            errors.append(f"Missing required configuration: {config_name}")
        elif not isinstance(config_value, dict):
            errors.append(f"Configuration {config_name} must be a dictionary")

    # Validate specific configuration formats
    if hasattr(layout, "column_configuration") and isinstance(layout.column_configuration, dict):
        if "column_order" in layout.column_configuration:
            if not isinstance(layout.column_configuration["column_order"], list):
                errors.append("Column order must be a list")

    if hasattr(layout, "filter_configuration") and isinstance(layout.filter_configuration, dict):
        if "active_filters" in layout.filter_configuration:
            if not isinstance(layout.filter_configuration["active_filters"], list):
                errors.append("Active filters must be a list")

    if hasattr(layout, "search_configuration") and isinstance(layout.search_configuration, dict):
        if "search_term" in layout.search_configuration:
            if not isinstance(layout.search_configuration["search_term"], str):
                errors.append("Search term must be a string")

    if hasattr(layout, "sort_configuration") and isinstance(layout.sort_configuration, dict):
        if "sort_definitions" in layout.sort_configuration:
            if not isinstance(layout.sort_configuration["sort_definitions"], list):
                errors.append("Sort definitions must be a list")

    if hasattr(layout, "pagination_configuration") and isinstance(layout.pagination_configuration, dict):
        if "page_size" in layout.pagination_configuration:
            page_size = layout.pagination_configuration["page_size"]
            if not isinstance(page_size, int) or page_size < 1 or page_size > 1000:
                errors.append("Page size must be an integer between 1 and 1000")

    return len(errors) == 0, errors


# Helper functions


def _check_balanced_parentheses(formula: str) -> bool:
    """Check if parentheses are balanced in formula."""
    count = 0
    for char in formula:
        if char == "(":
            count += 1
        elif char == ")":
            count -= 1
            if count < 0:
                return False
    return count == 0


def _check_invalid_functions(formula: str) -> List[str]:
    """Check for invalid functions in formula."""
    # Extract function names from formula
    function_pattern = r"([A-Z_]+)\("
    functions = re.findall(function_pattern, formula)

    # List of valid Excel-like functions
    valid_functions = {
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "COUNT",
        "COUNTA",
        "COUNTIF",
        "IF",
        "AND",
        "OR",
        "NOT",
        "ROUND",
        "ROUNDUP",
        "ROUNDDOWN",
        "UPPER",
        "LOWER",
        "LEFT",
        "RIGHT",
        "MID",
        "LEN",
        "TRIM",
        "CONCATENATE",
        "VLOOKUP",
        "HLOOKUP",
        "INDEX",
        "MATCH",
        "TODAY",
        "NOW",
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "MINUTE",
        "ABS",
        "SQRT",
        "POWER",
        "MOD",
        "RAND",
        "RANDBETWEEN",
    }

    invalid_functions = [func for func in functions if func not in valid_functions]
    return invalid_functions


def _check_formula_security(formula: str) -> List[str]:
    """Check for security issues in formula."""
    errors = []

    # Check for dangerous patterns
    dangerous_patterns = [
        r"eval\s*\(",
        r"exec\s*\(",
        r"import\s+",
        r"__[a-zA-Z_]+__",
        r"system\s*\(",
        r"os\.",
        r"subprocess\.",
        r"file\s*\(",
        r"open\s*\(",
    ]

    for pattern in dangerous_patterns:
        if re.search(pattern, formula, re.IGNORECASE):
            errors.append(f"Formula contains potentially dangerous pattern: {pattern}")

    return errors


def _extract_cell_references(formula: str) -> Set[str]:
    """Extract cell references from formula."""
    # Find cell references like A1, B2, etc.
    cell_refs = re.findall(r"[A-Z]+\d+", formula)
    return set(cell_refs)


def _extract_column_references(formula: str) -> Set[str]:
    """Extract column references from formula."""
    # Find column references like [column_name]
    column_refs = re.findall(r"\[([^\]]+)\]", formula)
    return set(column_refs)


def _has_circular_translations(translations: Dict[str, str]) -> bool:
    """Check if translations contain circular references."""
    # Build a graph of translations
    graph = {}
    for key, value in translations.items():
        if value in translations:
            graph[key] = value

    # Check for cycles using DFS
    visited = set()
    rec_stack = set()

    def has_cycle(node: str) -> bool:
        if node in rec_stack:
            return True
        if node in visited:
            return False

        visited.add(node)
        rec_stack.add(node)

        if node in graph:
            if has_cycle(graph[node]):
                return True

        rec_stack.remove(node)
        return False

    for key in graph:
        if key not in visited:
            if has_cycle(key):
                return True

    return False
