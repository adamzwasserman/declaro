"""Table data repository implementation for Table Module V2.

This module provides function-based database operations for table data retrieval,
following the repository pattern with FastAPI dependency injection.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import and_, desc, func, or_, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from declaro_persistum.compat import get_db
from declaro_advise import error, info, success, warning
from declaro_tablix.domain.models import (
    ColumnDefinition,
    ColumnType,
    FilterDefinition,
    FilterOperator,
    PaginationSettings,
    SortDefinition,
    SortDirection,
    TableData,
)


def get_table_data(
    table_name: str,
    user_id: str,
    filters: Optional[List[FilterDefinition]] = None,
    sorts: Optional[List[SortDefinition]] = None,
    pagination: Optional[PaginationSettings] = None,
    search_term: Optional[str] = None,
    db_session: Session = None,
) -> TableData:
    """Get table data with filtering, sorting, and pagination.

    Args:
        table_name: Name of the table to query
        user_id: User ID for context and logging
        filters: List of filter conditions to apply
        sorts: List of sort conditions to apply
        pagination: Pagination settings
        search_term: Global search term
        db_session: Database session (injected)

    Returns:
        TableData object with results and metadata
    """
    try:
        info(f"Fetching table data for table '{table_name}' for user '{user_id}'")

        if not db_session:
            db_session = next(get_db())

        # Validate table exists
        if not table_exists(table_name, db_session):
            error(f"Table '{table_name}' does not exist")
            return TableData(rows=[], total_count=0)

        # Note: In proper architecture, columns are retrieved separately via get_table_schema()
        # This function should only handle data retrieval, not schema operations

        # Build base query
        query = text(f"SELECT * FROM {_sanitize_table_name(table_name)}")
        conditions = []
        parameters = {}

        # Apply search term across all text columns
        if search_term:
            # Get minimal column info for search without full schema call
            text_columns = _get_text_columns_for_search(table_name, db_session)
            search_conditions = _build_search_conditions_simple(text_columns, search_term, parameters)
            if search_conditions:
                conditions.append(search_conditions)

        # Apply filters
        if filters:
            filter_conditions = _build_filter_conditions(filters, parameters)
            if filter_conditions:
                conditions.extend(filter_conditions)

        # Build WHERE clause
        where_clause = ""
        if conditions:
            where_clause = f" WHERE {' AND '.join(conditions)}"

        # Apply sorting
        order_clause = ""
        if sorts:
            order_parts = []
            for sort_def in sorts:
                column_name = _sanitize_column_name(sort_def.column_id)
                direction = "DESC" if sort_def.direction == SortDirection.DESC else "ASC"
                order_parts.append(f"{column_name} {direction}")
            if order_parts:
                order_clause = f" ORDER BY {', '.join(order_parts)}"

        # Get total count for pagination
        count_query = text(f"SELECT COUNT(*) as total FROM {_sanitize_table_name(table_name)}{where_clause}")
        count_result = db_session.execute(count_query, parameters).fetchone()
        total_count = count_result.total if count_result else 0

        # Apply pagination
        limit_clause = ""
        if pagination:
            offset = (pagination.page - 1) * pagination.page_size
            limit_clause = f" LIMIT {pagination.page_size} OFFSET {offset}"

        # Execute final query
        final_query = text(f"{query}{where_clause}{order_clause}{limit_clause}")
        result = db_session.execute(final_query, parameters)

        # Convert results to list of dictionaries
        rows = [dict(row._mapping) for row in result]

        success(f"Retrieved {len(rows)} rows from table '{table_name}' (total: {total_count})")

        return TableData(
            rows=rows,
            total_count=total_count,
            metadata={
                "user_id": user_id,
                "table_name": table_name,
                "filters_applied": len(filters) if filters else 0,
                "sorts_applied": len(sorts) if sorts else 0,
                "search_term": search_term,
                "page": pagination.page if pagination else 1,
                "page_size": pagination.page_size if pagination else len(rows),
            },
        )

    except SQLAlchemyError as e:
        error(f"Database error retrieving table data: {str(e)}")
        return TableData(rows=[], total_count=0)
    except Exception as e:
        error(f"Error retrieving table data: {str(e)}")
        return TableData(rows=[], total_count=0)


def get_table_schema(table_name: str, db_session: Session = None) -> List[ColumnDefinition]:
    """Get table schema information.

    Args:
        table_name: Name of the table
        db_session: Database session (injected)

    Returns:
        List of ColumnDefinition objects
    """
    try:
        if not db_session:
            db_session = next(get_db())

        # Query information schema for column details
        schema_query = text(
            """
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns
            WHERE table_name = :table_name
            ORDER BY ordinal_position
        """
        )

        result = db_session.execute(schema_query, {"table_name": table_name})
        columns = []

        for row in result:
            # Map database types to our ColumnType enum
            column_type = _map_database_type_to_column_type(row.data_type)

            column_def = ColumnDefinition(
                id=row.column_name,
                name=_format_display_name(row.column_name),
                type=column_type,
                required=row.is_nullable.lower() == "no",
                sortable=True,
                filterable=True,
                system_alias=None,  # No system alias by default
            )
            columns.append(column_def)

        info(f"Retrieved schema for table '{table_name}': {len(columns)} columns")
        return columns

    except SQLAlchemyError as e:
        error(f"Database error retrieving table schema: {str(e)}")
        return []
    except Exception as e:
        error(f"Error retrieving table schema: {str(e)}")
        return []


def table_exists(table_name: str, db_session: Session = None) -> bool:
    """Check if table exists in database.

    Args:
        table_name: Name of the table to check
        db_session: Database session (injected)

    Returns:
        True if table exists, False otherwise
    """
    try:
        if not db_session:
            db_session = next(get_db())

        check_query = text(
            """
            SELECT COUNT(*) as count
            FROM information_schema.tables
            WHERE table_name = :table_name
        """
        )

        result = db_session.execute(check_query, {"table_name": table_name}).fetchone()
        exists = result.count > 0 if result else False

        if exists:
            info(f"Table '{table_name}' exists")
        else:
            warning(f"Table '{table_name}' does not exist")

        return exists

    except SQLAlchemyError as e:
        error(f"Database error checking table existence: {str(e)}")
        return False
    except Exception as e:
        error(f"Error checking table existence: {str(e)}")
        return False


def get_table_stats(table_name: str, db_session: Session = None) -> Dict[str, Any]:
    """Get table statistics (row count, size, etc.).

    Args:
        table_name: Name of the table
        db_session: Database session (injected)

    Returns:
        Dictionary with table statistics
    """
    try:
        if not db_session:
            db_session = next(get_db())

        # Get row count
        count_query = text(f"SELECT COUNT(*) as row_count FROM {_sanitize_table_name(table_name)}")
        count_result = db_session.execute(count_query).fetchone()
        row_count = count_result.row_count if count_result else 0

        # Get table size information (PostgreSQL specific)
        size_query = text(
            """
            SELECT
                pg_size_pretty(pg_total_relation_size(:table_name)) as total_size,
                pg_size_pretty(pg_relation_size(:table_name)) as table_size,
                pg_size_pretty(pg_total_relation_size(:table_name) - pg_relation_size(:table_name)) as index_size
        """
        )

        try:
            size_result = db_session.execute(size_query, {"table_name": table_name}).fetchone()
            total_size = size_result.total_size if size_result else "Unknown"
            table_size = size_result.table_size if size_result else "Unknown"
            index_size = size_result.index_size if size_result else "Unknown"
        except SQLAlchemyError:
            # Fallback for non-PostgreSQL databases
            total_size = table_size = index_size = "Unknown"

        stats = {
            "row_count": row_count,
            "total_size": total_size,
            "table_size": table_size,
            "index_size": index_size,
            "last_updated": "Unknown",  # Could be enhanced with actual timestamp
        }

        info(f"Retrieved stats for table '{table_name}': {row_count} rows")
        return stats

    except SQLAlchemyError as e:
        error(f"Database error retrieving table stats: {str(e)}")
        return {"error": str(e)}
    except Exception as e:
        error(f"Error retrieving table stats: {str(e)}")
        return {"error": str(e)}


def execute_custom_query(
    query: str, parameters: Optional[Dict[str, Any]] = None, db_session: Session = None
) -> List[Dict[str, Any]]:
    """Execute custom SQL query safely.

    Args:
        query: SQL query to execute
        parameters: Query parameters
        db_session: Database session (injected)

    Returns:
        List of result dictionaries
    """
    try:
        if not db_session:
            db_session = next(get_db())

        # Basic security: only allow SELECT statements
        if not _is_safe_query(query):
            error("Only SELECT statements are allowed in custom queries")
            return []

        result = db_session.execute(text(query), parameters or {})
        rows = [dict(row._mapping) for row in result]

        success(f"Executed custom query successfully: {len(rows)} rows returned")
        return rows

    except SQLAlchemyError as e:
        error(f"Database error executing custom query: {str(e)}")
        return []
    except Exception as e:
        error(f"Error executing custom query: {str(e)}")
        return []


def _sanitize_table_name(table_name: str) -> str:
    """Sanitize table name to prevent SQL injection."""
    # Only allow alphanumeric characters and underscores
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    return table_name


def _sanitize_column_name(column_name: str) -> str:
    """Sanitize column name to prevent SQL injection."""
    # Only allow alphanumeric characters and underscores
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", column_name):
        raise ValueError(f"Invalid column name: {column_name}")
    return column_name


def _build_search_conditions(columns: List[ColumnDefinition], search_term: str, parameters: Dict[str, Any]) -> Optional[str]:
    """Build search conditions for global search."""
    if not search_term or not columns:
        return None

    text_columns = [col for col in columns if col.type in [ColumnType.TEXT, ColumnType.EMAIL, ColumnType.URL]]
    if not text_columns:
        return None

    search_param = f"search_term_{len(parameters)}"
    parameters[search_param] = f"%{search_term}%"

    conditions = []
    for column in text_columns:
        column_name = _sanitize_column_name(column.id)
        conditions.append(f"{column_name} ILIKE :{search_param}")

    return f"({' OR '.join(conditions)})"


def _build_filter_conditions(filters: List[FilterDefinition], parameters: Dict[str, Any]) -> List[str]:
    """Build filter conditions for WHERE clause."""
    conditions = []

    for i, filter_def in enumerate(filters):
        column_name = _sanitize_column_name(filter_def.column_id)
        param_name = f"filter_{i}"

        if filter_def.operator == FilterOperator.EQUALS:
            conditions.append(f"{column_name} = :{param_name}")
            parameters[param_name] = filter_def.value
        elif filter_def.operator == FilterOperator.NOT_EQUALS:
            conditions.append(f"{column_name} != :{param_name}")
            parameters[param_name] = filter_def.value
        elif filter_def.operator == FilterOperator.CONTAINS:
            conditions.append(f"{column_name} ILIKE :{param_name}")
            parameters[param_name] = f"%{filter_def.value}%"
        elif filter_def.operator == FilterOperator.STARTS_WITH:
            conditions.append(f"{column_name} ILIKE :{param_name}")
            parameters[param_name] = f"{filter_def.value}%"
        elif filter_def.operator == FilterOperator.ENDS_WITH:
            conditions.append(f"{column_name} ILIKE :{param_name}")
            parameters[param_name] = f"%{filter_def.value}"
        elif filter_def.operator == FilterOperator.GREATER_THAN:
            conditions.append(f"{column_name} > :{param_name}")
            parameters[param_name] = filter_def.value
        elif filter_def.operator == FilterOperator.LESS_THAN:
            conditions.append(f"{column_name} < :{param_name}")
            parameters[param_name] = filter_def.value
        elif filter_def.operator == FilterOperator.GREATER_THAN_OR_EQUAL:
            conditions.append(f"{column_name} >= :{param_name}")
            parameters[param_name] = filter_def.value
        elif filter_def.operator == FilterOperator.LESS_THAN_OR_EQUAL:
            conditions.append(f"{column_name} <= :{param_name}")
            parameters[param_name] = filter_def.value
        elif filter_def.operator == FilterOperator.IS_NULL:
            conditions.append(f"{column_name} IS NULL")
        elif filter_def.operator == FilterOperator.IS_NOT_NULL:
            conditions.append(f"{column_name} IS NOT NULL")

    return conditions


def _map_database_type_to_column_type(db_type: str) -> ColumnType:
    """Map database data type to our ColumnType enum."""
    db_type = db_type.lower()

    if db_type in ["varchar", "text", "char", "character"]:
        return ColumnType.TEXT
    elif db_type in ["integer", "int", "bigint", "smallint"]:
        return ColumnType.NUMBER
    elif db_type in ["numeric", "decimal", "float", "double", "real"]:
        return ColumnType.NUMBER
    elif db_type in ["date"]:
        return ColumnType.DATE
    elif db_type in ["timestamp", "datetime"]:
        return ColumnType.DATETIME
    elif db_type in ["boolean", "bool"]:
        return ColumnType.BOOLEAN
    elif "money" in db_type:
        return ColumnType.CURRENCY
    else:
        return ColumnType.TEXT  # Default to text


def _format_display_name(column_name: str) -> str:
    """Format column name for display."""
    # Convert snake_case to Title Case
    return column_name.replace("_", " ").title()


def _calculate_default_width(column_type: ColumnType) -> int:
    """Calculate default width percentage for column type."""
    width_map = {
        ColumnType.TEXT: 25,
        ColumnType.NUMBER: 15,
        ColumnType.CURRENCY: 15,
        ColumnType.PERCENTAGE: 12,
        ColumnType.DATE: 12,
        ColumnType.DATETIME: 18,
        ColumnType.BOOLEAN: 10,
        ColumnType.EMAIL: 25,
        ColumnType.URL: 25,
        ColumnType.PHONE: 15,
        ColumnType.JSON: 30,
    }
    return width_map.get(column_type, 20)


def _is_safe_query(query: str) -> bool:
    """Check if query is safe (SELECT only)."""
    query_upper = query.strip().upper()

    # Only allow SELECT statements
    if not query_upper.startswith("SELECT"):
        return False

    # Block dangerous keywords
    dangerous_keywords = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "EXEC",
        "EXECUTE",
        "CALL",
        "DECLARE",
    ]

    for keyword in dangerous_keywords:
        if keyword in query_upper:
            return False

    return True


# FastAPI Dependency Injection Functions
def get_table_data_for_dependency() -> callable:
    """FastAPI dependency for table data operations."""
    return get_table_data


def get_table_schema_for_dependency() -> callable:
    """FastAPI dependency for table schema operations."""
    return get_table_schema


def get_table_stats_for_dependency() -> callable:
    """FastAPI dependency for table statistics operations."""
    return get_table_stats


def get_db_session_for_dependency() -> Session:
    """FastAPI dependency for database session."""
    return next(get_db())


def _get_text_columns_for_search(table_name: str, db_session: Session) -> List[str]:
    """Get minimal column names for search functionality without full schema call."""
    try:
        # Lightweight query to get column names and types for search
        search_query = text(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = :table_name
            AND data_type IN ('varchar', 'text', 'char', 'character')
            ORDER BY ordinal_position
        """
        )

        result = db_session.execute(search_query, {"table_name": table_name})
        text_columns = [row.column_name for row in result]

        return text_columns
    except Exception:
        # If column lookup fails, return empty list (search will be skipped)
        return []


def _build_search_conditions_simple(text_columns: List[str], search_term: str, parameters: Dict[str, Any]) -> Optional[str]:
    """Build search conditions using simple column names."""
    if not search_term or not text_columns:
        return None

    search_param = f"search_term_{len(parameters)}"
    parameters[search_param] = f"%{search_term}%"

    conditions = []
    for column_name in text_columns:
        sanitized_column = _sanitize_column_name(column_name)
        conditions.append(f"{sanitized_column} ILIKE :{search_param}")

    return f"({' OR '.join(conditions)})"
