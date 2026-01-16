"""Customization service functions for Table Module V2.

This module provides pure functions for column customization operations.
All dependencies are explicit parameters for clean library design.
"""

import time
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

from declaro_advise import error, info, success
from declaro_tablix.domain.models import (
    ChildColumnDefinition,
    ColumnAlias,
    ColumnDefinition,
    TableData,
    UserColumnRename,
    UserDefaultPreferences,
)
from declaro_tablix.domain.protocols import (
    ColumnCustomizationRepository,
    FormulaEvaluationService,
    TableCustomizationCache,
    create_noop_customization_cache,
    create_noop_customization_repository,
    create_noop_formula_service,
)
from declaro_tablix.domain.validators import (
    validate_formula_syntax,
    validate_value_translations,
)


async def apply_column_customizations(
    table_data: TableData,
    user_id: str,
    table_name: str,
    customization_repo: ColumnCustomizationRepository | None = None,
    formula_service: FormulaEvaluationService | None = None,
    cache_service: TableCustomizationCache | None = None,
) -> Tuple[TableData, Dict[str, Any]]:
    """Apply column customizations to table data.

    Args:
        table_data: Original table data
        user_id: User ID for user-specific customizations
        table_name: Name of the table
        customization_repo: Repository for customization data (required)
        formula_service: Optional formula evaluation service
        cache_service: Optional cache service for performance

    Returns:
        Tuple of (customized_table_data, customization_metadata)
    """
    if customization_repo is None:
        customization_repo = create_noop_customization_repository()
    if formula_service is None:
        formula_service = create_noop_formula_service()
    if cache_service is None:
        cache_service = create_noop_customization_cache()

    start_time = time.time()

    try:
        info(f"Applying customizations for table: {table_name}")

        # Get customization context
        context = await create_customization_context(
            user_id=user_id,
            table_name=table_name,
            customization_repo=customization_repo,
            cache_service=cache_service,
        )

        # Apply system aliases
        customized_data = await _apply_system_aliases(table_data, context["system_aliases"])

        # Apply user column renames
        customized_data = await _apply_user_renames(customized_data, context["user_renames"])

        # Apply calculated columns
        customized_data = await _apply_calculated_columns(
            customized_data, context["child_columns"], formula_service, user_id, table_name
        )

        # Apply value translations
        customized_data = await _apply_value_translations(customized_data, context["value_translations"])

        # Validate performance
        processing_time = time.time() - start_time
        performance_result = validate_customization_performance(processing_time, len(table_data.rows))

        metadata = {
            "customizations_applied": {
                "system_aliases": len(context["system_aliases"]),
                "user_renames": len(context["user_renames"]),
                "calculated_columns": len(context["child_columns"]),
                "value_translations": sum(len(vt) for vt in context["value_translations"].values()),
            },
            "performance": performance_result,
            "processing_time_ms": round(processing_time * 1000, 2),
        }

        success(f"Customizations applied successfully in {metadata['processing_time_ms']}ms")
        return customized_data, metadata

    except Exception as e:
        error(f"Failed to apply customizations: {str(e)}")
        return table_data, {"error": str(e), "customizations_applied": {}}


async def cache_calculated_values(
    column_id: str,
    formula: str,
    table_data: TableData,
    user_id: str,
    table_name: str,
    formula_service: FormulaEvaluationService | None = None,
    cache_service: TableCustomizationCache | None = None,
) -> Optional[List[Any]]:
    """Cache calculated values for a formula column with LRU eviction.

    Args:
        column_id: ID of the calculated column
        formula: Formula to evaluate
        table_data: Table data for formula context
        user_id: User ID for cache key
        table_name: Table name for cache key
        formula_service: Optional formula evaluation service
        cache_service: Optional cache service for storage

    Returns:
        List of calculated values or None if error
    """
    if formula_service is None:
        formula_service = create_noop_formula_service()
    if cache_service is None:
        cache_service = create_noop_customization_cache()

    try:
        # Generate cache key
        cache_key = f"calc_values:{table_name}:{user_id}:{column_id}:{hash(formula)}"

        # Check cache first
        cached_values = await cache_service.get_cached_customization_data(cache_key, user_id)
        if cached_values:
            info(f"Using cached calculated values for column: {column_id}")
            return cached_values

        # Calculate values
        calculated_values = []
        for row in table_data.rows:
            try:
                value = await formula_service.evaluate_formula(formula, row, table_data.metadata)
                calculated_values.append(value)
            except Exception as e:
                error(f"Formula evaluation failed for row: {str(e)}")
                calculated_values.append(None)

        # Cache with TTL (1 hour)
        await cache_service.cache_customization_data(cache_key, calculated_values, ttl_seconds=3600, user_id=user_id)

        info(f"Calculated and cached {len(calculated_values)} values for column: {column_id}")
        return calculated_values

    except Exception as e:
        error(f"Failed to cache calculated values: {str(e)}")
        return None


async def persist_user_customizations(
    user_id: str,
    table_name: str,
    customizations: Dict[str, Any],
    customization_repo: ColumnCustomizationRepository | None = None,
) -> bool:
    """Persist user customizations to the repository.

    Args:
        user_id: User ID
        table_name: Table name
        customizations: Dictionary of customizations to persist
        customization_repo: Repository for persistence (required)

    Returns:
        True if successful, False otherwise
    """
    if customization_repo is None:
        customization_repo = create_noop_customization_repository()
    try:
        info(f"Persisting customizations for user: {user_id}, table: {table_name}")

        # Persist column renames
        if "column_renames" in customizations:
            for rename_data in customizations["column_renames"]:
                rename = UserColumnRename(**rename_data)
                await customization_repo.save_user_column_rename(rename)

        # Persist calculated columns
        if "calculated_columns" in customizations:
            for calc_data in customizations["calculated_columns"]:
                # Validate formula before persisting
                is_valid, errors = validate_formula_syntax(calc_data["formula"])
                if not is_valid:
                    error(f"Invalid formula for calculated column: {', '.join(errors)}")
                    continue

                calc_column = ChildColumnDefinition(**calc_data)
                await customization_repo.save_child_column_definition(calc_column)

        # Persist value translations
        if "value_translations" in customizations:
            for column_id, translations in customizations["value_translations"].items():
                # Validate translations
                is_valid, errors = validate_value_translations(translations)
                if not is_valid:
                    error(f"Invalid value translations for column {column_id}: {', '.join(errors)}")
                    continue

                await customization_repo.save_value_translations(user_id, table_name, column_id, translations)

        success(f"Customizations persisted successfully")
        return True

    except Exception as e:
        error(f"Failed to persist customizations: {str(e)}")
        return False


async def create_customization_context(
    user_id: str,
    table_name: str,
    customization_repo: ColumnCustomizationRepository | None = None,
    cache_service: TableCustomizationCache | None = None,
) -> Dict[str, Any]:
    """Create customization context with all user and system customizations.

    Args:
        user_id: User ID
        table_name: Table name
        customization_repo: Repository for customization data (required)
        cache_service: Optional cache service for performance

    Returns:
        Dictionary containing all customization data
    """
    if customization_repo is None:
        customization_repo = create_noop_customization_repository()
    if cache_service is None:
        cache_service = create_noop_customization_cache()

    try:
        # Check cache first
        cache_key = f"customization_context:{table_name}:{user_id}"
        cached_context = await cache_service.get_cached_customization_data(cache_key, user_id)
        if cached_context:
            return cached_context

        # Build context from repository
        context = {
            "system_aliases": await customization_repo.get_column_aliases(table_name),
            "user_renames": await customization_repo.get_user_column_renames(user_id, table_name),
            "child_columns": await customization_repo.get_child_column_definitions(user_id, table_name),
            "value_translations": await customization_repo.get_value_translations(user_id, table_name),
        }

        # Cache for 30 minutes
        await cache_service.cache_customization_data(cache_key, context, ttl_seconds=1800, user_id=user_id)

        return context

    except Exception as e:
        error(f"Failed to create customization context: {str(e)}")
        return {
            "system_aliases": [],
            "user_renames": [],
            "child_columns": [],
            "value_translations": {},
        }


def validate_customization_performance(processing_time: float, row_count: int) -> Dict[str, Any]:
    """Validate customization performance against benchmarks.

    Args:
        processing_time: Time taken to process customizations (seconds)
        row_count: Number of rows processed

    Returns:
        Dictionary with performance metrics and validation results
    """
    # Performance targets
    max_time_per_1000_rows = 0.010  # 10ms per 1000 rows
    target_time = (row_count / 1000) * max_time_per_1000_rows

    performance_result = {
        "processing_time_seconds": processing_time,
        "processing_time_ms": round(processing_time * 1000, 2),
        "row_count": row_count,
        "target_time_ms": round(target_time * 1000, 2),
        "performance_ratio": round(processing_time / target_time if target_time > 0 else 0, 2),
        "meets_target": processing_time <= target_time,
        "rows_per_second": round(row_count / processing_time if processing_time > 0 else 0),
    }

    if performance_result["meets_target"]:
        info(f"Customization performance target met: {performance_result['processing_time_ms']}ms for {row_count} rows")
    else:
        error(
            f"Customization performance target missed: {performance_result['processing_time_ms']}ms (target: {performance_result['target_time_ms']}ms)"
        )

    return performance_result


# Helper functions


async def _apply_system_aliases(table_data: TableData, aliases: List[ColumnAlias]) -> TableData:
    """Apply system column aliases to table data."""
    if not aliases:
        return table_data

    # Create alias mapping
    alias_map = {alias.column_id: alias.alias for alias in aliases}

    # Apply to column definitions in metadata
    updated_metadata = table_data.metadata.copy()
    if "columns" in updated_metadata:
        for column in updated_metadata["columns"]:
            if column.get("id") in alias_map:
                column["system_alias"] = alias_map[column["id"]]

    return TableData(
        rows=table_data.rows,
        total_count=table_data.total_count,
        metadata=updated_metadata,
    )


async def _apply_user_renames(table_data: TableData, renames: List[UserColumnRename]) -> TableData:
    """Apply user column renames to table data."""
    if not renames:
        return table_data

    # Create rename mapping
    rename_map = {rename.column_id: rename.custom_name for rename in renames}

    # Apply to column definitions in metadata
    updated_metadata = table_data.metadata.copy()
    if "columns" in updated_metadata:
        for column in updated_metadata["columns"]:
            if column.get("id") in rename_map:
                column["user_rename"] = rename_map[column["id"]]

    return TableData(
        rows=table_data.rows,
        total_count=table_data.total_count,
        metadata=updated_metadata,
    )


async def _apply_calculated_columns(
    table_data: TableData,
    child_columns: List[ChildColumnDefinition],
    formula_service: FormulaEvaluationService,
    user_id: str,
    table_name: str,
) -> TableData:
    """Apply calculated columns to table data."""
    if not child_columns:
        return table_data

    updated_rows = []
    for row in table_data.rows:
        updated_row = row.copy()

        # Add calculated column values
        for child_column in child_columns:
            try:
                calculated_value = await formula_service.evaluate_formula(child_column.formula, row, table_data.metadata)
                updated_row[child_column.column_name] = calculated_value
            except Exception as e:
                error(f"Failed to calculate column {child_column.column_name}: {str(e)}")
                updated_row[child_column.column_name] = None

        updated_rows.append(updated_row)

    # Add child columns to metadata
    updated_metadata = table_data.metadata.copy()
    if "columns" not in updated_metadata:
        updated_metadata["columns"] = []

    for child_column in child_columns:
        column_def = {
            "id": child_column.column_name,
            "name": child_column.column_name,
            "type": child_column.data_type,
            "is_calculated": True,
            "formula": child_column.formula,
        }
        updated_metadata["columns"].append(column_def)

    return TableData(
        rows=updated_rows,
        total_count=table_data.total_count,
        metadata=updated_metadata,
    )


async def _apply_value_translations(table_data: TableData, translations: Dict[str, Dict[str, str]]) -> TableData:
    """Apply value translations to table data."""
    if not translations:
        return table_data

    updated_rows = []
    for row in table_data.rows:
        updated_row = row.copy()

        # Apply translations for each column
        for column_id, translation_map in translations.items():
            if column_id in updated_row:
                original_value = str(updated_row[column_id]) if updated_row[column_id] is not None else ""
                if original_value in translation_map:
                    updated_row[column_id] = translation_map[original_value]

        updated_rows.append(updated_row)

    # Add translation metadata
    updated_metadata = table_data.metadata.copy()
    updated_metadata["value_translations"] = translations

    return TableData(
        rows=updated_rows,
        total_count=table_data.total_count,
        metadata=updated_metadata,
    )
