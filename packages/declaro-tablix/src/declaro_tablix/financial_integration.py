"""
Financial data integration for TableV2.

This module integrates financial column types, formatters, and validators
into the existing TableV2 component architecture following function-based patterns.
"""

from typing import Any, Dict, List, Optional, Union

from declaro_tablix.domain.financial_columns import (
    FinancialColumnDefinition,
    FinancialColumnType,
    create_financial_column_definitions,
    create_market_category_column_sets,
    get_financial_table_configuration,
)
from declaro_tablix.domain.financial_validators import (
    ValidationResult,
    get_financial_validator,
    get_validation_summary,
    validate_bulk_financial_data,
    validate_financial_data,
)
from declaro_tablix.domain.models import ColumnType, TableConfig, TableData
from declaro_tablix.formatting.financial_formatters import (
    FINANCIAL_FORMATTERS,
    get_financial_formatter,
)
from declaro_tablix.formatting.formatting_strategy import FormattingContext, FormattingOptions


def register_financial_formatters_with_registry():
    """
    Register all financial formatters with the main formatter registry.

    This function integrates financial formatters into the existing
    TableV2 formatter registry system.
    """
    from .formatting.formatter_registry import register_formatter_plugin

    # Register each financial formatter
    for financial_type, formatter_func in FINANCIAL_FORMATTERS.items():
        # Create a wrapper that adapts the financial formatter to the standard interface
        def create_adapter(original_formatter, fin_type):
            def adapter(value: Any, options: FormattingOptions, context: FormattingContext) -> str:
                # Add financial type and options to context
                if hasattr(context, "__dict__"):
                    context.financial_type = fin_type
                    if hasattr(context, "column_definition"):
                        column_def = context.column_definition
                        if hasattr(column_def, "financial_options"):
                            context.financial_options = column_def.financial_options

                return original_formatter(value, options, context)

            return adapter

        # Register the adapted formatter
        adapter = create_adapter(formatter_func, financial_type)
        register_formatter_plugin(
            name=f"financial_{financial_type.value}",
            description=f"Financial formatter for {financial_type.value}",
            version="1.0.0",
            author="TableV2 Financial Extension",
            column_types=[financial_type.value],
            formatter_func=adapter,
            priority=10,
        )


def extend_column_type_enum():
    """
    Extend the base ColumnType enum with financial types.

    This dynamically adds financial column types to the main enum.
    """
    # Add financial types to the main ColumnType enum
    for financial_type in FinancialColumnType:
        if not hasattr(ColumnType, financial_type.name):
            setattr(ColumnType, financial_type.name, financial_type.value)


def create_financial_table_service_integration():
    """
    Create service functions for financial table operations.

    Returns:
        Dictionary of financial-specific service functions
    """

    async def get_financial_table_data(
        table_name: str,
        user_context: Dict[str, Any],
        filters: Optional[List[Dict[str, Any]]] = None,
        sorts: Optional[List[Dict[str, Any]]] = None,
        pagination: Optional[Dict[str, Any]] = None,
        search_term: Optional[str] = None,
        real_time: bool = False,
    ) -> TableData:
        """
        Get financial table data with specialized processing.

        Args:
            table_name: Name of the financial table
            user_context: User context for permissions
            filters: Column filters to apply
            sorts: Sort configurations
            pagination: Pagination settings
            search_term: Search term for filtering
            real_time: Whether to fetch real-time data

        Returns:
            TableData with financial data
        """
        from .controllers.financial_controller import get_market_data_for_table

        # Extract market category from table name
        category = table_name.replace("_stocks", "").replace("_market", "")

        # Build table config from parameters
        table_config = {
            "filters": filters or [],
            "sorts": sorts or [],
            "pagination": pagination or {"page": 1, "page_size": 25},
            "search_term": search_term,
            "real_time": real_time,
        }

        # Call the financial controller
        result = await get_market_data_for_table(
            category=category,
            user_context=user_context,
            cache_service=None,  # Will be injected by dependency system
            table_config=table_config,
        )

        if result["success"]:
            return result["data"]
        else:
            raise Exception(f"Failed to fetch financial data: {result.get('error', 'Unknown error')}")

    async def validate_financial_table_row(
        row_data: Dict[str, Any], table_config: TableConfig
    ) -> Dict[str, ValidationResult]:
        """
        Validate a single row of financial table data.

        Args:
            row_data: Row data to validate
            table_config: Table configuration with column definitions

        Returns:
            Validation results for each column
        """
        # Extract column definitions
        column_definitions = []
        for column in table_config.columns:
            if hasattr(column, "financial_type") and column.financial_type:
                column_definitions.append(
                    {
                        "id": column.id,
                        "financial_type": column.financial_type,
                        "validation_options": getattr(column, "validation_rules", {}),
                    }
                )

        return validate_financial_data(row_data, column_definitions)

    async def format_financial_table_data(
        table_data: TableData, table_config: TableConfig, formatting_strategy: str = "default"
    ) -> TableData:
        """
        Apply financial formatting to table data.

        Args:
            table_data: Raw table data
            table_config: Table configuration
            formatting_strategy: Formatting strategy to use

        Returns:
            Formatted table data
        """
        from .formatting.formatting_strategy import get_formatting_strategy

        # Get the formatting strategy
        strategy = get_formatting_strategy(formatting_strategy)

        # Process each row
        formatted_rows = []
        for row in table_data.rows:
            formatted_row = {}
            for column in table_config.columns:
                if column.id in row:
                    value = row[column.id]

                    # Check if this is a financial column
                    if hasattr(column, "financial_type") and column.financial_type:
                        formatter = get_financial_formatter(column.financial_type)
                        if formatter:
                            # Create formatting context
                            context = FormattingContext(
                                column_definition=column,
                                table_name=table_config.table_name,
                                user_context={},
                                financial_type=column.financial_type,
                                financial_options=getattr(column, "financial_options", None),
                            )

                            # Format the value
                            formatted_value = formatter(value, strategy.options, context)
                            formatted_row[column.id] = formatted_value
                        else:
                            formatted_row[column.id] = value
                    else:
                        formatted_row[column.id] = value

            formatted_rows.append(formatted_row)

        # Return new TableData with formatted rows
        return TableData(
            rows=formatted_rows, total_count=table_data.total_count, columns=table_data.columns, metadata=table_data.metadata
        )

    return {
        "get_financial_table_data": get_financial_table_data,
        "validate_financial_table_row": validate_financial_table_row,
        "format_financial_table_data": format_financial_table_data,
    }


def create_financial_table_configuration_factory():
    """
    Create a factory function for generating financial table configurations.

    Returns:
        Factory function for creating financial table configs
    """

    def create_financial_table_config(
        table_name: str,
        market_category: str,
        column_set: str = "extended",
        user_preferences: Optional[Dict[str, Any]] = None,
        real_time_enabled: bool = True,
    ) -> TableConfig:
        """
        Create a complete table configuration for financial data.

        Args:
            table_name: Name of the table
            market_category: Market category (tsx, nyse, nasdaq, etc.)
            column_set: Column set to use (basic, extended, comprehensive, mobile)
            user_preferences: User customization preferences
            real_time_enabled: Enable real-time updates

        Returns:
            Complete TableConfig object
        """
        # Get the base financial configuration
        config_dict = get_financial_table_configuration(
            table_name=table_name, column_set=column_set, user_customizations=user_preferences
        )

        # Convert to TableConfig object
        from .domain.models import TableConfig

        # Create column definitions
        columns = []
        for col_dict in config_dict["columns"]:
            if isinstance(col_dict, FinancialColumnDefinition):
                columns.append(col_dict)
            else:
                # Convert dict to FinancialColumnDefinition
                columns.append(FinancialColumnDefinition(**col_dict))

        # Build table config
        table_config = TableConfig(
            table_name=table_name,
            display_name=config_dict["display_name"],
            columns=columns,
            default_page_size=config_dict["default_settings"]["page_size"],
            default_sort_column=config_dict["default_settings"]["sort_column"],
            default_sort_direction=config_dict["default_settings"]["sort_direction"],
            features_enabled=config_dict["features"],
            metadata={
                "market_category": market_category,
                "column_set": column_set,
                "real_time_enabled": real_time_enabled,
                "endpoints": config_dict.get("real_time", {}).get("endpoints", {}),
                "mobile_settings": config_dict.get("mobile_settings", {}),
            },
        )

        return table_config

    return create_financial_table_config


def initialize_financial_integration():
    """
    Initialize the financial integration with TableV2.

    This function should be called during application startup to register
    all financial extensions with the TableV2 system.
    """
    # Extend the column type enum
    extend_column_type_enum()

    # Register financial formatters
    register_financial_formatters_with_registry()

    # Create service integration
    financial_services = create_financial_table_service_integration()

    # Create configuration factory
    config_factory = create_financial_table_configuration_factory()

    # Return integration components for use by the application
    return {
        "services": financial_services,
        "config_factory": config_factory,
        "column_definitions": create_financial_column_definitions(),
        "column_sets": create_market_category_column_sets(),
    }


def get_financial_table_plugin():
    """
    Create a financial table plugin for the TableV2 plugin system.

    Returns:
        Plugin configuration dictionary
    """
    return {
        "name": "financial_tables",
        "version": "1.0.0",
        "description": "Financial data support for TableV2",
        "author": "TableV2 Financial Extension",
        "dependencies": [],
        "hooks": {
            "before_table_render": "apply_financial_formatting",
            "before_data_validation": "validate_financial_data",
            "after_data_load": "process_financial_calculations",
        },
        "features": {
            "real_time_updates": True,
            "custom_formatters": True,
            "data_validation": True,
            "column_types": list(FinancialColumnType),
        },
        "configuration": {
            "default_update_interval": 30,
            "max_symbols_per_request": 100,
            "cache_ttl": 300,
            "supported_exchanges": ["NYSE", "NASDAQ", "TSX", "LSE"],
            "supported_currencies": ["USD", "CAD", "GBP", "EUR"],
        },
    }


def create_financial_market_templates():
    """
    Create predefined table templates for common financial markets.

    Returns:
        Dictionary of market templates
    """
    templates = {}

    # TSX Market Template
    templates["tsx_market"] = {
        "table_name": "tsx_market",
        "display_name": "TSX Market",
        "market_category": "tsx",
        "column_set": "extended",
        "default_sort": {"column": "market_cap", "direction": "desc"},
        "real_time": True,
        "features": {
            "export": True,
            "search": True,
            "filters": True,
            "customization": True,
        },
    }

    # NYSE Market Template
    templates["nyse_market"] = {
        "table_name": "nyse_market",
        "display_name": "NYSE Market",
        "market_category": "nyse",
        "column_set": "comprehensive",
        "default_sort": {"column": "trading_volume", "direction": "desc"},
        "real_time": True,
        "features": {
            "export": True,
            "search": True,
            "filters": True,
            "customization": True,
        },
    }

    # NASDAQ Market Template
    templates["nasdaq_market"] = {
        "table_name": "nasdaq_market",
        "display_name": "NASDAQ Market",
        "market_category": "nasdaq",
        "column_set": "extended",
        "default_sort": {"column": "price_change_percent", "direction": "desc"},
        "real_time": True,
        "features": {
            "export": True,
            "search": True,
            "filters": True,
            "customization": True,
        },
    }

    # Mobile Market Template
    templates["mobile_market"] = {
        "table_name": "mobile_market",
        "display_name": "Market Overview",
        "market_category": "mixed",
        "column_set": "mobile",
        "default_sort": {"column": "price_change_percent", "direction": "desc"},
        "real_time": True,
        "features": {
            "export": False,
            "search": True,
            "filters": False,
            "customization": False,
        },
    }

    return templates


# Global initialization flag
_financial_integration_initialized = False


def ensure_financial_integration_initialized():
    """
    Ensure financial integration is initialized (idempotent).

    This can be called multiple times safely.
    """
    global _financial_integration_initialized

    if not _financial_integration_initialized:
        initialize_financial_integration()
        _financial_integration_initialized = True


# Convenience functions for common operations
def create_tsx_table_config(user_preferences: Optional[Dict[str, Any]] = None) -> TableConfig:
    """Create a TSX market table configuration."""
    ensure_financial_integration_initialized()
    factory = create_financial_table_configuration_factory()
    return factory(
        table_name="tsx_market",
        market_category="tsx",
        column_set="extended",
        user_preferences=user_preferences,
        real_time_enabled=True,
    )


def create_nyse_table_config(user_preferences: Optional[Dict[str, Any]] = None) -> TableConfig:
    """Create a NYSE market table configuration."""
    ensure_financial_integration_initialized()
    factory = create_financial_table_configuration_factory()
    return factory(
        table_name="nyse_market",
        market_category="nyse",
        column_set="comprehensive",
        user_preferences=user_preferences,
        real_time_enabled=True,
    )


def create_nasdaq_table_config(user_preferences: Optional[Dict[str, Any]] = None) -> TableConfig:
    """Create a NASDAQ market table configuration."""
    ensure_financial_integration_initialized()
    factory = create_financial_table_configuration_factory()
    return factory(
        table_name="nasdaq_market",
        market_category="nasdaq",
        column_set="extended",
        user_preferences=user_preferences,
        real_time_enabled=True,
    )
