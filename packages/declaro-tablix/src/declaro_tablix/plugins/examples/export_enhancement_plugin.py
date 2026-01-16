"""Export enhancement plugin example for Table Module V2.

This plugin demonstrates advanced export capabilities including
multiple formats, custom styling, data transformation, and
notification integration for export completion.
"""

import json
from io import BytesIO, StringIO
from typing import Any, Dict, List

from declaro_advise import error, info, success, warning
from declaro_tablix.domain.models import ColumnDefinition, TableConfig, TableData
from declaro_tablix.plugins.protocols import SystemPlugin


class ExportEnhancementPlugin:
    """Example plugin for enhanced export capabilities."""

    def __init__(self):
        self._name = "export_enhancement_plugin"
        self._version = "1.0.0"
        self._description = "Provides enhanced export capabilities with multiple formats and styling"
        self._initialized = False
        self._supported_formats = []
        self._export_count = 0

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
        """Initialize the export enhancement plugin."""
        try:
            self._config = config
            self._supported_formats = config.get("supported_formats", ["csv", "excel", "json", "pdf"])
            self._include_styling = config.get("include_styling", True)
            self._max_export_size = config.get("max_export_size", 100000)
            self._notification_enabled = config.get("notification_enabled", True)
            self._initialized = True
            self._export_count = 0
            info(f"Export enhancement plugin initialized with formats: {', '.join(self._supported_formats)}")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize export enhancement plugin: {e}")

    def cleanup(self) -> None:
        """Cleanup plugin resources."""
        if self._export_count > 0:
            info(f"Export enhancement plugin processed {self._export_count} exports")
        self._config = {}
        self._supported_formats = []
        self._export_count = 0
        self._initialized = False
        info("Export enhancement plugin cleaned up")

    def extend_functionality(self, operation: str, data: Any, context: Dict[str, Any]) -> Any:
        """Extend system functionality with custom export operations."""
        if not self._initialized:
            return None

        try:
            if operation == "enhanced_export":
                return self._perform_enhanced_export(data, context)
            elif operation == "bulk_export":
                return self._perform_bulk_export(data, context)
            elif operation == "custom_format_export":
                return self._perform_custom_format_export(data, context)
            elif operation == "styled_export":
                return self._perform_styled_export(data, context)
            elif operation == "template_export":
                return self._perform_template_export(data, context)
            else:
                warning(f"Unsupported export operation: {operation}")
                return None

        except Exception as e:
            error(f"Export operation '{operation}' failed: {e}")
            return None

    def get_supported_operations(self) -> List[str]:
        """Get list of operations this plugin supports."""
        return ["enhanced_export", "bulk_export", "custom_format_export", "styled_export", "template_export"]

    def _perform_enhanced_export(self, data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Perform enhanced export with multiple format options."""
        try:
            table_data = data.get("table_data")
            export_format = data.get("format", "csv")
            include_metadata = data.get("include_metadata", False)

            if not table_data:
                raise ValueError("No table data provided for export")

            if export_format not in self._supported_formats:
                raise ValueError(f"Unsupported export format: {export_format}")

            info(f"Starting enhanced export in {export_format} format")

            # Prepare export data
            export_result = {
                "format": export_format,
                "timestamp": context.get("timestamp"),
                "user_id": context.get("user_id"),
                "table_name": context.get("table_name"),
                "row_count": len(table_data.rows) if table_data.rows else 0,
            }

            # Generate export content based on format
            if export_format == "csv":
                export_result["content"] = self._export_to_csv(table_data, include_metadata)
                export_result["mime_type"] = "text/csv"
                export_result["file_extension"] = ".csv"
            elif export_format == "excel":
                export_result["content"] = self._export_to_excel(table_data, include_metadata)
                export_result["mime_type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                export_result["file_extension"] = ".xlsx"
            elif export_format == "json":
                export_result["content"] = self._export_to_json(table_data, include_metadata)
                export_result["mime_type"] = "application/json"
                export_result["file_extension"] = ".json"
            elif export_format == "pdf":
                export_result["content"] = self._export_to_pdf(table_data, include_metadata)
                export_result["mime_type"] = "application/pdf"
                export_result["file_extension"] = ".pdf"

            self._export_count += 1

            if self._notification_enabled:
                success(f"Enhanced export completed: {export_result['row_count']} rows exported to {export_format}")

            return export_result

        except Exception as e:
            error(f"Enhanced export failed: {e}")
            raise

    def _perform_bulk_export(self, data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Perform bulk export of multiple tables."""
        try:
            tables_data = data.get("tables_data", [])
            export_format = data.get("format", "zip")

            if not tables_data:
                raise ValueError("No tables data provided for bulk export")

            info(f"Starting bulk export of {len(tables_data)} tables")

            bulk_results = []
            total_rows = 0

            for table_info in tables_data:
                table_name = table_info.get("name")
                table_data = table_info.get("data")

                if not table_data:
                    warning(f"Skipping table '{table_name}' - no data")
                    continue

                # Export each table
                single_export_data = {
                    "table_data": table_data,
                    "format": data.get("individual_format", "csv"),
                    "include_metadata": True,
                }

                table_context = context.copy()
                table_context["table_name"] = table_name

                export_result = self._perform_enhanced_export(single_export_data, table_context)
                export_result["table_name"] = table_name
                bulk_results.append(export_result)
                total_rows += export_result["row_count"]

            # Create bulk export package
            bulk_export = {
                "format": "bulk",
                "tables_count": len(bulk_results),
                "total_rows": total_rows,
                "individual_exports": bulk_results,
                "timestamp": context.get("timestamp"),
                "user_id": context.get("user_id"),
            }

            if self._notification_enabled:
                success(f"Bulk export completed: {len(bulk_results)} tables, {total_rows} total rows")

            return bulk_export

        except Exception as e:
            error(f"Bulk export failed: {e}")
            raise

    def _perform_custom_format_export(self, data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Perform export with custom formatting rules."""
        try:
            table_data = data.get("table_data")
            format_rules = data.get("format_rules", {})

            if not table_data:
                raise ValueError("No table data provided for custom format export")

            info("Starting custom format export")

            # Apply custom formatting to data
            formatted_data = self._apply_custom_formatting(table_data, format_rules)

            # Export formatted data
            export_data = {
                "table_data": formatted_data,
                "format": data.get("output_format", "csv"),
                "include_metadata": True,
            }

            result = self._perform_enhanced_export(export_data, context)
            result["custom_formatting_applied"] = True
            result["format_rules"] = format_rules

            return result

        except Exception as e:
            error(f"Custom format export failed: {e}")
            raise

    def _perform_styled_export(self, data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Perform export with styling for Excel/PDF formats."""
        try:
            table_data = data.get("table_data")
            style_config = data.get("style_config", {})

            if not table_data:
                raise ValueError("No table data provided for styled export")

            if not self._include_styling:
                warning("Styling disabled in plugin configuration")
                return self._perform_enhanced_export(data, context)

            info("Starting styled export")

            # Apply styling based on export format
            export_format = data.get("format", "excel")

            if export_format == "excel":
                styled_content = self._export_to_styled_excel(table_data, style_config)
            elif export_format == "pdf":
                styled_content = self._export_to_styled_pdf(table_data, style_config)
            else:
                warning(f"Styling not supported for format: {export_format}")
                return self._perform_enhanced_export(data, context)

            result = {
                "format": export_format,
                "content": styled_content,
                "styled": True,
                "style_config": style_config,
                "timestamp": context.get("timestamp"),
                "user_id": context.get("user_id"),
                "table_name": context.get("table_name"),
                "row_count": len(table_data.rows) if table_data.rows else 0,
            }

            self._export_count += 1

            if self._notification_enabled:
                success(f"Styled export completed: {result['row_count']} rows with custom styling")

            return result

        except Exception as e:
            error(f"Styled export failed: {e}")
            raise

    def _perform_template_export(self, data: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """Perform export using predefined templates."""
        try:
            table_data = data.get("table_data")
            template_name = data.get("template_name")
            template_params = data.get("template_params", {})

            if not table_data:
                raise ValueError("No table data provided for template export")

            if not template_name:
                raise ValueError("No template name provided")

            info(f"Starting template export using template: {template_name}")

            # Load and apply template
            template_config = self._load_export_template(template_name)

            # Merge template config with params
            merged_config = {**template_config, **template_params}

            # Apply template formatting
            template_data = self._apply_template_formatting(table_data, merged_config)

            # Export with template settings
            export_data = {
                "table_data": template_data,
                "format": merged_config.get("output_format", "excel"),
                "include_metadata": merged_config.get("include_metadata", True),
            }

            if merged_config.get("include_styling", False):
                export_data["style_config"] = merged_config.get("style_config", {})
                result = self._perform_styled_export(export_data, context)
            else:
                result = self._perform_enhanced_export(export_data, context)

            result["template_used"] = template_name
            result["template_config"] = merged_config

            return result

        except Exception as e:
            error(f"Template export failed: {e}")
            raise

    def _export_to_csv(self, table_data: TableData, include_metadata: bool) -> str:
        """Export table data to CSV format."""
        output = StringIO()

        # Write headers
        headers = [col.display_name or col.id for col in table_data.columns]
        output.write(",".join(f'"{header}"' for header in headers))
        output.write("\n")

        # Write data rows
        for row in table_data.rows:
            row_values = []
            for column in table_data.columns:
                value = row.get(column.id, "")
                # Escape quotes and handle None values
                if value is None:
                    value = ""
                else:
                    value = str(value).replace('"', '""')
                row_values.append(f'"{value}"')
            output.write(",".join(row_values))
            output.write("\n")

        # Add metadata if requested
        if include_metadata and table_data.metadata:
            output.write("\n# Metadata\n")
            for key, value in table_data.metadata.items():
                output.write(f"# {key}: {value}\n")

        return output.getvalue()

    def _export_to_excel(self, table_data: TableData, include_metadata: bool) -> bytes:
        """Export table data to Excel format (placeholder)."""
        # This would use openpyxl or xlsxwriter in a real implementation
        info("Excel export (placeholder implementation)")
        return b"Excel export placeholder"

    def _export_to_json(self, table_data: TableData, include_metadata: bool) -> str:
        """Export table data to JSON format."""
        export_data = {
            "columns": [
                {"id": col.id, "display_name": col.display_name, "type": col.type, "required": col.required}
                for col in table_data.columns
            ],
            "rows": table_data.rows,
            "total_count": table_data.total_count,
        }

        if include_metadata and table_data.metadata:
            export_data["metadata"] = table_data.metadata

        return json.dumps(export_data, indent=2, default=str)

    def _export_to_pdf(self, table_data: TableData, include_metadata: bool) -> bytes:
        """Export table data to PDF format (placeholder)."""
        # This would use reportlab or similar in a real implementation
        info("PDF export (placeholder implementation)")
        return b"PDF export placeholder"

    def _export_to_styled_excel(self, table_data: TableData, style_config: Dict[str, Any]) -> bytes:
        """Export table data to styled Excel format (placeholder)."""
        info("Styled Excel export (placeholder implementation)")
        return b"Styled Excel export placeholder"

    def _export_to_styled_pdf(self, table_data: TableData, style_config: Dict[str, Any]) -> bytes:
        """Export table data to styled PDF format (placeholder)."""
        info("Styled PDF export (placeholder implementation)")
        return b"Styled PDF export placeholder"

    def _apply_custom_formatting(self, table_data: TableData, format_rules: Dict[str, Any]) -> TableData:
        """Apply custom formatting rules to table data."""
        if not format_rules:
            return table_data

        # Create a copy of the data
        formatted_data = TableData(
            columns=table_data.columns.copy(),
            rows=[],
            total_count=table_data.total_count,
            metadata=table_data.metadata.copy() if table_data.metadata else {},
        )

        # Apply formatting rules to each row
        for row in table_data.rows:
            formatted_row = {}
            for column in table_data.columns:
                column_id = column.id
                value = row.get(column_id)

                # Apply column-specific formatting if exists
                if column_id in format_rules:
                    column_rules = format_rules[column_id]
                    formatted_value = self._apply_column_formatting(value, column_rules)
                    formatted_row[column_id] = formatted_value
                else:
                    formatted_row[column_id] = value

            formatted_data.rows.append(formatted_row)

        return formatted_data

    def _apply_column_formatting(self, value: Any, rules: Dict[str, Any]) -> Any:
        """Apply formatting rules to a single column value."""
        if value is None:
            return value

        # Apply transformation rules
        if "transform" in rules:
            transform = rules["transform"]
            if transform == "uppercase":
                value = str(value).upper()
            elif transform == "lowercase":
                value = str(value).lower()
            elif transform == "title":
                value = str(value).title()

        # Apply prefix/suffix
        if "prefix" in rules:
            value = f"{rules['prefix']}{value}"
        if "suffix" in rules:
            value = f"{value}{rules['suffix']}"

        return value

    def _load_export_template(self, template_name: str) -> Dict[str, Any]:
        """Load export template configuration."""
        # This would load from a template registry in a real implementation
        templates = {
            "financial_report": {
                "output_format": "excel",
                "include_styling": True,
                "style_config": {
                    "header_bg_color": "#4CAF50",
                    "header_font_color": "white",
                    "alternate_row_color": "#f5f5f5",
                },
                "include_metadata": True,
            },
            "simple_csv": {"output_format": "csv", "include_styling": False, "include_metadata": False},
            "detailed_pdf": {
                "output_format": "pdf",
                "include_styling": True,
                "style_config": {"font_family": "Arial", "font_size": 10, "include_charts": True},
                "include_metadata": True,
            },
        }

        if template_name not in templates:
            raise ValueError(f"Unknown template: {template_name}")

        return templates[template_name]

    def _apply_template_formatting(self, table_data: TableData, template_config: Dict[str, Any]) -> TableData:
        """Apply template-specific formatting to table data."""
        # Apply any template-specific data transformations
        format_rules = template_config.get("format_rules", {})
        if format_rules:
            return self._apply_custom_formatting(table_data, format_rules)
        return table_data
