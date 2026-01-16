"""Custom rendering plugin example for Table Module V2.

This plugin demonstrates how to customize cell rendering, headers,
and apply custom CSS styling to table elements.
"""

from typing import Any, Dict, List

from declaro_advise import info, success
from declaro_tablix.domain.models import ColumnDefinition, TableConfig
from declaro_tablix.plugins.protocols import RenderingPlugin


class CustomRenderingPlugin:
    """Example plugin for custom table rendering and styling."""

    def __init__(self):
        self._name = "custom_rendering_plugin"
        self._version = "1.0.0"
        self._description = "Provides custom cell rendering and styling capabilities"
        self._initialized = False

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
        """Initialize the custom rendering plugin."""
        try:
            self._config = config
            self._styles = config.get("custom_styles", {})
            self._formatters = config.get("custom_formatters", {})
            self._initialized = True
            info(f"Custom rendering plugin initialized with {len(self._styles)} styles")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize custom rendering plugin: {e}")

    def cleanup(self) -> None:
        """Cleanup plugin resources."""
        self._config = {}
        self._styles = {}
        self._formatters = {}
        self._initialized = False
        info("Custom rendering plugin cleaned up")

    def render_cell(self, value: Any, column: ColumnDefinition, row_data: Dict[str, Any], context: Dict[str, Any]) -> str:
        """Render custom cell content based on column type and value."""
        if not self._initialized:
            return str(value) if value is not None else ""

        try:
            column_id = column.id
            column_type = column.type

            # Handle priority/status columns with custom styling
            if column_id == "priority" and column_type == "TEXT":
                return self._render_priority_cell(value)

            # Handle currency with custom formatting
            if column_type == "CURRENCY":
                return self._render_currency_cell(value, column)

            # Handle percentage with custom bars
            if column_type == "PERCENTAGE":
                return self._render_percentage_cell(value)

            # Handle boolean with custom icons
            if column_type == "BOOLEAN":
                return self._render_boolean_cell(value)

            # Handle dates with relative time
            if column_type == "DATE":
                return self._render_date_cell(value)

            # Default rendering
            return str(value) if value is not None else ""

        except Exception as e:
            info(f"Custom rendering failed for column {column.id}: {e}")
            return str(value) if value is not None else ""

    def render_header(self, column: ColumnDefinition, context: Dict[str, Any]) -> str:
        """Render custom column headers with icons and tooltips."""
        if not self._initialized:
            return column.display_name or column.id

        try:
            header_html = f'<span class="column-header">'

            # Add icon based on column type
            icon = self._get_column_icon(column.type)
            if icon:
                header_html += f'<i class="{icon}" title="{column.type}"></i> '

            # Add column name
            header_html += column.display_name or column.id

            # Add sort indicator if applicable
            sort_direction = context.get("sort_direction")
            if context.get("sorted_column") == column.id and sort_direction:
                sort_icon = "fa-sort-up" if sort_direction == "asc" else "fa-sort-down"
                header_html += f' <i class="fas {sort_icon}"></i>'

            header_html += "</span>"
            return header_html

        except Exception as e:
            info(f"Custom header rendering failed for column {column.id}: {e}")
            return column.display_name or column.id

    def render_controls(self, config: TableConfig, context: Dict[str, Any]) -> str:
        """Render custom table controls and action buttons."""
        if not self._initialized:
            return ""

        try:
            controls_html = '<div class="custom-table-controls">'

            # Add refresh button
            controls_html += """
                <button class="btn btn-sm btn-outline-primary me-2"
                        hx-get="/api/table/refresh"
                        hx-target="#table-container">
                    <i class="fas fa-sync-alt"></i> Refresh
                </button>
            """

            # Add view mode toggle
            controls_html += """
                <div class="btn-group me-2" role="group">
                    <button class="btn btn-sm btn-outline-secondary" data-view="table">
                        <i class="fas fa-table"></i> Table
                    </button>
                    <button class="btn btn-sm btn-outline-secondary" data-view="cards">
                        <i class="fas fa-th-large"></i> Cards
                    </button>
                </div>
            """

            # Add density toggle
            controls_html += """
                <button class="btn btn-sm btn-outline-secondary"
                        onclick="toggleTableDensity()">
                    <i class="fas fa-compress-alt"></i> Compact
                </button>
            """

            controls_html += "</div>"
            return controls_html

        except Exception as e:
            info(f"Custom controls rendering failed: {e}")
            return ""

    def get_css_classes(self, value: Any, column: ColumnDefinition, context: Dict[str, Any]) -> List[str]:
        """Get custom CSS classes for cell styling."""
        if not self._initialized:
            return []

        try:
            classes = []
            column_id = column.id
            column_type = column.type

            # Priority-based styling
            if column_id == "priority" and isinstance(value, str):
                if value.lower() == "high":
                    classes.append("priority-high")
                elif value.lower() == "medium":
                    classes.append("priority-medium")
                elif value.lower() == "low":
                    classes.append("priority-low")

            # Status-based styling
            if column_id == "status" and isinstance(value, str):
                if value.lower() in ["active", "complete", "success"]:
                    classes.append("status-success")
                elif value.lower() in ["pending", "in_progress", "warning"]:
                    classes.append("status-warning")
                elif value.lower() in ["inactive", "failed", "error"]:
                    classes.append("status-danger")

            # Numeric value styling
            if column_type in ["NUMBER", "CURRENCY", "PERCENTAGE"]:
                if isinstance(value, (int, float)):
                    if value < 0:
                        classes.append("negative-value")
                    elif value > 0:
                        classes.append("positive-value")
                    else:
                        classes.append("zero-value")

            # Add column type class
            classes.append(f"column-type-{column_type.lower()}")

            return classes

        except Exception as e:
            info(f"CSS class generation failed for column {column.id}: {e}")
            return []

    def _render_priority_cell(self, value: Any) -> str:
        """Render priority cell with colored badge."""
        if not value:
            return ""

        priority = str(value).lower()
        if priority == "high":
            return '<span class="badge bg-danger">High</span>'
        elif priority == "medium":
            return '<span class="badge bg-warning text-dark">Medium</span>'
        elif priority == "low":
            return '<span class="badge bg-success">Low</span>'
        else:
            return f'<span class="badge bg-secondary">{value}</span>'

    def _render_currency_cell(self, value: Any, column: ColumnDefinition) -> str:
        """Render currency with custom formatting."""
        if value is None:
            return ""

        try:
            amount = float(value)
            currency_symbol = column.format_options.get("currency_symbol", "$")

            # Color code based on value
            if amount < 0:
                return f'<span class="text-danger">{currency_symbol}{amount:,.2f}</span>'
            elif amount > 1000:
                return f'<span class="text-success fw-bold">{currency_symbol}{amount:,.2f}</span>'
            else:
                return f"{currency_symbol}{amount:,.2f}"

        except (ValueError, TypeError):
            return str(value)

    def _render_percentage_cell(self, value: Any) -> str:
        """Render percentage with progress bar."""
        if value is None:
            return ""

        try:
            percent = float(value)
            # Clamp between 0 and 100
            percent = max(0, min(100, percent))

            # Determine color based on value
            if percent >= 80:
                color = "success"
            elif percent >= 60:
                color = "info"
            elif percent >= 40:
                color = "warning"
            else:
                color = "danger"

            return f"""
                <div class="d-flex align-items-center">
                    <div class="progress me-2" style="width: 60px; height: 8px;">
                        <div class="progress-bar bg-{color}"
                             style="width: {percent}%"></div>
                    </div>
                    <small>{percent:.1f}%</small>
                </div>
            """
        except (ValueError, TypeError):
            return str(value)

    def _render_boolean_cell(self, value: Any) -> str:
        """Render boolean with custom icons."""
        if value is None:
            return ""

        if value in [True, 1, "true", "True", "1", "yes", "Yes"]:
            return '<i class="fas fa-check-circle text-success"></i>'
        elif value in [False, 0, "false", "False", "0", "no", "No"]:
            return '<i class="fas fa-times-circle text-danger"></i>'
        else:
            return '<i class="fas fa-question-circle text-muted"></i>'

    def _render_date_cell(self, value: Any) -> str:
        """Render date with relative time tooltip."""
        if not value:
            return ""

        try:
            from datetime import datetime

            if isinstance(value, str):
                # Try to parse common date formats
                for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%m/%d/%Y"]:
                    try:
                        date_obj = datetime.strptime(value, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    return value  # Return original if can't parse
            elif isinstance(value, datetime):
                date_obj = value
            else:
                return str(value)

            # Calculate relative time
            now = datetime.now()
            diff = now - date_obj

            if diff.days == 0:
                relative = "Today"
            elif diff.days == 1:
                relative = "Yesterday"
            elif diff.days < 7:
                relative = f"{diff.days} days ago"
            else:
                relative = date_obj.strftime("%b %d, %Y")

            formatted_date = date_obj.strftime("%m/%d/%Y")
            return f'<span title="{relative}">{formatted_date}</span>'

        except Exception:
            return str(value)

    def _get_column_icon(self, column_type: str) -> str:
        """Get Font Awesome icon for column type."""
        icon_map = {
            "TEXT": "fas fa-font",
            "NUMBER": "fas fa-hashtag",
            "CURRENCY": "fas fa-dollar-sign",
            "PERCENTAGE": "fas fa-percent",
            "DATE": "fas fa-calendar",
            "BOOLEAN": "fas fa-toggle-on",
            "EMAIL": "fas fa-envelope",
            "URL": "fas fa-link",
            "PHONE": "fas fa-phone",
            "JSON": "fas fa-code",
        }
        return icon_map.get(column_type, "fas fa-question")
