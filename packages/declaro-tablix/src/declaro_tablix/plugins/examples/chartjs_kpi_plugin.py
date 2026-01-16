"""Chart.js KPI plugin example for Table Module V2.

This plugin demonstrates how to create KPI visualizations in table cells
using Chart.js integration with single-cell rendering and real-time data binding.
"""

import json
from typing import Any, Dict, List

from declaro_advise import info
from declaro_tablix.domain.models import ColumnDefinition


class ChartJsKpiPlugin:
    """Example plugin for creating KPI visualizations using Chart.js in table cells."""

    def __init__(self):
        self._name = "chartjs_kpi_plugin"
        self._version = "1.0.0"
        self._description = "Provides Chart.js KPI visualization capabilities for single table cells"
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
        """Initialize the Chart.js KPI plugin."""
        try:
            self._config = config
            self._chart_types = config.get("chart_types", ["gauge", "donut", "line", "bar"])
            self._color_schemes = config.get(
                "color_schemes",
                {
                    "success": ["#28a745", "#20c997", "#17a2b8"],
                    "warning": ["#ffc107", "#fd7e14", "#e83e8c"],
                    "danger": ["#dc3545", "#e74c3c", "#c0392b"],
                    "primary": ["#007bff", "#6610f2", "#6f42c1"],
                },
            )
            self._default_thresholds = config.get("default_thresholds", {"low": 30, "medium": 70, "high": 90})
            self._chart_cdn = config.get("chart_cdn", "https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.js")
            self._initialized = True
            info(f"Chart.js KPI plugin initialized with {len(self._chart_types)} chart types")
        except Exception as e:
            raise RuntimeError(f"Failed to initialize Chart.js KPI plugin: {e}")

    def cleanup(self) -> None:
        """Cleanup plugin resources."""
        self._config = {}
        self._chart_types = []
        self._color_schemes = {}
        self._default_thresholds = {}
        self._chart_cdn = ""
        self._initialized = False
        info("Chart.js KPI plugin cleaned up")

    def render_cell(self, value: Any, column: ColumnDefinition, row_data: Dict[str, Any], context: Dict[str, Any]) -> str:
        """Render KPI chart in table cell based on column configuration."""
        if not self._initialized:
            return str(value) if value is not None else ""

        try:
            # Check if this column is configured for KPI visualization
            column_config = column.format_options.get("kpi_config", {})
            if not column_config:
                return str(value) if value is not None else ""

            chart_type = column_config.get("chart_type", "gauge")
            chart_id = f"kpi-chart-{column.id}-{hash(str(row_data))}"

            # Process the KPI data
            kpi_data = self._process_kpi_data(value, column_config, row_data)

            # Generate chart based on type
            if chart_type == "gauge":
                return self._render_gauge_chart(chart_id, kpi_data, column_config)
            elif chart_type == "donut":
                return self._render_donut_chart(chart_id, kpi_data, column_config)
            elif chart_type == "line":
                return self._render_line_chart(chart_id, kpi_data, column_config)
            elif chart_type == "bar":
                return self._render_bar_chart(chart_id, kpi_data, column_config)
            else:
                return self._render_gauge_chart(chart_id, kpi_data, column_config)

        except Exception as e:
            info(f"Chart.js KPI rendering failed for column {column.id}: {e}")
            return str(value) if value is not None else ""

    def render_header(self, column: ColumnDefinition, context: Dict[str, Any]) -> str:
        """Render column header with KPI chart icon if configured."""
        if not self._initialized:
            return column.name or column.id

        try:
            # Check if this column has KPI configuration
            kpi_config = column.format_options.get("kpi_config", {})
            if not kpi_config:
                return column.display_name or column.id

            chart_type = kpi_config.get("chart_type", "gauge")
            chart_icon = self._get_chart_icon(chart_type)

            header_html = f'<span class="kpi-column-header">'
            header_html += f'<i class="{chart_icon}" title="KPI {chart_type.title()} Chart"></i> '
            header_html += column.name or column.id
            header_html += "</span>"

            return header_html

        except Exception as e:
            info(f"KPI header rendering failed for column {column.id}: {e}")
            return column.name or column.id

    def get_css_classes(self, value: Any, column: ColumnDefinition, context: Dict[str, Any]) -> List[str]:
        """Get CSS classes for KPI cell styling."""
        if not self._initialized:
            return []

        try:
            # Check if this column has KPI configuration
            kpi_config = column.format_options.get("kpi_config", {})
            if not kpi_config:
                return []

            classes = ["kpi-cell"]
            chart_type = kpi_config.get("chart_type", "gauge")
            classes.append(f"kpi-{chart_type}")

            # Add threshold-based classes
            if isinstance(value, (int, float)):
                thresholds = kpi_config.get("thresholds", self._default_thresholds)
                if value >= thresholds.get("high", 90):
                    classes.append("kpi-high")
                elif value >= thresholds.get("medium", 70):
                    classes.append("kpi-medium")
                elif value >= thresholds.get("low", 30):
                    classes.append("kpi-low")
                else:
                    classes.append("kpi-critical")

            return classes

        except Exception as e:
            info(f"KPI CSS class generation failed for column {column.id}: {e}")
            return ["kpi-cell"]

    def _process_kpi_data(self, value: Any, config: Dict[str, Any], row_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process and normalize KPI data for chart rendering."""
        try:
            # Extract numeric value
            if isinstance(value, (int, float)):
                current_value = float(value)
            elif isinstance(value, str):
                # Try to extract number from string
                import re

                numbers = re.findall(r"-?\d+\.?\d*", value)
                current_value = float(numbers[0]) if numbers else 0
            else:
                current_value = 0

            # Get target value if configured
            target_field = config.get("target_field")
            target_value = None
            if target_field and target_field in row_data:
                target_value = float(row_data[target_field])
            else:
                target_value = config.get("target_value", 100)

            # Calculate percentage if needed
            percentage = (current_value / target_value * 100) if target_value > 0 else 0

            # Get historical data if available
            history_field = config.get("history_field")
            historical_data = []
            if history_field and history_field in row_data:
                hist_data = row_data[history_field]
                if isinstance(hist_data, list):
                    historical_data = [float(x) for x in hist_data if isinstance(x, (int, float))]
                elif isinstance(hist_data, str):
                    # Parse comma-separated values
                    try:
                        historical_data = [float(x.strip()) for x in hist_data.split(",")]
                    except ValueError:
                        historical_data = []

            return {
                "current_value": current_value,
                "target_value": target_value,
                "percentage": percentage,
                "historical_data": historical_data,
                "label": config.get("label", "KPI"),
                "unit": config.get("unit", ""),
                "thresholds": config.get("thresholds", self._default_thresholds),
            }

        except Exception as e:
            info(f"KPI data processing failed: {e}")
            return {
                "current_value": 0,
                "target_value": 100,
                "percentage": 0,
                "historical_data": [],
                "label": "KPI",
                "unit": "",
                "thresholds": self._default_thresholds,
            }

    def _render_gauge_chart(self, chart_id: str, data: Dict[str, Any], config: Dict[str, Any]) -> str:
        """Render a gauge chart for KPI visualization."""
        try:
            # Determine color based on thresholds
            percentage = data["percentage"]
            thresholds = data["thresholds"]

            if percentage >= thresholds.get("high", 90):
                color = self._color_schemes["success"][0]
            elif percentage >= thresholds.get("medium", 70):
                color = self._color_schemes["primary"][0]
            elif percentage >= thresholds.get("low", 30):
                color = self._color_schemes["warning"][0]
            else:
                color = self._color_schemes["danger"][0]

            chart_config = {
                "type": "doughnut",
                "data": {
                    "datasets": [
                        {
                            "data": [percentage, 100 - percentage],
                            "backgroundColor": [color, "#e9ecef"],
                            "borderWidth": 0,
                            "circumference": 180,
                            "rotation": 270,
                        }
                    ]
                },
                "options": {
                    "responsive": True,
                    "maintainAspectRatio": False,
                    "plugins": {"legend": {"display": False}, "tooltip": {"enabled": False}},
                    "cutout": "80%",
                },
            }

            return f"""
                <div class="kpi-gauge-container" style="position: relative; height: 60px; width: 80px;">
                    <canvas id="{chart_id}" width="80" height="60"></canvas>
                    <div class="kpi-gauge-text" style="position: absolute; top: 75%; left: 50%;
                         transform: translate(-50%, -50%); text-align: center;
                         font-size: 12px; font-weight: bold;">
                        {data['current_value']:.1f}{data['unit']}<br>
                        <small style="font-size: 10px; color: #6c757d;">{percentage:.0f}%</small>
                    </div>
                    <script>
                        if (typeof Chart !== 'undefined') {{
                            new Chart(document.getElementById('{chart_id}'), {json.dumps(chart_config)});
                        }}
                    </script>
                </div>
            """

        except Exception as e:
            info(f"Gauge chart rendering failed: {e}")
            return f"<div class='kpi-error'>Chart Error</div>"

    def _render_donut_chart(self, chart_id: str, data: Dict[str, Any], config: Dict[str, Any]) -> str:
        """Render a donut chart for KPI visualization."""
        try:
            percentage = data["percentage"]

            # Use color scheme based on performance
            if percentage >= 90:
                colors = self._color_schemes["success"]
            elif percentage >= 70:
                colors = self._color_schemes["primary"]
            elif percentage >= 30:
                colors = self._color_schemes["warning"]
            else:
                colors = self._color_schemes["danger"]

            chart_config = {
                "type": "doughnut",
                "data": {
                    "labels": ["Current", "Remaining"],
                    "datasets": [
                        {
                            "data": [data["current_value"], max(0, data["target_value"] - data["current_value"])],
                            "backgroundColor": [colors[0], "#e9ecef"],
                            "borderWidth": 2,
                            "borderColor": "#fff",
                        }
                    ],
                },
                "options": {
                    "responsive": True,
                    "maintainAspectRatio": False,
                    "plugins": {
                        "legend": {"display": False},
                        "tooltip": {
                            "callbacks": {
                                "label": f"function(ctx) {{ return ctx.label + ': ' + ctx.parsed + '{data['unit']}'; }}"
                            }
                        },
                    },
                    "cutout": "60%",
                },
            }

            return f"""
                <div class="kpi-donut-container" style="position: relative; height: 60px; width: 60px;">
                    <canvas id="{chart_id}" width="60" height="60"></canvas>
                    <div class="kpi-donut-text" style="position: absolute; top: 50%; left: 50%;
                         transform: translate(-50%, -50%); text-align: center;
                         font-size: 10px; font-weight: bold;">
                        {percentage:.0f}%
                    </div>
                    <script>
                        if (typeof Chart !== 'undefined') {{
                            new Chart(document.getElementById('{chart_id}'), {json.dumps(chart_config)});
                        }}
                    </script>
                </div>
            """

        except Exception as e:
            info(f"Donut chart rendering failed: {e}")
            return f"<div class='kpi-error'>Chart Error</div>"

    def _render_line_chart(self, chart_id: str, data: Dict[str, Any], config: Dict[str, Any]) -> str:
        """Render a mini line chart for KPI trend visualization."""
        try:
            historical_data = data["historical_data"]
            if not historical_data:
                # Generate sample trend data if no history available
                historical_data = [data["current_value"]] * 7

            # Add current value to the end
            historical_data = historical_data[-6:] + [data["current_value"]]

            chart_config = {
                "type": "line",
                "data": {
                    "labels": ["", "", "", "", "", "", ""],
                    "datasets": [
                        {
                            "data": historical_data,
                            "borderColor": self._color_schemes["primary"][0],
                            "backgroundColor": self._color_schemes["primary"][0] + "20",
                            "borderWidth": 2,
                            "fill": True,
                            "tension": 0.4,
                            "pointRadius": 0,
                            "pointHoverRadius": 3,
                        }
                    ],
                },
                "options": {
                    "responsive": True,
                    "maintainAspectRatio": False,
                    "plugins": {"legend": {"display": False}, "tooltip": {"enabled": False}},
                    "scales": {"x": {"display": False}, "y": {"display": False}},
                    "elements": {"point": {"radius": 0}},
                },
            }

            return f"""
                <div class="kpi-line-container" style="height: 40px; width: 80px;">
                    <canvas id="{chart_id}" width="80" height="40"></canvas>
                    <div class="kpi-line-value" style="text-align: center; font-size: 11px;
                         font-weight: bold; margin-top: 2px;">
                        {data['current_value']:.1f}{data['unit']}
                    </div>
                    <script>
                        if (typeof Chart !== 'undefined') {{
                            new Chart(document.getElementById('{chart_id}'), {json.dumps(chart_config)});
                        }}
                    </script>
                </div>
            """

        except Exception as e:
            info(f"Line chart rendering failed: {e}")
            return f"<div class='kpi-error'>Chart Error</div>"

    def _render_bar_chart(self, chart_id: str, data: Dict[str, Any], config: Dict[str, Any]) -> str:
        """Render a mini bar chart for KPI comparison."""
        try:
            # Create comparison data: current vs target
            comparison_data = [data["current_value"], data["target_value"]]

            # Color based on performance
            percentage = data["percentage"]
            if percentage >= 100:
                colors = [self._color_schemes["success"][0], self._color_schemes["success"][1]]
            elif percentage >= 70:
                colors = [self._color_schemes["primary"][0], self._color_schemes["primary"][1]]
            else:
                colors = [self._color_schemes["warning"][0], self._color_schemes["warning"][1]]

            chart_config = {
                "type": "bar",
                "data": {
                    "labels": ["Current", "Target"],
                    "datasets": [{"data": comparison_data, "backgroundColor": colors, "borderWidth": 0, "barThickness": 12}],
                },
                "options": {
                    "responsive": True,
                    "maintainAspectRatio": False,
                    "plugins": {"legend": {"display": False}, "tooltip": {"enabled": False}},
                    "scales": {"x": {"display": False}, "y": {"display": False}},
                },
            }

            return f"""
                <div class="kpi-bar-container" style="height: 40px; width: 60px;">
                    <canvas id="{chart_id}" width="60" height="40"></canvas>
                    <div class="kpi-bar-value" style="text-align: center; font-size: 11px;
                         font-weight: bold; margin-top: 2px;">
                        {percentage:.0f}%
                    </div>
                    <script>
                        if (typeof Chart !== 'undefined') {{
                            new Chart(document.getElementById('{chart_id}'), {json.dumps(chart_config)});
                        }}
                    </script>
                </div>
            """

        except Exception as e:
            info(f"Bar chart rendering failed: {e}")
            return f"<div class='kpi-error'>Chart Error</div>"

    def _get_chart_icon(self, chart_type: str) -> str:
        """Get Font Awesome icon for chart type."""
        icon_map = {
            "gauge": "fas fa-tachometer-alt",
            "donut": "fas fa-chart-pie",
            "line": "fas fa-chart-line",
            "bar": "fas fa-chart-bar",
        }
        return icon_map.get(chart_type, "fas fa-chart-area")

    def get_required_scripts(self) -> List[str]:
        """Get list of required JavaScript libraries."""
        return [self._chart_cdn]

    def get_required_styles(self) -> List[str]:
        """Get list of required CSS styles."""
        return [
            """
            <style>
                .kpi-cell {
                    text-align: center;
                    vertical-align: middle;
                    padding: 8px;
                }
                .kpi-gauge-container, .kpi-donut-container, .kpi-line-container, .kpi-bar-container {
                    display: inline-block;
                    margin: 0 auto;
                }
                .kpi-error {
                    color: #dc3545;
                    font-size: 11px;
                    padding: 4px;
                }
                .kpi-high { background-color: #d4edda; }
                .kpi-medium { background-color: #cce5ff; }
                .kpi-low { background-color: #fff3cd; }
                .kpi-critical { background-color: #f8d7da; }
                .kpi-column-header i { color: #6c757d; margin-right: 4px; }
            </style>
        """
        ]
