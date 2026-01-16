"""Plugin preview routes for TableV2 system.

This module provides FastAPI routes for previewing and testing TableV2 plugins
with real-time configuration and sample data integration.
"""

import time
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from declaro_tablix.domain.models import ColumnDefinition, ColumnType
from declaro_tablix.plugins.examples.chartjs_kpi_plugin import ChartJsKpiPlugin

router = APIRouter(prefix="/api/v1/plugins", tags=["Plugin Preview"])


@router.get("/test")
async def test_route():
    """Simple test route to verify router is working."""
    return {"status": "success", "message": "Plugin routes are working!"}


@router.get("/chartjs-kpi/health")
async def plugin_health_check():
    """Plugin health check - simplified version."""
    return {"status": "healthy", "plugin": "chartjs-kpi", "version": "1.0.0", "timestamp": int(time.time())}


class KPIPreviewRequest(BaseModel):
    """Request model for KPI plugin preview."""

    current_value: float = Field(..., description="Current KPI value")
    target_value: float = Field(100, description="Target KPI value")
    chart_type: str = Field("gauge", description="Chart type (gauge, donut, line, bar)")
    unit: str = Field("%", description="Value unit")
    label: str = Field("KPI", description="KPI label")
    thresholds: Dict[str, float] = Field({"low": 30, "medium": 70, "high": 90}, description="Performance thresholds")
    historical_data: Optional[List[float]] = Field(None, description="Historical data for trend charts")


class PluginPreviewResponse(BaseModel):
    """Response model for plugin preview."""

    rendered_html: str = Field(..., description="Rendered chart HTML")
    css_classes: List[str] = Field(..., description="CSS classes for styling")
    performance_metrics: Dict[str, Any] = Field(..., description="Rendering performance metrics")
    configuration: Dict[str, Any] = Field(..., description="Plugin configuration used")


def generate_sample_historical_data(current_value: float, points: int = 7) -> List[float]:
    """Generate sample historical data for trend visualization."""
    import random

    data = []
    base_value = current_value * 0.7
    variation = current_value * 0.3

    for i in range(points - 1):
        # Create trending data that generally moves toward current value
        trend_factor = i / (points - 1)
        value = base_value + (variation * trend_factor) + (random.random() - 0.5) * (current_value * 0.2)
        data.append(max(0, value))

    data.append(current_value)
    return data


def create_sample_column_definition(request: KPIPreviewRequest) -> ColumnDefinition:
    """Create a sample column definition for plugin testing."""
    return ColumnDefinition(
        id="preview_kpi",
        name=f"{request.label} Preview",
        type=ColumnType.NUMBER,
        format_options={
            "kpi_config": {
                "chart_type": request.chart_type,
                "target_value": request.target_value,
                "label": request.label,
                "unit": request.unit,
                "thresholds": request.thresholds,
                "history_field": "historical_values" if request.historical_data else None,
            }
        },
    )


@router.post("/chartjs-kpi/preview", response_model=PluginPreviewResponse)
async def preview_chartjs_kpi_plugin(request: KPIPreviewRequest) -> PluginPreviewResponse:
    """
    Preview Chart.js KPI plugin with custom configuration.

    This endpoint allows real-time testing of the Chart.js KPI plugin
    with different configurations and sample data.
    """
    start_time = time.time()

    try:
        # Initialize the plugin
        plugin = ChartJsKpiPlugin()
        plugin.initialize({"chart_types": ["gauge", "donut", "line", "bar"], "default_thresholds": request.thresholds})

        # Create sample column definition
        column = create_sample_column_definition(request)

        # Prepare row data
        row_data = {}
        if request.historical_data:
            row_data["historical_values"] = request.historical_data
        elif request.chart_type == "line":
            # Generate sample data for line charts
            row_data["historical_values"] = generate_sample_historical_data(request.current_value)

        # Render the chart
        rendered_html = plugin.render_cell(value=request.current_value, column=column, row_data=row_data, context={})

        # Get CSS classes
        css_classes = plugin.get_css_classes(value=request.current_value, column=column, context={})

        # Calculate performance metrics
        end_time = time.time()
        render_time = round((end_time - start_time) * 1000, 2)  # Convert to milliseconds

        performance_metrics = {
            "render_time_ms": render_time,
            "chart_type": request.chart_type,
            "data_points": len(request.historical_data) if request.historical_data else 1,
            "percentage": round((request.current_value / request.target_value) * 100, 1) if request.target_value > 0 else 0,
        }

        # Build configuration summary
        configuration = {
            "plugin_name": plugin.name,
            "plugin_version": plugin.version,
            "chart_type": request.chart_type,
            "kpi_config": column.format_options["kpi_config"],
            "required_scripts": plugin.get_required_scripts(),
            "required_styles": plugin.get_required_styles(),
        }

        return PluginPreviewResponse(
            rendered_html=rendered_html,
            css_classes=css_classes,
            performance_metrics=performance_metrics,
            configuration=configuration,
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Plugin preview failed: {str(e)}")


@router.get("/chartjs-kpi/sample-scenarios")
async def get_sample_scenarios() -> Dict[str, Dict[str, Any]]:
    """
    Get predefined sample scenarios for Chart.js KPI plugin testing.

    Returns a collection of realistic KPI scenarios for different use cases.
    """
    scenarios = {
        "sales_performance": {
            "name": "Sales Performance",
            "description": "Quarterly sales targets and achievements",
            "current_value": 125000,
            "target_value": 150000,
            "unit": "$",
            "label": "Sales Revenue",
            "chart_type": "gauge",
            "thresholds": {"low": 60, "medium": 80, "high": 95},
            "historical_data": [95000, 105000, 110000, 118000, 122000, 125000],
        },
        "project_completion": {
            "name": "Project Completion",
            "description": "Sprint progress and milestone tracking",
            "current_value": 87,
            "target_value": 100,
            "unit": "%",
            "label": "Completion Rate",
            "chart_type": "donut",
            "thresholds": {"low": 70, "medium": 85, "high": 95},
            "historical_data": [45, 52, 61, 72, 78, 82, 87],
        },
        "quality_score": {
            "name": "Quality Score",
            "description": "Product quality ratings and customer satisfaction",
            "current_value": 4.2,
            "target_value": 5.0,
            "unit": "★",
            "label": "Quality Rating",
            "chart_type": "bar",
            "thresholds": {"low": 60, "medium": 80, "high": 90},
            "historical_data": [3.8, 3.9, 4.0, 4.1, 4.0, 4.1, 4.2],
        },
        "performance_metrics": {
            "name": "Performance Metrics",
            "description": "System performance and optimization targets",
            "current_value": 850,
            "target_value": 1000,
            "unit": "pts",
            "label": "Performance Score",
            "chart_type": "line",
            "thresholds": {"low": 40, "medium": 75, "high": 90},
            "historical_data": [720, 745, 780, 795, 820, 835, 850],
        },
        "critical_alert": {
            "name": "Critical Alert",
            "description": "System health and critical issue tracking",
            "current_value": 15,
            "target_value": 100,
            "unit": "%",
            "label": "System Health",
            "chart_type": "gauge",
            "thresholds": {"low": 30, "medium": 70, "high": 90},
            "historical_data": [85, 72, 58, 42, 28, 20, 15],
        },
        "revenue_growth": {
            "name": "Revenue Growth",
            "description": "Monthly recurring revenue growth tracking",
            "current_value": 2.8,
            "target_value": 3.5,
            "unit": "%",
            "label": "MRR Growth",
            "chart_type": "line",
            "thresholds": {"low": 50, "medium": 75, "high": 90},
            "historical_data": [1.2, 1.8, 2.1, 2.4, 2.6, 2.7, 2.8],
        },
    }

    return scenarios


@router.get("/chartjs-kpi/configuration-template")
async def get_configuration_template() -> Dict[str, Any]:
    """
    Get a complete configuration template for implementing Chart.js KPI plugin.

    Returns the full configuration structure needed to integrate the plugin
    into a TableV2 implementation.
    """
    template = {
        "column_definition": {
            "id": "your_kpi_column_id",
            "name": "Your KPI Column Name",
            "type": "NUMBER",
            "format_options": {
                "kpi_config": {
                    "chart_type": "gauge",  # or "donut", "line", "bar"
                    "target_value": 100,
                    "target_field": None,  # Reference to another column for dynamic targets
                    "label": "KPI Label",
                    "unit": "%",
                    "thresholds": {"low": 30, "medium": 70, "high": 90},
                    "history_field": None,  # Reference to column with historical data
                }
            },
        },
        "plugin_configuration": {
            "name": "chartjs_kpi_plugin",
            "version": "1.0.0",
            "chart_types": ["gauge", "donut", "line", "bar"],
            "color_schemes": {
                "success": ["#28a745", "#20c997", "#17a2b8"],
                "warning": ["#ffc107", "#fd7e14", "#e83e8c"],
                "danger": ["#dc3545", "#e74c3c", "#c0392b"],
                "primary": ["#007bff", "#6610f2", "#6f42c1"],
            },
            "default_thresholds": {"low": 30, "medium": 70, "high": 90},
            "chart_cdn": "https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.js",
        },
        "usage_examples": {
            "basic_gauge": {
                "description": "Simple gauge chart with percentage",
                "data_example": {"current": 75, "target": 100, "unit": "%"},
            },
            "target_reference": {
                "description": "Using target_field to reference another column",
                "data_example": {"current": 1250, "target_column": "monthly_goal", "unit": "$"},
            },
            "historical_trend": {
                "description": "Line chart with historical data",
                "data_example": {"current": 85, "history_column": "monthly_scores", "unit": "pts"},
            },
        },
    }

    return template


@router.get("/chartjs-kpi/health")
async def chartjs_kpi_health_check() -> Dict[str, Any]:
    """
    Health check endpoint for Chart.js KPI plugin system.

    Verifies that the plugin can be initialized and basic functionality works.
    """
    try:
        # Test plugin initialization
        plugin = ChartJsKpiPlugin()
        plugin.initialize({})

        # Test basic rendering
        test_column = ColumnDefinition(
            id="test",
            name="Test",
            type=ColumnType.NUMBER,
            format_options={"kpi_config": {"chart_type": "gauge", "target_value": 100}},
        )

        test_render = plugin.render_cell(50, test_column, {}, {})

        # Cleanup
        plugin.cleanup()

        return {
            "status": "healthy",
            "plugin_name": "chartjs_kpi_plugin",
            "version": "1.0.0",
            "test_render_length": len(test_render),
            "supported_chart_types": ["gauge", "donut", "line", "bar"],
            "timestamp": time.time(),
        }

    except Exception as e:
        return {"status": "unhealthy", "error": str(e), "timestamp": time.time()}
