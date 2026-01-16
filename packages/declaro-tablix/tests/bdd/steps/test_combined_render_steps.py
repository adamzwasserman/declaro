"""Step definitions for combined table rendering feature."""

import pytest
from pytest_bdd import given, when, then, scenarios, parsers

# Load scenarios from feature file
scenarios("../features/combined_render.feature")


# Fixtures
@pytest.fixture
def table_config():
    """Fixture to hold table config."""
    return {}


@pytest.fixture
def header_config():
    """Fixture to hold header config."""
    return {"has_header": False, "config": None}


@pytest.fixture
def filter_config():
    """Fixture to hold filter config."""
    return {"has_filters": False, "config": None}


@pytest.fixture
def table_data():
    """Fixture to hold table data."""
    return {"rows": []}


@pytest.fixture
def rendered_output():
    """Fixture to hold rendered output."""
    return {"html": ""}


# Background
@given("the rendering module is available")
def rendering_module_available():
    """Verify rendering module can be imported."""
    from declaro_tablix.templates import render_table
    assert render_table is not None


# Given steps
@given(parsers.parse('a TableConfig with table name "{table_name}"'))
def create_table_config(table_config, table_name):
    """Create table config with given name."""
    from declaro_tablix.domain.models import TableConfig, ColumnDefinition

    table_config["config"] = TableConfig(
        table_name=table_name,
        columns=[
            ColumnDefinition(
                id="id",
                name="ID",
                type="text",
                visible=True,
                sortable=True,
                filterable=False,
            ),
            ColumnDefinition(
                id="name",
                name="Name",
                type="text",
                visible=True,
                sortable=True,
                filterable=True,
            ),
        ],
        filters=[],
        sorts=[],
    )


@given(parsers.parse('a TableHeaderConfig with title "{title}"'))
def create_header_config(header_config, title):
    """Create header config with given title."""
    from declaro_tablix.domain.models import TableHeaderConfig

    header_config["has_header"] = True
    header_config["config"] = TableHeaderConfig(
        title=title,
        subtitle=None,
        logo_url=None,
        stats=[],
    )


@given(parsers.parse('a FilterLayoutConfig with {count:d} filter controls'))
def create_filter_config(filter_config, count):
    """Create filter config with specified number of controls."""
    from declaro_tablix.domain.filter_layout import (
        FilterLayoutConfig,
        FilterControlConfig,
        FilterControlType,
    )

    controls = []
    for i in range(count):
        controls.append(
            FilterControlConfig(
                id=f"filter_{i}",
                control_type=FilterControlType.SEARCH_INPUT,
                column_id=f"column_{i}",
                label=f"Filter {i}",
            )
        )

    filter_config["has_filters"] = True
    filter_config["config"] = FilterLayoutConfig(
        id="test_filters",
        table_id="holdings",
        controls=controls,
    )


@given(parsers.parse('TableData with {row_count:d} rows'))
def create_table_data(table_data, row_count):
    """Create table data with specified number of rows."""
    from declaro_tablix.domain.models import TableData

    rows = []
    for i in range(row_count):
        rows.append({"id": str(i), "name": f"Row {i}"})

    table_data["data"] = TableData(
        rows=rows,
        total_count=row_count,
        metadata={},
    )


@given("no header configuration")
def no_header_config(header_config):
    """Mark that no header is configured."""
    header_config["has_header"] = False
    header_config["config"] = None


@given("no filter configuration")
def no_filter_config(filter_config):
    """Mark that no filters are configured."""
    filter_config["has_filters"] = False
    filter_config["config"] = None


# When steps
@when("I import the rendering module")
def import_rendering_module():
    """Import the rendering module."""
    from declaro_tablix import templates
    assert templates is not None


@when("I call render_table_ui with all sections")
def call_render_table_ui_all(table_config, header_config, filter_config, table_data, rendered_output):
    """Call render_table_ui with all sections."""
    from declaro_tablix.templates import render_table_ui

    rendered_output["html"] = render_table_ui(
        config=table_config["config"],
        data=table_data["data"].rows,
        header=header_config["config"] if header_config["has_header"] else None,
        filter_layout=filter_config["config"] if filter_config["has_filters"] else None,
    )


@when("I call render_table_ui")
def call_render_table_ui(table_config, header_config, filter_config, table_data, rendered_output):
    """Call render_table_ui with configured sections."""
    from declaro_tablix.templates import render_table_ui

    rendered_output["html"] = render_table_ui(
        config=table_config["config"],
        data=table_data["data"].rows,
        header=header_config["config"] if header_config["has_header"] else None,
        filter_layout=filter_config["config"] if filter_config["has_filters"] else None,
    )


# Then steps
@then("render_table_ui function is available")
def render_table_ui_available():
    """Verify render_table_ui function exists."""
    from declaro_tablix.templates import render_table_ui
    assert callable(render_table_ui)


@then(parsers.parse('the output contains "{expected_text}"'))
def output_contains_text(rendered_output, expected_text):
    """Verify output contains expected text."""
    html = rendered_output["html"]
    assert expected_text in html, \
        f"Text '{expected_text}' not found in output"


@then("the output contains filter controls")
def output_contains_filters(rendered_output):
    """Verify output contains filter controls."""
    html = rendered_output["html"]
    assert "filter" in html.lower() or "Filter" in html, \
        "Filter controls not found in output"


@then(parsers.parse('the output contains table with {row_count:d} rows'))
def output_contains_table_rows(rendered_output, row_count):
    """Verify output contains table with expected number of rows."""
    html = rendered_output["html"]
    # Check for table element and row indicators
    assert "<table" in html or "table" in html.lower(), \
        "Table element not found in output"
    # Count <tr> elements in tbody (approximate check)
    # Note: This is a simplified check - in production you might parse HTML
    assert html.count("<tr") >= row_count or f"Row {row_count-1}" in html, \
        f"Expected {row_count} rows not found in output"


@then("the output does not contain header section")
def output_does_not_contain_header(rendered_output):
    """Verify output does not contain header section."""
    html = rendered_output["html"]
    # Check for common header indicators
    # Since we don't have a specific header marker, check for title elements
    assert "<h1" not in html and "<h2" not in html, \
        "Header section should not be present but found in output"


@then("the output does not contain filter controls")
def output_does_not_contain_filters(rendered_output):
    """Verify output does not contain filter controls."""
    html = rendered_output["html"]
    # Check that filter-specific elements are not present
    assert "filter-layout" not in html and "filter-control" not in html, \
        "Filter controls should not be present but found in output"
