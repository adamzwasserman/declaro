"""Step definitions for cell ID template feature."""

import re
import pytest
from pytest_bdd import given, when, then, scenarios, parsers

# Load scenarios from feature file
scenarios("../features/cell_id_template.feature")


# Fixtures
@pytest.fixture
def column_config():
    """Fixture to hold column config between steps."""
    return {}


@pytest.fixture
def table_config():
    """Fixture to hold table config between steps."""
    return {}


@pytest.fixture
def table_data():
    """Fixture to hold table data between steps."""
    return {"rows": []}


@pytest.fixture
def rendered_output():
    """Fixture to hold rendered HTML output between steps."""
    return {"html": ""}


# Background
@given("the tablix domain module is available")
def tablix_domain_available():
    """Verify tablix domain module can be imported."""
    from declaro_tablix.domain.models import ColumnDefinition, TableConfig
    assert ColumnDefinition is not None
    assert TableConfig is not None


# Given steps
@given("a ColumnDefinition model")
def create_column_definition(column_config):
    """Create a basic column definition."""
    column_config["id"] = "test_column"
    column_config["name"] = "Test Column"
    column_config["type"] = "text"


@given("a ColumnDefinition without cell_id_template specified")
def create_column_without_template(column_config):
    """Create a column definition without cell_id_template."""
    from declaro_tablix.domain.models import ColumnDefinition, ColumnType

    config = ColumnDefinition(
        id="test_column",
        name="Test Column",
        type=ColumnType.TEXT
    )
    column_config["model"] = config


@given(parsers.parse('a TableConfig with a column having cell_id_template="{template}"'))
def create_table_with_cell_template(table_config, template):
    """Create a table config with a column that has cell_id_template."""
    from declaro_tablix.domain.models import TableConfig, ColumnDefinition, ColumnType

    column = ColumnDefinition(
        id="value",
        name="Value",
        type=ColumnType.TEXT,
        cell_id_template=template
    )

    config = TableConfig(
        table_name="test_table",
        columns=[column]
    )

    table_config["model"] = config


@given("a TableConfig with a column without cell_id_template")
def create_table_without_cell_template(table_config):
    """Create a table config with a column that has no cell_id_template."""
    from declaro_tablix.domain.models import TableConfig, ColumnDefinition, ColumnType

    column = ColumnDefinition(
        id="value",
        name="Value",
        type=ColumnType.TEXT
    )

    config = TableConfig(
        table_name="test_table",
        columns=[column]
    )

    table_config["model"] = config


@given(parsers.parse("table data with {num_rows:d} rows"))
def create_table_data(table_data, num_rows):
    """Create table data with specified number of rows."""
    rows = []
    for i in range(num_rows):
        rows.append({"value": f"Row {i}"})

    table_data["rows"] = rows


# When steps
@when(parsers.parse('I set cell_id_template to "{template}"'))
def set_cell_id_template(column_config, template):
    """Set cell_id_template field on column config."""
    from declaro_tablix.domain.models import ColumnDefinition, ColumnType

    config = ColumnDefinition(
        id=column_config["id"],
        name=column_config["name"],
        type=ColumnType(column_config["type"]),
        cell_id_template=template
    )
    column_config["model"] = config


@when("the table is rendered")
def render_table(table_config, table_data, rendered_output):
    """Render the table using the template."""
    from declaro_tablix.templates import get_jinja_env

    config = table_config["model"]
    env = get_jinja_env()
    template = env.get_template("components/table.html")

    html = template.render(
        config=config,
        data=table_data["rows"],
        sort_field=None,
        sort_dir=None,
        sort_url="/test/sort"
    )

    rendered_output["html"] = html


# Then steps
@then("the config is valid")
def config_is_valid(column_config):
    """Verify the column config is valid."""
    assert "model" in column_config
    assert column_config["model"] is not None


@then(parsers.parse('config.cell_id_template equals "{expected}"'))
def check_cell_id_template(column_config, expected):
    """Verify cell_id_template has expected value."""
    config = column_config["model"]
    assert config.cell_id_template == expected


@then("config.cell_id_template is None")
def check_cell_id_template_is_none(column_config):
    """Verify cell_id_template is None."""
    config = column_config["model"]
    assert config.cell_id_template is None


@then(parsers.parse('cell in row {row_idx:d} has id="{expected_id}"'))
def check_cell_has_id(rendered_output, row_idx, expected_id):
    """Verify a specific cell has the expected ID."""
    html = rendered_output["html"]

    # Extract tbody content
    tbody_match = re.search(r'<tbody[^>]*>(.*?)</tbody>', html, re.DOTALL)
    assert tbody_match, "No tbody found in rendered HTML"
    tbody_html = tbody_match.group(1)

    # Find all tr elements in tbody
    tr_matches = re.findall(r'<tr[^>]*>.*?</tr>', tbody_html, re.DOTALL)
    assert len(tr_matches) > row_idx, f"Row {row_idx} not found in table (found {len(tr_matches)} rows)"

    # Get the row at row_idx
    row_html = tr_matches[row_idx]

    # Find td with expected id
    assert f'id="{expected_id}"' in row_html, \
        f"Expected cell with id='{expected_id}' not found in row {row_idx}: {row_html}"


@then("cells have no id attribute")
def check_cells_have_no_id(rendered_output):
    """Verify cells have no id attribute."""
    html = rendered_output["html"]

    # Extract tbody content
    tbody_match = re.search(r'<tbody[^>]*>(.*?)</tbody>', html, re.DOTALL)
    assert tbody_match, f"No tbody found in rendered HTML: {html}"
    tbody_html = tbody_match.group(1)

    # Find all td elements - account for whitespace and newlines
    td_matches = re.findall(r'<td[^>]*>', tbody_html, re.DOTALL)

    # If no data rows, that's fine - no cells to check
    if len(td_matches) == 0:
        return

    # If there are cells, check that none have id attribute
    for td_tag in td_matches:
        assert 'id=' not in td_tag, \
            f"Cell should not have id attribute, found: {td_tag}"
