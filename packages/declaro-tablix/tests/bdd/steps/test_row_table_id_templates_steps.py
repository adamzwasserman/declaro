"""Step definitions for row and table ID templates feature."""

import re
import pytest
from pytest_bdd import given, when, then, scenarios, parsers

# Load scenarios from feature file
scenarios("../features/row_table_id_templates.feature")


# Fixtures
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
    from declaro_tablix.domain.models import TableConfig
    assert TableConfig is not None


# Given steps
@given("a TableConfig model")
def create_table_config(table_config):
    """Create a basic table config."""
    from declaro_tablix.domain.models import ColumnDefinition, ColumnType

    column = ColumnDefinition(
        id="value",
        name="Value",
        type=ColumnType.TEXT
    )

    table_config["table_name"] = "test_table"
    table_config["columns"] = [column]


@given("a TableConfig without row_id_template specified")
def create_table_without_row_template(table_config):
    """Create a table config without row_id_template."""
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


@given(parsers.parse('a TableConfig with row_id_template="{template}"'))
def create_table_with_row_template(table_config, template):
    """Create a table config with row_id_template."""
    from declaro_tablix.domain.models import TableConfig, ColumnDefinition, ColumnType

    column = ColumnDefinition(
        id="value",
        name="Value",
        type=ColumnType.TEXT
    )

    config = TableConfig(
        table_name="test_table",
        columns=[column],
        row_id_template=template
    )

    table_config["model"] = config


@given(parsers.parse('a TableConfig with table_id_override="{override_id}"'))
def create_table_with_id_override(table_config, override_id):
    """Create a table config with table_id_override."""
    from declaro_tablix.domain.models import TableConfig, ColumnDefinition, ColumnType

    column = ColumnDefinition(
        id="value",
        name="Value",
        type=ColumnType.TEXT
    )

    config = TableConfig(
        table_name="test_table",
        columns=[column],
        table_id_override=override_id
    )

    table_config["model"] = config


@given(parsers.parse('a TableConfig with table_name="{table_name}" and no table_id_override'))
def create_table_with_default_id(table_config, table_name):
    """Create a table config without table_id_override."""
    from declaro_tablix.domain.models import TableConfig, ColumnDefinition, ColumnType

    column = ColumnDefinition(
        id="value",
        name="Value",
        type=ColumnType.TEXT
    )

    config = TableConfig(
        table_name=table_name,
        columns=[column]
    )

    table_config["model"] = config


@given(parsers.parse("table data with {num_rows:d} rows"))
@given(parsers.parse("table data with {num_rows:d} row"))
def create_table_data(table_data, num_rows):
    """Create table data with specified number of rows."""
    rows = []
    for i in range(num_rows):
        rows.append({"value": f"Row {i}"})

    table_data["rows"] = rows


# When steps
@when(parsers.parse('I set row_id_template to "{template}"'))
def set_row_id_template(table_config, template):
    """Set row_id_template field on table config."""
    from declaro_tablix.domain.models import TableConfig

    config = TableConfig(
        table_name=table_config["table_name"],
        columns=table_config["columns"],
        row_id_template=template
    )
    table_config["model"] = config


@when(parsers.parse('I set table_id_override to "{override_id}"'))
def set_table_id_override(table_config, override_id):
    """Set table_id_override field on table config."""
    from declaro_tablix.domain.models import TableConfig

    config = TableConfig(
        table_name=table_config["table_name"],
        columns=table_config["columns"],
        table_id_override=override_id
    )
    table_config["model"] = config


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
def config_is_valid(table_config):
    """Verify the table config is valid."""
    assert "model" in table_config
    assert table_config["model"] is not None


@then(parsers.parse('config.row_id_template equals "{expected}"'))
def check_row_id_template(table_config, expected):
    """Verify row_id_template has expected value."""
    config = table_config["model"]
    assert config.row_id_template == expected


@then(parsers.parse('config.table_id_override equals "{expected}"'))
def check_table_id_override(table_config, expected):
    """Verify table_id_override has expected value."""
    config = table_config["model"]
    assert config.table_id_override == expected


@then("config.row_id_template is None")
def check_row_id_template_is_none(table_config):
    """Verify row_id_template is None."""
    config = table_config["model"]
    assert config.row_id_template is None


@then(parsers.parse('row {row_idx:d} has id="{expected_id}"'))
def check_row_has_id(rendered_output, row_idx, expected_id):
    """Verify a specific row has the expected ID."""
    html = rendered_output["html"]

    # Extract tbody content
    tbody_match = re.search(r'<tbody[^>]*>(.*?)</tbody>', html, re.DOTALL)
    assert tbody_match, "No tbody found in rendered HTML"
    tbody_html = tbody_match.group(1)

    # Find all tr elements in tbody
    tr_matches = re.findall(r'<tr[^>]*>', tbody_html, re.DOTALL)
    assert len(tr_matches) > row_idx, f"Row {row_idx} not found in table (found {len(tr_matches)} rows)"

    # Get the row at row_idx
    tr_tag = tr_matches[row_idx]

    # Find id attribute
    assert f'id="{expected_id}"' in tr_tag, \
        f"Expected row with id='{expected_id}' not found in row {row_idx}: {tr_tag}"


@then(parsers.parse('the table element has id="{expected_id}"'))
def check_table_has_id(rendered_output, expected_id):
    """Verify the table element has the expected ID."""
    html = rendered_output["html"]

    # Find table element
    table_match = re.search(r'<table[^>]*>', html, re.DOTALL)
    assert table_match, "No table found in rendered HTML"
    table_tag = table_match.group(0)

    # Find id attribute
    assert f'id="{expected_id}"' in table_tag, \
        f"Expected table with id='{expected_id}' not found: {table_tag}"
