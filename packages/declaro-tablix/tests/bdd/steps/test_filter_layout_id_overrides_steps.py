"""Step definitions for filter layout ID overrides feature."""

import re
import pytest
from pytest_bdd import given, when, then, scenarios, parsers

# Load scenarios from feature file
scenarios("../features/filter_layout_id_overrides.feature")


# Fixtures
@pytest.fixture
def layout_config():
    """Fixture to hold layout config between steps."""
    return {}


@pytest.fixture
def rendered_output():
    """Fixture to hold rendered HTML output between steps."""
    return {"html": ""}


# Background
@given("the filter layout module is available")
def filter_layout_available():
    """Verify filter layout module can be imported."""
    from declaro_tablix.domain.filter_layout import FilterLayoutConfig
    assert FilterLayoutConfig is not None


# Given steps
@given("a FilterLayoutConfig model")
def create_filter_layout_config(layout_config):
    """Create a basic filter layout config."""
    layout_config["id"] = "test_layout"
    layout_config["table_id"] = "test_table"


@given("a FilterLayoutConfig without ID overrides specified")
def create_layout_without_overrides(layout_config):
    """Create a filter layout config without ID overrides."""
    from declaro_tablix.domain.filter_layout import FilterLayoutConfig

    config = FilterLayoutConfig(
        id="test_layout",
        table_id="test_table"
    )

    layout_config["model"] = config


@given(parsers.parse('a FilterLayoutConfig with container_id_override="{override_id}"'))
def create_layout_with_container_override(layout_config, override_id):
    """Create a filter layout config with container_id_override."""
    from declaro_tablix.domain.filter_layout import FilterLayoutConfig

    config = FilterLayoutConfig(
        id="test_layout",
        table_id="test_table",
        container_id_override=override_id
    )

    layout_config["model"] = config


@given(parsers.parse('a FilterLayoutConfig with id="{layout_id}" and no container_id_override'))
def create_layout_with_default_container_id(layout_config, layout_id):
    """Create a filter layout config without container_id_override."""
    from declaro_tablix.domain.filter_layout import FilterLayoutConfig

    config = FilterLayoutConfig(
        id=layout_id,
        table_id="test_table"
    )

    layout_config["model"] = config


@given(parsers.parse('a FilterLayoutConfig with form_id_override="{override_id}"'))
def create_layout_with_form_override(layout_config, override_id):
    """Create a filter layout config with form_id_override."""
    from declaro_tablix.domain.filter_layout import FilterLayoutConfig

    config = FilterLayoutConfig(
        id="test_layout",
        table_id="test_table",
        form_id_override=override_id
    )

    layout_config["model"] = config


@given(parsers.parse('a FilterLayoutConfig with id="{layout_id}" and no form_id_override'))
def create_layout_with_default_form_id(layout_config, layout_id):
    """Create a filter layout config without form_id_override."""
    from declaro_tablix.domain.filter_layout import FilterLayoutConfig

    config = FilterLayoutConfig(
        id=layout_id,
        table_id="test_table"
    )

    layout_config["model"] = config


@given(parsers.parse('a FilterLayoutConfig with controls_id_override="{override_id}"'))
def create_layout_with_controls_override(layout_config, override_id):
    """Create a filter layout config with controls_id_override."""
    from declaro_tablix.domain.filter_layout import FilterLayoutConfig

    config = FilterLayoutConfig(
        id="test_layout",
        table_id="test_table",
        controls_id_override=override_id
    )

    layout_config["model"] = config


@given(parsers.parse('a FilterLayoutConfig with id="{layout_id}" and no controls_id_override'))
def create_layout_with_default_controls_id(layout_config, layout_id):
    """Create a filter layout config without controls_id_override."""
    from declaro_tablix.domain.filter_layout import FilterLayoutConfig

    config = FilterLayoutConfig(
        id=layout_id,
        table_id="test_table"
    )

    layout_config["model"] = config


# When steps
@when(parsers.parse('I set container_id_override to "{override_id}"'))
def set_container_id_override(layout_config, override_id):
    """Set container_id_override field on layout config."""
    from declaro_tablix.domain.filter_layout import FilterLayoutConfig

    config = FilterLayoutConfig(
        id=layout_config["id"],
        table_id=layout_config["table_id"],
        container_id_override=override_id
    )
    layout_config["model"] = config


@when(parsers.parse('I set form_id_override to "{override_id}"'))
def set_form_id_override(layout_config, override_id):
    """Set form_id_override field on layout config."""
    from declaro_tablix.domain.filter_layout import FilterLayoutConfig

    config = FilterLayoutConfig(
        id=layout_config["id"],
        table_id=layout_config["table_id"],
        form_id_override=override_id
    )
    layout_config["model"] = config


@when(parsers.parse('I set controls_id_override to "{override_id}"'))
def set_controls_id_override(layout_config, override_id):
    """Set controls_id_override field on layout config."""
    from declaro_tablix.domain.filter_layout import FilterLayoutConfig

    config = FilterLayoutConfig(
        id=layout_config["id"],
        table_id=layout_config["table_id"],
        controls_id_override=override_id
    )
    layout_config["model"] = config


@when("the filter layout is rendered")
def render_filter_layout(layout_config, rendered_output):
    """Render the filter layout using the template."""
    from declaro_tablix.templates import get_jinja_env

    config = layout_config["model"]
    env = get_jinja_env()
    template = env.get_template("components/filter_layout.html")

    html = template.render(
        layout=config,
        state=None,
        options={}
    )

    rendered_output["html"] = html


# Then steps
@then("the config is valid")
def config_is_valid(layout_config):
    """Verify the layout config is valid."""
    assert "model" in layout_config
    assert layout_config["model"] is not None


@then(parsers.parse('config.container_id_override equals "{expected}"'))
def check_container_id_override(layout_config, expected):
    """Verify container_id_override has expected value."""
    config = layout_config["model"]
    assert config.container_id_override == expected


@then(parsers.parse('config.form_id_override equals "{expected}"'))
def check_form_id_override(layout_config, expected):
    """Verify form_id_override has expected value."""
    config = layout_config["model"]
    assert config.form_id_override == expected


@then(parsers.parse('config.controls_id_override equals "{expected}"'))
def check_controls_id_override(layout_config, expected):
    """Verify controls_id_override has expected value."""
    config = layout_config["model"]
    assert config.controls_id_override == expected


@then("config.container_id_override is None")
def check_container_id_override_is_none(layout_config):
    """Verify container_id_override is None."""
    config = layout_config["model"]
    assert config.container_id_override is None


@then("config.form_id_override is None")
def check_form_id_override_is_none(layout_config):
    """Verify form_id_override is None."""
    config = layout_config["model"]
    assert config.form_id_override is None


@then("config.controls_id_override is None")
def check_controls_id_override_is_none(layout_config):
    """Verify controls_id_override is None."""
    config = layout_config["model"]
    assert config.controls_id_override is None


@then(parsers.parse('the container element has id="{expected_id}"'))
def check_container_has_id(rendered_output, expected_id):
    """Verify the container element has the expected ID."""
    html = rendered_output["html"]

    # Find outermost div element
    div_match = re.search(r'<div[^>]*>', html, re.DOTALL)
    assert div_match, "No div found in rendered HTML"
    div_tag = div_match.group(0)

    # Find id attribute
    assert f'id="{expected_id}"' in div_tag, \
        f"Expected container with id='{expected_id}' not found: {div_tag}"


@then(parsers.parse('the form element has id="{expected_id}"'))
def check_form_has_id(rendered_output, expected_id):
    """Verify the form element has the expected ID."""
    html = rendered_output["html"]

    # Find form element
    form_match = re.search(r'<form[^>]*>', html, re.DOTALL)
    assert form_match, "No form found in rendered HTML"
    form_tag = form_match.group(0)

    # Find id attribute
    assert f'id="{expected_id}"' in form_tag, \
        f"Expected form with id='{expected_id}' not found: {form_tag}"


@then(parsers.parse('the controls element has id="{expected_id}"'))
def check_controls_has_id(rendered_output, expected_id):
    """Verify the controls element has the expected ID."""
    html = rendered_output["html"]

    # Find the div with class filter-controls
    controls_match = re.search(r'<div[^>]*class="filter-controls"[^>]*>', html, re.DOTALL)
    assert controls_match, "No filter-controls div found in rendered HTML"
    controls_tag = controls_match.group(0)

    # Find id attribute
    assert f'id="{expected_id}"' in controls_tag, \
        f"Expected controls div with id='{expected_id}' not found: {controls_tag}"
