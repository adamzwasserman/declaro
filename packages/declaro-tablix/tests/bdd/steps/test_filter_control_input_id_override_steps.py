"""Step definitions for filter control input ID override feature."""

import pytest
from pytest_bdd import given, when, then, scenarios, parsers

# Load scenarios from feature file
scenarios("../features/filter_control_input_id_override.feature")


# Fixtures
@pytest.fixture
def control_config():
    """Fixture to hold control config between steps."""
    return {}


@pytest.fixture
def rendered_output():
    """Fixture to hold rendered HTML output between steps."""
    return {"html": ""}


# Background
@given("the filter layout module is available")
def filter_layout_available():
    """Verify filter layout module can be imported."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig
    assert FilterControlConfig is not None


# Given steps
@given(parsers.parse('a FilterControlConfig with type "{control_type}"'))
def create_control_config_with_type(control_config, control_type):
    """Create a control config with specified type."""
    from declaro_tablix.domain.filter_layout import FilterControlType

    control_config["id"] = "test_control"
    control_config["control_type"] = FilterControlType(control_type)
    control_config["column_id"] = "test_column"


@given(parsers.parse('a FilterControlConfig with type "{control_type}" and id "{control_id}"'))
def create_control_with_id(control_config, control_type, control_id):
    """Create a control config with specified type and id."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig, FilterControlType

    config = FilterControlConfig(
        id=control_id,
        control_type=FilterControlType(control_type),
        column_id="test_column"
    )

    control_config["model"] = config


@given(parsers.parse('input_id_override is "{override_id}"'))
def set_input_id_override_value(control_config, override_id):
    """Set input_id_override on existing model."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig

    # Get the model or create from config dict
    if "model" in control_config:
        existing = control_config["model"]
        control_config["model"] = FilterControlConfig(
            id=existing.id,
            control_type=existing.control_type,
            column_id=existing.column_id,
            input_id_override=override_id
        )
    else:
        # Create new model with override
        control_config["input_id_override"] = override_id


# When steps
@when(parsers.parse('I set input_id_override to "{override_id}"'))
def set_input_id_override(control_config, override_id):
    """Set input_id_override field on control config."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig

    config = FilterControlConfig(
        id=control_config["id"],
        control_type=control_config["control_type"],
        column_id=control_config["column_id"],
        input_id_override=override_id
    )
    control_config["model"] = config


@when("the filter control is rendered")
def render_filter_control(control_config, rendered_output):
    """Render the filter control based on type."""
    from declaro_tablix.domain.filter_layout import FilterControlType
    from declaro_tablix.templates import get_jinja_env

    config = control_config["model"]
    env = get_jinja_env()

    # Choose template based on control type
    template_map = {
        FilterControlType.SEARCH_INPUT: "components/filters/search_input.html",
        FilterControlType.SINGLE_SELECT: "components/filters/single_select.html",
        FilterControlType.MULTI_SELECT: "components/filters/multi_select.html",
        FilterControlType.NUMBER_RANGE: "components/filters/number_range.html",
        FilterControlType.DATE_RANGE: "components/filters/date_range.html",
        FilterControlType.CHECKBOX_GROUP: "components/filters/checkbox_group.html",
    }

    template_path = template_map.get(config.control_type)
    if not template_path:
        raise ValueError(f"Unsupported control type: {config.control_type}")

    template = env.get_template(template_path)
    rendered_output["html"] = template.render(
        control=config,
        control_value=None,
        control_options=[]
    )


# Then steps
@then("the config is valid")
def config_is_valid(control_config):
    """Verify the control config is valid."""
    assert "model" in control_config
    assert control_config["model"] is not None


@then(parsers.parse('config.input_id_override equals "{expected}"'))
def check_input_id_override(control_config, expected):
    """Verify input_id_override has expected value."""
    config = control_config["model"]
    assert config.input_id_override == expected


@then("config.input_id_override is None")
def check_input_id_override_is_none(control_config):
    """Verify input_id_override is None."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig

    # Create config from dict to test defaults
    config = FilterControlConfig(
        id=control_config["id"],
        control_type=control_config["control_type"],
        column_id=control_config["column_id"]
    )
    assert config.input_id_override is None


@then(parsers.parse('the rendered HTML contains id="{expected_id}"'))
def check_html_contains_id(rendered_output, expected_id):
    """Verify rendered HTML contains the expected ID."""
    html = rendered_output["html"]
    assert f'id="{expected_id}"' in html, \
        f"Expected id='{expected_id}' not found in HTML: {html}"
