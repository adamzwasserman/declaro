"""Step definitions for static display controls feature."""

import pytest
from pytest_bdd import given, when, then, scenarios, parsers

# Load scenarios from feature file
scenarios("../features/static_controls.feature")


# Fixtures
@pytest.fixture
def filter_config():
    """Fixture to hold filter config between steps."""
    return {}


@pytest.fixture
def rendered_output():
    """Fixture to hold rendered output between steps."""
    return {"html": ""}


# Background
@given("the filter layout module is available")
def filter_layout_available():
    """Verify filter layout module can be imported."""
    from declaro_tablix.domain.filter_layout import (
        FilterControlConfig,
        FilterControlType,
        FilterLayoutConfig,
    )
    assert FilterControlConfig is not None
    assert FilterControlType is not None


# Given steps
@given(parsers.parse('a FilterControlConfig with type "{control_type}"'))
def create_filter_config_with_type(filter_config, control_type):
    """Create a filter config with specified type."""
    from declaro_tablix.domain.filter_layout import FilterControlType

    filter_config["control_type"] = FilterControlType(control_type)
    filter_config["id"] = "test_static"
    filter_config["column_id"] = "static_column"


@given(parsers.parse('text_content is "{text}"'))
def set_text_content(filter_config, text):
    """Set text content field."""
    filter_config["text_content"] = text


@given(parsers.parse('image_url is "{url}"'))
def set_image_url(filter_config, url):
    """Set image URL field."""
    filter_config["image_url"] = url


@given(parsers.parse('image_alt is "{alt}"'))
def set_image_alt(filter_config, alt):
    """Set image alt text field."""
    filter_config["image_alt"] = alt


@given(parsers.parse('css_class is "{css_class}"'))
def set_css_class(filter_config, css_class):
    """Set CSS class field."""
    filter_config["css_class"] = css_class


# When steps
@when("I check FilterControlType enum values")
def check_enum_values():
    """Just trigger enum check - actual assertion in then step."""
    pass


@when("the filter control is rendered")
def render_filter_control(filter_config, rendered_output):
    """Render the filter control based on type."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig, FilterControlType
    from declaro_tablix.templates import get_jinja_env

    config = FilterControlConfig(**filter_config)
    env = get_jinja_env()

    # Choose template based on control type
    if config.control_type == FilterControlType.STATIC_TEXT:
        template = env.get_template("components/filters/static_text.html")
    elif config.control_type == FilterControlType.STATIC_IMAGE:
        template = env.get_template("components/filters/static_image.html")
    else:
        raise ValueError(f"Unsupported control type: {config.control_type}")

    rendered_output["html"] = template.render(control=config)


# Then steps
@then("STATIC_TEXT is a valid FilterControlType")
def static_text_is_valid_enum():
    """Verify STATIC_TEXT enum exists."""
    from declaro_tablix.domain.filter_layout import FilterControlType

    assert hasattr(FilterControlType, "STATIC_TEXT")
    assert FilterControlType.STATIC_TEXT.value == "static_text"


@then("STATIC_IMAGE is a valid FilterControlType")
def static_image_is_valid_enum():
    """Verify STATIC_IMAGE enum exists."""
    from declaro_tablix.domain.filter_layout import FilterControlType

    assert hasattr(FilterControlType, "STATIC_IMAGE")
    assert FilterControlType.STATIC_IMAGE.value == "static_image"


@then("the config is valid")
def config_is_valid(filter_config):
    """Verify config can be created without errors."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig

    config = FilterControlConfig(**filter_config)
    assert config is not None


@then(parsers.parse('config.text_content equals "{expected_text}"'))
def verify_text_content(filter_config, expected_text):
    """Verify text_content field value."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig

    config = FilterControlConfig(**filter_config)
    assert config.text_content == expected_text


@then(parsers.parse('config.image_url equals "{expected_url}"'))
def verify_image_url(filter_config, expected_url):
    """Verify image_url field value."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig

    config = FilterControlConfig(**filter_config)
    assert config.image_url == expected_url


@then(parsers.parse('config.image_alt equals "{expected_alt}"'))
def verify_image_alt(filter_config, expected_alt):
    """Verify image_alt field value."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig

    config = FilterControlConfig(**filter_config)
    assert config.image_alt == expected_alt


@then(parsers.parse('the output contains "{expected_text}"'))
def output_contains_text(rendered_output, expected_text):
    """Verify output contains expected text."""
    html = rendered_output["html"]
    assert expected_text in html, \
        f"Text '{expected_text}' not found in output: {html}"


@then(parsers.parse('the output contains class "{expected_class}"'))
def output_contains_class(rendered_output, expected_class):
    """Verify output contains expected CSS class."""
    html = rendered_output["html"]
    # Check if class appears in a class attribute (may be combined with other classes)
    assert f'class="' in html and expected_class in html, \
        f"Class '{expected_class}' not found in output: {html}"


@then(parsers.parse('the output contains img element with src "{expected_src}"'))
def output_contains_img_with_src(rendered_output, expected_src):
    """Verify output contains img element with expected src."""
    html = rendered_output["html"]
    assert "<img" in html and f'src="{expected_src}"' in html, \
        f"img element with src '{expected_src}' not found in output: {html}"


@then(parsers.parse('the output contains alt "{expected_alt}"'))
def output_contains_alt(rendered_output, expected_alt):
    """Verify output contains expected alt text."""
    html = rendered_output["html"]
    assert f'alt="{expected_alt}"' in html, \
        f"Alt text '{expected_alt}' not found in output: {html}"


@then("the output does not contain input element")
def output_does_not_contain_input(rendered_output):
    """Verify no input elements are present."""
    html = rendered_output["html"]
    assert "<input" not in html, \
        f"Input element should not be present but found in: {html}"


@then("the output does not contain select element")
def output_does_not_contain_select(rendered_output):
    """Verify no select elements are present."""
    html = rendered_output["html"]
    assert "<select" not in html, \
        f"Select element should not be present but found in: {html}"
