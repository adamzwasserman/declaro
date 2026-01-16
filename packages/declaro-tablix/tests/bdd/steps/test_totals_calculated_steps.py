"""Step definitions for totals and calculated fields feature."""

import pytest
from pytest_bdd import given, when, then, scenarios, parsers

# Load scenarios from feature file
scenarios("../features/totals_calculated.feature")


# Fixtures
@pytest.fixture
def filter_config():
    """Fixture to hold filter config between steps."""
    return {}


@pytest.fixture
def calculation_context():
    """Fixture to hold calculation context data."""
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
    filter_config["id"] = "test_total"
    filter_config["column_id"] = "total_column"


@given(parsers.parse('source_field is "{field}"'))
def set_source_field(filter_config, field):
    """Set source field."""
    filter_config["source_field"] = field


@given(parsers.parse('format is "{fmt}"'))
def set_format(filter_config, fmt):
    """Set format field."""
    filter_config["format"] = fmt


@given(parsers.parse('label is "{label}"'))
def set_label(filter_config, label):
    """Set label field."""
    filter_config["label"] = label


@given(parsers.parse('formula is "{formula}"'))
def set_formula(filter_config, formula):
    """Set formula field."""
    filter_config["formula"] = formula


@given("badge_thresholds are defined")
def set_badge_thresholds(filter_config):
    """Set badge thresholds."""
    filter_config["badge_thresholds"] = [
        {"min": 0, "max": 50, "css_class": "bg-danger"},
        {"min": 50, "max": 80, "css_class": "bg-warning"},
        {"min": 80, "max": 100, "css_class": "bg-success"},
    ]


@given("badge_thresholds define danger below 50, warning 50-80, success above 80")
def set_badge_thresholds_detailed(filter_config):
    """Set badge thresholds with detailed ranges."""
    filter_config["badge_thresholds"] = [
        {"min": 0, "max": 50, "css_class": "bg-danger"},
        {"min": 50, "max": 80, "css_class": "bg-warning"},
        {"min": 80, "max": 100, "css_class": "bg-success"},
    ]


@given(parsers.parse('total value is {value:d}'))
def set_total_value(calculation_context, value):
    """Set the total value for rendering."""
    calculation_context["total_value"] = value


@given(parsers.parse('calculation context has reviewed_count={reviewed:d} and total_count={total:d}'))
def set_calculation_context_reviewed(calculation_context, reviewed, total):
    """Set calculation context with reviewed and total counts."""
    calculation_context["reviewed_count"] = reviewed
    calculation_context["total_count"] = total


@given(parsers.parse('calculation context has value={value:d}'))
def set_calculation_context_value(calculation_context, value):
    """Set calculation context with single value."""
    calculation_context["value"] = value


# When steps
@when("I check FilterControlType enum values")
def check_enum_values():
    """Just trigger enum check - actual assertion in then step."""
    pass


@when("the total field is rendered")
def render_total_field(filter_config, calculation_context, rendered_output):
    """Render the total field control."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig
    from declaro_tablix.templates import get_jinja_env

    config = FilterControlConfig(**filter_config)
    env = get_jinja_env()
    template = env.get_template("components/filters/total_field.html")

    # Format the total value based on format type
    total_value = calculation_context.get("total_value", 0)
    formatted_value = format_value(total_value, config.format)

    rendered_output["html"] = template.render(
        control=config,
        total_value=formatted_value
    )


@when("the calculated field is rendered")
def render_calculated_field(filter_config, calculation_context, rendered_output):
    """Render the calculated field control."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig
    from declaro_tablix.templates import get_jinja_env

    config = FilterControlConfig(**filter_config)
    env = get_jinja_env()
    template = env.get_template("components/filters/calculated_field.html")

    # Evaluate the formula with context
    calculated_value = evaluate_formula(config.formula, calculation_context)
    formatted_value = format_value(calculated_value, config.format)

    # Determine badge class if thresholds defined
    badge_class = None
    if config.badge_thresholds:
        badge_class = get_badge_class(calculated_value, config.badge_thresholds)

    rendered_output["html"] = template.render(
        control=config,
        calculated_value=formatted_value,
        badge_class=badge_class
    )


# Then steps
@then("TOTAL_ABSOLUTE is a valid FilterControlType")
def total_absolute_is_valid_enum():
    """Verify TOTAL_ABSOLUTE enum exists."""
    from declaro_tablix.domain.filter_layout import FilterControlType

    assert hasattr(FilterControlType, "TOTAL_ABSOLUTE")
    assert FilterControlType.TOTAL_ABSOLUTE.value == "total_absolute"


@then("TOTAL_VISIBLE is a valid FilterControlType")
def total_visible_is_valid_enum():
    """Verify TOTAL_VISIBLE enum exists."""
    from declaro_tablix.domain.filter_layout import FilterControlType

    assert hasattr(FilterControlType, "TOTAL_VISIBLE")
    assert FilterControlType.TOTAL_VISIBLE.value == "total_visible"


@then("CALCULATED_FIELD is a valid FilterControlType")
def calculated_field_is_valid_enum():
    """Verify CALCULATED_FIELD enum exists."""
    from declaro_tablix.domain.filter_layout import FilterControlType

    assert hasattr(FilterControlType, "CALCULATED_FIELD")
    assert FilterControlType.CALCULATED_FIELD.value == "calculated_field"


@then("the config is valid")
def config_is_valid(filter_config):
    """Verify config can be created without errors."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig

    config = FilterControlConfig(**filter_config)
    assert config is not None


@then(parsers.parse('config.source_field equals "{expected_field}"'))
def verify_source_field(filter_config, expected_field):
    """Verify source_field value."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig

    config = FilterControlConfig(**filter_config)
    assert config.source_field == expected_field


@then(parsers.parse('config.format equals "{expected_format}"'))
def verify_format(filter_config, expected_format):
    """Verify format value."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig

    config = FilterControlConfig(**filter_config)
    assert config.format == expected_format


@then(parsers.parse('config.formula equals "{expected_formula}"'))
def verify_formula(filter_config, expected_formula):
    """Verify formula value."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig

    config = FilterControlConfig(**filter_config)
    assert config.formula == expected_formula


@then(parsers.parse('config.badge_thresholds has {count:d} entries'))
def verify_badge_thresholds_count(filter_config, count):
    """Verify badge_thresholds has expected number of entries."""
    from declaro_tablix.domain.filter_layout import FilterControlConfig

    config = FilterControlConfig(**filter_config)
    assert len(config.badge_thresholds) == count


@then(parsers.parse('the output contains "{expected_text}"'))
def output_contains_text(rendered_output, expected_text):
    """Verify output contains expected text."""
    html = rendered_output["html"]
    assert expected_text in html, \
        f"Text '{expected_text}' not found in output: {html}"


@then(parsers.parse('the output contains badge with class "{expected_class}"'))
def output_contains_badge_class(rendered_output, expected_class):
    """Verify output contains badge with expected class."""
    html = rendered_output["html"]
    assert "badge" in html.lower() and expected_class in html, \
        f"Badge with class '{expected_class}' not found in output: {html}"


# Helper functions
def format_value(value: float, format_type: str | None) -> str:
    """Format a numeric value based on format type."""
    if format_type == "currency":
        return f"${value:,.0f}"
    elif format_type == "percentage":
        return f"{value:.0f}%"
    elif format_type == "number":
        return f"{value:,.0f}"
    else:
        return str(value)


def evaluate_formula(formula: str | None, context: dict) -> float:
    """Evaluate a formula with given context.

    Formula format: {var1} + {var2} * 100, etc.
    """
    if not formula:
        return 0.0

    # Replace variables in formula
    result = formula
    for key, value in context.items():
        result = result.replace(f"{{{key}}}", str(value))

    # Safely evaluate the expression
    try:
        return float(eval(result))
    except Exception:
        return 0.0


def get_badge_class(value: float, thresholds: list[dict]) -> str | None:
    """Get badge CSS class based on value and thresholds."""
    for threshold in thresholds:
        min_val = threshold.get("min", float("-inf"))
        max_val = threshold.get("max", float("inf"))
        if min_val <= value < max_val:
            return threshold.get("css_class")
    return None
