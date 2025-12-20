"""Common step definitions shared across features."""

from __future__ import annotations

import re
from pathlib import Path

from pytest_bdd import given, when, then, parsers

from ..conftest import (
    XimenezConfig,
    XimenezResult,
    create_python_file,
    create_schema_file,
    run_ximenez,
    parse_violations,
)


# ============================================================================
# GIVEN steps - Configuration
# ============================================================================


@given("ximenez is configured with inline style allowed")
def config_inline_allowed(ximenez_config: XimenezConfig):
    """Configure ximenez to allow inline style."""
    ximenez_config["allow_inline_style"] = True


@given("ximenez is configured with block style allowed")
def config_block_allowed(ximenez_config: XimenezConfig):
    """Configure ximenez to allow block style."""
    ximenez_config["allow_block_style"] = True


@given("ximenez is configured with style enforcement at module level")
def config_module_style_enforcement(ximenez_config: XimenezConfig):
    """Configure ximenez to enforce style at module level."""
    ximenez_config["style_enforcement"] = "module"


@given(parsers.parse('ximenez is configured with declaro schema path "{path}"'))
def config_declaro_schema(ximenez_config: XimenezConfig, path: str):
    """Configure ximenez with declaro schema path."""
    ximenez_config["declaro_enabled"] = True
    ximenez_config["declaro_schema_paths"] = [path]


# ============================================================================
# GIVEN steps - Files
# ============================================================================


@given(parsers.parse('a Python file with content:\n"""\n{content}\n"""'))
def create_python_file_step(
    temp_dir: Path,
    python_file_content: dict,
    content: str,
):
    """Create a Python file with the given content."""
    file_path = create_python_file(temp_dir, content)
    python_file_content["content"] = content
    python_file_content["path"] = str(file_path)


@given(parsers.parse('a TOML schema file "{path}" with content:\n"""\n{content}\n"""'))
def create_schema_file_step(
    temp_dir: Path,
    schema_files: dict,
    path: str,
    content: str,
):
    """Create a TOML schema file."""
    file_path = create_schema_file(temp_dir, path, content)
    schema_files[path] = str(file_path)


@given("the file has an unused import")
def add_unused_import(temp_dir: Path, python_file_content: dict):
    """Add an unused import to the current Python file."""
    content = python_file_content["content"]
    new_content = "import os  # unused\n" + content
    file_path = Path(python_file_content["path"])
    file_path.write_text(new_content)
    python_file_content["content"] = new_content


# ============================================================================
# WHEN steps - Running ximenez
# ============================================================================


@when("ximenez checks the file")
def run_ximenez_check(
    temp_dir: Path,
    python_file_content: dict,
    ximenez_config: XimenezConfig,
    ximenez_result: XimenezResult,
    cli_flags: list,
):
    """Run ximenez on the current Python file."""
    file_path = Path(python_file_content["path"])
    result = run_ximenez(file_path, ximenez_config, cli_flags)

    ximenez_result["exit_code"] = result["exit_code"]
    ximenez_result["output"] = result["output"]
    ximenez_result["violations"] = result["violations"]
    ximenez_result["violation_count"] = result["violation_count"]


@when(parsers.parse("ximenez checks the file with {flag} flag"))
def run_ximenez_with_flag(
    temp_dir: Path,
    python_file_content: dict,
    ximenez_config: XimenezConfig,
    ximenez_result: XimenezResult,
    cli_flags: list,
    flag: str,
):
    """Run ximenez with a specific CLI flag."""
    cli_flags.append(flag)
    file_path = Path(python_file_content["path"])
    result = run_ximenez(file_path, ximenez_config, cli_flags)

    ximenez_result["exit_code"] = result["exit_code"]
    ximenez_result["output"] = result["output"]
    ximenez_result["violations"] = result["violations"]
    ximenez_result["violation_count"] = result["violation_count"]


# ============================================================================
# THEN steps - Violation counts
# ============================================================================


@then("no violations are reported")
def check_no_violations(ximenez_result: XimenezResult):
    """Assert no violations were reported."""
    assert ximenez_result["violation_count"] == 0, (
        f"Expected no violations, got {ximenez_result['violation_count']}: "
        f"{ximenez_result['output']}"
    )


@then(parsers.parse("{count:d} violation is reported"))
def check_one_violation(ximenez_result: XimenezResult, count: int):
    """Assert exactly N violation was reported."""
    assert ximenez_result["violation_count"] == count, (
        f"Expected {count} violation(s), got {ximenez_result['violation_count']}: "
        f"{ximenez_result['output']}"
    )


@then(parsers.parse("{count:d} violations are reported"))
def check_violations_count(ximenez_result: XimenezResult, count: int):
    """Assert exactly N violations were reported."""
    assert ximenez_result["violation_count"] == count, (
        f"Expected {count} violations, got {ximenez_result['violation_count']}: "
        f"{ximenez_result['output']}"
    )


@then("the violation count is greater than 4")
def check_violations_greater_than_four(ximenez_result: XimenezResult):
    """Assert more than 4 violations were reported."""
    assert ximenez_result["violation_count"] > 4, (
        f"Expected more than 4 violations, got {ximenez_result['violation_count']}"
    )


# ============================================================================
# THEN steps - Output content
# ============================================================================


@then(parsers.parse('the output contains "{text}"'))
def check_output_contains(ximenez_result: XimenezResult, text: str):
    """Assert the output contains the given text."""
    assert text in ximenez_result["output"], (
        f"Expected output to contain '{text}', got:\n{ximenez_result['output']}"
    )


@then(parsers.parse('the output does not contain "{text}"'))
def check_output_not_contains(ximenez_result: XimenezResult, text: str):
    """Assert the output does not contain the given text."""
    assert text not in ximenez_result["output"], (
        f"Expected output NOT to contain '{text}', got:\n{ximenez_result['output']}"
    )


@then(parsers.parse('the violation message contains "{text}"'))
def check_violation_message_contains(ximenez_result: XimenezResult, text: str):
    """Assert at least one violation message contains the given text."""
    violations = ximenez_result["violations"]
    messages = [v["message"] for v in violations]
    assert any(text in msg for msg in messages), (
        f"Expected a violation message to contain '{text}', got: {messages}"
    )


@then("the output contains the violation count and locations only")
def check_quiet_output(ximenez_result: XimenezResult):
    """Assert output is minimal (quiet mode)."""
    output = ximenez_result["output"]
    # Should have count and file:line:col format, but no comedy
    assert "NOBODY" not in output
    assert "Inquisition" not in output
    # Should have location format
    assert re.search(r"\w+\.py:\d+:\d+", output), (
        f"Expected file:line:col format in output:\n{output}"
    )


@then(parsers.parse('the output matches pattern "{pattern}"'))
def check_output_matches_pattern(ximenez_result: XimenezResult, pattern: str):
    """Assert the output matches the given regex pattern."""
    assert re.search(pattern, ximenez_result["output"]), (
        f"Expected output to match pattern '{pattern}', got:\n{ximenez_result['output']}"
    )


@then(parsers.parse('the output contains "{text}" instead of "{other}"'))
def check_output_contains_instead_of(ximenez_result: XimenezResult, text: str, other: str):
    """Assert output contains one text but not another."""
    output = ximenez_result["output"]
    assert text in output, f"Expected '{text}' in output"
    assert other not in output, f"Expected '{other}' NOT in output"


# ============================================================================
# THEN steps - Exit codes
# ============================================================================


@then(parsers.parse("the exit code is {code:d}"))
def check_exit_code(ximenez_result: XimenezResult, code: int):
    """Assert the exit code matches."""
    assert ximenez_result["exit_code"] == code, (
        f"Expected exit code {code}, got {ximenez_result['exit_code']}"
    )
