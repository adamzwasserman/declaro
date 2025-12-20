"""Common step definitions shared across features."""

from __future__ import annotations

import re
from pathlib import Path

from pytest_bdd import given, when, then, parsers

from ..conftest import (
    XiminezConfig,
    XiminezResult,
    create_python_file,
    create_schema_file,
    run_ximinez,
    parse_violations,
)


# ============================================================================
# GIVEN steps - Configuration
# ============================================================================


@given("ximinez is configured with inline style allowed")
def config_inline_allowed(ximinez_config: XiminezConfig):
    """Configure ximinez to allow inline style."""
    ximinez_config["allow_inline_style"] = True


@given("ximinez is configured with block style allowed")
def config_block_allowed(ximinez_config: XiminezConfig):
    """Configure ximinez to allow block style."""
    ximinez_config["allow_block_style"] = True


@given("ximinez is configured with style enforcement at module level")
def config_module_style_enforcement(ximinez_config: XiminezConfig):
    """Configure ximinez to enforce style at module level."""
    ximinez_config["style_enforcement"] = "module"


@given(parsers.parse('ximinez is configured with declaro schema path "{path}"'))
def config_declaro_schema(ximinez_config: XiminezConfig, temp_dir: Path, path: str):
    """Configure ximinez with declaro schema path."""
    ximinez_config["declaro_enabled"] = True
    # Store both the relative path and the temp_dir for path resolution
    full_path = temp_dir / path
    ximinez_config["declaro_schema_paths"] = [str(full_path)]


# ============================================================================
# GIVEN steps - Files
# ============================================================================


@given("a Python file with content:")
def create_python_file_step(
    temp_dir: Path,
    python_file_content: dict,
    docstring: str,
):
    """Create a Python file with the given content.

    The 'docstring' parameter is automatically populated by pytest-bdd
    with the docstring content following the step.
    """
    file_path = create_python_file(temp_dir, docstring)
    python_file_content["content"] = docstring
    python_file_content["path"] = str(file_path)


@given(parsers.parse('a TOML schema file "{path}" with content:'))
def create_schema_file_step(
    temp_dir: Path,
    schema_files: dict,
    path: str,
    docstring: str,
):
    """Create a TOML schema file.

    The 'docstring' parameter is automatically populated by pytest-bdd
    with the docstring content following the step.
    """
    file_path = create_schema_file(temp_dir, path, docstring)
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
# WHEN steps - Running ximinez
# ============================================================================


@when("ximinez checks the file")
def run_ximinez_check(
    temp_dir: Path,
    python_file_content: dict,
    ximinez_config: XiminezConfig,
    ximinez_result: XiminezResult,
    cli_flags: list,
):
    """Run ximinez on the current Python file."""
    file_path = Path(python_file_content["path"])
    result = run_ximinez(file_path, ximinez_config, cli_flags)

    ximinez_result["exit_code"] = result["exit_code"]
    ximinez_result["output"] = result["output"]
    ximinez_result["violations"] = result["violations"]
    ximinez_result["violation_count"] = result["violation_count"]


@when(parsers.parse("ximinez checks the file with {flag} flag"))
def run_ximinez_with_flag(
    temp_dir: Path,
    python_file_content: dict,
    ximinez_config: XiminezConfig,
    ximinez_result: XiminezResult,
    cli_flags: list,
    flag: str,
):
    """Run ximinez with a specific CLI flag."""
    cli_flags.append(flag)
    file_path = Path(python_file_content["path"])
    result = run_ximinez(file_path, ximinez_config, cli_flags)

    ximinez_result["exit_code"] = result["exit_code"]
    ximinez_result["output"] = result["output"]
    ximinez_result["violations"] = result["violations"]
    ximinez_result["violation_count"] = result["violation_count"]


# ============================================================================
# THEN steps - Violation counts
# ============================================================================


@then("no violations are reported")
def check_no_violations(ximinez_result: XiminezResult):
    """Assert no violations were reported."""
    assert ximinez_result["violation_count"] == 0, (
        f"Expected no violations, got {ximinez_result['violation_count']}: "
        f"{ximinez_result['output']}"
    )


@then(parsers.parse("{count:d} violation is reported"))
def check_one_violation(ximinez_result: XiminezResult, count: int):
    """Assert exactly N violation was reported."""
    assert ximinez_result["violation_count"] == count, (
        f"Expected {count} violation(s), got {ximinez_result['violation_count']}: "
        f"{ximinez_result['output']}"
    )


@then(parsers.parse("{count:d} violations are reported"))
def check_violations_count(ximinez_result: XiminezResult, count: int):
    """Assert exactly N violations were reported."""
    assert ximinez_result["violation_count"] == count, (
        f"Expected {count} violations, got {ximinez_result['violation_count']}: "
        f"{ximinez_result['output']}"
    )


@then("the violation count is greater than 4")
def check_violations_greater_than_four(ximinez_result: XiminezResult):
    """Assert more than 4 violations were reported."""
    assert ximinez_result["violation_count"] > 4, (
        f"Expected more than 4 violations, got {ximinez_result['violation_count']}"
    )


# ============================================================================
# THEN steps - Output content
# ============================================================================


@then(parsers.parse('the output contains "{text}"'))
def check_output_contains(ximinez_result: XiminezResult, text: str):
    """Assert the output contains the given text."""
    assert text in ximinez_result["output"], (
        f"Expected output to contain '{text}', got:\n{ximinez_result['output']}"
    )


@then(parsers.parse('the output does not contain "{text}"'))
def check_output_not_contains(ximinez_result: XiminezResult, text: str):
    """Assert the output does not contain the given text."""
    assert text not in ximinez_result["output"], (
        f"Expected output NOT to contain '{text}', got:\n{ximinez_result['output']}"
    )


@then(parsers.parse('the violation message contains "{text}"'))
def check_violation_message_contains(ximinez_result: XiminezResult, text: str):
    """Assert at least one violation message contains the given text."""
    violations = ximinez_result["violations"]
    messages = [v["message"] for v in violations]
    assert any(text in msg for msg in messages), (
        f"Expected a violation message to contain '{text}', got: {messages}"
    )


@then("the output contains the violation count and locations only")
def check_quiet_output(ximinez_result: XiminezResult):
    """Assert output is minimal (quiet mode)."""
    output = ximinez_result["output"]
    # Should have count and file:line:col format, but no comedy
    assert "NOBODY" not in output
    assert "Inquisition" not in output
    # Should have location format
    assert re.search(r"\w+\.py:\d+:\d+", output), (
        f"Expected file:line:col format in output:\n{output}"
    )


@then(parsers.parse('the output matches pattern "{pattern}"'))
def check_output_matches_pattern(ximinez_result: XiminezResult, pattern: str):
    """Assert the output matches the given regex pattern."""
    assert re.search(pattern, ximinez_result["output"]), (
        f"Expected output to match pattern '{pattern}', got:\n{ximinez_result['output']}"
    )


@then(parsers.parse('the output contains "{text}" instead of "{other}"'))
def check_output_contains_instead_of(ximinez_result: XiminezResult, text: str, other: str):
    """Assert output contains one text but not another."""
    output = ximinez_result["output"]
    assert text in output, f"Expected '{text}' in output"
    assert other not in output, f"Expected '{other}' NOT in output"


# ============================================================================
# THEN steps - Exit codes
# ============================================================================


@then(parsers.parse("the exit code is {code:d}"))
def check_exit_code(ximinez_result: XiminezResult, code: int):
    """Assert the exit code matches."""
    assert ximinez_result["exit_code"] == code, (
        f"Expected exit code {code}, got {ximinez_result['exit_code']}"
    )
