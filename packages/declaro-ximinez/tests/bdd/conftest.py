"""BDD test fixtures for declaro-ximinez."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TypedDict

import pytest


class XiminezConfig(TypedDict, total=False):
    """Configuration for ximinez test runs."""

    enabled: bool
    stage: int
    allow_inline_style: bool
    allow_block_style: bool
    style_enforcement: str | None  # None, "inline", "block"
    paths: list[str]
    declaro_enabled: bool
    declaro_schema_paths: list[str]
    declaro_strict_models: bool


class XiminezResult(TypedDict):
    """Result from a ximinez check."""

    exit_code: int
    output: str
    violations: list[dict]
    violation_count: int


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def ximinez_config() -> XiminezConfig:
    """Default ximinez configuration."""
    return {
        "enabled": True,
        "stage": 1,
        "allow_inline_style": True,
        "allow_block_style": True,
        "style_enforcement": None,
        "paths": ["."],
        "declaro_enabled": False,
        "declaro_schema_paths": [],
        "declaro_strict_models": False,
    }


@pytest.fixture
def schema_files(temp_dir: Path) -> dict[str, str]:
    """Storage for TOML schema files created during tests."""
    return {}


@pytest.fixture
def python_file_content() -> dict[str, str]:
    """Storage for the current Python file content being tested."""
    return {"content": "", "path": ""}


@pytest.fixture
def ximinez_result() -> XiminezResult:
    """Storage for the result of a ximinez check."""
    return {
        "exit_code": -1,
        "output": "",
        "violations": [],
        "violation_count": 0,
    }


@pytest.fixture
def cli_flags() -> list[str]:
    """CLI flags to pass to ximinez."""
    return []


def create_python_file(temp_dir: Path, content: str, filename: str = "test_file.py") -> Path:
    """Create a Python file with the given content."""
    file_path = temp_dir / filename
    file_path.write_text(content)
    return file_path


def create_schema_file(temp_dir: Path, relative_path: str, content: str) -> Path:
    """Create a TOML schema file."""
    file_path = temp_dir / relative_path
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(content)
    return file_path


def parse_violations(output: str) -> list[dict]:
    """Parse violation messages from ximinez output."""
    violations = []
    for line in output.split("\n"):
        line = line.strip()
        if line.startswith("- ") and ":" in line:
            # Format: - file:line:col: message
            line = line[2:]  # Remove "- "
            parts = line.split(":", 3)
            if len(parts) >= 4:
                violations.append({
                    "file": parts[0],
                    "line": int(parts[1]) if parts[1].isdigit() else 0,
                    "col": int(parts[2]) if parts[2].isdigit() else 0,
                    "message": parts[3].strip(),
                })
    return violations


def run_ximinez(
    file_path: Path,
    config: XiminezConfig,
    flags: list[str] | None = None,
) -> XiminezResult:
    """
    Run ximinez on a file and return the result.
    """
    from declaro_ximinez.checker import check_file
    from declaro_ximinez.errors import (
        format_violations_standard,
        format_violations_inquisition,
        format_violations_quiet,
        format_violations_machine,
        format_comfy_chair,
        _get_text,
    )

    flags = flags or []

    # Convert test config to ximinez config format
    ximinez_config = {
        "enabled": config.get("enabled", True),
        "stage": config.get("stage", 1),
        "allow_inline_style": config.get("allow_inline_style", True),
        "allow_block_style": config.get("allow_block_style", True),
        "declaro_enabled": config.get("declaro_enabled", False),
        "declaro_schema_paths": config.get("declaro_schema_paths", []),
    }

    # Run the actual checker
    result = check_file(file_path, ximinez_config)
    violations = result["violations"]
    violation_count = len(violations)

    # Determine exit code
    # Exit code 2 for parse errors (XI000)
    has_parse_error = any(v.get("code") == "XI000" for v in violations)
    if has_parse_error:
        exit_code = 2
    elif violations:
        exit_code = 1
    else:
        exit_code = 0

    # Format output based on flags
    if "--machine" in flags:
        output = format_violations_machine(violations)
    elif "--quiet" in flags:
        output = format_violations_quiet(violations)
    elif "--comfy-chair" in flags:
        output = format_comfy_chair(violations)
        exit_code = 0  # Comfy chair never fails
    elif "--full" in flags:
        if violations:
            output = format_violations_inquisition(violations, "type")
        else:
            output = _get_text("dismissed")
    else:
        output = format_violations_standard(violations)

    return {
        "exit_code": exit_code,
        "output": output,
        "violations": violations,
        "violation_count": violation_count,
    }
