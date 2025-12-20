"""Configuration loading for declaro-ximenez."""

from __future__ import annotations

import tomllib
from pathlib import Path

from .types import XimenezConfig


DEFAULT_CONFIG: XimenezConfig = {
    "enabled": True,
    "stage": 1,
    "allow_inline_style": True,
    "allow_block_style": True,
    "style_enforcement": None,
    "paths": ["src/", "tests/"],
    "declaro_enabled": False,
    "declaro_schema_paths": [],
    "declaro_strict_models": False,
}


def load_config(project_root: Path | None = None) -> XimenezConfig:
    """Load ximenez configuration from pyproject.toml.

    Args:
        project_root: Root directory to search for pyproject.toml.
                     Defaults to current directory.

    Returns:
        Merged configuration with defaults.
    """
    if project_root is None:
        project_root = Path.cwd()

    pyproject_path = project_root / "pyproject.toml"

    if not pyproject_path.exists():
        return DEFAULT_CONFIG.copy()

    return load_config_from_file(pyproject_path)


def load_config_from_file(pyproject_path: Path) -> XimenezConfig:
    """Load ximenez configuration from a specific pyproject.toml.

    Args:
        pyproject_path: Path to pyproject.toml.

    Returns:
        Merged configuration with defaults.
    """
    with open(pyproject_path, "rb") as f:
        data = tomllib.load(f)

    config = DEFAULT_CONFIG.copy()

    tool_config = data.get("tool", {}).get("ximenez", {})

    # Merge tool.ximenez settings
    if "enabled" in tool_config:
        config["enabled"] = tool_config["enabled"]
    if "stage" in tool_config:
        config["stage"] = tool_config["stage"]
    if "allow_inline_style" in tool_config:
        config["allow_inline_style"] = tool_config["allow_inline_style"]
    if "allow_block_style" in tool_config:
        config["allow_block_style"] = tool_config["allow_block_style"]
    if "style_enforcement" in tool_config:
        config["style_enforcement"] = tool_config["style_enforcement"]
    if "paths" in tool_config:
        config["paths"] = tool_config["paths"]

    # Merge tool.ximenez.declaro settings
    declaro_config = tool_config.get("declaro", {})
    if "enabled" in declaro_config:
        config["declaro_enabled"] = declaro_config["enabled"]
    if "schema_paths" in declaro_config:
        config["declaro_schema_paths"] = declaro_config["schema_paths"]
    if "strict_models" in declaro_config:
        config["declaro_strict_models"] = declaro_config["strict_models"]

    return config


def parse_file_directive(source: str) -> dict[str, str]:
    """Parse ximenez directives from file comments.

    Looks for comments like:
        # ximenez: enable, style=block
        # ximenez: declaro-model=User

    Args:
        source: Python source code.

    Returns:
        Dictionary of directive key-value pairs.
    """
    directives: dict[str, str] = {}

    for line in source.split("\n"):
        line = line.strip()
        if not line.startswith("# ximenez:"):
            continue

        # Parse the directive
        directive_str = line[10:].strip()  # Remove "# ximenez:"

        for part in directive_str.split(","):
            part = part.strip()
            if "=" in part:
                key, value = part.split("=", 1)
                directives[key.strip()] = value.strip()
            else:
                directives[part] = "true"

    return directives


def merge_file_directives(
    config: XimenezConfig,
    directives: dict[str, str],
) -> XimenezConfig:
    """Merge file-level directives into configuration.

    Args:
        config: Base configuration.
        directives: File-level directives.

    Returns:
        New configuration with directives applied.
    """
    result = config.copy()

    if directives.get("enable") == "true":
        result["enabled"] = True
    if directives.get("disable") == "true":
        result["enabled"] = False

    if "style" in directives:
        style = directives["style"]
        if style == "inline":
            result["allow_inline_style"] = True
            result["allow_block_style"] = False
        elif style == "block":
            result["allow_inline_style"] = False
            result["allow_block_style"] = True

    return result
