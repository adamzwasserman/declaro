"""
declaro-ximinez: Type enforcement with memorable errors.

Nobody expects the Ximínez Inquisition!
"""

from .types import (
    Violation,
    Symbol,
    FunctionScope,
    CheckResult,
    XiminezConfig,
    Model,
    ModelField,
    ModelRelationship,
)
from .checker import check_file, check_source, check_module
from .config import load_config, load_config_from_file
from .errors import (
    format_violations_inquisition,
    format_violations_quiet,
    format_violations_machine,
    format_comfy_chair,
    suggest_similar,
)
from .cli import main

__version__ = "0.1.0"

__all__ = [
    # Types
    "Violation",
    "Symbol",
    "FunctionScope",
    "CheckResult",
    "XiminezConfig",
    "Model",
    "ModelField",
    "ModelRelationship",
    # Checking
    "check_file",
    "check_source",
    "check_module",
    # Config
    "load_config",
    "load_config_from_file",
    # Formatting
    "format_violations_inquisition",
    "format_violations_quiet",
    "format_violations_machine",
    "format_comfy_chair",
    "suggest_similar",
    # CLI
    "main",
]
