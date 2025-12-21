"""Type definitions for declaro-ximinez."""

from __future__ import annotations

from typing import TypedDict, Literal


class Position(TypedDict):
    """Source code position."""

    line: int
    col: int


class Violation(TypedDict):
    """A type violation found during checking."""

    file: str
    line: int
    col: int
    message: str
    code: str  # XI001, XIM001, etc.


class Symbol(TypedDict):
    """A declared symbol in a function scope."""

    name: str
    type_annotation: str
    line: int
    col: int
    initialized: bool
    used: bool


class FunctionScope(TypedDict):
    """Scope information for a function."""

    name: str
    style: Literal["inline", "block", "none"]
    symbols: dict[str, Symbol]
    has_types_block: bool
    return_type: str | None
    params: dict[str, str]  # param_name -> type_annotation


class CheckResult(TypedDict):
    """Result of checking a file."""

    file: str
    violations: list[Violation]
    scopes: list[FunctionScope]


class XiminezConfig(TypedDict, total=False):
    """Configuration for ximinez."""

    enabled: bool
    stage: int
    allow_inline_style: bool
    allow_block_style: bool
    style_enforcement: Literal["module", "function"] | None
    paths: list[str]
    declaro_enabled: bool
    declaro_schema_paths: list[str]
    declaro_model_paths: list[str]  # Python module paths with @table models
    declaro_strict_models: bool


class ModelField(TypedDict):
    """A field in a Declaro model."""

    name: str
    type: str
    nullable: bool
    validate: list[str]


class ModelRelationship(TypedDict):
    """A relationship in a Declaro model."""

    name: str
    type: Literal["has_one", "has_many", "belongs_to"]
    target: str
    foreign_key: str


class Model(TypedDict):
    """A Declaro model loaded from TOML."""

    name: str
    table: str
    fields: dict[str, ModelField]
    relationships: dict[str, ModelRelationship]
