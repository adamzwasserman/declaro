"""Python source code parsing for declaro-ximinez."""

from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass

import libcst as cst
from libcst.metadata import MetadataWrapper, PositionProvider

from .types import FunctionScope, Position
from .symbols import create_scope, declare_symbol
from .preprocessor import preprocess_file, preprocess_source as preprocess, PreprocessResult


@dataclass
class ParseResult:
    """Result of parsing a file with preprocessing."""

    module: cst.Module
    preprocess_result: PreprocessResult


def parse_file(file_path: Path) -> ParseResult:
    """Parse a Python file into a CST, preprocessing types: blocks.

    Args:
        file_path: Path to the Python file.

    Returns:
        ParseResult with CST module and preprocessing metadata.

    Raises:
        SyntaxError: If the file cannot be parsed.
    """
    preprocess_result = preprocess_file(file_path)
    module = cst.parse_module(preprocess_result.source)
    return ParseResult(module=module, preprocess_result=preprocess_result)


def parse_source(source: str) -> ParseResult:
    """Parse Python source code into a CST, preprocessing types: blocks.

    Args:
        source: Python source code string.

    Returns:
        ParseResult with CST module and preprocessing metadata.

    Raises:
        SyntaxError: If the source cannot be parsed.
    """
    preprocess_result = preprocess(source)
    module = cst.parse_module(preprocess_result.source)
    return ParseResult(module=module, preprocess_result=preprocess_result)


def get_position(node: cst.CSTNode, wrapper: MetadataWrapper) -> Position:
    """Get the source position of a CST node.

    Args:
        node: The CST node.
        wrapper: The metadata wrapper with position info.

    Returns:
        Position with line and column.
    """
    pos = wrapper.resolve(PositionProvider)[node]
    return {"line": pos.start.line, "col": pos.start.column}


def extract_type_annotation(annotation: cst.Annotation | None) -> str | None:
    """Extract type annotation as a string.

    Args:
        annotation: The CST annotation node.

    Returns:
        The annotation as a string, or None if no annotation.
    """
    if annotation is None:
        return None
    # Convert the annotation node back to source code
    return cst.parse_module("").code_for_node(annotation.annotation)


def extract_function_params(
    func: cst.FunctionDef,
) -> tuple[dict[str, str], list[str]]:
    """Extract parameter names and types from a function definition.

    Args:
        func: The function definition node.

    Returns:
        Tuple of (params dict mapping name to type, list of untyped param names).
    """
    params: dict[str, str] = {}
    untyped: list[str] = []

    for param in func.params.params:
        name = param.name.value
        if param.annotation is not None:
            type_str = extract_type_annotation(param.annotation)
            if type_str:
                params[name] = type_str
        else:
            untyped.append(name)

    # Handle *args
    if func.params.star_arg and isinstance(func.params.star_arg, cst.Param):
        name = func.params.star_arg.name.value
        if func.params.star_arg.annotation is not None:
            type_str = extract_type_annotation(func.params.star_arg.annotation)
            if type_str:
                params[name] = type_str
        else:
            untyped.append(name)

    # Handle **kwargs
    if func.params.star_kwarg:
        name = func.params.star_kwarg.name.value
        if func.params.star_kwarg.annotation is not None:
            type_str = extract_type_annotation(func.params.star_kwarg.annotation)
            if type_str:
                params[name] = type_str
        else:
            untyped.append(name)

    return params, untyped


def extract_return_type(func: cst.FunctionDef) -> str | None:
    """Extract return type annotation from a function definition.

    Args:
        func: The function definition node.

    Returns:
        The return type as a string, or None if not annotated.
    """
    if func.returns is None:
        return None
    return extract_type_annotation(func.returns)


def collect_function_scopes(
    module: cst.Module,
    preprocess_result: PreprocessResult | None = None,
) -> list[FunctionScope]:
    """Collect all function scopes from a module.

    Args:
        module: The parsed CST module.
        preprocess_result: Optional preprocessing result with types: block info.

    Returns:
        List of function scopes with their declarations.
    """
    import re

    scopes: list[FunctionScope] = []

    # Build a list of types: blocks that haven't been matched yet
    # Each entry is (indent_len, block)
    unmatched_blocks: list[tuple[int, object]] = []
    if preprocess_result:
        for block in preprocess_result.types_blocks:
            indent_len = len(block.indent)
            unmatched_blocks.append((indent_len, block))
    # Sort by line number so we match in order
    unmatched_blocks.sort(key=lambda x: x[1].start_line)

    class FunctionCollector(cst.CSTVisitor):
        def __init__(self) -> None:
            self.current_indent = 0

        def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
            nonlocal unmatched_blocks

            scope = create_scope(node.name.value)

            # Extract params
            params, _ = extract_function_params(node)
            scope["params"] = params

            # Extract return type
            scope["return_type"] = extract_return_type(node)

            # Check if this function had a types: block based on indent matching
            # Only match the first unmatched block at the correct indent level
            has_types_block = False
            body_indent = self.current_indent + 4  # Assumes 4-space indent

            matched_idx = None
            for idx, (indent_len, block) in enumerate(unmatched_blocks):
                if indent_len == body_indent:
                    has_types_block = True
                    matched_idx = idx
                    # Extract symbols from the types: block declarations
                    for decl_text, decl_line in block.declarations:
                        # Parse declaration: "name: type" or "name: type = value"
                        match = re.match(r'\s*(\w+)\s*:\s*([^=]+)', decl_text)
                        if match:
                            var_name = match.group(1)
                            var_type = match.group(2).strip()
                            declare_symbol(scope, var_name, var_type, decl_line, 0)
                    break  # Only match the first one

            # Remove matched block from unmatched list
            if matched_idx is not None:
                unmatched_blocks.pop(matched_idx)

            if has_types_block:
                scope["has_types_block"] = True
                scope["style"] = "block"
            else:
                scope["style"] = "inline"

            scopes.append(scope)
            self.current_indent += 4
            return True  # Continue visiting nested functions

        def leave_FunctionDef(self, node: cst.FunctionDef) -> None:
            self.current_indent -= 4

    # Use MetadataWrapper for visiting
    wrapper = MetadataWrapper(module)
    wrapper.visit(FunctionCollector())
    return scopes
