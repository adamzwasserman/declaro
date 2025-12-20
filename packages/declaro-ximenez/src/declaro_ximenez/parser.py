"""Python source code parsing for declaro-ximenez."""

from __future__ import annotations

from pathlib import Path

import libcst as cst
from libcst.metadata import MetadataWrapper, PositionProvider

from .types import FunctionScope, Position
from .symbols import create_scope, declare_symbol


def parse_file(file_path: Path) -> cst.Module:
    """Parse a Python file into a CST.

    Args:
        file_path: Path to the Python file.

    Returns:
        The parsed CST module.

    Raises:
        SyntaxError: If the file cannot be parsed.
    """
    source = file_path.read_text()
    return cst.parse_module(source)


def parse_source(source: str) -> cst.Module:
    """Parse Python source code into a CST.

    Args:
        source: Python source code string.

    Returns:
        The parsed CST module.

    Raises:
        SyntaxError: If the source cannot be parsed.
    """
    return cst.parse_module(source)


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


def is_types_block(stmt: cst.BaseStatement) -> bool:
    """Check if a statement is a types: block.

    Args:
        stmt: The statement to check.

    Returns:
        True if this is a types: block.
    """
    # types: block looks like:
    # types:
    #     x: int
    #     y: str
    #
    # In CST this is a SimpleStatementLine with an Expr containing a Name "types"
    # followed by an IndentedBlock... but that's not valid Python!
    #
    # Actually, types: needs to be implemented as a special syntax extension
    # or we parse it as a labeled statement. For now, we'll look for:
    # if TYPE_CHECKING: block with special comment, or a custom pattern.
    #
    # TODO: Implement types: block detection - this is a syntax extension
    return False


def find_types_block(func: cst.FunctionDef) -> cst.BaseStatement | None:
    """Find a types: block in a function body.

    Args:
        func: The function definition node.

    Returns:
        The types: block statement if found, None otherwise.
    """
    body = func.body
    if not isinstance(body, cst.IndentedBlock):
        return None

    for stmt in body.body:
        if is_types_block(stmt):
            return stmt

    return None


def extract_types_block_declarations(
    block: cst.BaseStatement,
) -> list[tuple[str, str, bool, Position]]:
    """Extract variable declarations from a types: block.

    Args:
        block: The types: block statement.

    Returns:
        List of (name, type, has_initializer, position) tuples.
    """
    # TODO: Implement once types: block syntax is defined
    return []


def collect_function_scopes(module: cst.Module) -> list[FunctionScope]:
    """Collect all function scopes from a module.

    Args:
        module: The parsed CST module.

    Returns:
        List of function scopes with their declarations.
    """
    scopes: list[FunctionScope] = []

    class FunctionCollector(cst.CSTVisitor):
        def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
            scope = create_scope(node.name.value)

            # Extract params
            params, _ = extract_function_params(node)
            scope["params"] = params

            # Extract return type
            scope["return_type"] = extract_return_type(node)

            # Check for types: block
            types_block = find_types_block(node)
            if types_block:
                scope["has_types_block"] = True
                scope["style"] = "block"
                # Extract declarations from types: block
                # TODO: implement
            else:
                scope["style"] = "inline"

            scopes.append(scope)
            return True  # Continue visiting nested functions

    module.walk(FunctionCollector())
    return scopes
