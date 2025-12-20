"""Type checking logic for declaro-ximenez."""

from __future__ import annotations

from pathlib import Path

import libcst as cst
from libcst.metadata import MetadataWrapper, PositionProvider

from .types import Violation, FunctionScope, CheckResult, XimenezConfig
from .parser import (
    parse_file,
    parse_source,
    collect_function_scopes,
    extract_function_params,
    extract_return_type,
    get_position,
)
from .symbols import (
    lookup_symbol,
    mark_symbol_used,
    get_undeclared_symbols,
    get_unused_symbols,
)


def check_file(file_path: Path, config: XimenezConfig) -> CheckResult:
    """Check a Python file for type violations.

    Args:
        file_path: Path to the Python file.
        config: Ximenez configuration.

    Returns:
        CheckResult with violations and scope info.
    """
    try:
        module = parse_file(file_path)
    except SyntaxError as e:
        return {
            "file": str(file_path),
            "violations": [{
                "file": str(file_path),
                "line": e.lineno or 1,
                "col": e.offset or 0,
                "message": str(e.msg),
                "code": "XI000",
            }],
            "scopes": [],
        }

    return check_module(module, str(file_path), config)


def check_source(source: str, filename: str, config: XimenezConfig) -> CheckResult:
    """Check Python source code for type violations.

    Args:
        source: Python source code string.
        filename: Filename to use in error messages.
        config: Ximenez configuration.

    Returns:
        CheckResult with violations and scope info.
    """
    try:
        module = parse_source(source)
    except SyntaxError as e:
        return {
            "file": filename,
            "violations": [{
                "file": filename,
                "line": e.lineno or 1,
                "col": e.offset or 0,
                "message": str(e.msg),
                "code": "XI000",
            }],
            "scopes": [],
        }

    return check_module(module, filename, config)


def check_module(
    module: cst.Module,
    filename: str,
    config: XimenezConfig,
) -> CheckResult:
    """Check a parsed module for type violations.

    Args:
        module: The parsed CST module.
        filename: Filename to use in error messages.
        config: Ximenez configuration.

    Returns:
        CheckResult with violations and scope info.
    """
    violations: list[Violation] = []
    wrapper = MetadataWrapper(module)

    # Collect function scopes
    scopes = collect_function_scopes(module)

    # Check each function
    for scope in scopes:
        func_violations = check_function(module, wrapper, scope, filename, config)
        violations.extend(func_violations)

    return {
        "file": filename,
        "violations": violations,
        "scopes": scopes,
    }


def check_function(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
    filename: str,
    config: XimenezConfig,
) -> list[Violation]:
    """Check a single function for type violations.

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to check.
        filename: Filename to use in error messages.
        config: Ximenez configuration.

    Returns:
        List of violations found in this function.
    """
    violations: list[Violation] = []

    # Check for missing return type
    if scope["return_type"] is None:
        violations.append({
            "file": filename,
            "line": 1,  # TODO: get actual line from function def
            "col": 0,
            "message": f"missing return type annotation for function '{scope['name']}'",
            "code": "XI001",
        })

    # Check for untyped parameters
    violations.extend(
        check_untyped_params(module, wrapper, scope, filename)
    )

    # Check for undeclared locals
    violations.extend(
        check_undeclared_locals(module, wrapper, scope, filename)
    )

    # Check for unused declarations (in block style)
    if scope["has_types_block"]:
        violations.extend(
            check_unused_declarations(scope, filename)
        )

    # Check for style mixing
    violations.extend(
        check_style_mixing(module, wrapper, scope, filename, config)
    )

    return violations


def check_untyped_params(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
    filename: str,
) -> list[Violation]:
    """Check for untyped function parameters.

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to check.
        filename: Filename to use in error messages.

    Returns:
        List of violations for untyped parameters.
    """
    violations: list[Violation] = []

    # Find the function definition to get positions
    class ParamChecker(cst.CSTVisitor):
        def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
            if node.name.value != scope["name"]:
                return True

            for param in node.params.params:
                if param.annotation is None:
                    pos = get_position(param, wrapper)
                    violations.append({
                        "file": filename,
                        "line": pos["line"],
                        "col": pos["col"],
                        "message": f"missing type annotation for parameter '{param.name.value}'",
                        "code": "XI002",
                    })

            return False  # Don't check nested functions in this pass

    module.walk(ParamChecker())
    return violations


def check_undeclared_locals(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
    filename: str,
) -> list[Violation]:
    """Check for local variables used without declaration.

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to check.
        filename: Filename to use in error messages.

    Returns:
        List of violations for undeclared locals.
    """
    violations: list[Violation] = []

    class LocalChecker(cst.CSTVisitor):
        def __init__(self) -> None:
            self.in_target_func = False
            self.assigned_names: dict[str, tuple[int, int]] = {}  # name -> (line, col)
            self.annotated_names: set[str] = set()

        def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
            if node.name.value == scope["name"]:
                self.in_target_func = True
                return True
            elif self.in_target_func:
                # Nested function - skip it
                return False
            return True

        def leave_FunctionDef(self, node: cst.FunctionDef) -> None:
            if node.name.value == scope["name"]:
                self.in_target_func = False

        def visit_AnnAssign(self, node: cst.AnnAssign) -> bool:
            if not self.in_target_func:
                return True

            if isinstance(node.target, cst.Name):
                self.annotated_names.add(node.target.value)
            return True

        def visit_Assign(self, node: cst.Assign) -> bool:
            if not self.in_target_func:
                return True

            for target in node.targets:
                if isinstance(target.target, cst.Name):
                    name = target.target.value
                    if name not in self.annotated_names and name not in scope["params"]:
                        if name not in self.assigned_names:
                            pos = get_position(target.target, wrapper)
                            self.assigned_names[name] = (pos["line"], pos["col"])
            return True

    checker = LocalChecker()
    module.walk(checker)

    # Report violations for assignments without prior annotation
    for name, (line, col) in checker.assigned_names.items():
        if name not in scope["symbols"] and name not in scope["params"]:
            violations.append({
                "file": filename,
                "line": line,
                "col": col,
                "message": f"local variable '{name}' used without type declaration",
                "code": "XI003",
            })

    return violations


def check_unused_declarations(
    scope: FunctionScope,
    filename: str,
) -> list[Violation]:
    """Check for declared but unused variables in block style.

    Args:
        scope: The function scope to check.
        filename: Filename to use in error messages.

    Returns:
        List of violations for unused declarations.
    """
    violations: list[Violation] = []

    for symbol in get_unused_symbols(scope):
        violations.append({
            "file": filename,
            "line": symbol["line"],
            "col": symbol["col"],
            "message": f"'{symbol['name']}' declared but never used",
            "code": "XI004",
        })

    return violations


def check_style_mixing(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
    filename: str,
    config: XimenezConfig,
) -> list[Violation]:
    """Check for mixing inline and block styles.

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to check.
        filename: Filename to use in error messages.
        config: Ximenez configuration.

    Returns:
        List of violations for style mixing.
    """
    violations: list[Violation] = []

    if not scope["has_types_block"]:
        return violations

    # If we have a types: block, check for inline annotations in the body
    class InlineChecker(cst.CSTVisitor):
        def __init__(self) -> None:
            self.in_target_func = False
            self.past_types_block = False

        def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
            if node.name.value == scope["name"]:
                self.in_target_func = True
                return True
            elif self.in_target_func:
                return False
            return True

        def leave_FunctionDef(self, node: cst.FunctionDef) -> None:
            if node.name.value == scope["name"]:
                self.in_target_func = False

        def visit_AnnAssign(self, node: cst.AnnAssign) -> bool:
            if self.in_target_func and self.past_types_block:
                if isinstance(node.target, cst.Name):
                    pos = get_position(node, wrapper)
                    violations.append({
                        "file": filename,
                        "line": pos["line"],
                        "col": pos["col"],
                        "message": "inline annotation not allowed when 'types:' block is present",
                        "code": "XI005",
                    })
            return True

    module.walk(InlineChecker())
    return violations
