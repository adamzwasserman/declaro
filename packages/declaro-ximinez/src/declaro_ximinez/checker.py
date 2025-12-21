"""Type checking logic for declaro-ximinez."""

from __future__ import annotations

from pathlib import Path

import libcst as cst
from libcst._exceptions import ParserSyntaxError
from libcst.metadata import MetadataWrapper, PositionProvider

from .types import Violation, FunctionScope, CheckResult, XiminezConfig, Model
from .parser import (
    parse_file,
    parse_source,
    ParseResult,
    collect_function_scopes,
    extract_function_params,
    extract_return_type,
    get_position,
)
from .preprocessor import PreprocessResult
from .symbols import (
    lookup_symbol,
    mark_symbol_used,
    get_undeclared_symbols,
    get_unused_symbols,
)
from .declaro import (
    load_schema,
    load_models_from_paths,
    validate_field_access,
    validate_relationship_access,
    validate_field_type,
    validate_query_column,
    validate_insert_fields,
)
from .config import parse_file_directive, merge_file_directives


class MultiPassAnalyzer:
    """Multi-pass type analyzer for a function scope.

    Implements the 4-pass analysis specified in the architecture:
    1. Declaration collection - Gather all type declarations
    2. Usage verification - Verify declarations are used correctly
    3. Type compatibility - Check assignment type compatibility
    4. Coverage - Ensure no undeclared locals

    Usage:
        analyzer = MultiPassAnalyzer(module, wrapper, scope, filename, config, models)
        violations = analyzer.analyze()
    """

    def __init__(
        self,
        module: cst.Module,
        wrapper: MetadataWrapper,
        scope: FunctionScope,
        filename: str,
        config: XiminezConfig,
        models: dict[str, Model] | None = None,
    ) -> None:
        """Initialize the multi-pass analyzer.

        Args:
            module: The parsed CST module.
            wrapper: Metadata wrapper for position info.
            scope: The function scope to analyze.
            filename: Filename for error messages.
            config: Ximinez configuration.
            models: Declaro models (optional).
        """
        self.module = module
        self.wrapper = wrapper
        self.scope = scope
        self.filename = filename
        self.config = config
        self.models = models or {}
        self.violations: list[Violation] = []

    def analyze(self) -> list[Violation]:
        """Run all analysis passes and return violations.

        Returns:
            List of all violations found across all passes.
        """
        # Pass 1: Declaration collection (already done in collect_function_scopes)
        # The scope already has symbols populated from types: blocks and inline annotations

        # Pass 2: Usage verification
        self._pass_usage_verification()

        # Pass 3: Type compatibility
        self._pass_type_compatibility()

        # Pass 4: Coverage (no undeclared locals)
        self._pass_coverage()

        # Additional checks (style, model access)
        self._check_style_constraints()
        self._check_model_access()

        return self.violations

    def _pass_usage_verification(self) -> None:
        """Pass 2: Verify declaration usage patterns.

        Checks:
        - Declared variables are used (block style)
        - Variables not used before declaration (inline style)
        - Duplicate declarations (inline style)
        """
        if self.scope["has_types_block"]:
            # Block style: track usage and check for unused declarations
            track_symbol_usage(self.module, self.wrapper, self.scope)
            self.violations.extend(
                check_unused_declarations(self.scope, self.filename)
            )
        else:
            # Inline style: check declaration order and duplicates
            self.violations.extend(
                check_duplicate_declarations(
                    self.module, self.wrapper, self.scope, self.filename
                )
            )
            self.violations.extend(
                check_declaration_order(
                    self.module, self.wrapper, self.scope, self.filename
                )
            )

    def _pass_type_compatibility(self) -> None:
        """Pass 3: Check assignment and operation type compatibility.

        Checks:
        - Annotated assignment types match inferred types
        """
        self.violations.extend(
            check_type_mismatch(
                self.module, self.wrapper, self.scope, self.filename
            )
        )

    def _pass_coverage(self) -> None:
        """Pass 4: Ensure no undeclared local variables.

        Checks:
        - All local variables have declarations
        - All parameters have type annotations
        - Walrus operator variables are declared
        - Comprehension variables are declared
        - Function has return type
        """
        # Check return type
        if self.scope["return_type"] is None:
            self.violations.append({
                "file": self.filename,
                "line": 1,  # TODO: get actual line from function def
                "col": 0,
                "message": f"missing return type annotation for function '{self.scope['name']}'",
                "code": "XI001",
            })

        # Check parameter types
        self.violations.extend(
            check_untyped_params(
                self.module, self.wrapper, self.scope, self.filename
            )
        )

        # Check undeclared locals
        self.violations.extend(
            check_undeclared_locals(
                self.module, self.wrapper, self.scope, self.filename
            )
        )

        # Check walrus operator variables
        self.violations.extend(
            check_walrus_operators(
                self.module, self.wrapper, self.scope, self.filename
            )
        )

        # Check comprehension variables
        self.violations.extend(
            check_comprehension_variables(
                self.module, self.wrapper, self.scope, self.filename
            )
        )

    def _check_style_constraints(self) -> None:
        """Check style-related constraints.

        Checks:
        - No mixing of inline and block styles in same function
        """
        self.violations.extend(
            check_style_mixing(
                self.module, self.wrapper, self.scope, self.filename, self.config
            )
        )

    def _check_model_access(self) -> None:
        """Check Declaro model access patterns.

        Checks:
        - Valid field access
        - Valid relationship access
        - Type compatibility with model fields
        - Query builder validation
        - Insert field validation
        """
        if self.models:
            self.violations.extend(
                check_model_access(
                    self.module, self.wrapper, self.scope, self.filename, self.models
                )
            )


def check_file(file_path: Path, config: XiminezConfig) -> CheckResult:
    """Check a Python file for type violations.

    Args:
        file_path: Path to the Python file.
        config: Ximinez configuration.

    Returns:
        CheckResult with violations and scope info.
    """
    try:
        parse_result = parse_file(file_path)
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
    except ParserSyntaxError as e:
        return {
            "file": str(file_path),
            "violations": [{
                "file": str(file_path),
                "line": getattr(e, "lines", (1,))[0] if hasattr(e, "lines") else 1,
                "col": 0,
                "message": str(e),
                "code": "XI000",
            }],
            "scopes": [],
        }

    # Load Declaro schema if enabled
    models: dict[str, Model] = {}
    if config.get("declaro_enabled"):
        # Load from TOML schema files
        schema_paths = config.get("declaro_schema_paths", [])
        for schema_path in schema_paths:
            # Use path as-is if it looks like an absolute path, otherwise resolve relative to file
            path_obj = Path(schema_path)
            if path_obj.is_absolute():
                full_path = path_obj
            else:
                full_path = file_path.parent / schema_path
            if full_path.exists():
                try:
                    models.update(load_schema(full_path))
                except FileNotFoundError:
                    pass
            else:
                # Schema path not found - return error
                return {
                    "file": str(file_path),
                    "violations": [{
                        "file": str(file_path),
                        "line": 1,
                        "col": 0,
                        "message": f"Could not load schema from: {schema_path}",
                        "code": "XI000",  # Use parse error code for exit 2
                    }],
                    "scopes": [],
                }

        # Load from Pydantic model paths (Python modules with @table decorators)
        model_paths = config.get("declaro_model_paths", [])
        if model_paths:
            # Resolve relative paths
            resolved_paths: list[str] = []
            for model_path in model_paths:
                path_obj = Path(model_path)
                if path_obj.is_absolute():
                    resolved_paths.append(str(path_obj))
                else:
                    resolved_paths.append(str(file_path.parent / model_path))
            try:
                models.update(load_models_from_paths(resolved_paths))
            except Exception:
                # Skip failed imports, continue with other models
                pass

    return check_module(parse_result, str(file_path), config, models)


def check_source(source: str, filename: str, config: XiminezConfig) -> CheckResult:
    """Check Python source code for type violations.

    Args:
        source: Python source code string.
        filename: Filename to use in error messages.
        config: Ximinez configuration.

    Returns:
        CheckResult with violations and scope info.
    """
    try:
        parse_result = parse_source(source)
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
    except ParserSyntaxError as e:
        return {
            "file": filename,
            "violations": [{
                "file": filename,
                "line": getattr(e, "lines", (1,))[0] if hasattr(e, "lines") else 1,
                "col": 0,
                "message": str(e),
                "code": "XI000",
            }],
            "scopes": [],
        }

    return check_module(parse_result, filename, config, {})


def check_module(
    parse_result: ParseResult,
    filename: str,
    config: XiminezConfig,
    models: dict[str, Model] | None = None,
) -> CheckResult:
    """Check a parsed module for type violations.

    Args:
        parse_result: The parsed result with CST module and preprocessing info.
        filename: Filename to use in error messages.
        config: Ximinez configuration.
        models: Declaro models loaded from schema (optional).

    Returns:
        CheckResult with violations and scope info.
    """
    if models is None:
        models = {}
    module = parse_result.module
    preprocess_result = parse_result.preprocess_result

    violations: list[Violation] = []
    wrapper = MetadataWrapper(module)

    # Parse file-level directives and merge with config
    directives = parse_file_directive(preprocess_result.original_source)
    effective_config = merge_file_directives(config, directives)

    # Add preprocessing violations (types: block position, duplicates)
    for pv in preprocess_result.violations:
        violations.append({
            "file": filename,
            "line": pv.line,
            "col": pv.col,
            "message": pv.message,
            "code": pv.code,
        })

    # If there are preprocessing violations, stop here - the structure is invalid
    if violations:
        return {
            "file": filename,
            "violations": violations,
            "scopes": [],
        }

    # Collect function scopes, passing preprocess info to detect types: blocks
    scopes = collect_function_scopes(module, preprocess_result)

    # Check module-level style enforcement
    if config.get("style_enforcement") == "module":
        violations.extend(
            check_module_style_enforcement(scopes, filename, effective_config, directives)
        )

    # Check each function
    for scope in scopes:
        func_violations = check_function(module, wrapper, scope, filename, effective_config, models)
        violations.extend(func_violations)

    return {
        "file": filename,
        "violations": violations,
        "scopes": scopes,
    }


def check_module_style_enforcement(
    scopes: list[FunctionScope],
    filename: str,
    config: XiminezConfig,
    directives: dict[str, str],
) -> list[Violation]:
    """Check that all functions use the module-enforced style.

    Args:
        scopes: List of function scopes.
        filename: Filename to use in error messages.
        config: Ximinez configuration (with directives applied).
        directives: File-level directives.

    Returns:
        List of violations for style enforcement.
    """
    violations: list[Violation] = []

    # Determine enforced style from directives
    enforced_style = directives.get("style")
    if not enforced_style:
        return violations

    for scope in scopes:
        func_style = scope.get("style", "inline")

        if enforced_style == "block" and func_style != "block":
            violations.append({
                "file": filename,
                "line": 1,  # TODO: get actual line from function def
                "col": 0,
                "message": f"'{scope['name']}' uses inline style but module enforces block style",
                "code": "XI007",
            })
        elif enforced_style == "inline" and func_style != "inline":
            violations.append({
                "file": filename,
                "line": 1,  # TODO: get actual line from function def
                "col": 0,
                "message": f"'{scope['name']}' uses block style but module enforces inline style",
                "code": "XI007",
            })

    return violations


def check_function(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
    filename: str,
    config: XiminezConfig,
    models: dict[str, Model] | None = None,
) -> list[Violation]:
    """Check a single function for type violations.

    Uses the MultiPassAnalyzer to run all analysis passes:
    1. Declaration collection (done in collect_function_scopes)
    2. Usage verification
    3. Type compatibility
    4. Coverage

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to check.
        filename: Filename to use in error messages.
        config: Ximinez configuration.
        models: Declaro models loaded from schema (optional).

    Returns:
        List of violations found in this function.
    """
    analyzer = MultiPassAnalyzer(module, wrapper, scope, filename, config, models)
    return analyzer.analyze()


def infer_type(
    node: cst.BaseExpression,
    scope: FunctionScope,
) -> str | None:
    """Infer the type of an expression.

    Args:
        node: The expression node.
        scope: The function scope with variable types.

    Returns:
        The inferred type as a string, or None if unknown.
    """
    # Integer literal
    if isinstance(node, cst.Integer):
        return "int"

    # Float literal
    if isinstance(node, cst.Float):
        return "float"

    # String literal
    if isinstance(node, (cst.SimpleString, cst.ConcatenatedString, cst.FormattedString)):
        return "str"

    # Name reference - look up in params or symbols
    if isinstance(node, cst.Name):
        name = node.value

        # Check params (params is a dict mapping name -> type)
        params = scope.get("params", {})
        if name in params:
            return params[name]

        # Check symbols
        symbol = scope.get("symbols", {}).get(name)
        if symbol:
            return symbol.get("type_annotation")

        return None

    # Binary operation - infer from operands
    if isinstance(node, cst.BinaryOperation):
        left_type = infer_type(node.left, scope)
        right_type = infer_type(node.right, scope)

        # If both operands are the same type, result is that type
        if left_type == right_type and left_type is not None:
            return left_type

        # int + int = int, float + float = float, etc.
        # For mixed int/float, result is float
        if {left_type, right_type} == {"int", "float"}:
            return "float"

        # str + str = str (concatenation)
        if left_type == "str" and right_type == "str":
            return "str"

        return left_type or right_type

    # Unary operation
    if isinstance(node, cst.UnaryOperation):
        return infer_type(node.expression, scope)

    # Comparison - always bool
    if isinstance(node, cst.Comparison):
        return "bool"

    # Boolean operations - always bool
    if isinstance(node, cst.BooleanOperation):
        return "bool"

    # IfExp (ternary) - infer from body
    if isinstance(node, cst.IfExp):
        return infer_type(node.body, scope)

    # List literal
    if isinstance(node, cst.List):
        return "list"

    # Dict literal
    if isinstance(node, cst.Dict):
        return "dict"

    # Tuple literal
    if isinstance(node, cst.Tuple):
        return "tuple"

    # Set literal
    if isinstance(node, cst.Set):
        return "set"

    return None


def check_type_mismatch(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
    filename: str,
) -> list[Violation]:
    """Check for type mismatches on annotated assignments.

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to check.
        filename: Filename to use in error messages.

    Returns:
        List of violations for type mismatches.
    """
    violations: list[Violation] = []

    class TypeMismatchChecker(cst.CSTVisitor):
        def __init__(self) -> None:
            self.in_target_func = False

        def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
            if node.name.value == scope["name"]:
                self.in_target_func = True
                return True
            elif self.in_target_func:
                return False  # Skip nested functions
            return True

        def leave_FunctionDef(self, node: cst.FunctionDef) -> None:
            if node.name.value == scope["name"]:
                self.in_target_func = False

        def visit_AnnAssign(self, node: cst.AnnAssign) -> bool:
            if not self.in_target_func:
                return True

            # Only check if there's a value (not just a declaration)
            if node.value is None:
                return True

            # Get the declared type
            declared_type = None
            if isinstance(node.annotation.annotation, cst.Name):
                declared_type = node.annotation.annotation.value
            elif isinstance(node.annotation.annotation, cst.Subscript):
                # Handle generic types like list[int]
                if isinstance(node.annotation.annotation.value, cst.Name):
                    declared_type = node.annotation.annotation.value.value

            if declared_type is None:
                return True

            # Infer the type of the value
            inferred_type = infer_type(node.value, scope)

            if inferred_type is not None and inferred_type != declared_type:
                pos = get_position(node, wrapper)
                violations.append({
                    "file": filename,
                    "line": pos["line"],
                    "col": pos["col"],
                    "message": f"type mismatch: expected '{declared_type}', got '{inferred_type}'",
                    "code": "XI012",
                })

            return True

    wrapper.visit(TypeMismatchChecker())
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

    wrapper.visit(ParamChecker())
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
    wrapper.visit(checker)

    # Report violations for assignments without prior annotation
    for name, (line, col) in checker.assigned_names.items():
        if name not in scope["symbols"] and name not in scope["params"]:
            # Use different message depending on whether function has types: block
            if scope.get("has_types_block"):
                message = f"'{name}' not declared in types: block"
            else:
                message = f"local variable '{name}' used without type declaration"
            violations.append({
                "file": filename,
                "line": line,
                "col": col,
                "message": message,
                "code": "XI003",
            })

    return violations


def check_duplicate_declarations(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
    filename: str,
) -> list[Violation]:
    """Check for duplicate type declarations in inline style.

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to check.
        filename: Filename to use in error messages.

    Returns:
        List of violations for duplicate declarations.
    """
    violations: list[Violation] = []

    class DuplicateChecker(cst.CSTVisitor):
        def __init__(self) -> None:
            self.in_target_func = False
            self.declared_names: dict[str, tuple[int, int]] = {}  # name -> first (line, col)

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
            if not self.in_target_func:
                return True

            if isinstance(node.target, cst.Name):
                name = node.target.value
                pos = get_position(node, wrapper)

                if name in self.declared_names:
                    # Duplicate declaration
                    violations.append({
                        "file": filename,
                        "line": pos["line"],
                        "col": pos["col"],
                        "message": f"'{name}' already declared",
                        "code": "XI008",
                    })
                else:
                    self.declared_names[name] = (pos["line"], pos["col"])

            return True

    wrapper.visit(DuplicateChecker())
    return violations


def check_declaration_order(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
    filename: str,
) -> list[Violation]:
    """Check for variables used before their declaration in inline style.

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to check.
        filename: Filename to use in error messages.

    Returns:
        List of violations for usage before declaration.
    """
    violations: list[Violation] = []

    class OrderChecker(cst.CSTVisitor):
        def __init__(self) -> None:
            self.in_target_func = False
            self.declaration_lines: dict[str, int] = {}  # name -> declaration line
            self.usage_before_decl: list[tuple[str, int, int]] = []  # (name, line, col)

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
            if not self.in_target_func:
                return True

            if isinstance(node.target, cst.Name):
                name = node.target.value
                pos = get_position(node, wrapper)
                if name not in self.declaration_lines:
                    self.declaration_lines[name] = pos["line"]

            return True

        def visit_Name(self, node: cst.Name) -> bool:
            if not self.in_target_func:
                return True

            name = node.value
            # Skip if it's a parameter
            if name in scope["params"]:
                return True

            pos = get_position(node, wrapper)

            # If we haven't seen the declaration yet, this might be usage before declaration
            # We'll check after the full pass
            if name not in self.declaration_lines:
                # Store for later checking
                self.usage_before_decl.append((name, pos["line"], pos["col"]))

            return True

    checker = OrderChecker()
    wrapper.visit(checker)

    # Now check which usages were before declarations
    for name, use_line, use_col in checker.usage_before_decl:
        if name in checker.declaration_lines:
            decl_line = checker.declaration_lines[name]
            if use_line < decl_line:
                violations.append({
                    "file": filename,
                    "line": use_line,
                    "col": use_col,
                    "message": f"'{name}' used before declaration",
                    "code": "XI009",
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


def track_symbol_usage(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
) -> None:
    """Track which declared symbols are actually used in the function.

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to update with usage info.
    """
    class UsageTracker(cst.CSTVisitor):
        def __init__(self) -> None:
            self.func_depth = 0  # Track function nesting level
            self.skip_next_name = False  # Skip name in AnnAssign target

        def visit_FunctionDef(self, node: cst.FunctionDef) -> bool:
            if node.name.value == scope["name"] and self.func_depth == 0:
                self.func_depth = 1
                return True
            elif self.func_depth > 0:
                # In target function, entering a nested function
                self.func_depth += 1
                return True  # Continue visiting but track depth
            return True

        def leave_FunctionDef(self, node: cst.FunctionDef) -> None:
            if self.func_depth > 0:
                self.func_depth -= 1

        def visit_AnnAssign(self, node: cst.AnnAssign) -> bool:
            if self.func_depth != 1:
                return True
            # For AnnAssign without value (declaration only), skip the target name
            if node.value is None:
                self.skip_next_name = True
            return True

        def leave_AnnAssign(self, node: cst.AnnAssign) -> None:
            self.skip_next_name = False

        def visit_Name(self, node: cst.Name) -> bool:
            if self.func_depth != 1:  # Only track uses in the target function, not nested
                return True
            if self.skip_next_name:
                self.skip_next_name = False
                return True
            # Mark the symbol as used if it's in scope
            mark_symbol_used(scope, node.value)
            return True

    wrapper.visit(UsageTracker())


def check_style_mixing(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
    filename: str,
    config: XiminezConfig,
) -> list[Violation]:
    """Check for mixing inline and block styles.

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to check.
        filename: Filename to use in error messages.
        config: Ximinez configuration.

    Returns:
        List of violations for style mixing.
    """
    violations: list[Violation] = []

    if not scope["has_types_block"]:
        return violations

    # If we have a types: block, check for inline annotations that have values
    # (assignments). The types: block declarations are converted to AnnAssign
    # without values, so any AnnAssign WITH a value is an inline annotation.
    class InlineChecker(cst.CSTVisitor):
        def __init__(self) -> None:
            self.in_target_func = False
            self.first_non_decl_seen = False

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

        def visit_SimpleStatementLine(self, node: cst.SimpleStatementLine) -> bool:
            if not self.in_target_func:
                return True
            # Check if this line contains something other than AnnAssign or Expr (docstring)
            for stmt in node.body:
                if not isinstance(stmt, (cst.AnnAssign, cst.Expr)):
                    self.first_non_decl_seen = True
            return True

        def visit_AnnAssign(self, node: cst.AnnAssign) -> bool:
            if not self.in_target_func:
                return True

            # If we've seen a non-declaration statement, this is a late inline annotation
            if self.first_non_decl_seen:
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

    wrapper.visit(InlineChecker())
    return violations


def check_walrus_operators(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
    filename: str,
) -> list[Violation]:
    """Check that walrus operator variables are declared.

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to check.
        filename: Filename to use in error messages.

    Returns:
        List of violations for undeclared walrus operator variables.
    """
    violations: list[Violation] = []

    class WalrusChecker(cst.CSTVisitor):
        def __init__(self) -> None:
            self.in_target_func = False
            self.declared_names: set[str] = set()

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
            if not self.in_target_func:
                return True

            if isinstance(node.target, cst.Name):
                self.declared_names.add(node.target.value)
            return True

        def visit_NamedExpr(self, node: cst.NamedExpr) -> bool:
            if not self.in_target_func:
                return True

            # NamedExpr is the walrus operator (:=)
            if isinstance(node.target, cst.Name):
                name = node.target.value
                # Check if name is declared in symbols or params
                if name not in scope["symbols"] and name not in scope["params"] and name not in self.declared_names:
                    pos = get_position(node, wrapper)
                    if scope.get("has_types_block"):
                        message = f"walrus operator variable '{name}' must be declared in types: block"
                    else:
                        message = f"walrus operator variable '{name}' used without type declaration"
                    violations.append({
                        "file": filename,
                        "line": pos["line"],
                        "col": pos["col"],
                        "message": message,
                        "code": "XI010",
                    })
            return True

    wrapper.visit(WalrusChecker())
    return violations


def check_comprehension_variables(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
    filename: str,
) -> list[Violation]:
    """Check that comprehension loop variables are declared.

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to check.
        filename: Filename to use in error messages.

    Returns:
        List of violations for undeclared comprehension variables.
    """
    violations: list[Violation] = []

    class ComprehensionChecker(cst.CSTVisitor):
        def __init__(self) -> None:
            self.in_target_func = False
            self.declared_names: set[str] = set()

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
            if not self.in_target_func:
                return True

            if isinstance(node.target, cst.Name):
                self.declared_names.add(node.target.value)
            return True

        def _check_comp_for(self, comp_for: cst.CompFor, pos_node: cst.CSTNode) -> None:
            """Check comprehension for clause variables."""
            if not self.in_target_func:
                return

            # Handle target - can be Name, Tuple, etc.
            targets = self._extract_names(comp_for.target)
            for name in targets:
                if name not in scope["symbols"] and name not in scope["params"] and name not in self.declared_names:
                    pos = get_position(pos_node, wrapper)
                    if scope.get("has_types_block"):
                        message = f"comprehension variable '{name}' must be declared in types: block"
                    else:
                        message = f"comprehension variable '{name}' used without type declaration"
                    violations.append({
                        "file": filename,
                        "line": pos["line"],
                        "col": pos["col"],
                        "message": message,
                        "code": "XI011",
                    })

            # Check nested for clauses
            if comp_for.inner_for_in:
                self._check_comp_for(comp_for.inner_for_in, pos_node)

        def _extract_names(self, node: cst.BaseExpression) -> list[str]:
            """Extract variable names from a target expression."""
            names = []
            if isinstance(node, cst.Name):
                names.append(node.value)
            elif isinstance(node, cst.Tuple):
                for elem in node.elements:
                    if isinstance(elem.value, cst.Name):
                        names.append(elem.value.value)
                    elif isinstance(elem.value, cst.Tuple):
                        names.extend(self._extract_names(elem.value))
            return names

        def visit_ListComp(self, node: cst.ListComp) -> bool:
            if self.in_target_func:
                self._check_comp_for(node.for_in, node)
            return True

        def visit_SetComp(self, node: cst.SetComp) -> bool:
            if self.in_target_func:
                self._check_comp_for(node.for_in, node)
            return True

        def visit_DictComp(self, node: cst.DictComp) -> bool:
            if self.in_target_func:
                self._check_comp_for(node.for_in, node)
            return True

        def visit_GeneratorExp(self, node: cst.GeneratorExp) -> bool:
            if self.in_target_func:
                self._check_comp_for(node.for_in, node)
            return True

    wrapper.visit(ComprehensionChecker())
    return violations


def check_model_access(
    module: cst.Module,
    wrapper: MetadataWrapper,
    scope: FunctionScope,
    filename: str,
    models: dict[str, Model],
) -> list[Violation]:
    """Check for invalid Declaro model field/relationship access.

    Args:
        module: The parsed CST module.
        wrapper: Metadata wrapper for position info.
        scope: The function scope to check.
        filename: Filename to use in error messages.
        models: Declaro models loaded from schema.

    Returns:
        List of violations for invalid model access.
    """
    violations: list[Violation] = []

    # Build map of variable name -> model type from function params
    model_vars: dict[str, Model] = {}

    # Check params for model types
    for param_name, param_type in scope["params"].items():
        # Match model names (e.g., User, Order) - case insensitive match
        model_name = param_type.lower()
        if model_name in models:
            model_vars[param_name] = models[model_name]

    # Also check declared types in scope
    for sym_name, symbol in scope["symbols"].items():
        type_ann = symbol.get("type_annotation", "")
        # Extract base type (e.g., "list[Order]" -> "order")
        base_type = type_ann.lower().rstrip("s")  # Simple pluralization
        for model_name, model in models.items():
            if model_name.lower() in base_type:
                model_vars[sym_name] = model
                break

    if not model_vars:
        return violations

    class ModelAccessChecker(cst.CSTVisitor):
        def __init__(self) -> None:
            self.in_target_func = False
            self.dict_vars: dict[str, set[str]] = {}  # Track dict literal keys

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

        def check_field_access_type(
            self,
            subscript_node: cst.Subscript,
            declared_type: str | None,
        ) -> cst.Subscript:
            """Check if subscript access matches declared type."""
            if not isinstance(subscript_node.value, cst.Name):
                return subscript_node

            var_name = subscript_node.value.value
            if var_name not in model_vars:
                return subscript_node

            model = model_vars[var_name]
            for slice_item in subscript_node.slice:
                if isinstance(slice_item.slice, cst.Index):
                    index = slice_item.slice.value
                    if isinstance(index, cst.SimpleString):
                        field_name = index.value[1:-1]

                        # Check type mismatch for valid fields
                        if field_name in model["fields"] and declared_type:
                            field_info = model["fields"][field_name]
                            field_type = field_info.get("type", "")

                            if field_type and field_type != declared_type:
                                pos = get_position(subscript_node, wrapper)
                                violation = validate_field_type(
                                    model, field_name, declared_type,
                                    filename, pos["line"], pos["col"]
                                )
                                if violation:
                                    violations.append(violation)

            return subscript_node

        def visit_Assign(self, node: cst.Assign) -> bool:
            if not self.in_target_func:
                return True

            # Check for var = model["field"] pattern
            if isinstance(node.value, cst.Subscript):
                # Get the target variable name
                for target in node.targets:
                    if isinstance(target.target, cst.Name):
                        target_name = target.target.value
                        # Look up declared type
                        symbol = scope.get("symbols", {}).get(target_name)
                        if symbol:
                            declared_type = symbol.get("type_annotation")
                            self.check_field_access_type(node.value, declared_type)

            # Track dict literals for insert validation
            if isinstance(node.value, cst.Dict):
                for target in node.targets:
                    if isinstance(target.target, cst.Name):
                        target_name = target.target.value
                        # Extract dict keys
                        keys = set()
                        for element in node.value.elements:
                            if isinstance(element, cst.DictElement):
                                if isinstance(element.key, cst.SimpleString):
                                    keys.add(element.key.value[1:-1])
                        self.dict_vars[target_name] = keys

            return True

        def visit_Subscript(self, node: cst.Subscript) -> bool:
            if not self.in_target_func:
                return True

            # Check for model["field"] pattern
            if isinstance(node.value, cst.Name):
                var_name = node.value.value
                if var_name in model_vars:
                    model = model_vars[var_name]
                    # Get the subscript key
                    for slice_item in node.slice:
                        if isinstance(slice_item.slice, cst.Index):
                            index = slice_item.slice.value
                            if isinstance(index, cst.SimpleString):
                                # Extract string value (remove quotes)
                                field_name = index.value[1:-1]
                                pos = get_position(node, wrapper)

                                # Check if it's a valid field or relationship
                                if field_name in model["fields"]:
                                    continue  # Valid field access
                                if field_name in model["relationships"]:
                                    continue  # Valid relationship access

                                # Not found - determine if it looks like a relationship or field
                                # Simple heuristic: if name ends with 's' (plural), treat as relationship
                                if field_name.endswith('s') and len(field_name) > 2:
                                    # Likely a relationship access (plural name pattern)
                                    violation = validate_relationship_access(
                                        model, field_name, filename, pos["line"], pos["col"]
                                    )
                                else:
                                    # Default to field access error
                                    violation = validate_field_access(
                                        model, field_name, filename, pos["line"], pos["col"]
                                    )

                                if violation:
                                    violations.append(violation)
            return True

        def extract_table_from_chain(self, call_node: cst.Call) -> str | None:
            """Extract table name from a query chain like query.select("users").where()"""
            current = call_node

            while current:
                if isinstance(current.func, cst.Attribute):
                    attr_name = current.func.attr.value

                    if attr_name == "select":
                        if current.args and isinstance(current.args[0].value, cst.SimpleString):
                            return current.args[0].value.value[1:-1]

                    # Follow the chain: call.func.value is the receiver
                    receiver = current.func.value
                    if isinstance(receiver, cst.Call):
                        current = receiver
                    else:
                        break
                else:
                    break

            return None

        def visit_Call(self, node: cst.Call) -> bool:
            if not self.in_target_func:
                return True

            # Check for query.select("table").where(field=value) pattern
            if isinstance(node.func, cst.Attribute):
                attr_name = node.func.attr.value

                # Handle .where(field=value) or .insert(table, data)
                if attr_name == "where":
                    # Extract table name from the method chain
                    table_name = self.extract_table_from_chain(node)

                    if table_name:
                        # Find model for this table
                        target_model = None
                        for model in models.values():
                            if model["table"] == table_name:
                                target_model = model
                                break

                        if target_model:
                            for arg in node.args:
                                if arg.keyword:
                                    col_name = arg.keyword.value
                                    if col_name not in target_model["fields"]:
                                        pos = get_position(node, wrapper)
                                        violation = validate_query_column(
                                            models, table_name, col_name,
                                            filename, pos["line"], pos["col"]
                                        )
                                        if violation:
                                            violations.append(violation)

                # Handle query.insert("table", data)
                elif attr_name == "insert":
                    if len(node.args) >= 2:
                        # First arg is table name
                        table_arg = node.args[0].value
                        if isinstance(table_arg, cst.SimpleString):
                            table_name = table_arg.value[1:-1]

                            # Find model for this table
                            target_model = None
                            for model in models.values():
                                if model["table"] == table_name:
                                    target_model = model
                                    break

                            if target_model:
                                # Second arg should be data dict
                                data_arg = node.args[1].value
                                provided_fields: set[str] = set()

                                if isinstance(data_arg, cst.Name):
                                    # Look up tracked dict variable
                                    var_name = data_arg.value
                                    if var_name in self.dict_vars:
                                        provided_fields = self.dict_vars[var_name]
                                elif isinstance(data_arg, cst.Dict):
                                    # Inline dict literal
                                    for element in data_arg.elements:
                                        if isinstance(element, cst.DictElement):
                                            if isinstance(element.key, cst.SimpleString):
                                                provided_fields.add(element.key.value[1:-1])

                                # Validate required fields
                                if provided_fields:
                                    pos = get_position(node, wrapper)
                                    insert_violations = validate_insert_fields(
                                        target_model, provided_fields,
                                        filename, pos["line"], pos["col"]
                                    )
                                    violations.extend(insert_violations)

            return True

    wrapper.visit(ModelAccessChecker())
    return violations
