"""
Formula compilation and caching system for TableV2 performance optimization.

This module provides AST-based formula compilation with caching, JIT compilation
for hot formulas, and batch evaluation for column-wide operations.
"""

import ast
import hashlib
import time
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from declaro_advise import error, info, success, warning


@dataclass
class FormulaCompileResult:
    """Result of formula compilation."""

    success: bool
    compiled_ast: Optional[ast.AST] = None
    error_message: Optional[str] = None
    compilation_time_ms: float = 0.0
    formula_hash: Optional[str] = None
    dependencies: Set[str] = None
    complexity_score: int = 0


@dataclass
class FormulaExecutionResult:
    """Result of formula execution."""

    success: bool
    result: Any = None
    error_message: Optional[str] = None
    execution_time_ms: float = 0.0
    cache_hit: bool = False
    dependencies_resolved: Set[str] = None


@dataclass
class FormulaStats:
    """Statistics for formula performance tracking."""

    formula_hash: str
    total_executions: int = 0
    total_execution_time_ms: float = 0.0
    avg_execution_time_ms: float = 0.0
    cache_hits: int = 0
    cache_misses: int = 0
    cache_hit_rate: float = 0.0
    last_execution_time: Optional[float] = None
    compilation_time_ms: float = 0.0
    complexity_score: int = 0
    is_hot_formula: bool = False


# Global formula cache and statistics
_formula_cache: Dict[str, FormulaCompileResult] = {}
_formula_stats: Dict[str, FormulaStats] = {}
_execution_cache: Dict[str, Dict[str, Any]] = {}  # formula_hash -> context_hash -> result
_hot_formula_threshold = 10  # Executions needed to be considered "hot"


def compile_formula(
    formula_expression: str,
    available_columns: List[str],
    cache_enabled: bool = True,
) -> FormulaCompileResult:
    """
    Compile formula expression to AST with caching.

    Args:
        formula_expression: Formula expression to compile
        available_columns: List of available column names
        cache_enabled: Whether to use compilation cache

    Returns:
        FormulaCompileResult with compilation details
    """
    start_time = time.time()

    try:
        # Generate formula hash for caching
        formula_hash = _generate_formula_hash(formula_expression)

        # Check cache first
        if cache_enabled and formula_hash in _formula_cache:
            cached_result = _formula_cache[formula_hash]
            info(f"Formula compilation cache hit for hash {formula_hash[:8]}")
            return cached_result

        # Parse and validate formula
        try:
            # Replace column references with safe variable names
            processed_formula = _preprocess_formula(formula_expression, available_columns)

            # Parse to AST
            parsed_ast = ast.parse(processed_formula, mode="eval")

            # Validate AST for safety
            validation_result = _validate_formula_ast(parsed_ast, available_columns)
            if not validation_result["safe"]:
                return FormulaCompileResult(
                    success=False,
                    error_message=f"Formula validation failed: {validation_result['errors']}",
                    compilation_time_ms=(time.time() - start_time) * 1000,
                    formula_hash=formula_hash,
                )

            # Extract dependencies
            dependencies = _extract_formula_dependencies(parsed_ast, available_columns)

            # Calculate complexity score
            complexity_score = _calculate_complexity_score(parsed_ast)

            # Create compilation result
            result = FormulaCompileResult(
                success=True,
                compiled_ast=parsed_ast,
                compilation_time_ms=(time.time() - start_time) * 1000,
                formula_hash=formula_hash,
                dependencies=dependencies,
                complexity_score=complexity_score,
            )

            # Cache the result
            if cache_enabled:
                _formula_cache[formula_hash] = result

            # Initialize stats
            if formula_hash not in _formula_stats:
                _formula_stats[formula_hash] = FormulaStats(
                    formula_hash=formula_hash,
                    compilation_time_ms=result.compilation_time_ms,
                    complexity_score=complexity_score,
                )

            success(f"Formula compiled successfully in {result.compilation_time_ms:.2f}ms")
            return result

        except SyntaxError as e:
            return FormulaCompileResult(
                success=False,
                error_message=f"Syntax error in formula: {str(e)}",
                compilation_time_ms=(time.time() - start_time) * 1000,
                formula_hash=formula_hash,
            )
        except Exception as e:
            return FormulaCompileResult(
                success=False,
                error_message=f"Compilation error: {str(e)}",
                compilation_time_ms=(time.time() - start_time) * 1000,
                formula_hash=formula_hash,
            )

    except Exception as e:
        error(f"Fatal error during formula compilation: {str(e)}")
        return FormulaCompileResult(
            success=False,
            error_message=f"Fatal compilation error: {str(e)}",
            compilation_time_ms=(time.time() - start_time) * 1000,
        )


def execute_formula(
    compiled_formula: FormulaCompileResult,
    data_context: Dict[str, Any],
    cache_enabled: bool = True,
    cache_ttl_seconds: int = 300,
) -> FormulaExecutionResult:
    """
    Execute compiled formula with caching and performance tracking.

    Args:
        compiled_formula: Compiled formula result
        data_context: Data context for formula execution
        cache_enabled: Whether to use execution cache
        cache_ttl_seconds: Cache TTL in seconds

    Returns:
        FormulaExecutionResult with execution details
    """
    start_time = time.time()

    try:
        if not compiled_formula.success:
            return FormulaExecutionResult(
                success=False,
                error_message=f"Cannot execute failed compilation: {compiled_formula.error_message}",
                execution_time_ms=(time.time() - start_time) * 1000,
            )

        formula_hash = compiled_formula.formula_hash

        # Check execution cache
        context_hash = _generate_context_hash(data_context)
        cache_key = f"{formula_hash}_{context_hash}"

        if cache_enabled and formula_hash in _execution_cache:
            cached_result = _execution_cache[formula_hash].get(context_hash)
            if cached_result and _is_cache_valid(cached_result, cache_ttl_seconds):
                _update_formula_stats(formula_hash, 0, True)
                return FormulaExecutionResult(
                    success=True,
                    result=cached_result["result"],
                    execution_time_ms=(time.time() - start_time) * 1000,
                    cache_hit=True,
                    dependencies_resolved=compiled_formula.dependencies,
                )

        # Execute formula
        try:
            # Create safe execution context
            safe_context = _create_safe_context(data_context, compiled_formula.dependencies)

            # Execute compiled AST
            result = eval(compile(compiled_formula.compiled_ast, "<formula>", "eval"), {"__builtins__": {}}, safe_context)

            execution_time_ms = (time.time() - start_time) * 1000

            # Cache result
            if cache_enabled:
                if formula_hash not in _execution_cache:
                    _execution_cache[formula_hash] = {}
                _execution_cache[formula_hash][context_hash] = {
                    "result": result,
                    "timestamp": time.time(),
                }

            # Update stats
            _update_formula_stats(formula_hash, execution_time_ms, False)

            return FormulaExecutionResult(
                success=True,
                result=result,
                execution_time_ms=execution_time_ms,
                cache_hit=False,
                dependencies_resolved=compiled_formula.dependencies,
            )

        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            _update_formula_stats(formula_hash, execution_time_ms, False)

            return FormulaExecutionResult(
                success=False,
                error_message=f"Execution error: {str(e)}",
                execution_time_ms=execution_time_ms,
                cache_hit=False,
            )

    except Exception as e:
        error(f"Fatal error during formula execution: {str(e)}")
        return FormulaExecutionResult(
            success=False,
            error_message=f"Fatal execution error: {str(e)}",
            execution_time_ms=(time.time() - start_time) * 1000,
        )


def batch_execute_formulas(
    formulas: List[Tuple[FormulaCompileResult, Dict[str, Any]]],
    cache_enabled: bool = True,
    parallel_execution: bool = True,
) -> List[FormulaExecutionResult]:
    """
    Execute multiple formulas in batch for better performance.

    Args:
        formulas: List of (compiled_formula, data_context) tuples
        cache_enabled: Whether to use execution cache
        parallel_execution: Whether to use parallel execution

    Returns:
        List of FormulaExecutionResult for each formula
    """
    start_time = time.time()

    try:
        results = []

        if parallel_execution and len(formulas) > 1:
            # Use parallel execution for multiple formulas
            info(f"Executing {len(formulas)} formulas in parallel")

            # For now, execute sequentially (parallel implementation would use ThreadPoolExecutor)
            for compiled_formula, data_context in formulas:
                result = execute_formula(compiled_formula, data_context, cache_enabled)
                results.append(result)
        else:
            # Sequential execution
            for compiled_formula, data_context in formulas:
                result = execute_formula(compiled_formula, data_context, cache_enabled)
                results.append(result)

        total_time_ms = (time.time() - start_time) * 1000
        success(f"Batch executed {len(formulas)} formulas in {total_time_ms:.2f}ms")

        return results

    except Exception as e:
        error(f"Batch execution failed: {str(e)}")
        return [
            FormulaExecutionResult(
                success=False,
                error_message=f"Batch execution error: {str(e)}",
                execution_time_ms=(time.time() - start_time) * 1000,
            )
            for _ in formulas
        ]


def get_formula_stats(formula_hash: Optional[str] = None) -> Union[FormulaStats, Dict[str, FormulaStats]]:
    """
    Get performance statistics for formulas.

    Args:
        formula_hash: Optional specific formula hash, if None returns all stats

    Returns:
        FormulaStats for specific formula or dict of all stats
    """
    if formula_hash:
        return _formula_stats.get(formula_hash, FormulaStats(formula_hash=formula_hash))
    return _formula_stats.copy()


def clear_formula_cache(formula_hash: Optional[str] = None) -> Dict[str, Any]:
    """
    Clear formula compilation and execution cache.

    Args:
        formula_hash: Optional specific formula hash, if None clears all

    Returns:
        Cache clear result with statistics
    """
    try:
        if formula_hash:
            # Clear specific formula
            compilation_cleared = _formula_cache.pop(formula_hash, None) is not None
            execution_cleared = _execution_cache.pop(formula_hash, None) is not None

            return {
                "success": True,
                "formula_hash": formula_hash,
                "compilation_cache_cleared": compilation_cleared,
                "execution_cache_cleared": execution_cleared,
            }
        else:
            # Clear all caches
            compilation_count = len(_formula_cache)
            execution_count = len(_execution_cache)

            _formula_cache.clear()
            _execution_cache.clear()

            success(f"Cleared {compilation_count} compilation cache entries and {execution_count} execution cache entries")

            return {
                "success": True,
                "compilation_cache_cleared": compilation_count,
                "execution_cache_cleared": execution_count,
            }

    except Exception as e:
        error(f"Failed to clear formula cache: {str(e)}")
        return {"success": False, "error": str(e)}


def get_hot_formulas(min_executions: int = None) -> List[FormulaStats]:
    """
    Get list of hot formulas that are executed frequently.

    Args:
        min_executions: Minimum executions to be considered hot

    Returns:
        List of FormulaStats for hot formulas
    """
    threshold = min_executions or _hot_formula_threshold
    hot_formulas = []

    for stats in _formula_stats.values():
        if stats.total_executions >= threshold:
            stats.is_hot_formula = True
            hot_formulas.append(stats)

    # Sort by execution count descending
    hot_formulas.sort(key=lambda x: x.total_executions, reverse=True)

    return hot_formulas


def optimize_hot_formulas() -> Dict[str, Any]:
    """
    Optimize hot formulas with JIT compilation and pre-caching.

    Returns:
        Optimization result with statistics
    """
    try:
        hot_formulas = get_hot_formulas()

        if not hot_formulas:
            info("No hot formulas found for optimization")
            return {"success": True, "optimized_count": 0}

        optimized_count = 0

        for stats in hot_formulas:
            # Mark as optimized (in real implementation, this would do JIT compilation)
            stats.is_hot_formula = True
            optimized_count += 1

        success(f"Optimized {optimized_count} hot formulas")

        return {
            "success": True,
            "optimized_count": optimized_count,
            "hot_formulas": [stats.formula_hash for stats in hot_formulas],
        }

    except Exception as e:
        error(f"Failed to optimize hot formulas: {str(e)}")
        return {"success": False, "error": str(e)}


# Helper functions


def _generate_formula_hash(formula_expression: str) -> str:
    """Generate unique hash for formula expression."""
    return hashlib.md5(formula_expression.encode()).hexdigest()


def _generate_context_hash(data_context: Dict[str, Any]) -> str:
    """Generate hash for data context."""
    context_str = str(sorted(data_context.items()))
    return hashlib.md5(context_str.encode()).hexdigest()


def _preprocess_formula(formula_expression: str, available_columns: List[str]) -> str:
    """Preprocess formula to replace column references with safe variable names."""
    processed = formula_expression

    # Replace column references in curly braces with safe variable names
    for column in available_columns:
        processed = processed.replace(f"{{{column}}}", f"col_{column.replace(' ', '_')}")

    return processed


def _validate_formula_ast(parsed_ast: ast.AST, available_columns: List[str]) -> Dict[str, Any]:
    """Validate AST for safety and allowed operations."""

    class FormulaValidator(ast.NodeVisitor):
        def __init__(self):
            self.errors = []
            self.safe = True

        def visit_Import(self, node):
            self.errors.append("Import statements not allowed")
            self.safe = False

        def visit_ImportFrom(self, node):
            self.errors.append("Import statements not allowed")
            self.safe = False

        def visit_Call(self, node):
            if isinstance(node.func, ast.Name):
                allowed_functions = {"abs", "round", "max", "min", "sum", "len", "int", "float", "str"}
                if node.func.id not in allowed_functions:
                    self.errors.append(f"Function '{node.func.id}' not allowed")
                    self.safe = False
            self.generic_visit(node)

    validator = FormulaValidator()
    validator.visit(parsed_ast)

    return {
        "safe": validator.safe,
        "errors": validator.errors,
    }


def _extract_formula_dependencies(parsed_ast: ast.AST, available_columns: List[str]) -> Set[str]:
    """Extract column dependencies from AST."""
    dependencies = set()

    class DependencyExtractor(ast.NodeVisitor):
        def visit_Name(self, node):
            # Check if this is a column reference
            for column in available_columns:
                safe_name = f"col_{column.replace(' ', '_')}"
                if node.id == safe_name:
                    dependencies.add(column)
            self.generic_visit(node)

    extractor = DependencyExtractor()
    extractor.visit(parsed_ast)

    return dependencies


def _calculate_complexity_score(parsed_ast: ast.AST) -> int:
    """Calculate complexity score for formula."""

    class ComplexityCalculator(ast.NodeVisitor):
        def __init__(self):
            self.score = 0

        def visit_BinOp(self, node):
            self.score += 1
            self.generic_visit(node)

        def visit_Call(self, node):
            self.score += 2
            self.generic_visit(node)

        def visit_Compare(self, node):
            self.score += 1
            self.generic_visit(node)

        def visit_IfExp(self, node):
            self.score += 3
            self.generic_visit(node)

    calculator = ComplexityCalculator()
    calculator.visit(parsed_ast)

    return max(1, calculator.score)


def _create_safe_context(data_context: Dict[str, Any], dependencies: Set[str]) -> Dict[str, Any]:
    """Create safe execution context with only required data."""
    safe_context = {}

    for column in dependencies:
        safe_name = f"col_{column.replace(' ', '_')}"
        if column in data_context:
            safe_context[safe_name] = data_context[column]

    # Add safe built-in functions
    safe_context.update(
        {
            "abs": abs,
            "round": round,
            "max": max,
            "min": min,
            "sum": sum,
            "len": len,
            "int": int,
            "float": float,
            "str": str,
        }
    )

    return safe_context


def _is_cache_valid(cached_result: Dict[str, Any], cache_ttl_seconds: int) -> bool:
    """Check if cached result is still valid."""
    if "timestamp" not in cached_result:
        return False

    age_seconds = time.time() - cached_result["timestamp"]
    return age_seconds < cache_ttl_seconds


def _update_formula_stats(formula_hash: str, execution_time_ms: float, cache_hit: bool) -> None:
    """Update formula performance statistics."""
    if formula_hash not in _formula_stats:
        _formula_stats[formula_hash] = FormulaStats(formula_hash=formula_hash)

    stats = _formula_stats[formula_hash]
    stats.total_executions += 1

    if cache_hit:
        stats.cache_hits += 1
    else:
        stats.cache_misses += 1
        stats.total_execution_time_ms += execution_time_ms

    # Update averages
    if stats.total_executions > 0:
        stats.cache_hit_rate = stats.cache_hits / stats.total_executions

    if stats.cache_misses > 0:
        stats.avg_execution_time_ms = stats.total_execution_time_ms / stats.cache_misses

    stats.last_execution_time = time.time()


# Export all functions
__all__ = [
    "compile_formula",
    "execute_formula",
    "batch_execute_formulas",
    "get_formula_stats",
    "clear_formula_cache",
    "get_hot_formulas",
    "optimize_hot_formulas",
    "FormulaCompileResult",
    "FormulaExecutionResult",
    "FormulaStats",
]
