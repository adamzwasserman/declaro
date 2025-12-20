"""Symbol table management for declaro-ximenez."""

from __future__ import annotations

from .types import Symbol, FunctionScope


def create_scope(name: str) -> FunctionScope:
    """Create a new function scope.

    Args:
        name: The function name.

    Returns:
        A new empty function scope.
    """
    return {
        "name": name,
        "style": "none",
        "symbols": {},
        "has_types_block": False,
        "return_type": None,
        "params": {},
    }


def declare_symbol(
    scope: FunctionScope,
    name: str,
    type_annotation: str,
    line: int,
    col: int,
    initialized: bool = False,
) -> Symbol | None:
    """Declare a symbol in a scope.

    Args:
        scope: The function scope.
        name: The variable name.
        type_annotation: The type annotation string.
        line: Line number of declaration.
        col: Column number of declaration.
        initialized: Whether the variable is initialized at declaration.

    Returns:
        The created symbol, or None if already declared (error case).
    """
    if name in scope["symbols"]:
        return None  # Already declared - caller should report error

    symbol: Symbol = {
        "name": name,
        "type_annotation": type_annotation,
        "line": line,
        "col": col,
        "initialized": initialized,
        "used": False,
    }
    scope["symbols"][name] = symbol
    return symbol


def lookup_symbol(scope: FunctionScope, name: str) -> Symbol | None:
    """Look up a symbol in a scope.

    Args:
        scope: The function scope.
        name: The variable name.

    Returns:
        The symbol if found, None otherwise.
    """
    return scope["symbols"].get(name)


def mark_symbol_used(scope: FunctionScope, name: str) -> bool:
    """Mark a symbol as used.

    Args:
        scope: The function scope.
        name: The variable name.

    Returns:
        True if symbol exists and was marked, False if not found.
    """
    symbol = scope["symbols"].get(name)
    if symbol is None:
        return False
    symbol["used"] = True
    return True


def mark_symbol_initialized(scope: FunctionScope, name: str) -> bool:
    """Mark a symbol as initialized.

    Args:
        scope: The function scope.
        name: The variable name.

    Returns:
        True if symbol exists and was marked, False if not found.
    """
    symbol = scope["symbols"].get(name)
    if symbol is None:
        return False
    symbol["initialized"] = True
    return True


def get_undeclared_symbols(scope: FunctionScope, used_names: set[str]) -> set[str]:
    """Get names that were used but never declared.

    Args:
        scope: The function scope.
        used_names: Set of all names used in the function.

    Returns:
        Set of names used but not declared.
    """
    declared = set(scope["symbols"].keys()) | set(scope["params"].keys())
    return used_names - declared


def get_unused_symbols(scope: FunctionScope) -> list[Symbol]:
    """Get symbols that were declared but never used.

    Args:
        scope: The function scope.

    Returns:
        List of unused symbols.
    """
    return [s for s in scope["symbols"].values() if not s["used"]]


def get_uninitialized_symbols(scope: FunctionScope) -> list[Symbol]:
    """Get symbols that were declared but never initialized.

    Args:
        scope: The function scope.

    Returns:
        List of uninitialized symbols.
    """
    return [s for s in scope["symbols"].values() if not s["initialized"]]
