"""
PRAGMA compatibility abstraction for Turso Database (Rust).

Turso Database (Rust) supports most PRAGMAs needed for introspection:
- Supported: table_info, table_list, table_xinfo, index_list, index_info, index_xinfo,
             foreign_keys, integrity_check, schema_version, and more
- NOT Supported: foreign_key_list (requires emulation)

This module provides compatibility functions that:
1. Try native PRAGMA first
2. Fall back to emulation via sqlite_master parsing if not supported
3. Return exact same format as native SQLite PRAGMA
4. Log emulation usage for monitoring

See: https://github.com/tursodatabase/turso/blob/main/COMPAT.md
"""

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

# Emulation counters for monitoring
_emulation_counters = {
    "index_list": 0,
    "index_info": 0,
    "foreign_key_list": 0,
}

_native_success_counters = {
    "index_list": 0,
    "index_info": 0,
    "foreign_key_list": 0,
}

_affected_tables: set[str] = set()


def get_emulation_count(pragma_name: str) -> int:
    """Get emulation count for a specific PRAGMA."""
    return _emulation_counters.get(pragma_name, 0)


def get_native_success_count(pragma_name: str) -> int:
    """Get native success count for a specific PRAGMA."""
    return _native_success_counters.get(pragma_name, 0)


def get_affected_tables() -> set[str]:
    """Get set of tables that required emulation."""
    return _affected_tables.copy()


def reset_counters() -> None:
    """Reset all monitoring counters."""
    global _emulation_counters, _native_success_counters, _affected_tables
    _emulation_counters = {k: 0 for k in _emulation_counters}
    _native_success_counters = {k: 0 for k in _native_success_counters}
    _affected_tables = set()


# =============================================================================
# PRAGMA table_info - Native pass-through
# =============================================================================


async def pragma_table_info(conn: Any, table: str) -> list[tuple]:
    """
    Get table column information.

    This is natively supported by both SQLite and Turso, so we pass through directly.

    Args:
        conn: Database connection (async for SQLite/Turso)
        table: Table name

    Returns:
        List of tuples: (cid, name, type, notnull, dflt_value, pk)
    """
    cursor = await conn.execute(f"PRAGMA table_info('{table}')")
    rows = await cursor.fetchall()
    return [tuple(row) for row in rows]


# =============================================================================
# PRAGMA index_list - Try native, fall back to emulation
# =============================================================================


async def pragma_index_list(conn: Any, table: str) -> list[tuple]:
    """
    Get index list for a table.

    Tries native PRAGMA first. If not supported (Turso), falls back to
    emulation via sqlite_master parsing.

    Args:
        conn: Database connection
        table: Table name

    Returns:
        List of tuples: (seq, name, unique, origin, partial)
        - seq: Index sequence number
        - name: Index name
        - unique: 1 if unique, 0 otherwise
        - origin: 'c' (CREATE INDEX), 'u' (UNIQUE constraint), 'pk' (PRIMARY KEY)
        - partial: 1 if partial index (has WHERE clause), 0 otherwise
    """
    try:
        cursor = await conn.execute(f"PRAGMA index_list('{table}')")
        rows = await cursor.fetchall()

        # If we get here with Turso, it means native support was added
        if _is_turso_connection(conn):
            logger.warning(
                f"PRAGMA index_list native support detected for Turso - "
                f"emulation may no longer be needed (table: {table})"
            )
            _native_success_counters["index_list"] += 1

        return [tuple(row) for row in rows]
    except Exception as e:
        # Check if it's a "not supported" error
        error_msg = str(e).lower()
        if "not supported" in error_msg or "no such pragma" in error_msg or "unknown" in error_msg:
            logger.info(f"Emulating PRAGMA index_list for table '{table}' (native not supported)")
            _emulation_counters["index_list"] += 1
            _affected_tables.add(table)
            return await _emulate_index_list(conn, table)
        raise


async def _emulate_index_list(conn: Any, table: str) -> list[tuple]:
    """
    Emulate PRAGMA index_list via sqlite_master parsing.

    Returns same format as native PRAGMA: (seq, name, unique, origin, partial)
    """
    cursor = await conn.execute(
        "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND tbl_name = ?",
        (table,)
    )
    rows = await cursor.fetchall()

    results = []
    for seq, (name, sql) in enumerate(rows):
        if not sql:
            # Auto-generated index (e.g., for UNIQUE constraint or PRIMARY KEY)
            # Determine origin based on name
            if name.startswith("sqlite_autoindex"):
                origin = "u"  # UNIQUE constraint
            elif "pk" in name.lower():
                origin = "pk"  # PRIMARY KEY
            else:
                origin = "u"  # Default to UNIQUE constraint
            unique = 1
            partial = 0
        else:
            # Parse CREATE INDEX statement
            sql_upper = sql.upper()

            # Check for UNIQUE
            unique = 1 if "UNIQUE INDEX" in sql_upper else 0

            # Origin is always 'c' for explicit CREATE INDEX
            origin = "c"

            # Check for partial index (WHERE clause)
            partial = 1 if " WHERE " in sql_upper else 0

        results.append((seq, name, unique, origin, partial))

    return results


# =============================================================================
# PRAGMA index_info - Try native, fall back to emulation
# =============================================================================


async def pragma_index_info(conn: Any, index_name: str) -> list[tuple]:
    """
    Get column information for an index.

    Tries native PRAGMA first. If not supported (Turso), falls back to
    emulation via sqlite_master parsing.

    Args:
        conn: Database connection
        index_name: Index name

    Returns:
        List of tuples: (seqno, cid, name)
        - seqno: Column sequence in index (0-based)
        - cid: Column ID in table (-1 for expressions, -2 for rowid)
        - name: Column name (or expression text)
    """
    try:
        cursor = await conn.execute(f"PRAGMA index_info('{index_name}')")
        rows = await cursor.fetchall()

        # If we get here with Turso, it means native support was added
        if _is_turso_connection(conn):
            logger.warning(
                f"PRAGMA index_info native support detected for Turso - "
                f"emulation may no longer be needed (index: {index_name})"
            )
            _native_success_counters["index_info"] += 1

        return [tuple(row) for row in rows]
    except Exception as e:
        # Check if it's a "not supported" error
        error_msg = str(e).lower()
        if "not supported" in error_msg or "no such pragma" in error_msg or "unknown" in error_msg:
            logger.info(f"Emulating PRAGMA index_info for index '{index_name}' (native not supported)")
            _emulation_counters["index_info"] += 1
            return await _emulate_index_info(conn, index_name)
        raise


async def _emulate_index_info(conn: Any, index_name: str) -> list[tuple]:
    """
    Emulate PRAGMA index_info via sqlite_master parsing.

    Returns same format as native PRAGMA: (seqno, cid, name)
    """
    cursor = await conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?",
        (index_name,)
    )
    row = await cursor.fetchone()

    if not row or not row[0]:
        # Index not found or auto-generated (no SQL)
        return []

    sql = row[0]

    # Parse: CREATE [UNIQUE] INDEX name ON table(col1, col2, ...)
    # Extract column list between parentheses after ON table
    match = re.search(r'\bON\s+\w+\s*\((.+?)\)', sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return []

    columns_str = match.group(1)

    # Parse individual columns (handle expressions, DESC, COLLATE, etc.)
    results = []
    seqno = 0

    # Split by comma, but be careful with nested parentheses (for expressions)
    parts = _split_columns(columns_str)

    for part in parts:
        part = part.strip()

        # Extract column name or expression
        # Remove DESC, ASC, COLLATE clauses
        col_match = re.match(r'(.+?)(?:\s+(?:ASC|DESC|COLLATE\s+\w+))*$', part, re.IGNORECASE)
        if col_match:
            col_expr = col_match.group(1).strip()

            # Check if it's an expression (contains function call or operators)
            if '(' in col_expr or any(op in col_expr for op in ['+', '-', '*', '/', '||']):
                # It's an expression
                name = col_expr
                cid = -1  # Expression
            else:
                # Simple column reference
                name = col_expr.strip('"').strip("'").strip('`')
                cid = -2  # We don't have table schema here, use -2 as placeholder

            results.append((seqno, cid, name))
            seqno += 1

    return results


def _split_columns(columns_str: str) -> list[str]:
    """Split column list by comma, respecting nested parentheses."""
    parts = []
    current = []
    depth = 0

    for char in columns_str:
        if char == '(':
            depth += 1
            current.append(char)
        elif char == ')':
            depth -= 1
            current.append(char)
        elif char == ',' and depth == 0:
            parts.append(''.join(current))
            current = []
        else:
            current.append(char)

    if current:
        parts.append(''.join(current))

    return parts


# =============================================================================
# PRAGMA foreign_key_list - Try native, fall back to emulation
# =============================================================================


async def pragma_foreign_key_list(conn: Any, table: str) -> list[tuple]:
    """
    Get foreign key constraints for a table.

    Tries native PRAGMA first. If not supported (Turso), falls back to
    emulation via CREATE TABLE parsing.

    Args:
        conn: Database connection
        table: Table name

    Returns:
        List of tuples: (id, seq, table, from, to, on_update, on_delete, match)
        - id: Foreign key ID (0-based)
        - seq: Column sequence in FK (0 for single-column, 0+ for multi-column)
        - table: Referenced table name
        - from: Column name in this table
        - to: Column name in referenced table
        - on_update: ON UPDATE action (CASCADE, SET NULL, etc.)
        - on_delete: ON DELETE action
        - match: MATCH clause (usually "NONE")
    """
    try:
        cursor = await conn.execute(f"PRAGMA foreign_key_list('{table}')")
        rows = await cursor.fetchall()

        # If we get here with Turso, it means native support was added
        if _is_turso_connection(conn):
            logger.warning(
                f"PRAGMA foreign_key_list native support detected for Turso - "
                f"emulation may no longer be needed (table: {table})"
            )
            _native_success_counters["foreign_key_list"] += 1

        return [tuple(row) for row in rows]
    except Exception as e:
        # Check if it's a "not supported" error
        error_msg = str(e).lower()
        if "not supported" in error_msg or "no such pragma" in error_msg or "unknown" in error_msg:
            logger.info(f"Emulating PRAGMA foreign_key_list for table '{table}' (native not supported)")
            _emulation_counters["foreign_key_list"] += 1
            _affected_tables.add(table)
            return await _emulate_foreign_key_list(conn, table)
        raise


async def _emulate_foreign_key_list(conn: Any, table: str) -> list[tuple]:
    """
    Emulate PRAGMA foreign_key_list via CREATE TABLE parsing.

    Returns same format as native PRAGMA:
    (id, seq, table, from, to, on_update, on_delete, match)
    """
    cursor = await conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,)
    )
    row = await cursor.fetchone()

    if not row or not row[0]:
        return []

    sql = row[0]

    # Parse foreign keys from CREATE TABLE
    fks = []
    fk_id = 0

    # Pattern 1: Inline foreign key
    # col_name TYPE REFERENCES table(col) [ON DELETE action] [ON UPDATE action]
    inline_pattern = re.compile(
        r'(\w+|"[^"]+"|\'[^\']+\'|`[^`]+`)\s+'  # Column name
        r'[^\s,]+\s+'  # Type
        r'(?:.*?\s+)?'  # Optional constraints
        r'REFERENCES\s+'
        r'(\w+|"[^"]+"|\'[^\']+\'|`[^`]+`)\s*'  # Referenced table
        r'\((\w+|"[^"]+"|\'[^\']+\'|`[^`]+`)\)'  # Referenced column
        r'(?:\s+ON\s+DELETE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION))?'
        r'(?:\s+ON\s+UPDATE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION))?',
        re.IGNORECASE
    )

    for match in inline_pattern.finditer(sql):
        from_col = _unquote(match.group(1))
        ref_table = _unquote(match.group(2))
        to_col = _unquote(match.group(3))
        on_delete = match.group(4).upper() if match.group(4) else "NO ACTION"
        on_update = match.group(5).upper() if match.group(5) else "NO ACTION"

        fks.append((fk_id, 0, ref_table, from_col, to_col, on_update, on_delete, "NONE"))
        fk_id += 1

    # Pattern 2: Table-level foreign key
    # FOREIGN KEY(col1, col2) REFERENCES table(col1, col2) [actions]
    table_level_pattern = re.compile(
        r'FOREIGN\s+KEY\s*\(([^)]+)\)\s+'
        r'REFERENCES\s+'
        r'(\w+|"[^"]+"|\'[^\']+\'|`[^`]+`)\s*'
        r'\(([^)]+)\)'
        r'(?:\s+ON\s+DELETE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION))?'
        r'(?:\s+ON\s+UPDATE\s+(CASCADE|SET\s+NULL|SET\s+DEFAULT|RESTRICT|NO\s+ACTION))?',
        re.IGNORECASE
    )

    for match in table_level_pattern.finditer(sql):
        from_cols = [_unquote(c.strip()) for c in match.group(1).split(',')]
        ref_table = _unquote(match.group(2))
        to_cols = [_unquote(c.strip()) for c in match.group(3).split(',')]
        on_delete = match.group(4).upper() if match.group(4) else "NO ACTION"
        on_update = match.group(5).upper() if match.group(5) else "NO ACTION"

        for seq, (from_col, to_col) in enumerate(zip(from_cols, to_cols)):
            fks.append((fk_id, seq, ref_table, from_col, to_col, on_update, on_delete, "NONE"))

        fk_id += 1

    # If no FKs found, try best-effort parsing
    if not fks:
        logger.debug(f"No foreign keys found in standard patterns for table '{table}'")

    return fks


def _unquote(identifier: str) -> str:
    """Remove quotes from SQL identifier."""
    identifier = identifier.strip()
    if identifier.startswith('"') and identifier.endswith('"'):
        return identifier[1:-1]
    if identifier.startswith("'") and identifier.endswith("'"):
        return identifier[1:-1]
    if identifier.startswith("`") and identifier.endswith("`"):
        return identifier[1:-1]
    return identifier


def _is_turso_connection(conn: Any) -> bool:
    """
    Detect if connection is Turso (libsql) vs SQLite.

    This is a heuristic check based on connection type/module.
    """
    conn_type = type(conn).__name__
    conn_module = type(conn).__module__

    # Check if it's libsql
    if "libsql" in conn_module.lower():
        return True

    # aiosqlite is definitely SQLite
    if "aiosqlite" in conn_module.lower():
        return False

    # Default to False (assume SQLite)
    return False
