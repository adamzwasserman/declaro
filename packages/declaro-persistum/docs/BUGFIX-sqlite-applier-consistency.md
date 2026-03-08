# SQLite Applier Bug Fixes and Enhancements

## Date Identified: 2026-02-01

## Overview

Two issues were discovered in the SQLite applier during production use in the multicardz project:

1. **Bug**: Inconsistent method signature for `_drop_table_sql`
2. **Missing Feature**: `add_foreign_key` operation should use table reconstruction

---

## Issue 1: `_drop_table_sql` Signature Mismatch

### Location
`src/declaro_persistum/applier/sqlite.py`

### Problem

Line 207 calls all operation generators with a uniform signature:

```python
return generator(table, details)
```

However, `_drop_table_sql` (line 293) only accepts one parameter:

```python
def _drop_table_sql(self, table: str) -> str:
    """Generate DROP TABLE statement."""
    return f'DROP TABLE "{table}"'
```

This causes a `TypeError` when attempting to drop a table:
```
TypeError: SQLiteApplier._drop_table_sql() takes 2 positional arguments but 3 were given
```

### Root Cause

Other generators like `_create_table_sql`, `_rename_table_sql`, `_add_column_sql`, `_drop_view_sql` all accept `(table, details)`. The `_drop_table_sql` method was not updated to match the uniform calling convention.

### Fix Required

Update the method signature to accept `details` (even if unused):

```python
def _drop_table_sql(self, table: str, details: dict[str, Any] = None) -> str:
    """Generate DROP TABLE statement."""
    return f'DROP TABLE "{table}"'
```

### Tests Required

Add test case in `tests/unit/test_sqlite_applier.py`:

```python
async def test_drop_table_operation():
    """Verify drop_table operation works with standard generator interface."""
    applier = SQLiteApplier()
    operations = [
        {
            "op": "drop_table",
            "table": "old_table",
            "details": {}
        }
    ]

    sql = applier.generate_sql(operations, [0])
    assert sql == ['DROP TABLE "old_table"']
```

---

## Issue 2: `add_foreign_key` Should Use Table Reconstruction

### Location
`src/declaro_persistum/applier/sqlite.py`, line 398

### Problem

The `_add_foreign_key_sql` method raises `NotImplementedError`:

```python
def _add_foreign_key_sql(self, table: str, details: dict[str, Any]) -> str:
    raise NotImplementedError(
        f"SQLite doesn't support adding foreign keys after table creation. "
        f"Foreign key on '{details.get('column')}' requires table reconstruction."
    )
```

This violates declaro_persistum's core design principle: **abstract away backend limitations by implementing workarounds in Python**.

### Root Cause

SQLite does not support `ALTER TABLE ADD CONSTRAINT FOREIGN KEY`. The only way to add a foreign key to an existing table is table reconstruction.

### Fix Required

The `_add_foreign_key_sql` method should use the existing table reconstruction infrastructure (documented in `table_reconstruction.md`) to:

1. Introspect current table schema
2. Add the new foreign key constraint to the schema
3. Perform table reconstruction with the updated schema
4. Preserve all data, indexes, and existing constraints

### Proposed Implementation

```python
async def _add_foreign_key(self, conn, table: str, details: dict[str, Any]) -> str:
    """Add foreign key via table reconstruction.

    SQLite doesn't support ALTER TABLE ADD CONSTRAINT FOREIGN KEY.
    This method uses table reconstruction to add the FK transparently.

    Args:
        conn: Database connection
        table: Table to modify
        details: Foreign key specification containing:
            - column: Column name
            - references_table: Referenced table
            - references_column: Referenced column
            - on_delete: Optional ON DELETE action
            - on_update: Optional ON UPDATE action

    Returns:
        Empty string (operation performed directly, no SQL to return)
    """
    from declaro_persistum.abstractions.table_reconstruction import reconstruct_table
    from declaro_persistum.inspector.sqlite import SQLiteInspector

    # Introspect current schema
    inspector = SQLiteInspector()
    current_schema = await inspector.introspect_table(conn, table)

    # Add foreign key to schema
    column = details["column"]
    current_schema["columns"][column]["foreign_key"] = {
        "table": details["references_table"],
        "column": details["references_column"],
        "on_delete": details.get("on_delete", "NO ACTION"),
        "on_update": details.get("on_update", "NO ACTION"),
    }

    # Reconstruct table with new FK
    await reconstruct_table(conn, table, current_schema["columns"])

    return ""  # No SQL to return; operation performed directly
```

### Architecture Consideration

This requires the applier to have access to a database connection during SQL generation, which is a design change. Two approaches:

**Option A: Two-Phase Application**
- `generate_sql()` returns placeholder for complex operations
- `apply()` detects placeholders and executes reconstruction directly

**Option B: Operation Callbacks**
- Operations can include a `callback` that is executed during `apply()`
- `_add_foreign_key` registers a callback instead of returning SQL

### Tests Required

Add BDD scenario in `tests/bdd/features/table_reconstruction.feature`:

```gherkin
Scenario: Add foreign key to existing table via reconstruction
  Given a table "orders" with columns:
    | name        | type    | nullable |
    | id          | INTEGER | false    |
    | customer_id | INTEGER | true     |
  And a table "customers" with columns:
    | name | type    | nullable |
    | id   | INTEGER | false    |
  When I add a foreign key on "orders.customer_id" referencing "customers.id"
  Then the operation should succeed
  And the "orders" table should have a foreign key constraint
  And all existing data should be preserved
```

Add unit test in `tests/unit/test_sqlite_applier.py`:

```python
async def test_add_foreign_key_uses_reconstruction():
    """Verify add_foreign_key transparently uses table reconstruction."""
    # Setup: Create tables
    await conn.execute('CREATE TABLE customers (id INTEGER PRIMARY KEY)')
    await conn.execute('CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)')
    await conn.execute('INSERT INTO orders VALUES (1, 1)')

    # Execute
    applier = SQLiteApplier()
    operations = [{
        "op": "add_foreign_key",
        "table": "orders",
        "details": {
            "column": "customer_id",
            "references_table": "customers",
            "references_column": "id",
        }
    }]
    result = await applier.apply(conn, operations, [0])

    # Verify
    assert result["success"]

    # Check FK exists via pragma
    cursor = await conn.execute("PRAGMA foreign_key_list(orders)")
    fks = cursor.fetchall()
    assert len(fks) == 1
    assert fks[0][2] == "customers"  # referenced table

    # Verify data preserved
    cursor = await conn.execute("SELECT * FROM orders")
    assert cursor.fetchone() == (1, 1)
```

---

## Priority

- **Issue 1 (signature bug)**: P0 - Blocks any migration that includes a DROP TABLE
- **Issue 2 (FK reconstruction)**: P1 - Blocks migrations adding FKs to existing tables

## Related Files

- `src/declaro_persistum/applier/sqlite.py`
- `src/declaro_persistum/applier/shared.py` (shared SQL generation extracted post-bugfix)
- `src/declaro_persistum/abstractions/table_reconstruction.py`
- `docs/table_reconstruction.md`
- `tests/unit/test_sqlite_applier.py`
- `tests/bdd/features/table_reconstruction.feature`

> **Note**: The applier code referenced in this document has since been refactored. Pure SQL generation methods (e.g. `_drop_table_sql`, `_column_definition`) were extracted from the class into `applier/shared.py` as standalone pure functions. The fixes described here are preserved in the shared module.
