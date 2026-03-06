# Implementation Plan: declaro_persistum Addendum Features

**Source**: `declaro_persistum_architecture_addendum.md`
**Date**: 2025-12-14
**Status**: Ready for Implementation

---

## Overview

This plan implements the addendum features in 6 phases:

0. **Query Builder Redesign**: Schema-validated dot notation API
1. **Core Extensions**: Enums, Triggers, Procedures, Views
2. **Portable Abstractions**: Arrays, Maps, Ranges, Hierarchies
3. **Query Layer**: Native functions, Dialect translation
4. **Observability**: Timing, Slow queries, Auto-indexing
5. **Advanced**: Full-text search, Events (optional)

Each phase builds on the previous. Tests precede implementation.

---

## bd Issue Structure

```
Epic: Implement declaro_persistum Addendum Features
├── Feature: Phase 0 - Query Builder Redesign
│   ├── Task: Create TableProxy and ColumnProxy classes
│   ├── Task: Implement SelectQuery with fluent API
│   ├── Task: Implement InsertQuery, UpdateQuery, DeleteQuery
│   ├── Task: Add schema validation on column access
│   ├── Task: Add join support with dot notation
│   └── Task: Migrate executor to work with new Query types
├── Feature: Phase 1 - Core Schema Extensions
│   ├── Task: Add Enum TypedDict and loader support
│   ├── Task: Add Enum applier (PostgreSQL native, SQLite CHECK)
│   ├── Task: Add Trigger TypedDict and loader support
│   ├── Task: Add Trigger applier (PostgreSQL, SQLite)
│   ├── Task: Add Procedure TypedDict and loader support
│   ├── Task: Add Procedure applier (PostgreSQL only)
│   ├── Task: Add View TypedDict and loader support
│   └── Task: Add View applier (with materialized support)
├── Feature: Phase 2 - Portable Abstractions
│   ├── Task: Implement array → junction table abstraction
│   ├── Task: Implement map → junction table abstraction
│   ├── Task: Implement range → start/end columns abstraction
│   └── Task: Implement hierarchy → closure table abstraction
├── Feature: Phase 3 - Query Layer Extensions
│   ├── Task: Add aggregate functions (sum_, count_, avg_, etc.)
│   ├── Task: Add scalar functions (lower_, coalesce_, etc.)
│   └── Task: Add dialect-aware function translation
├── Feature: Phase 4 - Observability
│   ├── Task: Add query timing instrumentation
│   ├── Task: Add slow query recording
│   ├── Task: Add index recommendation analysis
│   └── Task: Add auto-index creation
└── Feature: Phase 5 - Advanced (Optional)
    ├── Task: Add full-text search abstraction
    └── Task: Add events/polling abstraction
```

---

## Phase 0: Query Builder Redesign

### Priority: 0 (Foundation - must complete first)

Replace the string-based query builder with a schema-validated, dot-notation API.

#### Design Principles

1. **Schema-validated**: `users.emial` raises `AttributeError` immediately, not at DB execution
2. **Fluent API**: Chain methods like SQLAlchemy but without ORM baggage
3. **Immutable queries**: Each method returns a new query object
4. **No dataclasses**: Results are `dict[str, Any]` or `TypedDict` - no class instantiation
5. **Stateless proxies**: TableProxy/ColumnProxy hold references, not state

#### API Design

```python
from declaro_persistum.query import table

# Load schema-aware table proxies
users = table("users")
orders = table("orders")

# SELECT with dot notation
query = (
    users
    .select(users.id, users.email, users.name)
    .where(users.status == "active")
    .order_by(users.created_at.desc())
    .limit(10)
)
results: list[dict[str, Any]] = await query.execute(conn)

# SELECT single row
user = await users.select(users.id, users.email).where(users.id == ":id").params(id=user_id).execute_one(conn)

# SELECT with JOIN
query = (
    orders
    .select(orders.id, orders.total, users.email)
    .join(users, on=orders.user_id == users.id)
    .where(orders.status == "pending")
)

# INSERT
await (
    users
    .insert(email=":email", name=":name")
    .params(email="alice@example.com", name="Alice")
    .execute(conn)
)

# UPDATE
await (
    users
    .update(name=":name", updated_at=now_())
    .where(users.id == ":id")
    .params(id=user_id, name="New Name")
    .execute(conn)
)

# DELETE
await (
    users
    .delete()
    .where(users.id == ":id")
    .params(id=user_id)
    .execute(conn)
)

# Scalar query
count = await users.select(count_("*")).where(users.status == "active").execute_scalar(conn)
```

#### Type Definitions

```python
# query/types.py

class Condition(TypedDict):
    """SQL condition from comparison."""
    sql: str
    params: dict[str, Any]

class OrderBy(TypedDict):
    """ORDER BY clause."""
    sql: str

class JoinClause(TypedDict):
    """JOIN clause."""
    table: str
    on: str
    type: Literal["inner", "left", "right", "full"]
```

#### Files to Create/Modify

**New files:**
- `src/declaro_persistum/query/table.py` - TableProxy, ColumnProxy
- `src/declaro_persistum/query/select.py` - SelectQuery
- `src/declaro_persistum/query/insert.py` - InsertQuery
- `src/declaro_persistum/query/update.py` - UpdateQuery
- `src/declaro_persistum/query/delete.py` - DeleteQuery
- `src/declaro_persistum/query/conditions.py` - Condition, OrderBy, JoinClause
- `tests/unit/test_query_builder.py`
- `tests/integration/test_query_*.py`

**Modify:**
- `src/declaro_persistum/query/__init__.py` - Export new API
- `src/declaro_persistum/query/executor.py` - Handle new Query types

#### Task 0.1: Create TableProxy and ColumnProxy

```python
# query/table.py

from declaro_persistum.loader import load_schema
from declaro_persistum.types import Schema, Column

_default_schema: Schema | None = None

def set_default_schema(schema: Schema) -> None:
    """Set the default schema for table() calls."""
    global _default_schema
    _default_schema = schema

def load_default_schema(models_dir: str = "./models") -> None:
    """Load schema from Pydantic model directory and set as default."""
    global _default_schema
    _default_schema = load_schema_from_models(models_dir)

def table(name: str, schema: Schema | None = None) -> "TableProxy":
    """
    Create a schema-validated table proxy.

    Args:
        name: Table name (must exist in schema)
        schema: Schema dict (uses default if not provided)

    Raises:
        ValueError: If table not found in schema
    """
    s = schema or _default_schema
    if s is None:
        raise ValueError("No schema loaded. Call load_default_schema() first.")
    if name not in s:
        raise ValueError(f"Table '{name}' not found in schema. Available: {list(s.keys())}")
    return TableProxy(name, s)


class TableProxy:
    """
    Schema-validated table proxy for query building.

    Stateless - only holds table name and schema reference.
    All query methods return new immutable query objects.
    """
    __slots__ = ("_name", "_schema", "_columns")

    def __init__(self, name: str, schema: Schema):
        self._name = name
        self._schema = schema
        table_def = schema[name]
        self._columns: dict[str, ColumnProxy] = {
            col_name: ColumnProxy(name, col_name, col_def)
            for col_name, col_def in table_def.get("columns", {}).items()
        }

    @property
    def _table_name(self) -> str:
        return self._name

    def __getattr__(self, name: str) -> "ColumnProxy":
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in self._columns:
            available = ", ".join(sorted(self._columns.keys()))
            raise AttributeError(
                f"Table '{self._name}' has no column '{name}'.\n"
                f"Available columns: {available}"
            )
        return self._columns[name]

    def select(self, *columns: "ColumnProxy") -> "SelectQuery":
        from declaro_persistum.query.select import SelectQuery
        return SelectQuery(self._name, self._schema, columns)

    def insert(self, **values: Any) -> "InsertQuery":
        from declaro_persistum.query.insert import InsertQuery
        return InsertQuery(self._name, self._schema, values)

    def update(self, **values: Any) -> "UpdateQuery":
        from declaro_persistum.query.update import UpdateQuery
        return UpdateQuery(self._name, self._schema, values)

    def delete(self) -> "DeleteQuery":
        from declaro_persistum.query.delete import DeleteQuery
        return DeleteQuery(self._name, self._schema)


class ColumnProxy:
    """
    Schema-validated column proxy for expressions.

    Supports comparison operators for WHERE clauses.
    """
    __slots__ = ("_table", "_name", "_definition")

    def __init__(self, table: str, name: str, definition: Column):
        self._table = table
        self._name = name
        self._definition = definition

    @property
    def _full_name(self) -> str:
        return f"{self._table}.{self._name}"

    @property
    def _col_name(self) -> str:
        return self._name

    def __eq__(self, other: Any) -> "Condition":  # type: ignore[override]
        return Condition(self._full_name, "=", other)

    def __ne__(self, other: Any) -> "Condition":  # type: ignore[override]
        return Condition(self._full_name, "!=", other)

    def __lt__(self, other: Any) -> "Condition":
        return Condition(self._full_name, "<", other)

    def __le__(self, other: Any) -> "Condition":
        return Condition(self._full_name, "<=", other)

    def __gt__(self, other: Any) -> "Condition":
        return Condition(self._full_name, ">", other)

    def __ge__(self, other: Any) -> "Condition":
        return Condition(self._full_name, ">=", other)

    def like(self, pattern: str) -> "Condition":
        return Condition(self._full_name, "LIKE", pattern)

    def ilike(self, pattern: str) -> "Condition":
        """Case-insensitive LIKE (PostgreSQL). Falls back to LIKE on SQLite."""
        return Condition(self._full_name, "ILIKE", pattern)

    def in_(self, values: list[Any]) -> "Condition":
        return Condition(self._full_name, "IN", values)

    def is_null(self) -> "Condition":
        return Condition(self._full_name, "IS", None)

    def is_not_null(self) -> "Condition":
        return Condition(self._full_name, "IS NOT", None)

    def between(self, low: Any, high: Any) -> "Condition":
        return Condition(self._full_name, "BETWEEN", (low, high))

    def desc(self) -> "OrderBy":
        return OrderBy(self._full_name, "DESC")

    def asc(self) -> "OrderBy":
        return OrderBy(self._full_name, "ASC")


class Condition:
    """Represents a WHERE condition."""
    __slots__ = ("column", "operator", "value")

    def __init__(self, column: str, operator: str, value: Any):
        self.column = column
        self.operator = operator
        self.value = value

    def to_sql(self, dialect: str) -> tuple[str, dict[str, Any]]:
        """Generate SQL and params for this condition."""
        # Handle special cases
        if self.value is None and self.operator == "IS":
            return f"{self.column} IS NULL", {}
        if self.value is None and self.operator == "IS NOT":
            return f"{self.column} IS NOT NULL", {}
        if self.operator == "IN":
            # Generate :param_0, :param_1, etc.
            placeholders = ", ".join(f":_in_{i}" for i in range(len(self.value)))
            params = {f"_in_{i}": v for i, v in enumerate(self.value)}
            return f"{self.column} IN ({placeholders})", params
        if self.operator == "BETWEEN":
            return f"{self.column} BETWEEN :_between_low AND :_between_high", {
                "_between_low": self.value[0],
                "_between_high": self.value[1],
            }
        if self.operator == "ILIKE" and dialect != "postgresql":
            # SQLite doesn't have ILIKE, use LIKE with LOWER()
            return f"LOWER({self.column}) LIKE LOWER(:_like)", {"_like": self.value}

        # Standard comparison - if value is string starting with :, it's a param reference
        if isinstance(self.value, str) and self.value.startswith(":"):
            return f"{self.column} {self.operator} {self.value}", {}

        # Generate param
        param_name = f"_p_{self.column.replace('.', '_')}"
        return f"{self.column} {self.operator} :{param_name}", {param_name: self.value}

    def __and__(self, other: "Condition") -> "ConditionGroup":
        return ConditionGroup([self, other], "AND")

    def __or__(self, other: "Condition") -> "ConditionGroup":
        return ConditionGroup([self, other], "OR")


class ConditionGroup:
    """Group of conditions with AND/OR."""
    __slots__ = ("conditions", "operator")

    def __init__(self, conditions: list[Condition | "ConditionGroup"], operator: str):
        self.conditions = conditions
        self.operator = operator

    def to_sql(self, dialect: str) -> tuple[str, dict[str, Any]]:
        parts = []
        params = {}
        for cond in self.conditions:
            sql, p = cond.to_sql(dialect)
            parts.append(f"({sql})")
            params.update(p)
        return f" {self.operator} ".join(parts), params


class OrderBy:
    """ORDER BY clause."""
    __slots__ = ("column", "direction")

    def __init__(self, column: str, direction: str):
        self.column = column
        self.direction = direction

    def to_sql(self) -> str:
        return f"{self.column} {self.direction}"
```

#### Task 0.2: Implement SelectQuery

```python
# query/select.py

class SelectQuery:
    """Immutable SELECT query builder."""
    __slots__ = (
        "_table", "_schema", "_columns", "_where", "_order_by",
        "_limit", "_offset", "_joins", "_group_by", "_having", "_params"
    )

    def __init__(
        self,
        table: str,
        schema: Schema,
        columns: tuple[ColumnProxy, ...],
        where: Condition | ConditionGroup | None = None,
        order_by: list[OrderBy] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        joins: list[JoinClause] | None = None,
        group_by: list[ColumnProxy] | None = None,
        having: Condition | None = None,
        params: dict[str, Any] | None = None,
    ):
        self._table = table
        self._schema = schema
        self._columns = columns
        self._where = where
        self._order_by = order_by or []
        self._limit = limit
        self._offset = offset
        self._joins = joins or []
        self._group_by = group_by or []
        self._having = having
        self._params = params or {}

    def where(self, condition: Condition | ConditionGroup) -> "SelectQuery":
        """Add WHERE clause (returns new query)."""
        return SelectQuery(
            self._table, self._schema, self._columns,
            where=condition,
            order_by=self._order_by,
            limit=self._limit,
            offset=self._offset,
            joins=self._joins,
            group_by=self._group_by,
            having=self._having,
            params=self._params,
        )

    def order_by(self, *orders: OrderBy) -> "SelectQuery":
        """Add ORDER BY clause (returns new query)."""
        return SelectQuery(
            self._table, self._schema, self._columns,
            where=self._where,
            order_by=list(orders),
            limit=self._limit,
            offset=self._offset,
            joins=self._joins,
            group_by=self._group_by,
            having=self._having,
            params=self._params,
        )

    def limit(self, n: int) -> "SelectQuery":
        """Add LIMIT clause (returns new query)."""
        return SelectQuery(
            self._table, self._schema, self._columns,
            where=self._where,
            order_by=self._order_by,
            limit=n,
            offset=self._offset,
            joins=self._joins,
            group_by=self._group_by,
            having=self._having,
            params=self._params,
        )

    def offset(self, n: int) -> "SelectQuery":
        """Add OFFSET clause (returns new query)."""
        return SelectQuery(
            self._table, self._schema, self._columns,
            where=self._where,
            order_by=self._order_by,
            limit=self._limit,
            offset=n,
            joins=self._joins,
            group_by=self._group_by,
            having=self._having,
            params=self._params,
        )

    def join(
        self,
        other: TableProxy,
        on: Condition,
        type: Literal["inner", "left", "right", "full"] = "inner"
    ) -> "SelectQuery":
        """Add JOIN clause (returns new query)."""
        new_joins = self._joins + [JoinClause(other._table_name, on, type)]
        return SelectQuery(
            self._table, self._schema, self._columns,
            where=self._where,
            order_by=self._order_by,
            limit=self._limit,
            offset=self._offset,
            joins=new_joins,
            group_by=self._group_by,
            having=self._having,
            params=self._params,
        )

    def group_by(self, *columns: ColumnProxy) -> "SelectQuery":
        """Add GROUP BY clause (returns new query)."""
        return SelectQuery(
            self._table, self._schema, self._columns,
            where=self._where,
            order_by=self._order_by,
            limit=self._limit,
            offset=self._offset,
            joins=self._joins,
            group_by=list(columns),
            having=self._having,
            params=self._params,
        )

    def params(self, **kwargs: Any) -> "SelectQuery":
        """Add query parameters (returns new query)."""
        return SelectQuery(
            self._table, self._schema, self._columns,
            where=self._where,
            order_by=self._order_by,
            limit=self._limit,
            offset=self._offset,
            joins=self._joins,
            group_by=self._group_by,
            having=self._having,
            params={**self._params, **kwargs},
        )

    def to_sql(self, dialect: str) -> tuple[str, dict[str, Any]]:
        """Generate SQL and params."""
        # SELECT clause
        cols = ", ".join(c._full_name for c in self._columns) or "*"
        sql = f"SELECT {cols} FROM {self._table}"
        params = dict(self._params)

        # JOINs
        for join in self._joins:
            join_sql, join_params = join.on.to_sql(dialect)
            join_type = join.type.upper()
            sql += f" {join_type} JOIN {join.table} ON {join_sql}"
            params.update(join_params)

        # WHERE
        if self._where:
            where_sql, where_params = self._where.to_sql(dialect)
            sql += f" WHERE {where_sql}"
            params.update(where_params)

        # GROUP BY
        if self._group_by:
            cols = ", ".join(c._full_name for c in self._group_by)
            sql += f" GROUP BY {cols}"

        # HAVING
        if self._having:
            having_sql, having_params = self._having.to_sql(dialect)
            sql += f" HAVING {having_sql}"
            params.update(having_params)

        # ORDER BY
        if self._order_by:
            orders = ", ".join(o.to_sql() for o in self._order_by)
            sql += f" ORDER BY {orders}"

        # LIMIT/OFFSET
        if self._limit is not None:
            sql += f" LIMIT {self._limit}"
        if self._offset is not None:
            sql += f" OFFSET {self._offset}"

        return sql, params

    async def execute(self, connection: Any) -> list[dict[str, Any]]:
        """Execute query and return all rows."""
        from declaro_persistum.query.executor import execute
        dialect = _detect_dialect(connection)
        sql, params = self.to_sql(dialect)
        return await execute(Query(sql=sql, params=params, dialect=dialect), connection)

    async def execute_one(self, connection: Any) -> dict[str, Any] | None:
        """Execute query and return single row or None."""
        from declaro_persistum.query.executor import execute_one
        dialect = _detect_dialect(connection)
        sql, params = self.to_sql(dialect)
        return await execute_one(Query(sql=sql, params=params, dialect=dialect), connection)

    async def execute_scalar(self, connection: Any) -> Any:
        """Execute query and return scalar value."""
        from declaro_persistum.query.executor import execute_scalar
        dialect = _detect_dialect(connection)
        sql, params = self.to_sql(dialect)
        return await execute_scalar(Query(sql=sql, params=params, dialect=dialect), connection)
```

#### Validation

```bash
uv run pytest tests/unit/test_query_builder.py -v
uv run pytest tests/integration/test_query_postgresql.py -v
uv run pytest tests/integration/test_query_sqlite.py -v
uv run mypy src/declaro_persistum/query/ --ignore-missing-imports
```

---

## Phase 1: Core Schema Extensions

### Priority: 1 (Foundation)

#### Task 1.1: Add Enum Support

**TypedDict Definition** (`types.py`):
```python
class Enum(TypedDict):
    """Enum type definition."""
    type: Literal["enum"]
    values: list[str]
```

**Files to Create/Modify**:
- `src/declaro_persistum/types.py` - Add Enum TypedDict
- `src/declaro_persistum/pydantic_loader.py` - Detect Literal types for enum values
- `src/declaro_persistum/abstractions/enums.py` - Generate lookup table + FK constraint
- `src/declaro_persistum/applier/postgresql.py` - Generate lookup table + FK
- `src/declaro_persistum/applier/sqlite.py` - Generate lookup table + FK
- `src/declaro_persistum/applier/turso.py` - Same as SQLite
- `tests/unit/test_enums.py` - Unit tests
- `tests/integration/test_enums_*.py` - Integration tests per dialect

**Validation**:
```bash
uv run pytest tests/unit/test_enums.py -v
uv run pytest tests/integration/test_enums_sqlite.py -v
TEST_POSTGRESQL_URL="..." uv run pytest tests/integration/test_enums_postgresql.py -v
```

---

#### Task 1.2: Add Trigger Support

**TypedDict Definition** (`types.py`):
```python
class Trigger(TypedDict, total=False):
    """Trigger definition."""
    timing: Literal["before", "after", "instead_of"]
    event: str | list[str]  # insert, update, delete
    for_each: Literal["row", "statement"]
    when: str               # optional condition
    body: str               # inline trigger body
    execute: str            # reference to stored procedure
```

**Files to Create/Modify**:
- `src/declaro_persistum/types.py` - Add Trigger TypedDict
- `src/declaro_persistum/loader.py` - Parse `[table.triggers.*]` sections
- `src/declaro_persistum/applier/postgresql.py` - Generate trigger function + CREATE TRIGGER
- `src/declaro_persistum/applier/sqlite.py` - Generate CREATE TRIGGER with inline body
- `src/declaro_persistum/inspector/postgresql.py` - Introspect existing triggers
- `src/declaro_persistum/inspector/sqlite.py` - Introspect existing triggers
- `tests/unit/test_triggers.py`
- `tests/integration/test_triggers_*.py`

**PostgreSQL Output**:
```sql
CREATE OR REPLACE FUNCTION {table}_{trigger_name}() RETURNS TRIGGER AS $$
BEGIN
    {body}
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER {trigger_name}
    {timing} {event} ON {table}
    FOR EACH {for_each}
    EXECUTE FUNCTION {table}_{trigger_name}();
```

**SQLite Output**:
```sql
CREATE TRIGGER {table}_{trigger_name}
    {timing} {event} ON {table}
    FOR EACH {for_each}
BEGIN
    {body}
END;
```

---

#### Task 1.3: Add Stored Procedure Support

**TypedDict Definition** (`types.py`):
```python
class Parameter(TypedDict):
    """Procedure parameter."""
    name: str
    type: str
    default: str | None

class Procedure(TypedDict, total=False):
    """Stored procedure definition."""
    language: Literal["sql", "plpgsql"]
    returns: str
    parameters: list[Parameter]
    body: str
```

**Files to Create/Modify**:
- `src/declaro_persistum/types.py` - Add Parameter, Procedure TypedDicts
- `src/declaro_persistum/pydantic_loader.py` - Load @procedure decorated classes
- `src/declaro_persistum/applier/postgresql.py` - Generate CREATE FUNCTION
- `src/declaro_persistum/applier/sqlite.py` - Raise `NotSupportedError`
- `tests/unit/test_procedures.py`
- `tests/integration/test_procedures_postgresql.py`

**Error for SQLite**:
```python
raise NotSupportedError(
    f"Stored procedure '{name}' requires PostgreSQL.\n\n"
    "SQLite does not support stored procedures.\n\n"
    "Options:\n"
    "  1. Move logic to application layer\n"
    "  2. Use SQLite triggers with inline logic\n"
    "  3. Use PostgreSQL for this project"
)
```

---

#### Task 1.4: Add View Support

**TypedDict Definition** (`types.py`):
```python
class View(TypedDict, total=False):
    """View definition."""
    query: str
    materialized: bool        # PostgreSQL only
    refresh: Literal["on_demand", "on_commit"]
```

**Files to Create/Modify**:
- `src/declaro_persistum/types.py` - Add View TypedDict
- `src/declaro_persistum/pydantic_loader.py` - Load @view decorated classes
- `src/declaro_persistum/applier/postgresql.py` - CREATE [MATERIALIZED] VIEW
- `src/declaro_persistum/applier/sqlite.py` - CREATE VIEW (warn if materialized)
- `src/declaro_persistum/inspector/postgresql.py` - Introspect views
- `src/declaro_persistum/inspector/sqlite.py` - Introspect views
- `tests/unit/test_views.py`
- `tests/integration/test_views_*.py`

---

## Phase 2: Portable Abstractions

### Priority: 1 (Core functionality)

New directory: `src/declaro_persistum/abstractions/`

#### Task 2.1: Array → Junction Table

**Schema Definition**:
```python
@table("users")
class User(BaseModel):
    id: UUID = field(primary=True)
    roles: list[str] = field(db_type="array<text>")
```

**Generated Schema**:
```sql
CREATE TABLE users_roles (
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    value TEXT NOT NULL,
    position INTEGER NOT NULL,
    PRIMARY KEY (user_id, position)
);
CREATE INDEX users_roles_value_idx ON users_roles(value);
```

**Files to Create**:
- `src/declaro_persistum/abstractions/__init__.py`
- `src/declaro_persistum/abstractions/arrays.py`
  - `parse_array_type(type_str: str) -> tuple[str, str]` - Extract element type
  - `generate_junction_table(table: str, column: str, element_type: str) -> Table`
  - `array_insert(conn, table, column, values: list) -> None`
  - `array_append(conn, table, column, value) -> None`
  - `array_hydrate(row: dict, column: str, junction_data: list) -> dict`
- `tests/unit/test_arrays.py`
- `tests/integration/test_arrays_*.py`

---

#### Task 2.2: Map → Junction Table

**Schema Definition**:
```python
@table("products")
class Product(BaseModel):
    id: UUID = field(primary=True)
    attributes: dict[str, str] = field(db_type="map<text, text>")
```

**Generated Schema**:
```sql
CREATE TABLE products_attributes (
    product_id UUID NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (product_id, key)
);
```

**Files to Create**:
- `src/declaro_persistum/abstractions/maps.py`
  - `parse_map_type(type_str: str) -> tuple[str, str, str]` - Extract key/value types
  - `generate_junction_table(table: str, column: str, key_type: str, value_type: str) -> Table`
  - `map_get(conn, table, column, key: str) -> str | None`
  - `map_set(conn, table, column, key: str, value: str) -> None`
  - `map_hydrate(row: dict, column: str, junction_data: list) -> dict`
- `tests/unit/test_maps.py`
- `tests/integration/test_maps_*.py`

---

#### Task 2.3: Range → Start/End Columns

**Schema Definition**:
```python
@table("reservations")
class Reservation(BaseModel):
    id: UUID = field(primary=True)
    during: RangeType[datetime] = field(
        db_type="range<timestamptz>",
        start_required=False,
        end_required=False,
    )
```

**Generated Schema**:
```sql
CREATE TABLE reservations (
    id UUID PRIMARY KEY,
    during_start TIMESTAMPTZ,
    during_end TIMESTAMPTZ,
    CHECK (during_start IS NULL OR during_end IS NULL OR during_start < during_end)
);
```

**Files to Create**:
- `src/declaro_persistum/abstractions/ranges.py`
  - `parse_range_type(type_str: str) -> str` - Extract element type
  - `generate_range_columns(column: str, element_type: str, start_req: bool, end_req: bool) -> dict[str, Column]`
  - `range_overlaps(start1, end1, start2, end2) -> str` - Generate SQL condition
  - `range_contains_point(start, end, point) -> str`
  - `range_contains_range(outer_start, outer_end, inner_start, inner_end) -> str`
- `tests/unit/test_ranges.py`
- `tests/integration/test_ranges_*.py`

---

#### Task 2.4: Hierarchy → Closure Table

**Schema Definition**:
```python
@table("categories")
class Category(BaseModel):
    id: UUID = field(primary=True)
    name: str
    parent_id: UUID | None = field(
        references="categories.id",
        closure=True,
    )
```

**Generated Schema**:
```sql
CREATE TABLE categories_closure (
    ancestor_id UUID NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    descendant_id UUID NOT NULL REFERENCES categories(id) ON DELETE CASCADE,
    depth INTEGER NOT NULL,
    PRIMARY KEY (ancestor_id, descendant_id)
);
```

**Files to Create**:
- `src/declaro_persistum/abstractions/hierarchy.py`
  - `generate_closure_table(table: str, column: str) -> Table`
  - `closure_insert(conn, table, node_id, parent_id) -> None`
  - `closure_update_parent(conn, table, node_id, new_parent_id) -> None`
  - `descendants_query(table: str, ancestor_id) -> str`
  - `ancestors_query(table: str, descendant_id) -> str`
  - `path_query(table: str, node_id) -> str`
- `tests/unit/test_hierarchy.py`
- `tests/integration/test_hierarchy_*.py`

---

## Phase 3: Query Layer Extensions

### Priority: 2 (Enhances usability)

New directory: `src/declaro_persistum/functions/`

#### Task 3.1: Aggregate Functions

**Files to Create**:
- `src/declaro_persistum/functions/__init__.py`
- `src/declaro_persistum/functions/aggregates.py`
  - `sum_(column: str, alias: str | None = None) -> SQLFunction`
  - `count_(column: str = "*", alias: str | None = None) -> SQLFunction`
  - `avg_(column: str, alias: str | None = None) -> SQLFunction`
  - `min_(column: str, alias: str | None = None) -> SQLFunction`
  - `max_(column: str, alias: str | None = None) -> SQLFunction`
  - `string_agg_(column: str, separator: str, alias: str | None = None) -> SQLFunction`
- `tests/unit/test_aggregates.py`

---

#### Task 3.2: Scalar Functions

**Files to Create**:
- `src/declaro_persistum/functions/scalars.py`
  - `lower_(column: str) -> SQLFunction`
  - `upper_(column: str) -> SQLFunction`
  - `coalesce_(*columns: str) -> SQLFunction`
  - `length_(column: str) -> SQLFunction`
  - `trim_(column: str) -> SQLFunction`
  - `now_() -> SQLFunction`
  - `gen_random_uuid_() -> SQLFunction`
  - `extract_year_(column: str) -> SQLFunction`
  - `date_add_days_(date_col: str, days: int) -> SQLFunction`
- `tests/unit/test_scalars.py`

---

#### Task 3.3: Dialect-Aware Translation

**Files to Create**:
- `src/declaro_persistum/functions/translations.py`
  - `FUNCTION_TRANSLATIONS: dict[str, dict[str, str]]`
  - `translate_function(name: str, dialect: str, **kwargs) -> str`
- Modify `src/declaro_persistum/query/builder.py` to use translations
- `tests/unit/test_translations.py`
- `tests/integration/test_functions_*.py` (per dialect)

**Translation Table**:
```python
FUNCTION_TRANSLATIONS = {
    "now": {
        "postgresql": "now()",
        "sqlite": "datetime('now')",
        "turso": "datetime('now')",
    },
    "gen_random_uuid": {
        "postgresql": "gen_random_uuid()",
        "sqlite": "lower(hex(randomblob(4))) || '-' || ...",
        "turso": "lower(hex(randomblob(4))) || '-' || ...",
    },
    # ... etc
}
```

---

## Phase 4: Observability

### Priority: 2 (Production readiness)

New directory: `src/declaro_persistum/observability/`

#### Task 4.1: Query Timing Instrumentation

**Files to Create**:
- `src/declaro_persistum/observability/__init__.py`
- `src/declaro_persistum/observability/timing.py`
  - `fingerprint_query(sql: str) -> str`
  - `execute_with_timing(conn, sql, params, observer=None) -> Any`
  - `QueryObserver` Protocol
- Modify `src/declaro_persistum/query/executor.py` to optionally use timing
- `tests/unit/test_timing.py`

---

#### Task 4.2: Slow Query Recording

**Config** (Python configuration):
```python
observability_config = ObservabilityConfig(
    enabled=True,
    slow_threshold_ms=500,
    retention_hours=168,
)
```

**Files to Create**:
- `src/declaro_persistum/observability/slow_queries.py`
  - `SlowQueryObserver` class
  - `setup_slow_query_table(conn) -> None`
  - `record_slow_query(conn, fingerprint, sql, elapsed_ms) -> None`
  - `get_slow_queries(conn, since: datetime | None = None) -> list[dict]`
  - `cleanup_old_queries(conn, retention_hours: int) -> int`
- `tests/unit/test_slow_queries.py`
- `tests/integration/test_slow_queries_*.py`

---

#### Task 4.3: Index Recommendation Analysis

**CLI Command**:
```bash
declaro analyze --connection $DATABASE_URL
```

**Files to Create**:
- `src/declaro_persistum/observability/analyzer.py`
  - `analyze_slow_queries(conn) -> list[IndexRecommendation]`
  - `estimate_improvement(conn, table, columns) -> float`
  - `format_recommendations(recommendations) -> str`
- `src/declaro_persistum/cli/commands.py` - Add `cmd_analyze`
- `tests/unit/test_analyzer.py`

---

#### Task 4.4: Auto-Index Creation

**Config** (Python configuration):
```python
auto_index_config = AutoIndexConfig(
    enabled=True,
    mode="recommend",  # or "auto"
    min_occurrences=1000,
    min_latency_ms=200,
    max_indexes_per_table=10,
)
```

**Files to Create**:
- `src/declaro_persistum/observability/auto_index.py`
  - `should_create_index(conn, table, columns, config) -> bool`
  - `estimate_index_size(conn, table, columns) -> int`
  - `check_disk_space(conn) -> bool`
  - `auto_create_index(conn, table, columns, unique=False, concurrently=True) -> str | None`
- `tests/unit/test_auto_index.py`
- `tests/integration/test_auto_index_postgresql.py`

---

## Phase 5: Advanced (Optional)

### Priority: 3 (Nice-to-have)

#### Task 5.1: Full-Text Search Abstraction

**Schema Definition**:
```python
@table("articles")
class Article(BaseModel):
    id: UUID = field(primary=True)
    title: str
    body: str = field(
        search=True,
        search_config={"min_word_length": 3, "stop_words": "english"},
    )
```

**Files to Create**:
- `src/declaro_persistum/abstractions/search.py`
  - `tokenize(text: str, config: SearchConfig) -> list[tuple[str, int]]`
  - `generate_search_table(table: str, column: str) -> Table`
  - `update_search_index(conn, table, column, row_id, text) -> None`
  - `search_query(table: str, column: str, terms: list[str]) -> str`
- `tests/unit/test_search.py`
- `tests/integration/test_search_*.py`

---

#### Task 5.2: Events/Polling Abstraction

**Config** (Python configuration):
```python
events_config = EventsConfig(
    enabled=True,
    poll_interval_ms=100,
    retention_hours=24,
    channels=["orders", "users"],
)
```

**Files to Create**:
- `src/declaro_persistum/abstractions/events.py`
  - `setup_events_table(conn) -> None`
  - `publish(conn, channel, payload) -> int`
  - `subscribe(conn, channel, last_seen_id=0, poll_interval_ms=100) -> AsyncIterator[Event]`
  - `cleanup_events(conn, retention_hours) -> int`
- `tests/unit/test_events.py`
- `tests/integration/test_events_*.py`

---

## bd Commands Script

```bash
#!/bin/bash
# Implementation plan for declaro_persistum addendum
# Generated: 2025-12-14

# Create epic
EPIC_ID=$(bd create "Implement declaro_persistum Addendum Features" -t epic -p 1 \
  -d "Extended schema objects, portable abstractions, query functions, and observability.
      Source: declaro_persistum_architecture_addendum.md
      Phases: 5 (Core Extensions, Abstractions, Query Layer, Observability, Advanced)" \
  --json | jq -r '.id')

echo "Created epic: $EPIC_ID"

# Phase 1: Core Schema Extensions
PHASE1_ID=$(bd create "Phase 1: Core Schema Extensions" -t feature -p 1 \
  -d "Add support for enums, triggers, stored procedures, and views.
      Enums: Lookup table + FK constraint (all backends)
      Triggers: Both dialects with syntax translation
      Procedures: PostgreSQL only (NotSupportedError for SQLite)
      Views: Regular and materialized (PostgreSQL)" \
  --deps parent-child:$EPIC_ID --json | jq -r '.id')

bd create "Add Enum support (types, loader, appliers)" -t task -p 1 \
  -d "1. Add Enum TypedDict to types.py
      2. Update pydantic_loader to detect Literal types for enum values
      3. Create abstractions/enums.py to generate lookup table + FK constraint
      4. All backends: lookup table + FK (portable pattern)
      5. Add tests: tests/unit/test_enums.py, tests/integration/test_enums_*.py
      Validation: uv run pytest tests/ -k enum -v" \
  --deps parent-child:$PHASE1_ID --json

bd create "Add Trigger support (types, loader, appliers, inspectors)" -t task -p 1 \
  -d "1. Add Trigger TypedDict to types.py
      2. Update loader to parse [table.triggers.*] sections
      3. PostgreSQL applier: trigger function + CREATE TRIGGER
      4. SQLite applier: CREATE TRIGGER with inline body
      5. Update inspectors to introspect triggers
      6. Add tests
      Validation: uv run pytest tests/ -k trigger -v" \
  --deps parent-child:$PHASE1_ID --json

bd create "Add Stored Procedure support (PostgreSQL only)" -t task -p 1 \
  -d "1. Add Parameter, Procedure TypedDicts to types.py
      2. Load procedures from @procedure decorated classes in Pydantic models
      3. PostgreSQL applier: CREATE FUNCTION
      4. SQLite applier: raise NotSupportedError with helpful message
      5. Add tests
      Validation: uv run pytest tests/ -k procedure -v" \
  --deps parent-child:$PHASE1_ID --json

bd create "Add View support (regular and materialized)" -t task -p 1 \
  -d "1. Add View TypedDict to types.py
      2. Load views from @view decorated classes in Pydantic models
      3. PostgreSQL applier: CREATE [MATERIALIZED] VIEW
      4. SQLite applier: CREATE VIEW (warn if materialized requested)
      5. Update inspectors to introspect views
      6. Add tests
      Validation: uv run pytest tests/ -k view -v" \
  --deps parent-child:$PHASE1_ID --json

# Phase 2: Portable Abstractions
PHASE2_ID=$(bd create "Phase 2: Portable Abstractions" -t feature -p 1 \
  -d "Implement portable patterns for PostgreSQL-like features.
      Arrays → junction tables with position
      Maps → junction tables with key/value
      Ranges → start/end columns with CHECK
      Hierarchies → closure tables for O(1) queries" \
  --deps parent-child:$EPIC_ID,blocks:$PHASE1_ID --json | jq -r '.id')

bd create "Implement array → junction table abstraction" -t task -p 1 \
  -d "Create: src/declaro_persistum/abstractions/arrays.py
      Functions:
        - parse_array_type(type_str) -> (element_type)
        - generate_junction_table(table, column, element_type) -> Table
        - array_insert, array_append, array_hydrate
      Generate: {table}_{column} junction table with position column
      Tests: tests/unit/test_arrays.py, tests/integration/test_arrays_*.py
      Validation: uv run pytest tests/ -k array -v" \
  --deps parent-child:$PHASE2_ID --json

bd create "Implement map → junction table abstraction" -t task -p 1 \
  -d "Create: src/declaro_persistum/abstractions/maps.py
      Functions:
        - parse_map_type(type_str) -> (key_type, value_type)
        - generate_junction_table(table, column, key_type, value_type) -> Table
        - map_get, map_set, map_hydrate
      Generate: {table}_{column} junction table with key/value
      Tests: tests/unit/test_maps.py, tests/integration/test_maps_*.py
      Validation: uv run pytest tests/ -k map -v" \
  --deps parent-child:$PHASE2_ID --json

bd create "Implement range → start/end columns abstraction" -t task -p 1 \
  -d "Create: src/declaro_persistum/abstractions/ranges.py
      Functions:
        - parse_range_type(type_str) -> element_type
        - generate_range_columns(column, type, start_req, end_req) -> dict
        - range_overlaps, range_contains_point, range_contains_range
      Generate: {column}_start, {column}_end with CHECK constraint
      NULL semantics: unbounded in that direction
      Tests: tests/unit/test_ranges.py, tests/integration/test_ranges_*.py
      Validation: uv run pytest tests/ -k range -v" \
  --deps parent-child:$PHASE2_ID --json

bd create "Implement hierarchy → closure table abstraction" -t task -p 1 \
  -d "Create: src/declaro_persistum/abstractions/hierarchy.py
      Functions:
        - generate_closure_table(table, column) -> Table
        - closure_insert, closure_update_parent
        - descendants_query, ancestors_query, path_query
      Generate: {table}_closure with ancestor_id, descendant_id, depth
      Maintain referential integrity on INSERT/UPDATE/DELETE
      Tests: tests/unit/test_hierarchy.py, tests/integration/test_hierarchy_*.py
      Validation: uv run pytest tests/ -k hierarchy -v" \
  --deps parent-child:$PHASE2_ID --json

# Phase 3: Query Layer Extensions
PHASE3_ID=$(bd create "Phase 3: Query Layer Extensions" -t feature -p 2 \
  -d "Add SQL function wrappers with dialect translation.
      Aggregates: sum_, count_, avg_, min_, max_, string_agg_
      Scalars: lower_, upper_, coalesce_, now_, gen_random_uuid_, etc.
      Dialect-aware translation for portable code" \
  --deps parent-child:$EPIC_ID,blocks:$PHASE2_ID --json | jq -r '.id')

bd create "Add aggregate function wrappers" -t task -p 2 \
  -d "Create: src/declaro_persistum/functions/aggregates.py
      Functions: sum_, count_, avg_, min_, max_, string_agg_
      Each returns SQLFunction with to_sql(dialect) method
      Integrate with query builder
      Tests: tests/unit/test_aggregates.py
      Validation: uv run pytest tests/ -k aggregate -v" \
  --deps parent-child:$PHASE3_ID --json

bd create "Add scalar function wrappers" -t task -p 2 \
  -d "Create: src/declaro_persistum/functions/scalars.py
      Functions: lower_, upper_, coalesce_, length_, trim_,
                 now_, gen_random_uuid_, extract_year_, date_add_days_
      Each returns SQLFunction with to_sql(dialect) method
      Tests: tests/unit/test_scalars.py
      Validation: uv run pytest tests/ -k scalar -v" \
  --deps parent-child:$PHASE3_ID --json

bd create "Add dialect-aware function translation" -t task -p 2 \
  -d "Create: src/declaro_persistum/functions/translations.py
      FUNCTION_TRANSLATIONS dict mapping function names to dialect SQL
      translate_function(name, dialect, **kwargs) -> str
      Critical translations:
        - now() -> datetime('now') for SQLite
        - gen_random_uuid() -> hex(randomblob()) expression for SQLite
        - EXTRACT(YEAR FROM x) -> strftime('%Y', x) for SQLite
      Tests: tests/unit/test_translations.py, tests/integration/test_functions_*.py
      Validation: uv run pytest tests/ -k translation -v" \
  --deps parent-child:$PHASE3_ID --json

# Phase 4: Observability
PHASE4_ID=$(bd create "Phase 4: Observability" -t feature -p 2 \
  -d "Add production monitoring capabilities.
      Query timing with <1ms overhead
      Slow query recording and analysis
      Automatic index recommendations and creation" \
  --deps parent-child:$EPIC_ID,blocks:$PHASE3_ID --json | jq -r '.id')

bd create "Add query timing instrumentation" -t task -p 2 \
  -d "Create: src/declaro_persistum/observability/timing.py
      Functions:
        - fingerprint_query(sql) -> normalized query string
        - execute_with_timing(conn, sql, params, observer) -> result
      QueryObserver protocol for pluggable recording
      Overhead target: <1ms per query
      Modify executor.py to optionally use timing wrapper
      Tests: tests/unit/test_timing.py
      Validation: uv run pytest tests/ -k timing -v" \
  --deps parent-child:$PHASE4_ID --json

bd create "Add slow query recording" -t task -p 2 \
  -d "Create: src/declaro_persistum/observability/slow_queries.py
      Config: ObservabilityConfig (Python)
      Functions:
        - SlowQueryObserver class implementing QueryObserver
        - setup_slow_query_table(conn) -> create declaro_slow_queries table
        - record_slow_query, get_slow_queries, cleanup_old_queries
      Schema: declaro_slow_queries table with fingerprint, sql_text, elapsed_ms
      Tests: tests/unit/test_slow_queries.py, tests/integration/test_slow_queries_*.py
      Validation: uv run pytest tests/ -k slow_quer -v" \
  --deps parent-child:$PHASE4_ID --json

bd create "Add index recommendation analysis" -t task -p 2 \
  -d "Create: src/declaro_persistum/observability/analyzer.py
      CLI: declaro analyze --connection URL
      Functions:
        - analyze_slow_queries(conn) -> list[IndexRecommendation]
        - estimate_improvement(conn, table, columns) -> float
        - format_recommendations(recs) -> str (formatted CLI output)
      Add cmd_analyze to cli/commands.py
      Tests: tests/unit/test_analyzer.py
      Validation: uv run pytest tests/ -k analyz -v" \
  --deps parent-child:$PHASE4_ID --json

bd create "Add auto-index creation" -t task -p 2 \
  -d "Create: src/declaro_persistum/observability/auto_index.py
      Config: [observability.auto_index] with mode, min_occurrences, etc.
      Functions:
        - should_create_index(conn, table, columns, config) -> bool
        - estimate_index_size, check_disk_space
        - auto_create_index(conn, table, columns, unique, concurrently) -> name
      Safety: max_indexes_per_table, disk space check, CONCURRENTLY for PG
      Tests: tests/unit/test_auto_index.py, tests/integration/test_auto_index_*.py
      Validation: uv run pytest tests/ -k auto_index -v" \
  --deps parent-child:$PHASE4_ID --json

# Phase 5: Advanced (Optional)
PHASE5_ID=$(bd create "Phase 5: Advanced Features (Optional)" -t feature -p 3 \
  -d "Optional advanced features.
      Full-text search via inverted index tables
      Events/polling for cross-dialect pub/sub
      Lower priority - implement if time permits" \
  --deps parent-child:$EPIC_ID,blocks:$PHASE4_ID --json | jq -r '.id')

bd create "Add full-text search abstraction (optional)" -t task -p 3 \
  -d "Create: src/declaro_persistum/abstractions/search.py
      Functions: tokenize, generate_search_table, update_search_index, search_query
      Schema: {table}_{column}_search inverted index table
      Features: single term, multi-term AND, prefix search
      Limitations vs PostgreSQL FTS: no ranking, basic stemming
      Tests: tests/unit/test_search.py, tests/integration/test_search_*.py
      Validation: uv run pytest tests/ -k search -v" \
  --deps parent-child:$PHASE5_ID --json

bd create "Add events/polling abstraction (optional)" -t task -p 3 \
  -d "Create: src/declaro_persistum/abstractions/events.py
      Config: EventsConfig (Python)
      Functions: setup_events_table, publish, subscribe (async iterator), cleanup_events
      Schema: declaro_events table with channel, payload, created_at
      Latency: configurable poll interval (10-100ms typical)
      Tests: tests/unit/test_events.py, tests/integration/test_events_*.py
      Validation: uv run pytest tests/ -k event -v" \
  --deps parent-child:$PHASE5_ID --json

echo ""
echo "Implementation plan created!"
echo "Epic ID: $EPIC_ID"
echo ""
echo "View dependency tree: bd dep tree $EPIC_ID"
echo "View ready tasks: bd ready --json"
```

---

## Validation Commands

```bash
# Run all tests
uv run pytest tests/ -v

# Run mypy
uv run mypy src/declaro_persistum --ignore-missing-imports

# Run specific phase tests
uv run pytest tests/ -k "enum or trigger or procedure or view" -v   # Phase 1
uv run pytest tests/ -k "array or map or range or hierarchy" -v     # Phase 2
uv run pytest tests/ -k "aggregate or scalar or translation" -v     # Phase 3
uv run pytest tests/ -k "timing or slow or analyz or auto_index" -v # Phase 4

# Integration tests with PostgreSQL
TEST_POSTGRESQL_URL="postgresql://postgres:postgres@localhost/postgres" \
  uv run pytest tests/integration/ -v

# Integration tests with SQLite
uv run pytest tests/integration/test_*_sqlite.py -v
```

---

## Acceptance Criteria

### Phase 1 Complete When:
- [ ] Enums work: Lookup table + FK constraint (all backends)
- [ ] Triggers work: both dialects with proper syntax
- [ ] Procedures work: PostgreSQL only, clear error for SQLite
- [ ] Views work: regular and materialized (PostgreSQL)
- [ ] All unit tests pass
- [ ] PostgreSQL and SQLite integration tests pass
- [ ] mypy passes with no errors

### Phase 2 Complete When:
- [ ] Arrays generate junction tables with position
- [ ] Maps generate junction tables with key/value
- [ ] Ranges generate start/end columns with CHECK
- [ ] Hierarchies generate closure tables
- [ ] All abstraction queries work across dialects
- [ ] Junction tables have proper CASCADE deletes

### Phase 3 Complete When:
- [ ] Aggregate functions generate correct SQL per dialect
- [ ] Scalar functions generate correct SQL per dialect
- [ ] Dialect translation works for now(), gen_random_uuid(), etc.
- [ ] Query builder integrates with function wrappers

### Phase 4 Complete When:
- [ ] Query timing has <1ms overhead
- [ ] Slow queries are recorded above threshold
- [ ] Analysis command produces useful recommendations
- [ ] Auto-index respects safety constraints

### Phase 5 Complete When:
- [ ] Full-text search works with basic queries
- [ ] Events/polling achieves target latency
- [ ] Both features are optional and don't break core
