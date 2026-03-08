"""
Schema-validated table and column proxies.

Provides dot-notation access to tables and columns that validates
against the loaded schema at query-build time, not execution time.
"""

from typing import TYPE_CHECKING, Any

from declaro_persistum.types import Column, Schema

if TYPE_CHECKING:
    from declaro_persistum.query.delete import DeleteQuery
    from declaro_persistum.query.django_style import QuerySet
    from declaro_persistum.query.insert import InsertQuery
    from declaro_persistum.query.prisma_style import PrismaQueryBuilder
    from declaro_persistum.query.select import SelectQuery
    from declaro_persistum.query.update import UpdateQuery


def table(name: str, schema: Schema) -> "TableProxy":
    """
    Create a schema-validated table proxy.

    Args:
        name: Table name (must exist in schema)
        schema: Schema dict

    Returns:
        TableProxy for building queries

    Raises:
        ValueError: If table not found in schema
    """
    if name not in schema:
        raise ValueError(f"Table '{name}' not found in schema. Available: {list(schema.keys())}")
    return TableProxy(name, schema)


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
                f"Table '{self._name}' has no column '{name}'.\nAvailable columns: {available}"
            )
        return self._columns[name]

    def select(self, *columns: "ColumnProxy | SQLFunction") -> "SelectQuery":
        """Start a SELECT query on this table."""
        from declaro_persistum.query.select import SelectQuery

        return SelectQuery(self._name, self._schema, columns)

    def insert(self, **values: Any) -> "InsertQuery":
        """Start an INSERT query on this table."""
        from declaro_persistum.query.insert import InsertQuery

        return InsertQuery(self._name, self._schema, values, self._columns)

    def update(self, **values: Any) -> "UpdateQuery":
        """Start an UPDATE query on this table."""
        from declaro_persistum.query.update import UpdateQuery

        return UpdateQuery(self._name, self._schema, values, self._columns)

    def delete(self) -> "DeleteQuery":
        """Start a DELETE query on this table."""
        from declaro_persistum.query.delete import DeleteQuery

        return DeleteQuery(self._name, self._schema)

    # =========================================================================
    # Django-style API
    # =========================================================================

    @property
    def objects(self) -> "QuerySet":
        """
        Django-style manager. Returns a QuerySet for this table.

        Example:
            users.objects.filter(status="active").order("-created_at")[:10]
        """
        from declaro_persistum.query.django_style import QuerySet

        return QuerySet(self._name, self._schema, self._columns)

    def filter(self, **kwargs: Any) -> "QuerySet":
        """
        Django-style filter. Shortcut for .objects.filter().

        Example:
            users.filter(status="active", age__gt=18)
        """
        return self.objects.filter(**kwargs)

    def exclude(self, **kwargs: Any) -> "QuerySet":
        """
        Django-style exclude. Shortcut for .objects.exclude().

        Example:
            users.exclude(status="deleted")
        """
        return self.objects.exclude(**kwargs)

    def order(self, *fields: str) -> "QuerySet":
        """
        Django-style ordering. Shortcut for .objects.order().

        Example:
            users.order("-created_at", "name")
        """
        return self.objects.order(*fields)

    # Alias
    order_by = order

    async def get(self, connection: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Django-style get. Returns single matching object.

        Example:
            user = await users.get(conn, id=user_id)

        Raises:
            DoesNotExist: If no object found
            MultipleObjectsReturned: If more than one object found
        """
        return await self.objects.get(connection, **kwargs)

    async def all(self, connection: Any) -> list[dict[str, Any]]:
        """
        Django-style all. Returns all rows.

        Example:
            all_users = await users.all(conn)
        """
        return await self.objects.all(connection)

    async def first(self, connection: Any) -> dict[str, Any] | None:
        """
        Django-style first. Returns first row or None.

        Example:
            user = await users.filter(status="active").first(conn)
        """
        return await self.objects.first(connection)

    # =========================================================================
    # Prisma-style API
    # =========================================================================

    @property
    def prisma(self) -> "PrismaQueryBuilder":
        """
        Prisma-style query builder.

        Example:
            users = await db.users.prisma.find_many(
                conn,
                where={"status": "active"},
                order={"created_at": "desc"},
                take=10
            )
        """
        from declaro_persistum.query.prisma_style import PrismaQueryBuilder

        return PrismaQueryBuilder(self._name, self._schema, self._columns)

    async def find_many(
        self,
        connection: Any,
        *,
        where: dict[str, Any] | None = None,
        order: dict[str, str] | list[dict[str, str]] | None = None,
        take: int | None = None,
        skip: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Prisma-style find_many. Shortcut for .prisma.find_many().

        Example:
            users = await db.users.find_many(
                conn,
                where={"status": "active"},
                order={"created_at": "desc"},
                take=10
            )
        """
        return await self.prisma.find_many(
            connection, where=where, order=order, take=take, skip=skip
        )

    async def find_one(
        self,
        connection: Any,
        *,
        where: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Prisma-style find_one. Returns single matching row or None.

        Example:
            user = await db.users.find_one(conn, where={"id": user_id})
        """
        return await self.prisma.find_one(connection, where=where)

    async def find_first(
        self,
        connection: Any,
        *,
        where: dict[str, Any] | None = None,
        order: dict[str, str] | list[dict[str, str]] | None = None,
    ) -> dict[str, Any] | None:
        """
        Prisma-style find_first. Returns first matching row or None.

        Example:
            user = await db.users.find_first(
                conn,
                where={"status": "active"},
                order={"created_at": "desc"}
            )
        """
        return await self.prisma.find_first(connection, where=where, order=order)

    async def create(
        self,
        connection: Any,
        *,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Prisma-style create. Creates a new record.

        Example:
            user = await db.users.create(
                conn,
                data={"email": "alice@example.com", "name": "Alice"}
            )
        """
        return await self.prisma.create(connection, data=data)

    async def update_one(
        self,
        connection: Any,
        *,
        where: dict[str, Any],
        data: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Prisma-style update. Updates a record matching the where clause.

        Example:
            user = await db.users.update_one(
                conn,
                where={"id": user_id},
                data={"name": "New Name"}
            )
        """
        return await self.prisma.update(connection, where=where, data=data)

    async def delete_one(
        self,
        connection: Any,
        *,
        where: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Prisma-style delete. Deletes a record matching the where clause.

        Example:
            user = await db.users.delete_one(conn, where={"id": user_id})
        """
        return await self.prisma.delete(connection, where=where)

    async def upsert(
        self,
        connection: Any,
        *,
        where: dict[str, Any],
        create: dict[str, Any],
        update: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Prisma-style upsert. Updates or creates a record.

        Example:
            user = await db.users.upsert(
                conn,
                where={"email": "alice@example.com"},
                create={"email": "alice@example.com", "name": "Alice"},
                update={"name": "Alice Updated"}
            )
        """
        return await self.prisma.upsert(connection, where=where, create=create, update=update)

    async def count(self, connection: Any, where: dict[str, Any] | None = None) -> int:
        """
        Prisma-style count.

        Example:
            active_count = await db.users.count(conn, where={"status": "active"})
        """
        return await self.prisma.count(connection, where=where)


class ColumnProxy:
    """
    Schema-validated column proxy for expressions.

    Supports comparison operators for WHERE clauses and JOIN ON clauses.
    When compared to another ColumnProxy, generates column-to-column SQL
    (e.g. ``orders.user_id = users.id``) with no parameters.
    When compared to a literal value, generates parameterized SQL.
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
        """LIKE pattern match."""
        return Condition(self._full_name, "LIKE", pattern)

    def ilike(self, pattern: str) -> "Condition":
        """Case-insensitive LIKE (PostgreSQL). Falls back to LIKE on SQLite."""
        return Condition(self._full_name, "ILIKE", pattern)

    def in_(self, values: list[Any]) -> "Condition":
        """IN clause."""
        return Condition(self._full_name, "IN", values)

    def is_null(self) -> "Condition":
        """IS NULL check."""
        return Condition(self._full_name, "IS", None)

    def is_not_null(self) -> "Condition":
        """IS NOT NULL check."""
        return Condition(self._full_name, "IS NOT", None)

    def between(self, low: Any, high: Any) -> "Condition":
        """BETWEEN range check."""
        return Condition(self._full_name, "BETWEEN", (low, high))

    def desc(self) -> "OrderBy":
        """Order by descending."""
        return OrderBy(self._full_name, "DESC")

    def asc(self) -> "OrderBy":
        """Order by ascending."""
        return OrderBy(self._full_name, "ASC")


class Condition:
    """Represents a WHERE condition."""

    __slots__ = ("column", "operator", "value", "_param_counter")

    # Class-level counter for unique parameter names
    _global_param_counter = 0

    def __init__(self, column: str, operator: str, value: Any):
        self.column = column
        self.operator = operator
        self.value = value
        # Get unique counter for this condition
        Condition._global_param_counter += 1
        self._param_counter = Condition._global_param_counter

    def to_sql(self, dialect: str) -> tuple[str, dict[str, Any]]:
        """Generate SQL and params for this condition."""
        # Handle special cases
        if self.value is None and self.operator == "IS":
            return f"{self.column} IS NULL", {}
        if self.value is None and self.operator == "IS NOT":
            return f"{self.column} IS NOT NULL", {}
        if self.operator == "IN":
            # Generate :param_0, :param_1, etc.
            placeholders = ", ".join(
                f":_in_{self._param_counter}_{i}" for i in range(len(self.value))
            )
            params = {f"_in_{self._param_counter}_{i}": v for i, v in enumerate(self.value)}
            return f"{self.column} IN ({placeholders})", params
        if self.operator == "BETWEEN":
            return (
                f"{self.column} BETWEEN :_between_{self._param_counter}_low AND :_between_{self._param_counter}_high",
                {
                    f"_between_{self._param_counter}_low": self.value[0],
                    f"_between_{self._param_counter}_high": self.value[1],
                },
            )
        if self.operator == "ILIKE" and dialect != "postgresql":
            # SQLite doesn't have ILIKE, use LIKE with LOWER()
            return f"LOWER({self.column}) LIKE LOWER(:_like_{self._param_counter})", {
                f"_like_{self._param_counter}": self.value
            }

        # Column-to-column comparison (for JOIN ON clauses)
        if isinstance(self.value, ColumnProxy):
            return f"{self.column} {self.operator} {self.value._full_name}", {}

        # Standard comparison - if value is string starting with :, it's a param reference
        if isinstance(self.value, str) and self.value.startswith(":"):
            return f"{self.column} {self.operator} {self.value}", {}

        # Generate param with unique name
        param_name = f"_p_{self._param_counter}"
        return f"{self.column} {self.operator} :{param_name}", {param_name: self.value}

    def __and__(self, other: "Condition | ConditionGroup") -> "ConditionGroup":
        return ConditionGroup([self, other], "AND")

    def __or__(self, other: "Condition | ConditionGroup") -> "ConditionGroup":
        return ConditionGroup([self, other], "OR")


class ConditionGroup:
    """Group of conditions with AND/OR."""

    __slots__ = ("conditions", "operator")

    def __init__(self, conditions: list["Condition | ConditionGroup"], operator: str):
        self.conditions = conditions
        self.operator = operator

    def to_sql(self, dialect: str) -> tuple[str, dict[str, Any]]:
        parts = []
        params: dict[str, Any] = {}
        for cond in self.conditions:
            sql, p = cond.to_sql(dialect)
            parts.append(f"({sql})")
            params.update(p)
        return f" {self.operator} ".join(parts), params

    def __and__(self, other: "Condition | ConditionGroup") -> "ConditionGroup":
        return ConditionGroup([self, other], "AND")

    def __or__(self, other: "Condition | ConditionGroup") -> "ConditionGroup":
        return ConditionGroup([self, other], "OR")


class OrderBy:
    """ORDER BY clause."""

    __slots__ = ("column", "direction")

    def __init__(self, column: str, direction: str):
        self.column = column
        self.direction = direction

    def to_sql(self) -> str:
        return f"{self.column} {self.direction}"


class JoinClause:
    """JOIN clause."""

    __slots__ = ("table", "on", "type")

    def __init__(self, table: str, on: "Condition", join_type: str = "inner"):
        self.table = table
        self.on = on
        self.type = join_type


class SQLFunction:
    """
    SQL function wrapper for use in SELECT.

    Allows functions like count_("*"), sum_(column) etc.
    """

    __slots__ = ("name", "args", "alias")

    def __init__(self, name: str, *args: Any, alias: str | None = None):
        self.name = name
        self.args = args
        self.alias = alias

    @property
    def _full_name(self) -> str:
        """Return the SQL representation."""
        args_str = ", ".join(
            str(a._full_name) if isinstance(a, ColumnProxy) else str(a) for a in self.args
        )
        result = f"{self.name}({args_str})"
        if self.alias:
            result = f"{result} AS {self.alias}"
        return result

    def as_(self, alias: str) -> "SQLFunction":
        """Return new function with alias."""
        return SQLFunction(self.name, *self.args, alias=alias)


# Function factories
def count_(column: str | ColumnProxy = "*", alias: str | None = None) -> SQLFunction:
    """COUNT aggregate function."""
    return SQLFunction("COUNT", column, alias=alias)


def sum_(column: ColumnProxy, alias: str | None = None) -> SQLFunction:
    """SUM aggregate function."""
    return SQLFunction("SUM", column, alias=alias)


def avg_(column: ColumnProxy, alias: str | None = None) -> SQLFunction:
    """AVG aggregate function."""
    return SQLFunction("AVG", column, alias=alias)


def min_(column: ColumnProxy, alias: str | None = None) -> SQLFunction:
    """MIN aggregate function."""
    return SQLFunction("MIN", column, alias=alias)


def max_(column: ColumnProxy, alias: str | None = None) -> SQLFunction:
    """MAX aggregate function."""
    return SQLFunction("MAX", column, alias=alias)


def now_() -> SQLFunction:
    """Current timestamp function (dialect-aware)."""
    return SQLFunction("NOW")
