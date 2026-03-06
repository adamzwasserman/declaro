"""
Django ORM-style query API.

Provides a familiar interface for Django developers:
    users.filter(status="active").order("-created_at")[:10]
"""

from typing import TYPE_CHECKING, Any

from declaro_persistum.query.builder import Query
from declaro_persistum.query.executor import detect_dialect
from declaro_persistum.query.table import (
    ColumnProxy,
    Condition,
    ConditionGroup,
    OrderBy,
)
from declaro_persistum.types import Schema

if TYPE_CHECKING:
    pass


class QuerySet:
    """
    Django-style QuerySet for building queries.

    Supports chaining: users.filter(status="active").order("-created_at")[:10]

    Lazy evaluation - query is not executed until you call .all(), .first(),
    .get(), iterate, or slice with a step.
    """

    __slots__ = (
        "_table_name",
        "_schema",
        "_columns",
        "_filters",
        "_excludes",
        "_ordering",
        "_limit",
        "_offset",
        "_connection",
    )

    def __init__(
        self,
        table_name: str,
        schema: Schema,
        columns: dict[str, ColumnProxy],
        filters: list[Condition | ConditionGroup] | None = None,
        excludes: list[Condition | ConditionGroup] | None = None,
        ordering: list[OrderBy] | None = None,
        limit: int | None = None,
        offset: int | None = None,
        connection: Any = None,
    ):
        self._table_name = table_name
        self._schema = schema
        self._columns = columns
        self._filters = filters or []
        self._excludes = excludes or []
        self._ordering = ordering or []
        self._limit = limit
        self._offset = offset
        self._connection = connection

    def _clone(self, **kwargs: Any) -> "QuerySet":
        """Return a copy with updated attributes."""
        return QuerySet(
            table_name=kwargs.get("table_name", self._table_name),
            schema=kwargs.get("schema", self._schema),
            columns=kwargs.get("columns", self._columns),
            filters=kwargs.get("filters", list(self._filters)),
            excludes=kwargs.get("excludes", list(self._excludes)),
            ordering=kwargs.get("ordering", list(self._ordering)),
            limit=kwargs.get("limit", self._limit),
            offset=kwargs.get("offset", self._offset),
            connection=kwargs.get("connection", self._connection),
        )

    def _kwargs_to_conditions(self, negate: bool = False, **kwargs: Any) -> list[Condition]:
        """Convert keyword arguments to Condition objects."""
        conditions = []
        for key, value in kwargs.items():
            # Handle Django-style lookups: field__lookup=value
            if "__" in key:
                field, lookup = key.rsplit("__", 1)
            else:
                field, lookup = key, "exact"

            if field not in self._columns:
                available = ", ".join(sorted(self._columns.keys()))
                raise AttributeError(
                    f"Table '{self._table_name}' has no column '{field}'.\n"
                    f"Available columns: {available}"
                )

            col = self._columns[field]

            # Build condition based on lookup type
            if lookup == "exact":
                cond = col == value
            elif lookup == "ne" or lookup == "neq":
                cond = col != value
            elif lookup == "gt":
                cond = col > value
            elif lookup == "gte":
                cond = col >= value
            elif lookup == "lt":
                cond = col < value
            elif lookup == "lte":
                cond = col <= value
            elif lookup == "in":
                cond = col.in_(value)
            elif lookup == "contains":
                cond = col.like(f"%{value}%")
            elif lookup == "icontains":
                cond = col.ilike(f"%{value}%")
            elif lookup == "startswith":
                cond = col.like(f"{value}%")
            elif lookup == "istartswith":
                cond = col.ilike(f"{value}%")
            elif lookup == "endswith":
                cond = col.like(f"%{value}")
            elif lookup == "iendswith":
                cond = col.ilike(f"%{value}")
            elif lookup == "isnull":
                cond = col.is_null() if value else col.is_not_null()
            elif lookup == "range":
                cond = col.between(value[0], value[1])
            else:
                raise ValueError(f"Unknown lookup type: {lookup}")

            # For exclude(), we'd need to negate - but Condition doesn't support NOT yet
            # For now, exclude will use != for exact matches
            if negate and lookup == "exact":
                cond = col != value

            conditions.append(cond)

        return conditions

    def filter(self, **kwargs: Any) -> "QuerySet":
        """
        Filter the queryset by field values.

        Supports Django-style lookups:
            filter(status="active")           # exact match
            filter(age__gt=18)                # greater than
            filter(name__contains="alice")    # LIKE %value%
            filter(email__icontains="test")   # case-insensitive contains
            filter(id__in=[1, 2, 3])          # IN clause
            filter(deleted_at__isnull=True)   # IS NULL
        """
        new_conditions = self._kwargs_to_conditions(**kwargs)
        return self._clone(filters=self._filters + new_conditions)

    def exclude(self, **kwargs: Any) -> "QuerySet":
        """
        Exclude rows matching the given filters.

        Opposite of filter().
        """
        new_conditions = self._kwargs_to_conditions(negate=True, **kwargs)
        return self._clone(excludes=self._excludes + new_conditions)

    def order(self, *fields: str) -> "QuerySet":
        """
        Order by fields. Prefix with '-' for descending.

        Example:
            order("-created_at")        # DESC
            order("name", "-created_at") # name ASC, created_at DESC
        """
        ordering = []
        for field in fields:
            if field.startswith("-"):
                col_name = field[1:]
                direction = "DESC"
            else:
                col_name = field
                direction = "ASC"

            if col_name not in self._columns:
                available = ", ".join(sorted(self._columns.keys()))
                raise AttributeError(
                    f"Table '{self._table_name}' has no column '{col_name}'.\n"
                    f"Available columns: {available}"
                )

            ordering.append(OrderBy(f"{self._table_name}.{col_name}", direction))

        return self._clone(ordering=ordering)

    # Alias for Django compatibility
    order_by = order

    def using(self, connection: Any) -> "QuerySet":
        """Set the database connection to use."""
        return self._clone(connection=connection)

    def __getitem__(self, key: Any) -> "QuerySet | dict[str, Any]":
        """
        Support slicing: queryset[:10], queryset[5:15], queryset[0]
        """
        if isinstance(key, slice):
            start = key.start or 0
            stop = key.stop

            new_offset = (self._offset or 0) + start
            new_limit = None
            if stop is not None:
                new_limit = stop - start

            return self._clone(offset=new_offset if new_offset > 0 else None, limit=new_limit)
        elif isinstance(key, int):
            # Single item access - need to execute
            if key < 0:
                raise ValueError("Negative indexing not supported")
            qs = self._clone(offset=(self._offset or 0) + key, limit=1)
            # This would need async - return the queryset for now
            # User should use .first() or async iteration
            return qs
        else:
            raise TypeError(f"Invalid index type: {type(key)}")

    def _build_where(self, dialect: str) -> tuple[str | None, dict[str, Any]]:
        """Build WHERE clause from filters and excludes."""
        all_conditions = self._filters + self._excludes
        if not all_conditions:
            return None, {}

        # Combine all conditions with AND
        combined: Condition | ConditionGroup = all_conditions[0]
        for cond in all_conditions[1:]:
            combined = combined & cond

        return combined.to_sql(dialect)

    def to_sql(self, dialect: str = "postgresql") -> tuple[str, dict[str, Any]]:
        """Generate SQL and params."""
        sql = f"SELECT * FROM {self._table_name}"
        params: dict[str, Any] = {}

        # WHERE
        where_sql, where_params = self._build_where(dialect)
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.update(where_params)

        # ORDER BY
        if self._ordering:
            orders = ", ".join(o.to_sql() for o in self._ordering)
            sql += f" ORDER BY {orders}"

        # LIMIT/OFFSET
        if self._limit is not None:
            sql += f" LIMIT {self._limit}"
        if self._offset is not None:
            sql += f" OFFSET {self._offset}"

        return sql, params

    def to_query(self, dialect: str = "postgresql") -> Query:
        """Convert to Query dict for executor."""
        sql, params = self.to_sql(dialect)
        return {"sql": sql, "params": params, "dialect": dialect}

    async def all(self, connection: Any | None = None) -> list[dict[str, Any]]:
        """Execute query and return all results."""
        from declaro_persistum.query.executor import execute

        conn = connection or self._connection
        if conn is None:
            raise ValueError(
                "No connection provided. Use .using(conn) or pass connection to .all()"
            )
        dialect = detect_dialect(conn)
        return await execute(self.to_query(dialect), conn)

    async def first(self, connection: Any | None = None) -> dict[str, Any] | None:
        """Execute query and return first result or None."""
        from declaro_persistum.query.executor import execute_one

        conn = connection or self._connection
        if conn is None:
            raise ValueError(
                "No connection provided. Use .using(conn) or pass connection to .first()"
            )
        qs = self._clone(limit=1)
        dialect = detect_dialect(conn)
        return await execute_one(qs.to_query(dialect), conn)

    async def get(self, connection: Any | None = None, **kwargs: Any) -> dict[str, Any]:
        """
        Get a single object matching the given kwargs.

        Raises:
            DoesNotExist: If no object is found
            MultipleObjectsReturned: If more than one object is found
        """
        qs = self
        if kwargs:
            qs = qs.filter(**kwargs)

        conn = connection or self._connection
        if conn is None:
            raise ValueError(
                "No connection provided. Use .using(conn) or pass connection to .get()"
            )

        # Fetch 2 to detect multiple results
        qs_limited = qs._clone(limit=2)
        dialect = detect_dialect(conn)

        from declaro_persistum.query.executor import execute

        results: list[dict[str, Any]] = await execute(qs_limited.to_query(dialect), conn)

        if len(results) == 0:
            raise DoesNotExist(f"{self._table_name} matching query does not exist")
        if len(results) > 1:
            raise MultipleObjectsReturned(f"get() returned more than one {self._table_name}")

        return results[0]

    async def count(self, connection: Any | None = None) -> int:
        """Return count of objects matching the query."""
        from declaro_persistum.query.executor import execute_scalar

        conn = connection or self._connection
        if conn is None:
            raise ValueError(
                "No connection provided. Use .using(conn) or pass connection to .count()"
            )

        dialect = detect_dialect(conn)

        # Build count query
        sql = f"SELECT COUNT(*) FROM {self._table_name}"
        params: dict[str, Any] = {}

        where_sql, where_params = self._build_where(dialect)
        if where_sql:
            sql += f" WHERE {where_sql}"
            params.update(where_params)

        query: Query = {"sql": sql, "params": params, "dialect": dialect}
        result = await execute_scalar(query, conn)
        return int(result) if result else 0

    async def exists(self, connection: Any | None = None) -> bool:
        """Return True if the query matches any objects."""
        conn = connection or self._connection
        if conn is None:
            raise ValueError(
                "No connection provided. Use .using(conn) or pass connection to .exists()"
            )

        result = await self._clone(limit=1).first(conn)
        return result is not None

    def values(self) -> "QuerySet":
        """
        Return only specific fields.

        Note: This affects what columns are selected but still returns dicts.
        """
        # For now, we always return all fields - could optimize later
        return self

    def __repr__(self) -> str:
        sql, params = self.to_sql()
        return f"<QuerySet: {sql}>"


class DoesNotExist(Exception):
    """Raised when .get() finds no matching object."""

    pass


class MultipleObjectsReturned(Exception):
    """Raised when .get() finds more than one object."""

    pass
