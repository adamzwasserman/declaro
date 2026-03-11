"""
Prisma-style query API.

Provides a dict-based interface familiar to Prisma users:
    users = await db.users.find_many(
        where={"status": "active"},
        order={"created_at": "desc"},
        take=10
    )
"""

from typing import TYPE_CHECKING, Any

from declaro_persistum.query.builder import Query
from declaro_persistum.query.table import ColumnProxy, Condition, ConditionGroup
from declaro_persistum.types import Schema

if TYPE_CHECKING:
    pass


class PrismaQueryBuilder:
    """
    Prisma-style query builder.

    Provides dict-based query methods:
        find_many(where={}, order={}, take=, skip=)
        find_one(where={})
        find_first(where={}, order={})
        create(data={})
        update(where={}, data={})
        delete(where={})
        upsert(where={}, create={}, update={})
    """

    __slots__ = ("_table_name", "_schema", "_columns", "_pool")

    def __init__(
        self,
        table_name: str,
        schema: Schema,
        columns: dict[str, ColumnProxy],
        pool: Any = None,
    ):
        self._table_name = table_name
        self._schema = schema
        self._columns = columns
        self._pool = pool

    def _where_to_conditions(self, where: dict[str, Any]) -> list[Condition]:
        """Convert Prisma-style where dict to Condition objects."""
        conditions = []

        for key, value in where.items():
            # Handle nested conditions
            if key == "AND":
                # AND is a list of conditions
                for sub_where in value:
                    conditions.extend(self._where_to_conditions(sub_where))
                continue
            elif key == "OR":
                # OR creates a ConditionGroup
                or_conditions = []
                for sub_where in value:
                    sub_conds = self._where_to_conditions(sub_where)
                    if sub_conds:
                        or_conditions.extend(sub_conds)
                if or_conditions:
                    # Combine with OR
                    combined: Condition | ConditionGroup = or_conditions[0]
                    for c in or_conditions[1:]:
                        combined = combined | c
                    conditions.append(combined)  # type: ignore[arg-type]
                continue
            elif key == "NOT":
                # NOT negates the conditions - for now just skip unsupported
                continue

            # Regular field condition
            if key not in self._columns:
                available = ", ".join(sorted(self._columns.keys()))
                raise AttributeError(
                    f"Table '{self._table_name}' has no column '{key}'.\n"
                    f"Available columns: {available}"
                )

            col = self._columns[key]

            if isinstance(value, dict):
                # Nested operators: {"email": {"contains": "test"}}
                for op, op_value in value.items():
                    if op == "equals":
                        conditions.append(col == op_value)
                    elif op == "not":
                        conditions.append(col != op_value)
                    elif op == "in":
                        conditions.append(col.in_(op_value))
                    elif op == "notIn":
                        # NOT IN - we'd need to negate, use != for single values
                        pass
                    elif op == "lt":
                        conditions.append(col < op_value)
                    elif op == "lte":
                        conditions.append(col <= op_value)
                    elif op == "gt":
                        conditions.append(col > op_value)
                    elif op == "gte":
                        conditions.append(col >= op_value)
                    elif op == "contains":
                        conditions.append(col.like(f"%{op_value}%"))
                    elif op == "startsWith":
                        conditions.append(col.like(f"{op_value}%"))
                    elif op == "endsWith":
                        conditions.append(col.like(f"%{op_value}"))
                    elif op == "mode" and op_value == "insensitive":
                        # Applied to sibling contains/startsWith/endsWith
                        pass
                    else:
                        raise ValueError(f"Unknown Prisma operator: {op}")
            else:
                # Simple equality: {"status": "active"}
                if value is None:
                    conditions.append(col.is_null())
                else:
                    conditions.append(col == value)

        return conditions

    def _order_to_sql(self, order: dict[str, str] | list[dict[str, str]]) -> str:
        """Convert Prisma-style order to SQL ORDER BY clause."""
        if isinstance(order, dict):
            order = [order]

        parts = []
        for item in order:
            for field, direction in item.items():
                if field not in self._columns:
                    available = ", ".join(sorted(self._columns.keys()))
                    raise AttributeError(
                        f"Table '{self._table_name}' has no column '{field}'.\n"
                        f"Available columns: {available}"
                    )
                dir_sql = "DESC" if direction.lower() == "desc" else "ASC"
                parts.append(f"{self._table_name}.{field} {dir_sql}")

        return ", ".join(parts)

    def _build_select_sql(
        self,
        where: dict[str, Any] | None,
        order: dict[str, str] | list[dict[str, str]] | None,
        take: int | None,
        skip: int | None,
        dialect: str,
    ) -> tuple[str, dict[str, Any]]:
        """Build SELECT SQL from Prisma-style arguments."""
        sql = f"SELECT * FROM {self._table_name}"
        params: dict[str, Any] = {}

        # WHERE
        if where:
            conditions = self._where_to_conditions(where)
            if conditions:
                combined: Condition | ConditionGroup = conditions[0]
                for c in conditions[1:]:
                    combined = combined & c
                where_sql, where_params = combined.to_sql(dialect)
                sql += f" WHERE {where_sql}"
                params.update(where_params)

        # ORDER BY
        if order:
            sql += f" ORDER BY {self._order_to_sql(order)}"

        # LIMIT (take)
        if take is not None:
            sql += f" LIMIT {take}"

        # OFFSET (skip)
        if skip is not None:
            sql += f" OFFSET {skip}"

        return sql, params

    async def find_many(
        self,
        *,
        where: dict[str, Any] | None = None,
        order: dict[str, str] | list[dict[str, str]] | None = None,
        take: int | None = None,
        skip: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Find multiple records matching the criteria.

        Example:
            users = await db.users.find_many(
                where={"status": "active"},
                order={"created_at": "desc"},
                take=10
            )
        """
        from declaro_persistum.query.executor import execute_with_pool

        pool = self._pool

        def _query(dialect: str) -> Query:
            sql, params = self._build_select_sql(where, order, take, skip, dialect)
            return {"sql": sql, "params": params, "dialect": dialect}

        return await execute_with_pool(pool, _query, mode="all")

    async def find_one(
        self,
        *,
        where: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Find a single record by unique constraint.

        Example:
            user = await db.users.find_one(where={"id": user_id})
        """
        from declaro_persistum.query.executor import execute_with_pool

        pool = self._pool

        def _query(dialect: str) -> Query:
            sql, params = self._build_select_sql(where, None, 1, None, dialect)
            return {"sql": sql, "params": params, "dialect": dialect}

        return await execute_with_pool(pool, _query, mode="one")

    async def find_first(
        self,
        *,
        where: dict[str, Any] | None = None,
        order: dict[str, str] | list[dict[str, str]] | None = None,
    ) -> dict[str, Any] | None:
        """
        Find the first record matching the criteria.

        Example:
            user = await db.users.find_first(
                where={"status": "active"},
                order={"created_at": "desc"}
            )
        """
        from declaro_persistum.query.executor import execute_with_pool

        pool = self._pool

        def _query(dialect: str) -> Query:
            sql, params = self._build_select_sql(where, order, 1, None, dialect)
            return {"sql": sql, "params": params, "dialect": dialect}

        return await execute_with_pool(pool, _query, mode="one")

    async def create(
        self,
        *,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Create a new record.

        Example:
            user = await db.users.create(
                data={"email": "alice@example.com", "name": "Alice"}
            )
        """
        from declaro_persistum.query.executor import execute_with_pool

        pool = self._pool

        # Validate columns
        for key in data:
            if key not in self._columns:
                available = ", ".join(sorted(self._columns.keys()))
                raise AttributeError(
                    f"Table '{self._table_name}' has no column '{key}'.\n"
                    f"Available columns: {available}"
                )

        columns = list(data.keys())
        placeholders = ", ".join(f":ins_{c}" for c in columns)
        cols_sql = ", ".join(columns)

        sql = f"INSERT INTO {self._table_name} ({cols_sql}) VALUES ({placeholders}) RETURNING *"
        params = {f"ins_{k}": v for k, v in data.items()}

        def _query(dialect: str) -> Query:
            return {"sql": sql, "params": params, "dialect": dialect}

        result = await execute_with_pool(pool, _query, mode="one")
        return result or data  # Return input if RETURNING not supported

    async def update(
        self,
        *,
        where: dict[str, Any],
        data: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Update a record matching the where clause.

        Example:
            user = await db.users.update(
                where={"id": user_id},
                data={"name": "New Name"}
            )
        """
        from declaro_persistum.query.executor import execute_with_pool

        pool = self._pool

        # Validate columns
        for key in data:
            if key not in self._columns:
                available = ", ".join(sorted(self._columns.keys()))
                raise AttributeError(
                    f"Table '{self._table_name}' has no column '{key}'.\n"
                    f"Available columns: {available}"
                )

        def _query(dialect: str) -> Query:
            # Build SET clause
            set_parts = []
            params: dict[str, Any] = {}
            for col, val in data.items():
                param_name = f"upd_{col}"
                set_parts.append(f"{col} = :{param_name}")
                params[param_name] = val

            set_sql = ", ".join(set_parts)
            sql = f"UPDATE {self._table_name} SET {set_sql}"

            # Build WHERE clause
            conditions = self._where_to_conditions(where)
            if conditions:
                combined: Condition | ConditionGroup = conditions[0]
                for c in conditions[1:]:
                    combined = combined & c
                where_sql, where_params = combined.to_sql(dialect)
                sql += f" WHERE {where_sql}"
                params.update(where_params)

            sql += " RETURNING *"
            return {"sql": sql, "params": params, "dialect": dialect}

        return await execute_with_pool(pool, _query, mode="one")

    async def delete(
        self,
        *,
        where: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Delete a record matching the where clause.

        Example:
            user = await db.users.delete(where={"id": user_id})
        """
        from declaro_persistum.query.executor import execute_with_pool

        pool = self._pool

        def _query(dialect: str) -> Query:
            sql = f"DELETE FROM {self._table_name}"
            params: dict[str, Any] = {}

            # Build WHERE clause
            conditions = self._where_to_conditions(where)
            if conditions:
                combined: Condition | ConditionGroup = conditions[0]
                for c in conditions[1:]:
                    combined = combined & c
                where_sql, where_params = combined.to_sql(dialect)
                sql += f" WHERE {where_sql}"
                params.update(where_params)

            sql += " RETURNING *"
            return {"sql": sql, "params": params, "dialect": dialect}

        return await execute_with_pool(pool, _query, mode="one")

    async def upsert(
        self,
        *,
        where: dict[str, Any],
        create: dict[str, Any],
        update: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Update or create a record.

        Example:
            user = await db.users.upsert(
                where={"email": "alice@example.com"},
                create={"email": "alice@example.com", "name": "Alice"},
                update={"name": "Alice Updated"}
            )
        """
        # Try to find existing
        existing = await self.find_one(where=where)

        if existing:
            result = await self.update(where=where, data=update)
            return result or existing
        else:
            return await self.create(data=create)

    async def count(
        self,
        *,
        where: dict[str, Any] | None = None,
    ) -> int:
        """
        Count records matching the criteria.

        Example:
            count = await db.users.count(where={"status": "active"})
        """
        from declaro_persistum.query.executor import execute_with_pool

        pool = self._pool

        def _query(dialect: str) -> Query:
            sql = f"SELECT COUNT(*) FROM {self._table_name}"
            params: dict[str, Any] = {}

            if where:
                conditions = self._where_to_conditions(where)
                if conditions:
                    combined: Condition | ConditionGroup = conditions[0]
                    for c in conditions[1:]:
                        combined = combined & c
                    where_sql, where_params = combined.to_sql(dialect)
                    sql += f" WHERE {where_sql}"
                    params.update(where_params)

            return {"sql": sql, "params": params, "dialect": dialect}

        result = await execute_with_pool(pool, _query, mode="scalar")
        return int(result) if result else 0
