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
from declaro_persistum.query.table import ColumnProxy, Condition, ConditionGroup, OrderBy
from declaro_persistum.types import Schema

if TYPE_CHECKING:
    from declaro_persistum.query.hooks import PostHook, PreHook


def _find_pk_column(schema: Schema, table_name: str) -> str:
    """Return the primary key column name for a table, or '' if not found."""
    table_def = schema.get(table_name, {})
    for col_name, col_def in table_def.get("columns", {}).items():
        if col_def.get("primary_key"):
            return col_name
    return ""


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

    __slots__ = ("_table_name", "_schema", "_columns", "_pool", "_pre", "_post")

    def __init__(
        self,
        table_name: str,
        schema: Schema,
        columns: dict[str, ColumnProxy],
        pool: Any = None,
        *,
        pre: "PreHook | None" = None,
        post: "PostHook | None" = None,
    ):
        self._table_name = table_name
        self._schema = schema
        self._columns = columns
        self._pool = pool
        self._pre = pre
        self._post = post

    def _where_to_conditions(
        self, where: dict[str, Any]
    ) -> list[Condition | ConditionGroup]:
        """Convert Prisma-style where dict to Condition/ConditionGroup objects."""
        conditions: list[Condition | ConditionGroup] = []

        for key, value in where.items():
            # Handle nested conditions
            if key == "AND":
                # AND is a list of conditions
                for sub_where in value:
                    conditions.extend(self._where_to_conditions(sub_where))
                continue
            elif key == "OR":
                # OR creates a ConditionGroup
                or_conditions: list[Condition | ConditionGroup] = []
                for sub_where in value:
                    sub_conds = self._where_to_conditions(sub_where)
                    if sub_conds:
                        or_conditions.extend(sub_conds)
                if or_conditions:
                    # Combine with OR
                    combined: Condition | ConditionGroup = or_conditions[0]
                    for c in or_conditions[1:]:
                        combined = combined | c
                    conditions.append(combined)
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

    def _combine_where(
        self, where: dict[str, Any] | None
    ) -> "Condition | ConditionGroup | None":
        """Merge a Prisma where-dict into a single Condition/ConditionGroup."""
        if not where:
            return None
        conditions = self._where_to_conditions(where)
        if not conditions:
            return None
        combined: Condition | ConditionGroup = conditions[0]
        for c in conditions[1:]:
            combined = combined & c
        return combined

    def _order_to_orderby(
        self, order: dict[str, str] | list[dict[str, str]] | None
    ) -> list[OrderBy]:
        """Convert Prisma-style order into a list of OrderBy."""
        if not order:
            return []
        if isinstance(order, dict):
            order = [order]
        result: list[OrderBy] = []
        for item in order:
            for field, direction in item.items():
                if field not in self._columns:
                    available = ", ".join(sorted(self._columns.keys()))
                    raise AttributeError(
                        f"Table '{self._table_name}' has no column '{field}'.\n"
                        f"Available columns: {available}"
                    )
                dir_sql = "DESC" if direction.lower() == "desc" else "ASC"
                result.append(OrderBy(f"{self._table_name}.{field}", dir_sql))
        return result

    def _build_select_query(
        self,
        *,
        where: dict[str, Any] | None,
        order: dict[str, str] | list[dict[str, str]] | None,
        take: int | None,
        skip: int | None,
    ) -> "Any":
        """Build a SelectQuery so hooks fire through the standard path."""
        from declaro_persistum.query.select import SelectQuery

        return SelectQuery(
            self._table_name,
            self._schema,
            (),
            where=self._combine_where(where),
            order_by=self._order_to_orderby(order),
            limit_val=take,
            offset_val=skip,
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )

    async def find_many(
        self,
        *,
        where: dict[str, Any] | None = None,
        order: dict[str, str] | list[dict[str, str]] | None = None,
        take: int | None = None,
        skip: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Find multiple records matching the criteria. Routes through hooks if configured.

        Example:
            users = await db.users.find_many(
                where={"status": "active"},
                order={"created_at": "desc"},
                take=10
            )
        """
        sq = self._build_select_query(where=where, order=order, take=take, skip=skip)
        return await sq.execute()

    async def find_one(
        self,
        *,
        where: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Find a single record by unique constraint. Routes through hooks if configured.

        Example:
            user = await db.users.find_one(where={"id": user_id})
        """
        sq = self._build_select_query(where=where, order=None, take=1, skip=None)
        return await sq.execute_one()

    async def find_first(
        self,
        *,
        where: dict[str, Any] | None = None,
        order: dict[str, str] | list[dict[str, str]] | None = None,
    ) -> dict[str, Any] | None:
        """
        Find the first record matching the criteria. Routes through hooks if configured.

        Example:
            user = await db.users.find_first(
                where={"status": "active"},
                order={"created_at": "desc"}
            )
        """
        sq = self._build_select_query(where=where, order=order, take=1, skip=None)
        return await sq.execute_one()

    async def create(
        self,
        *,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Create a new record. Routes through hooks if configured.

        Example:
            user = await db.users.create(
                data={"email": "alice@example.com", "name": "Alice"}
            )
        """
        from declaro_persistum.query.insert import InsertQuery

        # InsertQuery.__init__ validates columns against schema.
        iq = InsertQuery(
            self._table_name,
            self._schema,
            data,
            self._columns,
            returning=["*"],
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )
        result = await iq.execute_one()
        return result or data  # Return input if RETURNING not supported

    async def update(
        self,
        *,
        where: dict[str, Any],
        data: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Update a record matching the where clause. Routes through hooks if configured.

        Example:
            user = await db.users.update(
                where={"id": user_id},
                data={"name": "New Name"}
            )
        """
        from declaro_persistum.query.update import UpdateQuery

        uq = UpdateQuery(
            self._table_name,
            self._schema,
            data,
            self._columns,
            where=self._combine_where(where),
            returning=["*"],
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )
        return await uq.execute_one()

    async def delete(
        self,
        *,
        where: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Delete a record matching the where clause. Routes through hooks if configured.

        Example:
            user = await db.users.delete(where={"id": user_id})
        """
        from declaro_persistum.query.delete import DeleteQuery

        dq = DeleteQuery(
            self._table_name,
            self._schema,
            where=self._combine_where(where),
            returning=["*"],
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )
        return await dq.execute_one()

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
        Count records matching the criteria. Routes through hooks if configured.

        Example:
            count = await db.users.count(where={"status": "active"})
        """
        from declaro_persistum.query.select import SelectQuery
        from declaro_persistum.query.table import count_

        sq = SelectQuery(
            self._table_name,
            self._schema,
            (count_("*"),),
            where=self._combine_where(where),
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )
        result = await sq.execute_scalar()
        return int(result) if result else 0
