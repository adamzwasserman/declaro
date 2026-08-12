"""
Schema-validated table and column proxies.

Provides dot-notation access to tables and columns that validates
against the loaded schema at query-build time, not execution time.

-------------------------------------------------------------------------------
STOP. THIS MODULE CONTAINS INTERNAL IMPLEMENTATION DETAILS.
-------------------------------------------------------------------------------

``Condition``, ``ConditionGroup``, ``CaseExpression``, ``CaseOrderBy``,
``SubqueryExpr``, and ``SQLFunction`` are **internal classes**. Their
``.to_sql()`` and ``.to_sql_fragment()`` methods are called by ``SelectQuery``
as part of query assembly. They are not part of the public API. They carry no
stability guarantee. Their signatures may change without notice.

If you are calling ``.to_sql()`` on a ``Condition`` or any other internal class
directly, **stop doing that immediately**. You are bypassing the query layer and
will be broken by the next refactor with no sympathy.

The public API is ``SelectQuery``:

    # Build a query using the table proxy
    rows = await (
        users.select(users.id, users.email)
        .where(users.status == "active")
        .order_by(users.created_at.desc())
        .execute()
    )

    # Or get the SQL string + params if you need to inspect them
    sql, params = q.to_sql()           # defaults to postgresql dialect
    sql, params = q.to_sql("sqlite")   # explicit dialect

``SelectQuery.to_sql(dialect)`` is the one place the dialect is exposed.
It propagates to every internal component automatically. You do not touch
``Condition``, ``ConditionGroup``, or anything else in this module directly.
-------------------------------------------------------------------------------
"""

from typing import TYPE_CHECKING, Any, Literal, TypedDict

from declaro_persistum.types import Column, Schema

if TYPE_CHECKING:
    from declaro_persistum.query.delete import DeleteQuery
    from declaro_persistum.query.django_style import QuerySet
    from declaro_persistum.query.hooks import PostHook, PreHook
    from declaro_persistum.query.insert import InsertQuery
    from declaro_persistum.query.prisma_style import PrismaQueryBuilder
    from declaro_persistum.query.select import SelectQuery
    from declaro_persistum.query.update import UpdateQuery


def table(
    name: str,
    schema: Schema,
    pool: Any = None,
    *,
    pre: "PreHook | None" = None,
    post: "PostHook | None" = None,
) -> "TableProxy":
    """
    Create a schema-validated table proxy.

    Args:
        name: Table name (must exist in schema)
        schema: Schema dict
        pool: Connection pool with acquire() context manager
        pre: Optional pre-hook — runs before SQL is built, transforms the query object.
        post: Optional post-hook — runs after DB returns, transforms rows.

    Returns:
        TableProxy for building queries

    Raises:
        ValueError: If table not found in schema
    """
    if name not in schema:
        raise ValueError(f"Table '{name}' not found in schema. Available: {list(schema.keys())}")
    return TableProxy(name, schema, pool, pre=pre, post=post)


class TableProxy:
    """
    Schema-validated table proxy for query building.

    Stateless - only holds table name and schema reference.
    All query methods return new immutable query objects.
    """

    __slots__ = ("_name", "_schema", "_columns", "_pool", "_alias", "_pre", "_post")

    def __init__(
        self,
        name: str,
        schema: Schema,
        pool: Any,
        alias: str | None = None,
        *,
        pre: "PreHook | None" = None,
        post: "PostHook | None" = None,
    ):
        self._name = name
        self._schema = schema
        self._pool = pool
        self._alias = alias
        self._pre = pre
        self._post = post
        # Column refs use the alias as table prefix when aliasing is active
        col_table_ref = alias if alias is not None else name
        table_def = schema[name]
        self._columns: dict[str, ColumnProxy] = {
            col_name: ColumnProxy(col_table_ref, col_name, col_def)
            for col_name, col_def in table_def.get("columns", {}).items()
        }

    @property
    def _table_name(self) -> str:
        """SQL table reference — includes alias when set: 'table AS alias'."""
        if self._alias is not None:
            return f"{self._name} AS {self._alias}"
        return self._name

    def alias(self, alias_name: str) -> "TableProxy":
        """
        Return a new proxy with a SQL alias.

        Required for self-joins. Column refs on the returned proxy emit
        alias.column in SQL; the FROM/JOIN clause emits table AS alias.

        Example:
            comments = table("comments", schema, pool)
            replies = comments.alias("replies")

            # Generates: LEFT JOIN comments AS replies ON replies.parent_id = comments.id
            rows = await (
                comments.select(comments.id, count_(replies.id).as_("reply_count"))
                .join(replies, on=(replies.parent_id == comments.id), type="left")
                .group_by(comments.id)
                .execute()
            )
        """
        return TableProxy(
            self._name,
            self._schema,
            self._pool,
            alias=alias_name,
            pre=self._pre,
            post=self._post,
        )

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

        return SelectQuery(
            self._name,
            self._schema,
            columns,
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )

    def insert(self, **values: Any) -> "InsertQuery":
        """Start an INSERT query on this table."""
        from declaro_persistum.query.insert import InsertQuery

        return InsertQuery(
            self._name,
            self._schema,
            values,
            self._columns,
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )

    def update(self, **values: Any) -> "UpdateQuery":
        """Start an UPDATE query on this table."""
        from declaro_persistum.query.update import UpdateQuery

        return UpdateQuery(
            self._name,
            self._schema,
            values,
            self._columns,
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )

    def delete(self) -> "DeleteQuery":
        """Start a DELETE query on this table."""
        from declaro_persistum.query.delete import DeleteQuery

        return DeleteQuery(
            self._name,
            self._schema,
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )

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

        return QuerySet(
            self._name,
            self._schema,
            self._columns,
            pool=self._pool,
            pre=self._pre,
            post=self._post,
        )

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

    async def get(self, **kwargs: Any) -> dict[str, Any]:
        """
        Django-style get. Returns single matching object.

        Example:
            user = await users.get(id=user_id)

        Raises:
            DoesNotExist: If no object found
            MultipleObjectsReturned: If more than one object found
        """
        return await self.objects.get(**kwargs)

    async def all(self) -> list[dict[str, Any]]:
        """
        Django-style all. Returns all rows.

        Example:
            all_users = await users.all()
        """
        return await self.objects.all()

    async def first(self) -> dict[str, Any] | None:
        """
        Django-style first. Returns first row or None.

        Example:
            user = await users.filter(status="active").first()
        """
        return await self.objects.first()

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

        return PrismaQueryBuilder(
            self._name,
            self._schema,
            self._columns,
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
        Prisma-style find_many. Shortcut for .prisma.find_many().

        Example:
            users = await db.users.find_many(
                where={"status": "active"},
                order={"created_at": "desc"},
                take=10
            )
        """
        return await self.prisma.find_many(
            where=where, order=order, take=take, skip=skip
        )

    async def find_one(
        self,
        *,
        where: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Prisma-style find_one. Returns single matching row or None.

        Example:
            user = await db.users.find_one(where={"id": user_id})
        """
        return await self.prisma.find_one(where=where)

    async def find_first(
        self,
        *,
        where: dict[str, Any] | None = None,
        order: dict[str, str] | list[dict[str, str]] | None = None,
    ) -> dict[str, Any] | None:
        """
        Prisma-style find_first. Returns first matching row or None.

        Example:
            user = await db.users.find_first(
                where={"status": "active"},
                order={"created_at": "desc"}
            )
        """
        return await self.prisma.find_first(where=where, order=order)

    async def create(
        self,
        *,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Prisma-style create. Creates a new record.

        Example:
            user = await db.users.create(
                data={"email": "alice@example.com", "name": "Alice"}
            )
        """
        return await self.prisma.create(data=data)

    async def update_one(
        self,
        *,
        where: dict[str, Any],
        data: dict[str, Any] | None = None,
        increment: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """
        Prisma-style update. Updates a record matching the where clause.

        ``data`` sets literal values; ``increment`` applies atomic
        ``col = col + delta``. Both may be supplied in the same call.

        Example:
            user = await db.users.update_one(
                where={"id": user_id},
                data={"name": "New Name"},
            )

            # Atomic increment, no read-modify-write race:
            await db.tags.update_one(
                where={"tag_id": tag_id},
                increment={"card_count": 1},
            )
        """
        return await self.prisma.update(where=where, data=data, increment=increment)

    async def update_many(
        self,
        *,
        where: dict[str, Any],
        data: dict[str, Any] | None = None,
        increment: dict[str, Any] | None = None,
    ) -> int:
        """
        Prisma-style bulk update. Applies ``data`` and/or ``increment`` to
        every row matching ``where``. Returns the number of rows updated.

        Example:
            removed = await db.tags.update_many(
                where={"tag_id": {"in": list(removed_tags)}},
                increment={"card_count": -1},
            )
        """
        return await self.prisma.update_many(
            where=where, data=data, increment=increment
        )

    async def delete_one(
        self,
        *,
        where: dict[str, Any],
    ) -> dict[str, Any] | None:
        """
        Prisma-style delete. Deletes a record matching the where clause.

        Example:
            user = await db.users.delete_one(where={"id": user_id})
        """
        return await self.prisma.delete(where=where)

    async def upsert(
        self,
        *,
        where: dict[str, Any],
        create: dict[str, Any],
        update: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Prisma-style upsert. Updates or creates a record.

        Example:
            user = await db.users.upsert(
                where={"email": "alice@example.com"},
                create={"email": "alice@example.com", "name": "Alice"},
                update={"name": "Alice Updated"}
            )
        """
        return await self.prisma.upsert(where=where, create=create, update=update)

    async def count(self, where: dict[str, Any] | None = None) -> int:
        """
        Prisma-style count.

        Example:
            active_count = await db.users.count(where={"status": "active"})
        """
        return await self.prisma.count(where=where)


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

    def in_(self, values: "list[Any] | SubqueryExpr") -> "Condition":
        """IN clause. Accepts a list of values or a SubqueryExpr."""
        return Condition(self._full_name, "IN", values)

    def not_in_(self, values: "list[Any] | SubqueryExpr") -> "Condition":
        """NOT IN clause. Accepts a list of values or a SubqueryExpr.
        Raises ValueError if any value in a list is None — NOT IN with NULLs silently returns no rows."""
        if not is_subquery(values) and any(v is None for v in values):
            raise ValueError(
                f"not_in_() on '{self._full_name}' received a list containing None. "
                "NOT IN with NULLs always returns no rows (SQL NULL semantics). "
                "Filter out None values before calling not_in_()."
            )
        return Condition(self._full_name, "NOT IN", values)

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
        return OrderBy(kind="order_by", column=self._full_name, direction="DESC")

    def asc(self) -> "OrderBy":
        """Order by ascending."""
        return OrderBy(kind="order_by", column=self._full_name, direction="ASC")


class Condition:
    """A WHERE condition.

    Still a class, because `&` and `|` compose conditions and only a class
    can carry an operator. The RENDERING has moved out to `render_condition`,
    a pure function this method delegates to — the strangler shape: the
    caller-facing surface stays, the logic leaves, and the class is deleted
    when the surface is retired.

    THE PARAMETER COUNTER IS GONE. `_global_param_counter` was a
    process-global integer incremented in `__init__`, so the same query
    rendered different SQL every time it was built. Names now come from
    `path` — where the node sits in the statement being rendered — which is
    unique within a statement, the only place uniqueness is required, and
    identical between two renderings of the same query.
    """

    __slots__ = ("column", "operator", "value")

    def __init__(self, column: str, operator: str, value: Any):
        self.column = column
        self.operator = operator
        self.value = value

    def to_sql(self, dialect: str, path: str) -> tuple[str, dict[str, Any]]:
        """Render this condition at `path`. Delegates; holds no logic.

        `path` is REQUIRED. A default would let a caller silently render two
        conditions at the same path and collide their parameter names, which
        is the failure the counter was there to prevent (Rule 14).
        """
        return render_condition(self.column, self.operator, self.value, dialect, path)

    def __and__(self, other: "Condition | ConditionGroup") -> "ConditionGroup":
        return ConditionGroup([self, other], "AND")

    def __or__(self, other: "Condition | ConditionGroup") -> "ConditionGroup":
        return ConditionGroup([self, other], "OR")


def render_condition(
    column: str, operator: str, value: Any, dialect: str, path: str
) -> tuple[str, dict[str, Any]]:
    """Render one condition. Pure: fields in, (sql, params) out.

    Every parameter name is derived from `path`, so the result depends only
    on the arguments — no counter, no process history. Two renderings of the
    same condition at the same path are byte-identical.
    """
    if value is None and operator == "IS":
        return f"{column} IS NULL", {}
    if value is None and operator == "IS NOT":
        return f"{column} IS NOT NULL", {}

    if operator in ("IN", "NOT IN"):
        if is_subquery(value):
            sub_sql, sub_params = render_subquery(value, dialect)
            return f"{column} {operator} ({sub_sql})", sub_params
        placeholders = ", ".join(f":_in_{path}_{i}" for i in range(len(value)))
        params = {f"_in_{path}_{i}": v for i, v in enumerate(value)}
        return f"{column} {operator} ({placeholders})", params

    if operator == "BETWEEN":
        low, high = f"_between_{path}_low", f"_between_{path}_high"
        return (
            f"{column} BETWEEN :{low} AND :{high}",
            {low: value[0], high: value[1]},
        )

    if operator == "ILIKE" and dialect != "postgresql":
        # SQLite has no ILIKE; LOWER() on both sides is the portable form.
        name = f"_like_{path}"
        return f"LOWER({column}) LIKE LOWER(:{name})", {name: value}

    # Column-to-column comparison, as in a JOIN ON clause. Binds nothing.
    if isinstance(value, ColumnProxy):
        return f"{column} {operator} {value._full_name}", {}

    # A string already written as :name is a caller-supplied placeholder.
    if isinstance(value, str) and value.startswith(":"):
        return f"{column} {operator} {value}", {}

    name = f"_p_{path}"
    return f"{column} {operator} :{name}", {name: value}


class ConditionGroup:
    """Group of conditions with AND/OR."""

    __slots__ = ("conditions", "operator")

    def __init__(self, conditions: list["Condition | ConditionGroup"], operator: str):
        self.conditions = conditions
        self.operator = operator

    def to_sql(self, dialect: str, path: str) -> tuple[str, dict[str, Any]]:
        """Render the group, giving each child a distinct path below this one.

        The child paths are what keep parameter names apart now that the
        global counter is gone: sibling `i` renders at `{path}_{i}`, so two
        conditions on the same column in one group cannot collide, and the
        names are the same on every rendering.
        """
        parts = []
        params: dict[str, Any] = {}
        for i, cond in enumerate(self.conditions):
            sql, p = cond.to_sql(dialect, f"{path}_{i}")
            parts.append(f"({sql})")
            params.update(p)
        return f" {self.operator} ".join(parts), params

    def __and__(self, other: "Condition | ConditionGroup") -> "ConditionGroup":
        return ConditionGroup([self, other], "AND")

    def __or__(self, other: "Condition | ConditionGroup") -> "ConditionGroup":
        return ConditionGroup([self, other], "OR")


class OrderBy(TypedDict):
    """An ORDER BY term over a plain column. Data, not an object.

    `kind` is load-bearing. Rendering used to pick a branch with
    `hasattr(o, "to_sql_fragment")`, which classifies by shape and silently
    reclassifies anything that gains or loses a method. Terms now dispatch
    through ORDER_TERM_RENDERERS on this tag (Rule 1), which fails loudly on
    an unknown one.
    """

    kind: Literal["order_by"]
    column: str
    direction: str


class CaseExpression:
    """
    CASE WHEN ... THEN ... ELSE ... END expression.

    Usable in SELECT columns, ORDER BY, and as argument to aggregate functions.

    Example:
        priority = case_(
            (tickets.severity == "critical", 0),
            (tickets.severity == "high", 1),
            else_=2,
        ).as_("priority")

        rows = await (
            tickets.select(tickets.id, priority)
            .order_by(priority.asc())
            .execute()
        )
    """

    __slots__ = ("_whens", "_else", "_alias")

    def __init__(
        self,
        *whens: tuple[Any, Any],
        else_: Any = None,
        alias: str | None = None,
    ) -> None:
        self._whens = whens
        self._else = else_
        self._alias = alias

    def _bare_sql_fragment(
        self, dialect: str, path: str
    ) -> tuple[str, dict[str, Any]]:
        """Generate CASE ... END SQL and params, without alias.

        `_global_case_counter` used to name these parameters, so two
        renderings of one expression produced different SQL. They come from
        `path` now, for the same reason and with the same guarantee as
        Condition.
        """
        params: dict[str, Any] = {}
        parts = ["CASE"]

        for i, (cond, value) in enumerate(self._whens):
            cond_sql, cond_params = cond.to_sql(dialect, f"{path}_w{i}")
            params.update(cond_params)

            if isinstance(value, ColumnProxy):
                then_sql = value._full_name
            elif value is None:
                then_sql = "NULL"
            else:
                param_name = f"_case_{path}_then_{i}"
                params[param_name] = value
                then_sql = f":{param_name}"

            parts.append(f"WHEN {cond_sql} THEN {then_sql}")

        if self._else is not None:
            if isinstance(self._else, ColumnProxy):
                parts.append(f"ELSE {self._else._full_name}")
            else:
                param_name = f"_case_{path}_else"
                params[param_name] = self._else
                parts.append(f"ELSE :{param_name}")

        parts.append("END")
        return " ".join(parts), params

    def to_sql_fragment(self, dialect: str, path: str) -> tuple[str, dict[str, Any]]:
        """Generate SQL and params, including alias if set."""
        sql, params = self._bare_sql_fragment(dialect, path)
        if self._alias:
            return f"{sql} AS {self._alias}", params
        return sql, params

    @property
    def _full_name(self) -> str:
        """SQL string only — parameters are DISCARDED, so never use this to
        render a CASE that binds values. It exists for the alias-name case."""
        sql, _ = self.to_sql_fragment("postgresql", "n")
        return sql

    def as_(self, alias: str) -> "CaseExpression":
        """Return new expression with alias."""
        return CaseExpression(*self._whens, else_=self._else, alias=alias)

    def asc(self) -> "CaseOrderBy":
        """Order by this expression ascending."""
        return CaseOrderBy(kind="case_order_by", expr=self, direction="ASC")

    def desc(self) -> "CaseOrderBy":
        """Order by this expression descending."""
        return CaseOrderBy(kind="case_order_by", expr=self, direction="DESC")


class CaseOrderBy(TypedDict):
    """An ORDER BY term over a CASE expression. The other bounded member."""

    kind: Literal["case_order_by"]
    expr: "CaseExpression"
    direction: str


def _render_order_by(
    term: OrderBy, _dialect: str, _path: str
) -> tuple[str, dict[str, Any]]:
    """A plain column term takes no parameters."""
    return f"{term['column']} {term['direction']}", {}


def _render_case_order_by(
    term: CaseOrderBy, dialect: str, path: str
) -> tuple[str, dict[str, Any]]:
    """A CASE term emits the BARE expression — an alias is not valid here."""
    sql, params = term["expr"]._bare_sql_fragment(dialect, path)
    return f"{sql} {term['direction']}", params


ORDER_TERM_RENDERERS: dict[str, Any] = {
    "order_by": _render_order_by,
    "case_order_by": _render_case_order_by,
}
"""The bounded ORDER BY vocabulary. Two members, both rendered by tests.

Replaces `if hasattr(o, "to_sql_fragment"): ... else: ...` in select.py. A
term carrying an unrecognised tag raises KeyError instead of falling through
to whichever branch happened to be last.
"""


def render_order_term(
    term: "OrderBy | CaseOrderBy", dialect: str, path: str
) -> tuple[str, dict[str, Any]]:
    """Render one ORDER BY term. Pure: term in, (sql, params) out."""
    return ORDER_TERM_RENDERERS[term["kind"]](term, dialect, path)


class SubqueryExpr(TypedDict):
    """
    Subquery expression for use in IN/NOT IN.

    Example:
        admin_ids = subquery(
            roles.select(roles.user_id).where(roles.name == "admin")
        )
        rows = await users.select(users.id).where(users.id.in_(admin_ids)).execute()
    """

    kind: Literal["subquery"]
    query: Any


def is_subquery(value: Any) -> bool:
    """True when a value is a subquery expression.

    `isinstance` cannot be used on a TypedDict, and the operand it guards is
    caller-supplied — a list, a scalar, or a subquery — so the check is on
    the tag, not the type. A plain dict passed as a value has no `kind` and
    is correctly not a subquery.
    """
    return isinstance(value, dict) and value.get("kind") == "subquery"


def render_subquery(expr: SubqueryExpr, dialect: str) -> tuple[str, dict[str, Any]]:
    """Render the inner SELECT."""
    return expr["query"].to_sql(dialect)


class JoinClause(TypedDict):
    """A JOIN clause. Data, not an object — there is no behaviour here.

    This was a class whose whole body was an `__init__` assigning three
    fields. `query/builder.py` already rendered joins from plain dicts
    (`join["table"]`, `join.get("type")`), so one concept had two
    representations and only one of them needed a class.

    `join_type` also carried an implicit default of "inner", which made a
    caller who omitted it indistinguishable from one who chose it. A
    TypedDict has no defaults, so the omission is now a construction error
    instead of a silent decision (Rule 14). `SelectQuery.join` keeps its own
    documented default at the CONSUMER boundary, where a default is visible
    in the signature the caller reads.

    First slice of the strangler on this module: smallest possible unit,
    converted end to end rather than wrapped, because a facade that still
    reads `self` moves nothing.
    """

    table: str
    on: "Condition"
    type: str


class SQLFunction:
    """
    SQL function wrapper for use in SELECT.

    Allows functions like count_("*"), sum_(column) etc.
    Supports CaseExpression arguments for compositions like sum_(case_(...)).
    """

    __slots__ = ("name", "args", "alias")

    def __init__(self, name: str, *args: Any, alias: str | None = None):
        self.name = name
        self.args = args
        self.alias = alias

    def to_sql_fragment(self, dialect: str, path: str) -> tuple[str, dict[str, Any]]:
        """Generate SQL and params, including alias if set."""
        params: dict[str, Any] = {}
        arg_parts = []
        for i, a in enumerate(self.args):
            if hasattr(a, "_bare_sql_fragment"):
                # CaseExpression inside aggregate: use bare form (no alias)
                a_sql, a_params = a._bare_sql_fragment(dialect, f"{path}_a{i}")
                arg_parts.append(a_sql)
                params.update(a_params)
            elif hasattr(a, "to_sql_fragment"):
                a_sql, a_params = a.to_sql_fragment(dialect, f"{path}_a{i}")
                arg_parts.append(a_sql)
                params.update(a_params)
            elif isinstance(a, ColumnProxy):
                arg_parts.append(a._full_name)
            else:
                arg_parts.append(str(a))
        result = f"{self.name}({', '.join(arg_parts)})"
        if self.alias:
            result = f"{result} AS {self.alias}"
        return result, params

    @property
    def _full_name(self) -> str:
        """Return the SQL representation (no params — use to_sql_fragment for complex args)."""
        sql, _ = self.to_sql_fragment("postgresql", "n")
        return sql

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


def case_(*whens: tuple[Any, Any], else_: Any = None) -> CaseExpression:
    """
    Build a CASE WHEN ... THEN ... ELSE ... END expression.

    Each positional argument is a (condition, value) tuple.
    The optional else_ keyword sets the ELSE value.

    Example:
        priority = case_(
            (tickets.severity == "critical", 0),
            (tickets.severity == "high", 1),
            else_=2,
        ).as_("priority")

        # Use in SELECT + ORDER BY
        rows = await (
            tickets.select(tickets.id, priority)
            .order_by(priority.asc())
            .execute()
        )

        # Use inside an aggregate
        total = sum_(case_(
            (orders.status == "paid", orders.amount),
            else_=0,
        )).as_("paid_total")
    """
    return CaseExpression(*whens, else_=else_)


def subquery(query: Any) -> SubqueryExpr:
    """
    Wrap a SelectQuery for use in IN / NOT IN.

    Example:
        admin_ids = subquery(
            roles.select(roles.user_id).where(roles.name == "admin")
        )
        rows = await (
            users.select(users.id, users.email)
            .where(users.id.in_(admin_ids))
            .execute()
        )
    """
    return SubqueryExpr(kind="subquery", query=query)
