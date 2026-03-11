"""
SQLAlchemy-compatible API.

Provides drop-in replacement for common SQLAlchemy patterns:
    from declaro_persistum.query import (
        declarative_base, Column, Integer, String, ForeignKey, Session
    )

    Base = declarative_base()

    class User(Base):
        __tablename__ = 'users'
        id = Column(Integer, primary_key=True)
        email = Column(String, nullable=False, unique=True)

This allows existing SQLAlchemy code to work with minimal changes.
"""

from typing import Any, Generic, TypeVar

from declaro_persistum.query.django_style import QuerySet
from declaro_persistum.query.table import (
    TableProxy,
)
from declaro_persistum.query.table import (
    table as _table,
)
from declaro_persistum.types import Column as ColumnDef
from declaro_persistum.types import Schema, Table

# =============================================================================
# Type definitions (SQLAlchemy type names)
# =============================================================================


class TypeEngine:
    """Base class for SQL types."""

    _type_name: str = "text"

    def __init__(self, *args: Any, **kwargs: Any):
        self._args = args
        self._kwargs = kwargs

    def get_type_string(self) -> str:
        return self._type_name


class Integer(TypeEngine):
    """INTEGER type."""

    _type_name = "integer"


class BigInteger(TypeEngine):
    """BIGINT type."""

    _type_name = "bigint"


class SmallInteger(TypeEngine):
    """SMALLINT type."""

    _type_name = "smallint"


class String(TypeEngine):
    """VARCHAR/TEXT type."""

    _type_name = "text"

    def __init__(self, length: int | None = None):
        super().__init__(length)
        self._length = length

    def get_type_string(self) -> str:
        if self._length:
            return f"varchar({self._length})"
        return "text"


class Text(TypeEngine):
    """TEXT type."""

    _type_name = "text"


class Boolean(TypeEngine):
    """BOOLEAN type."""

    _type_name = "boolean"


class Float(TypeEngine):
    """FLOAT/REAL type."""

    _type_name = "real"


class Numeric(TypeEngine):
    """NUMERIC/DECIMAL type."""

    _type_name = "numeric"

    def __init__(self, precision: int | None = None, scale: int | None = None):
        super().__init__(precision, scale)
        self._precision = precision
        self._scale = scale

    def get_type_string(self) -> str:
        if self._precision and self._scale:
            return f"numeric({self._precision},{self._scale})"
        elif self._precision:
            return f"numeric({self._precision})"
        return "numeric"


class DateTime(TypeEngine):
    """TIMESTAMP type."""

    _type_name = "timestamp"

    def __init__(self, timezone: bool = False):
        super().__init__(timezone=timezone)
        self._timezone = timezone

    def get_type_string(self) -> str:
        return "timestamptz" if self._timezone else "timestamp"


class Date(TypeEngine):
    """DATE type."""

    _type_name = "date"


class Time(TypeEngine):
    """TIME type."""

    _type_name = "time"


class LargeBinary(TypeEngine):
    """BLOB/BYTEA type."""

    _type_name = "bytea"


class JSON(TypeEngine):
    """JSON type."""

    _type_name = "json"


class JSONB(TypeEngine):
    """JSONB type (PostgreSQL)."""

    _type_name = "jsonb"


class UUID(TypeEngine):
    """UUID type."""

    _type_name = "uuid"


# Aliases
VARCHAR = String
CHAR = String
TIMESTAMP = DateTime
BLOB = LargeBinary


# =============================================================================
# Column definition
# =============================================================================


class ForeignKey:
    """Foreign key reference."""

    def __init__(
        self,
        target: str,
        ondelete: str | None = None,
        onupdate: str | None = None,
    ):
        """
        Args:
            target: Target column as "table.column" or "table"
            ondelete: ON DELETE action (CASCADE, SET NULL, etc.)
            onupdate: ON UPDATE action
        """
        self.target = target
        self.ondelete = ondelete
        self.onupdate = onupdate


class Column:
    """
    SQLAlchemy-compatible column definition.

    Example:
        id = Column(Integer, primary_key=True)
        email = Column(String(255), nullable=False, unique=True)
        user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'))
    """

    def __init__(
        self,
        type_: TypeEngine | type[TypeEngine] | ForeignKey,
        *args: ForeignKey,
        primary_key: bool = False,
        nullable: bool = True,
        unique: bool = False,
        default: Any = None,
        server_default: str | None = None,
        index: bool = False,
        autoincrement: bool | str = "auto",
        name: str | None = None,
    ):
        self._type: TypeEngine
        self._foreign_key: ForeignKey | None

        # Handle type
        if isinstance(type_, ForeignKey):
            # Column(ForeignKey('users.id')) syntax
            self._foreign_key = type_
            self._type = Integer()  # Default to integer for FK
        elif isinstance(type_, type) and issubclass(type_, TypeEngine):
            self._type = type_()
            self._foreign_key = args[0] if args and isinstance(args[0], ForeignKey) else None
        else:
            self._type = type_
            self._foreign_key = args[0] if args and isinstance(args[0], ForeignKey) else None

        self.primary_key = primary_key
        self.nullable = nullable if not primary_key else False
        self.unique = unique
        self.default = default
        self.server_default = server_default
        self.index = index
        self.autoincrement = autoincrement
        self._name = name

    def to_column_def(self) -> ColumnDef:
        """Convert to internal Column TypedDict."""
        col: ColumnDef = {"type": self._type.get_type_string()}

        if self.primary_key:
            col["primary_key"] = True

        if not self.nullable:
            col["nullable"] = False

        if self.unique:
            col["unique"] = True

        if self.server_default:
            col["default"] = self.server_default
        elif self.default is not None:
            # Convert Python default to SQL expression
            if isinstance(self.default, str):
                col["default"] = f"'{self.default}'"
            elif isinstance(self.default, bool):
                col["default"] = "true" if self.default else "false"
            elif self.default is None:
                pass
            else:
                col["default"] = str(self.default)

        if self._foreign_key:
            col["references"] = self._foreign_key.target
            if self._foreign_key.ondelete:
                col["on_delete"] = self._foreign_key.ondelete.lower().replace(" ", "_")  # type: ignore
            if self._foreign_key.onupdate:
                col["on_update"] = self._foreign_key.onupdate.lower().replace(" ", "_")  # type: ignore

        return col


# =============================================================================
# Model base class
# =============================================================================

_model_registry: dict[str, type["ModelBase"]] = {}
_schema_cache: Schema = {}
_pool_ref: Any = None


class ModelMeta(type):
    """Metaclass for declarative models."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
    ) -> "ModelMeta":
        # Skip for ModelBase itself
        if name == "ModelBase":
            return super().__new__(mcs, name, bases, namespace)

        # Get table name
        tablename = namespace.get("__tablename__")
        if not tablename:
            # Auto-generate from class name
            tablename = name.lower() + "s"
            namespace["__tablename__"] = tablename

        # Collect columns
        columns: dict[str, Column] = {}
        for key, value in list(namespace.items()):
            if isinstance(value, Column):
                value._name = key
                columns[key] = value

        namespace["_columns"] = columns

        # Create class
        cls = super().__new__(mcs, name, bases, namespace)

        # Register model
        _model_registry[tablename] = cls  # type: ignore

        # Build schema
        table_def: Table = {"columns": {}}
        for col_name, col in columns.items():
            table_def["columns"][col_name] = col.to_column_def()

        _schema_cache[tablename] = table_def

        return cls


class ModelBase(metaclass=ModelMeta):
    """
    Base class for declarative models.

    Created via declarative_base():
        Base = declarative_base()

        class User(Base):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            email = Column(String, nullable=False)
    """

    __tablename__: str
    _columns: dict[str, Column]

    def __init__(self, **kwargs: Any):
        """Create model instance with column values."""
        for key, value in kwargs.items():
            if key not in self._columns:
                raise AttributeError(f"'{type(self).__name__}' has no column '{key}'")
            setattr(self, key, value)

        # Set defaults for unspecified columns
        for col_name, col in self._columns.items():
            if col_name not in kwargs:
                setattr(self, col_name, col.default)

    def to_dict(self) -> dict[str, Any]:
        """Convert model instance to dict."""
        return {col_name: getattr(self, col_name, None) for col_name in self._columns}

    @classmethod
    def _get_table_proxy(cls) -> TableProxy:
        """Get TableProxy for this model's table."""
        return _table(cls.__tablename__, schema=_schema_cache, pool=_pool_ref)

    # Class-level query methods (like Django)
    @classmethod
    def filter(cls, **kwargs: Any) -> QuerySet:
        """Filter query."""
        return cls._get_table_proxy().filter(**kwargs)

    @classmethod
    def exclude(cls, **kwargs: Any) -> QuerySet:
        """Exclude query."""
        return cls._get_table_proxy().exclude(**kwargs)

    @classmethod
    def order(cls, *fields: str) -> QuerySet:
        """Order query."""
        return cls._get_table_proxy().order(*fields)

    @classmethod
    async def all(cls) -> list[dict[str, Any]]:
        """Get all rows."""
        return await cls._get_table_proxy().all()

    @classmethod
    async def get(cls, **kwargs: Any) -> dict[str, Any]:
        """Get single row by kwargs."""
        return await cls._get_table_proxy().get(**kwargs)

    @classmethod
    async def first(cls) -> dict[str, Any] | None:
        """Get first row."""
        return await cls._get_table_proxy().first()


def declarative_base() -> type[ModelBase]:
    """
    Create a base class for declarative models.

    This is the SQLAlchemy-compatible entry point for defining models.

    Example:
        Base = declarative_base()

        class User(Base):
            __tablename__ = 'users'
            id = Column(Integer, primary_key=True)
            email = Column(String(255), nullable=False, unique=True)
            name = Column(String(100))
            created_at = Column(DateTime(timezone=True), server_default='now()')

        # The schema is automatically registered and can be used:
        schema = get_model_schema()
    """
    # Return the base class - models inherit from it
    return ModelBase


def get_model_schema() -> Schema:
    """
    Get the schema generated from all registered models.

    Returns:
        Schema dict that can be used with differ, applier, etc.
    """
    return _schema_cache.copy()


def get_registered_models() -> dict[str, type[ModelBase]]:
    """Get all registered model classes."""
    return _model_registry.copy()


def set_pool(pool: Any) -> None:
    """Set the connection pool for SQLAlchemy-style model queries."""
    global _pool_ref
    _pool_ref = pool


def clear_model_registry() -> None:
    """Clear the model registry (useful for testing)."""
    global _pool_ref
    _model_registry.clear()
    _schema_cache.clear()
    _pool_ref = None


# =============================================================================
# Session (SQLAlchemy-compatible query interface)
# =============================================================================

T = TypeVar("T", bound=ModelBase)


class Query(Generic[T]):
    """
    SQLAlchemy-compatible Query object.

    Provides the session.query(Model) interface.
    """

    def __init__(self, model: type[T], session: "Session"):
        self._model = model
        self._session = session
        self._queryset = model._get_table_proxy().objects

    def filter(self, **kwargs: Any) -> "Query[T]":
        """
        Filter by criterion.

        Supports keyword args.
        """
        # For now, only support keyword args
        # Full SQLAlchemy criterion support would require more work
        new_query: Query[T] = Query(self._model, self._session)
        new_query._queryset = self._queryset.filter(**kwargs)
        return new_query

    def filter_by(self, **kwargs: Any) -> "Query[T]":
        """Filter by keyword arguments."""
        new_query: Query[T] = Query(self._model, self._session)
        new_query._queryset = self._queryset.filter(**kwargs)
        return new_query

    def order_by(self, *columns: Any) -> "Query[T]":
        """Order by columns."""
        # Convert to string field names
        fields = []
        for col in columns:
            if isinstance(col, str):
                fields.append(col)
            elif hasattr(col, "_name"):
                fields.append(col._name)
            elif hasattr(col, "desc") and callable(col.desc):
                # Likely a column expression - for now just use string
                fields.append(str(col))
        new_query: Query[T] = Query(self._model, self._session)
        new_query._queryset = self._queryset.order(*fields)
        return new_query

    def limit(self, n: int) -> "Query[T]":
        """Limit results."""
        new_query: Query[T] = Query(self._model, self._session)
        sliced = self._queryset[:n]
        new_query._queryset = sliced  # type: ignore[assignment]
        return new_query

    def offset(self, n: int) -> "Query[T]":
        """Offset results."""
        new_query: Query[T] = Query(self._model, self._session)
        sliced = self._queryset[n:]
        new_query._queryset = sliced  # type: ignore[assignment]
        return new_query

    async def all(self) -> list[dict[str, Any]]:
        """Execute and return all results."""
        return await self._queryset.all()

    async def first(self) -> dict[str, Any] | None:
        """Execute and return first result."""
        return await self._queryset.first()

    async def one(self) -> dict[str, Any]:
        """Execute and return exactly one result, or raise."""
        return await self._queryset.get()

    async def one_or_none(self) -> dict[str, Any] | None:
        """Execute and return one result or None."""
        return await self._queryset.first()

    async def count(self) -> int:
        """Return count of results."""
        return await self._queryset.count()

    async def exists(self) -> bool:
        """Return True if any results exist."""
        return await self._queryset.exists()

    def __getitem__(self, key: Any) -> "Query[T]":
        """Support slicing."""
        new_query: Query[T] = Query(self._model, self._session)
        sliced = self._queryset[key]
        new_query._queryset = sliced  # type: ignore[assignment]
        return new_query


class Session:
    """
    SQLAlchemy-compatible Session.

    Provides familiar interface for database operations:
        async with Session(pool) as session:
            users = await session.query(User).filter_by(status='active').all()
            session.add(User(email='test@example.com'))
            await session.commit()
    """

    def __init__(self, pool: Any):
        """
        Create a session bound to a pool.

        Args:
            pool: Connection pool with acquire() context manager
        """
        self._pool = pool
        self._pending_adds: list[ModelBase] = []
        self._pending_deletes: list[ModelBase] = []
        self._in_transaction = False

    async def __aenter__(self) -> "Session":
        """Enter async context."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit async context - rollback on exception."""
        if exc_type:
            await self.rollback()

    def query(self, model: type[T]) -> Query[T]:
        """
        Create a query for a model.

        Args:
            model: Model class to query

        Returns:
            Query object for chaining
        """
        return Query(model, self)

    def add(self, instance: ModelBase) -> None:
        """
        Add a model instance to be inserted.

        Args:
            instance: Model instance to insert
        """
        self._pending_adds.append(instance)

    def add_all(self, instances: list[ModelBase]) -> None:
        """Add multiple instances."""
        self._pending_adds.extend(instances)

    def delete(self, instance: ModelBase) -> None:
        """
        Mark a model instance for deletion.

        Args:
            instance: Model instance to delete
        """
        self._pending_deletes.append(instance)

    async def commit(self) -> None:
        """
        Commit pending changes.

        Inserts all added instances and deletes all marked instances.
        """
        from declaro_persistum.query.builder import delete, insert
        from declaro_persistum.query.executor import execute

        async with self._pool.acquire() as conn:
            # Process inserts
            for instance in self._pending_adds:
                table_name = instance.__tablename__
                data = {k: v for k, v in instance.to_dict().items() if v is not None}
                if data:
                    query = insert(table_name, data)
                    await execute(query, conn)

            # Process deletes
            for instance in self._pending_deletes:
                table_name = instance.__tablename__
                # Find primary key column
                pk_col = None
                for col_name, col in instance._columns.items():
                    if col.primary_key:
                        pk_col = col_name
                        break

                if pk_col:
                    pk_value = getattr(instance, pk_col, None)
                    if pk_value is not None:
                        query = delete(table_name, where=f"{pk_col} = :pk", params={"pk": pk_value})
                        await execute(query, conn)

            # Commit if connection supports it
            if hasattr(conn, "commit") and callable(conn.commit):
                result = conn.commit()
                if hasattr(result, "__await__"):
                    await result

        # Clear pending
        self._pending_adds.clear()
        self._pending_deletes.clear()

    async def rollback(self) -> None:
        """Rollback pending changes."""
        self._pending_adds.clear()
        self._pending_deletes.clear()

    async def execute(self, query: Any) -> Any:
        """
        Execute a query directly.

        For SQLAlchemy 2.0 style:
            stmt = select(User).where(User.status == 'active')
            result = await session.execute(stmt)
        """
        from declaro_persistum.query.executor import execute

        async with self._pool.acquire() as conn:
            return await execute(query, conn)

    async def refresh(self, instance: ModelBase) -> None:
        """Refresh instance from database."""
        # Find primary key
        pk_col = None
        for col_name, col in instance._columns.items():
            if col.primary_key:
                pk_col = col_name
                break

        if pk_col:
            pk_value = getattr(instance, pk_col, None)
            if pk_value is not None:
                data = await instance.get(**{pk_col: pk_value})
                for key, value in data.items():
                    setattr(instance, key, value)


# =============================================================================
# Convenience exports
# =============================================================================

__all__ = [
    # Types
    "Integer",
    "BigInteger",
    "SmallInteger",
    "String",
    "Text",
    "Boolean",
    "Float",
    "Numeric",
    "DateTime",
    "Date",
    "Time",
    "LargeBinary",
    "JSON",
    "JSONB",
    "UUID",
    "VARCHAR",
    "CHAR",
    "TIMESTAMP",
    "BLOB",
    # Column/FK
    "Column",
    "ForeignKey",
    # Model
    "declarative_base",
    "ModelBase",
    "get_model_schema",
    "get_registered_models",
    "set_pool",
    "clear_model_registry",
    # Session
    "Session",
    "Query",
]
