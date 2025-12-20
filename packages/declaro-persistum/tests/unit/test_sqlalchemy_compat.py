"""
Unit tests for SQLAlchemy-compatible API.

Tests the declarative_base, Column, Session, and Query classes.
"""

import pytest
from typing import Any

from declaro_persistum.query.sqlalchemy import (
    declarative_base,
    Column,
    ForeignKey,
    Integer,
    BigInteger,
    String,
    Text,
    Boolean,
    Float,
    Numeric,
    DateTime,
    Date,
    JSON,
    JSONB,
    UUID,
    Session,
    Query,
    get_model_schema,
    clear_model_registry,
    get_registered_models,
)


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear model registry before and after each test."""
    clear_model_registry()
    yield
    clear_model_registry()


class TestTypeEngine:
    """Tests for SQL type classes."""

    def test_integer_type(self):
        """Integer returns correct type string."""
        col = Integer()
        assert col.get_type_string() == "integer"

    def test_bigint_type(self):
        """BigInteger returns correct type string."""
        col = BigInteger()
        assert col.get_type_string() == "bigint"

    def test_string_without_length(self):
        """String without length returns text."""
        col = String()
        assert col.get_type_string() == "text"

    def test_string_with_length(self):
        """String with length returns varchar(n)."""
        col = String(255)
        assert col.get_type_string() == "varchar(255)"

    def test_text_type(self):
        """Text returns correct type string."""
        col = Text()
        assert col.get_type_string() == "text"

    def test_boolean_type(self):
        """Boolean returns correct type string."""
        col = Boolean()
        assert col.get_type_string() == "boolean"

    def test_float_type(self):
        """Float returns correct type string."""
        col = Float()
        assert col.get_type_string() == "real"

    def test_numeric_without_precision(self):
        """Numeric without precision returns numeric."""
        col = Numeric()
        assert col.get_type_string() == "numeric"

    def test_numeric_with_precision(self):
        """Numeric with precision returns numeric(p,s)."""
        col = Numeric(10, 2)
        assert col.get_type_string() == "numeric(10,2)"

    def test_datetime_without_timezone(self):
        """DateTime without timezone returns timestamp."""
        col = DateTime()
        assert col.get_type_string() == "timestamp"

    def test_datetime_with_timezone(self):
        """DateTime with timezone returns timestamptz."""
        col = DateTime(timezone=True)
        assert col.get_type_string() == "timestamptz"

    def test_date_type(self):
        """Date returns correct type string."""
        col = Date()
        assert col.get_type_string() == "date"

    def test_json_type(self):
        """JSON returns correct type string."""
        col = JSON()
        assert col.get_type_string() == "json"

    def test_jsonb_type(self):
        """JSONB returns correct type string."""
        col = JSONB()
        assert col.get_type_string() == "jsonb"

    def test_uuid_type(self):
        """UUID returns correct type string."""
        col = UUID()
        assert col.get_type_string() == "uuid"


class TestColumn:
    """Tests for Column class."""

    def test_basic_column(self):
        """Basic column with type."""
        col = Column(Integer)
        col_def = col.to_column_def()
        assert col_def["type"] == "integer"

    def test_column_with_instance(self):
        """Column with type instance."""
        col = Column(String(100))
        col_def = col.to_column_def()
        assert col_def["type"] == "varchar(100)"

    def test_primary_key_column(self):
        """Primary key column."""
        col = Column(Integer, primary_key=True)
        col_def = col.to_column_def()
        assert col_def.get("primary_key") is True
        assert col_def.get("nullable") is False

    def test_nullable_column(self):
        """Nullable column."""
        col = Column(String, nullable=False)
        col_def = col.to_column_def()
        assert col_def.get("nullable") is False

    def test_unique_column(self):
        """Unique column."""
        col = Column(String, unique=True)
        col_def = col.to_column_def()
        assert col_def.get("unique") is True

    def test_server_default(self):
        """Column with server default."""
        col = Column(DateTime(timezone=True), server_default="now()")
        col_def = col.to_column_def()
        assert col_def.get("default") == "now()"

    def test_string_default(self):
        """Column with string default."""
        col = Column(String, default="active")
        col_def = col.to_column_def()
        assert col_def.get("default") == "'active'"

    def test_bool_default(self):
        """Column with boolean default."""
        col = Column(Boolean, default=False)
        col_def = col.to_column_def()
        assert col_def.get("default") == "false"

    def test_foreign_key(self):
        """Column with foreign key."""
        col = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
        col_def = col.to_column_def()
        assert col_def.get("references") == "users.id"
        assert col_def.get("on_delete") == "cascade"


class TestForeignKey:
    """Tests for ForeignKey class."""

    def test_basic_foreign_key(self):
        """Basic foreign key."""
        fk = ForeignKey("users.id")
        assert fk.target == "users.id"
        assert fk.ondelete is None
        assert fk.onupdate is None

    def test_foreign_key_with_actions(self):
        """Foreign key with ON DELETE/UPDATE."""
        fk = ForeignKey("users.id", ondelete="CASCADE", onupdate="SET NULL")
        assert fk.target == "users.id"
        assert fk.ondelete == "CASCADE"
        assert fk.onupdate == "SET NULL"


class TestDeclarativeBase:
    """Tests for declarative model definition."""

    def test_declarative_base_returns_type(self):
        """declarative_base returns a class."""
        Base = declarative_base()
        assert isinstance(Base, type)

    def test_model_definition(self):
        """Model class is properly defined."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            email = Column(String, nullable=False)
            name = Column(String(100))

        assert User.__tablename__ == "users"
        assert "id" in User._columns
        assert "email" in User._columns
        assert "name" in User._columns

    def test_model_registered(self):
        """Model is registered in registry."""
        Base = declarative_base()

        class Post(Base):
            __tablename__ = "posts"
            id = Column(Integer, primary_key=True)
            title = Column(String)

        models = get_registered_models()
        assert "posts" in models
        assert models["posts"] == Post

    def test_schema_generated(self):
        """Schema is generated from model."""
        Base = declarative_base()

        class Article(Base):
            __tablename__ = "articles"
            id = Column(Integer, primary_key=True)
            title = Column(String(255), nullable=False)
            body = Column(Text)

        schema = get_model_schema()
        assert "articles" in schema
        assert "id" in schema["articles"]["columns"]
        assert schema["articles"]["columns"]["id"]["type"] == "integer"
        assert schema["articles"]["columns"]["id"].get("primary_key") is True
        assert schema["articles"]["columns"]["title"]["type"] == "varchar(255)"

    def test_auto_tablename(self):
        """Auto-generated table name from class name."""
        Base = declarative_base()

        class Comment(Base):
            id = Column(Integer, primary_key=True)

        assert Comment.__tablename__ == "comments"


class TestModelInstance:
    """Tests for model instance creation."""

    def test_instance_creation(self):
        """Create model instance with kwargs."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            email = Column(String)
            name = Column(String)

        user = User(email="test@example.com", name="Test")
        assert user.email == "test@example.com"
        assert user.name == "Test"

    def test_instance_to_dict(self):
        """Convert instance to dict."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            email = Column(String)

        user = User(email="test@example.com")
        data = user.to_dict()
        assert data["email"] == "test@example.com"
        assert "id" in data

    def test_invalid_column_raises(self):
        """Invalid column raises AttributeError."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)

        with pytest.raises(AttributeError, match="has no column"):
            User(invalid_column="value")

    def test_default_values(self):
        """Default values are set."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            status = Column(String, default="active")

        user = User()
        assert user.status == "active"


class TestModelQueryMethods:
    """Tests for class-level query methods on models."""

    def test_filter_method(self):
        """Model.filter() returns QuerySet."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            status = Column(String)

        qs = User.filter(status="active")
        sql, _ = qs.to_sql()
        assert "WHERE" in sql
        assert "users.status" in sql

    def test_exclude_method(self):
        """Model.exclude() returns QuerySet."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            status = Column(String)

        qs = User.exclude(status="deleted")
        sql, _ = qs.to_sql()
        assert "WHERE" in sql

    def test_order_method(self):
        """Model.order() returns QuerySet."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            created_at = Column(DateTime)

        qs = User.order("-created_at")
        sql, _ = qs.to_sql()
        assert "ORDER BY" in sql
        assert "DESC" in sql


class TestQuery:
    """Tests for SQLAlchemy-style Query class."""

    def test_query_filter_by(self):
        """Query.filter_by() adds conditions."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            status = Column(String)

        # Create mock session
        class MockSession:
            _connection = None

        session = MockSession()
        query = Query(User, session)  # type: ignore
        filtered = query.filter_by(status="active")

        sql, _ = filtered._queryset.to_sql()
        assert "WHERE" in sql

    def test_query_order_by(self):
        """Query.order_by() adds ordering."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            name = Column(String)

        class MockSession:
            _connection = None

        session = MockSession()
        query = Query(User, session)  # type: ignore
        ordered = query.order_by("name")

        sql, _ = ordered._queryset.to_sql()
        assert "ORDER BY" in sql

    def test_query_limit(self):
        """Query.limit() adds limit."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)

        class MockSession:
            _connection = None

        session = MockSession()
        query = Query(User, session)  # type: ignore
        limited = query.limit(10)

        sql, _ = limited._queryset.to_sql()
        assert "LIMIT 10" in sql

    def test_query_offset(self):
        """Query.offset() adds offset."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)

        class MockSession:
            _connection = None

        session = MockSession()
        query = Query(User, session)  # type: ignore
        offset = query.offset(5)

        sql, _ = offset._queryset.to_sql()
        assert "OFFSET 5" in sql

    def test_query_slicing(self):
        """Query supports slicing."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)

        class MockSession:
            _connection = None

        session = MockSession()
        query = Query(User, session)  # type: ignore
        sliced = query[5:15]

        sql, _ = sliced._queryset.to_sql()
        assert "OFFSET 5" in sql
        assert "LIMIT 10" in sql


class TestSession:
    """Tests for Session class."""

    def test_session_query(self):
        """Session.query() returns Query."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)

        session = Session(None)
        query = session.query(User)
        assert isinstance(query, Query)

    def test_session_add(self):
        """Session.add() queues instance for insert."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            email = Column(String)

        session = Session(None)
        user = User(email="test@example.com")
        session.add(user)

        assert len(session._pending_adds) == 1
        assert session._pending_adds[0] is user

    def test_session_add_all(self):
        """Session.add_all() queues multiple instances."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            email = Column(String)

        session = Session(None)
        users = [User(email="a@example.com"), User(email="b@example.com")]
        session.add_all(users)

        assert len(session._pending_adds) == 2

    def test_session_delete(self):
        """Session.delete() queues instance for deletion."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)

        session = Session(None)
        user = User(id=1)
        session.delete(user)

        assert len(session._pending_deletes) == 1

    @pytest.mark.asyncio
    async def test_session_rollback(self):
        """Session.rollback() clears pending changes."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)
            email = Column(String)

        session = Session(None)
        session.add(User(email="test@example.com"))
        session.delete(User(id=1))

        assert len(session._pending_adds) == 1
        assert len(session._pending_deletes) == 1

        await session.rollback()

        assert len(session._pending_adds) == 0
        assert len(session._pending_deletes) == 0


class TestClearRegistry:
    """Tests for registry management."""

    def test_clear_registry(self):
        """clear_model_registry clears all models."""
        Base = declarative_base()

        class User(Base):
            __tablename__ = "users"
            id = Column(Integer, primary_key=True)

        assert len(get_registered_models()) > 0

        clear_model_registry()

        assert len(get_registered_models()) == 0
        assert len(get_model_schema()) == 0
