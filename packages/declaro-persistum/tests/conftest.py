"""
Shared test fixtures for declaro_persistum.

Test levels:
- precommit: Fast sanity checks (30s max total, mocked DBs)
- stress: Full stress testing (requires real databases)
"""

import os
import pytest
from dotenv import load_dotenv

# Load .env file for test credentials
load_dotenv()
from typing import Any

from declaro_persistum.types import Schema, Table, Column


# =============================================================================
# Test Level Configuration
# =============================================================================


@pytest.fixture(autouse=True)
def precommit_timeout(request: pytest.FixtureRequest) -> None:
    """Enforce 5-second timeout per test for precommit tests."""
    if request.node.get_closest_marker("precommit"):
        request.node.add_marker(pytest.mark.timeout(5))


@pytest.fixture
def require_postgresql() -> None:
    """Require PostgreSQL for stress tests - FAIL if not available."""
    try:
        import asyncpg  # noqa: F401
    except ImportError:
        pytest.fail("PostgreSQL (asyncpg) is REQUIRED for stress tests. Install with: uv pip install asyncpg")

    # Also check if database is accessible
    pg_url = os.environ.get("TEST_POSTGRESQL_URL", "postgresql://postgres:postgres@localhost/declarotodo")
    if not pg_url:
        pytest.fail("TEST_POSTGRESQL_URL environment variable not set")


@pytest.fixture
def require_turso() -> None:
    """Require Turso for stress tests - skip if not configured."""
    try:
        import turso  # noqa: F401
    except ImportError:
        pytest.skip("Turso (pyturso) not installed. Install with: uv sync --extra turso")

    # Also check if Turso is configured with a real URL
    turso_url = os.environ.get("TEST_TURSO_URL", "")
    if not turso_url:
        pytest.skip("TEST_TURSO_URL environment variable not set - skipping Turso tests")


@pytest.fixture
def require_sqlite() -> None:
    """Require SQLite for tests."""
    try:
        import aiosqlite  # noqa: F401
    except ImportError:
        pytest.fail("SQLite (aiosqlite) is REQUIRED. Install with: uv pip install aiosqlite")


# Sample schemas for testing


@pytest.fixture
def empty_schema() -> Schema:
    """Empty schema."""
    return {}


@pytest.fixture
def simple_schema() -> Schema:
    """Simple schema with one table."""
    return {
        "users": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "email": {"type": "text", "nullable": False, "unique": True},
                "created_at": {"type": "timestamptz", "nullable": False, "default": "now()"},
            }
        }
    }


@pytest.fixture
def complex_schema() -> Schema:
    """Complex schema with multiple tables and relationships."""
    return {
        "users": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "email": {"type": "text", "nullable": False, "unique": True},
                "name": {"type": "text"},
                "created_at": {"type": "timestamptz", "nullable": False, "default": "now()"},
            },
            "indexes": {
                "users_email_idx": {"columns": ["email"], "unique": True},
                "users_created_at_idx": {"columns": ["created_at"]},
            },
        },
        "orders": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "user_id": {
                    "type": "uuid",
                    "nullable": False,
                    "references": "users.id",
                    "on_delete": "cascade",
                },
                "total": {"type": "numeric(10,2)", "nullable": False, "check": "total >= 0"},
                "status": {
                    "type": "text",
                    "nullable": False,
                    "default": "'pending'",
                    "check": "status IN ('pending', 'confirmed', 'shipped', 'delivered')",
                },
                "created_at": {"type": "timestamptz", "nullable": False, "default": "now()"},
            },
            "indexes": {
                "orders_user_id_idx": {"columns": ["user_id"]},
                "orders_status_idx": {"columns": ["status"]},
            },
        },
        "order_items": {
            "columns": {
                "id": {"type": "uuid", "primary_key": True, "nullable": False},
                "order_id": {
                    "type": "uuid",
                    "nullable": False,
                    "references": "orders.id",
                    "on_delete": "cascade",
                },
                "product_name": {"type": "text", "nullable": False},
                "quantity": {"type": "integer", "nullable": False, "check": "quantity > 0"},
                "price": {"type": "numeric(10,2)", "nullable": False},
            },
        },
    }


@pytest.fixture
def schema_with_composite_pk() -> Schema:
    """Schema with composite primary key."""
    return {
        "user_roles": {
            "columns": {
                "user_id": {"type": "uuid", "nullable": False},
                "role_id": {"type": "uuid", "nullable": False},
                "granted_at": {"type": "timestamptz", "nullable": False, "default": "now()"},
            },
            "primary_key": ["user_id", "role_id"],
        }
    }


# Mock connection fixtures


class MockAsyncPGConnection:
    """Mock asyncpg connection for testing."""

    def __init__(self, schema: Schema | None = None):
        self.schema = schema or {}
        self.executed: list[str] = []
        self._in_transaction = False

    async def fetch(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        """Mock fetch."""
        self.executed.append(sql)
        # Return empty results by default
        return []

    async def fetchrow(self, sql: str, *args: Any) -> dict[str, Any] | None:
        """Mock fetchrow."""
        self.executed.append(sql)
        return None

    async def fetchval(self, sql: str, *args: Any) -> Any:
        """Mock fetchval."""
        self.executed.append(sql)
        return None

    async def execute(self, sql: str, *args: Any) -> str:
        """Mock execute."""
        self.executed.append(sql)
        return "OK"

    def transaction(self):
        """Return transaction context manager."""
        return MockTransaction(self)

    async def close(self) -> None:
        """Mock close."""
        pass


class MockTransaction:
    """Mock transaction context manager."""

    def __init__(self, conn: MockAsyncPGConnection):
        self.conn = conn

    async def __aenter__(self):
        self.conn._in_transaction = True
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.conn._in_transaction = False
        return False


class MockAioSQLiteConnection:
    """Mock aiosqlite connection for testing."""

    def __init__(self, schema: Schema | None = None):
        self.schema = schema or {}
        self.executed: list[str] = []
        self.row_factory = None

    async def execute(self, sql: str, params: Any = None) -> "MockCursor":
        """Mock execute."""
        self.executed.append(sql)
        return MockCursor()

    async def commit(self) -> None:
        """Mock commit."""
        pass

    async def rollback(self) -> None:
        """Mock rollback."""
        pass

    async def close(self) -> None:
        """Mock close."""
        pass


class MockCursor:
    """Mock cursor for aiosqlite."""

    def __init__(self, rows: list[tuple] | None = None):
        self.rows = rows or []
        self.description = []
        self.rowcount = 0

    async def fetchall(self) -> list[tuple]:
        """Mock fetchall."""
        return self.rows

    async def fetchone(self) -> tuple | None:
        """Mock fetchone."""
        return self.rows[0] if self.rows else None


@pytest.fixture
def mock_pg_connection() -> MockAsyncPGConnection:
    """Mock PostgreSQL connection."""
    return MockAsyncPGConnection()


@pytest.fixture
def mock_sqlite_connection() -> MockAioSQLiteConnection:
    """Mock SQLite connection."""
    return MockAioSQLiteConnection()


# Temporary directory fixtures


@pytest.fixture
def temp_schema_dir(tmp_path):
    """Create a temporary schema directory structure."""
    schema_dir = tmp_path / "schema"
    schema_dir.mkdir()
    (schema_dir / "tables").mkdir()
    return schema_dir


@pytest.fixture
def temp_schema_with_users(temp_schema_dir):
    """Create a temporary schema directory with a users table."""
    users_toml = temp_schema_dir / "tables" / "users.toml"
    users_toml.write_text('''
[users]
primary_key = ["id"]

[users.columns.id]
type = "uuid"
nullable = false
default = "gen_random_uuid()"

[users.columns.email]
type = "text"
nullable = false
unique = true

[users.columns.created_at]
type = "timestamptz"
nullable = false
default = "now()"
''')
    return temp_schema_dir
