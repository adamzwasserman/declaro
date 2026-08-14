"""
Shared test fixtures for declaro_persistum.

Test levels:
- precommit: Fast sanity checks (30s max total, mocked DBs)
- stress: Full stress testing (requires real databases)
"""

import asyncio
import os

import pytest
from dotenv import load_dotenv

# Load .env file for test credentials
load_dotenv()

from declaro_persistum.types import Schema

# =============================================================================
# Test Level Configuration
# =============================================================================


@pytest.fixture(autouse=True)
def precommit_timeout(request: pytest.FixtureRequest) -> None:
    """Enforce 5-second timeout per test for precommit tests."""
    if request.node.get_closest_marker("precommit"):
        request.node.add_marker(pytest.mark.timeout(5))


DEFAULT_POSTGRESQL_URL = "postgresql://postgres:postgres@localhost/declarotodo"


@pytest.fixture
def require_postgresql() -> None:
    """Require PostgreSQL for stress tests — by OPENING one.

    This used to check two proxies and no server:

        import asyncpg                     # is the driver installed
        if not pg_url:                     # a string that has a default,
            pytest.fail(...)               # so this could never be true

    Neither touches a database. The second is a guard whose condition is
    false by construction. On 2026-08-12 two PostgreSQL scenarios reported
    green, and they were green only because a server happened to be running
    on that machine — the fixture would have said the environment was fine
    either way, and the scenarios would have errored further in, blaming
    whatever they touched first.

    A check written against a proxy — a driver import, a string's length —
    tells you about the proxy. The only thing that establishes a database is
    there is connecting to it, so that is what this does.
    """
    try:
        import asyncpg
    except ImportError:
        pytest.fail(
            "PostgreSQL (asyncpg) is REQUIRED for stress tests. "
            "Install with: uv pip install asyncpg"
        )

    url = os.environ.get("TEST_POSTGRESQL_URL") or DEFAULT_POSTGRESQL_URL

    async def _probe() -> None:
        conn = await asyncpg.connect(url, timeout=5)
        try:
            await conn.execute("SELECT 1")
        finally:
            await conn.close()

    try:
        asyncio.run(_probe())
    except Exception as e:
        pytest.fail(
            f"PostgreSQL is REQUIRED for stress tests and is not reachable at "
            f"{url}: {type(e).__name__}: {e}. Start it, or point "
            f"TEST_POSTGRESQL_URL at one that is running."
        )


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


# `mock_pg_connection` and `mock_sqlite_connection` were here. Both returned
# classes that had already been deleted, so requesting either raised NameError,
# and nothing requested either. Deleted rather than rebuilt: a mock connection
# is the machinery a test needs when the code under test mixes I/O with the
# logic being asserted (Rule 10), and both SQLite and Postgres are available to
# these tests for real. `test_the_bulk_loaders_actually_load` and
# `test_fk_ordering` are what that looks like instead.


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
