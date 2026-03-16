"""
Unit tests for View support.

Tests the View TypedDict, loader parsing, and applier generation.
"""

import pytest
from typing import Any

from declaro_persistum.types import View


class TestViewTypedDict:
    """Tests for View TypedDict structure."""

    def test_view_basic(self):
        """Basic view definition."""
        view: View = {
            "name": "active_users",
            "query": "SELECT id, email, name FROM users WHERE status = 'active'",
        }
        assert view["name"] == "active_users"
        assert "SELECT" in view["query"]

    def test_view_with_joins(self):
        """View with JOIN."""
        view: View = {
            "name": "user_orders",
            "query": """
                SELECT u.id, u.email, COUNT(o.id) as order_count
                FROM users u
                LEFT JOIN orders o ON o.user_id = u.id
                GROUP BY u.id, u.email
            """,
        }
        assert "JOIN" in view["query"]

    def test_view_materialized(self):
        """Materialized view (PostgreSQL only)."""
        view: View = {
            "name": "monthly_stats",
            "query": """
                SELECT date_trunc('month', created_at) as month,
                       COUNT(*) as total_orders,
                       SUM(total) as revenue
                FROM orders
                GROUP BY date_trunc('month', created_at)
            """,
            "materialized": True,
        }
        assert view.get("materialized") is True

    def test_view_materialized_with_refresh(self):
        """Materialized view with refresh setting."""
        view: View = {
            "name": "cached_stats",
            "query": "SELECT COUNT(*) as user_count FROM users",
            "materialized": True,
            "refresh": "on_demand",
        }
        assert view.get("refresh") == "on_demand"


class TestViewLoading:
    """Tests for loading views from TOML."""

    def test_parse_view(self):
        """Parse view from TOML."""
        from declaro_persistum.loader import parse_view

        toml_data = {
            "query": "SELECT id, email FROM users WHERE status = 'active'",
        }
        view = parse_view("active_users", toml_data)
        assert view["name"] == "active_users"
        assert "SELECT" in view["query"]

    def test_parse_view_materialized(self):
        """Parse materialized view."""
        from declaro_persistum.loader import parse_view

        toml_data = {
            "query": "SELECT COUNT(*) FROM users",
            "materialized": True,
        }
        view = parse_view("user_count", toml_data)
        assert view.get("materialized") is True

    def test_load_views_from_directory(self):
        """Load views from schema/views/ directory."""
        from declaro_persistum.loader import load_views
        import tempfile
        import os

        with tempfile.TemporaryDirectory() as tmpdir:
            views_dir = os.path.join(tmpdir, "views")
            os.makedirs(views_dir)

            # Create a view TOML file
            view_file = os.path.join(views_dir, "active_users.toml")
            with open(view_file, "w") as f:
                f.write('query = "SELECT * FROM users WHERE status = \'active\'"')

            views = load_views(views_dir)
            assert "active_users" in views
            assert "SELECT" in views["active_users"]["query"]


class TestViewApplierPostgreSQL:
    """Tests for PostgreSQL view applier."""

    def test_create_view_sql(self):
        """Generate CREATE VIEW SQL."""
        from declaro_persistum.applier.postgresql import generate_create_view

        view: View = {
            "name": "active_users",
            "query": "SELECT id, email FROM users WHERE status = 'active'",
        }
        sql = generate_create_view(view)
        assert "CREATE OR REPLACE VIEW active_users AS" in sql
        assert "SELECT id, email FROM users" in sql

    def test_create_materialized_view_sql(self):
        """Generate CREATE MATERIALIZED VIEW SQL."""
        from declaro_persistum.applier.postgresql import generate_create_view

        view: View = {
            "name": "user_stats",
            "query": "SELECT COUNT(*) as total FROM users",
            "materialized": True,
        }
        sql = generate_create_view(view)
        assert "CREATE MATERIALIZED VIEW user_stats AS" in sql

    def test_drop_view_sql(self):
        """Generate DROP VIEW SQL."""
        from declaro_persistum.applier.postgresql import generate_drop_view

        sql = generate_drop_view("active_users", materialized=False)
        assert "DROP VIEW IF EXISTS active_users" in sql

    def test_drop_materialized_view_sql(self):
        """Generate DROP MATERIALIZED VIEW SQL."""
        from declaro_persistum.applier.postgresql import generate_drop_view

        sql = generate_drop_view("user_stats", materialized=True)
        assert "DROP MATERIALIZED VIEW IF EXISTS user_stats" in sql

    def test_refresh_materialized_view_sql(self):
        """Generate REFRESH MATERIALIZED VIEW SQL."""
        from declaro_persistum.applier.postgresql import generate_refresh_materialized_view

        sql = generate_refresh_materialized_view("user_stats")
        assert "REFRESH MATERIALIZED VIEW user_stats" in sql

    def test_refresh_materialized_view_concurrently(self):
        """Generate REFRESH MATERIALIZED VIEW CONCURRENTLY SQL."""
        from declaro_persistum.applier.postgresql import generate_refresh_materialized_view

        sql = generate_refresh_materialized_view("user_stats", concurrently=True)
        assert "REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats" in sql


class TestViewApplierSQLite:
    """Tests for SQLite view applier."""

    def test_create_view_sql_sqlite(self):
        """Generate SQLite CREATE VIEW SQL."""
        from declaro_persistum.applier.sqlite import generate_create_view

        view: View = {
            "name": "active_users",
            "query": "SELECT id, email FROM users WHERE status = 'active'",
        }
        sql = generate_create_view(view)
        assert 'CREATE VIEW IF NOT EXISTS "active_users" AS' in sql

    def test_create_materialized_view_uses_emulation(self):
        """SQLite uses table-based emulation for materialized views."""
        from declaro_persistum.applier.sqlite import generate_create_view
        import warnings

        view: View = {
            "name": "user_stats",
            "query": "SELECT COUNT(*) FROM users",
            "materialized": True,
        }
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            sql = generate_create_view(view)
            # Should use table-based emulation
            assert "CREATE TABLE" in sql
            assert "_dp_materialized_views" in sql
            # Should NOT warn - emulation replaces warning
            matview_warnings = [x for x in w if "materialized" in str(x.message).lower()]
            assert len(matview_warnings) == 0

    def test_drop_view_sql_sqlite(self):
        """Generate SQLite DROP VIEW SQL."""
        from declaro_persistum.applier.sqlite import generate_drop_view

        sql = generate_drop_view("active_users")
        assert "DROP VIEW IF EXISTS active_users" in sql


class TestViewValidation:
    """Tests for view validation."""

    def test_validate_view_requires_query(self):
        """View must have query."""
        from declaro_persistum.loader import validate_view

        view: View = {
            "name": "test_view",
        }
        with pytest.raises(ValueError, match="query"):
            validate_view(view)

    def test_validate_view_query_not_empty(self):
        """View query must not be empty."""
        from declaro_persistum.loader import validate_view

        view: View = {
            "name": "test_view",
            "query": "",
        }
        with pytest.raises(ValueError, match="query"):
            validate_view(view)

    def test_validate_view_name_valid(self):
        """View name must be valid identifier."""
        from declaro_persistum.loader import validate_view

        view: View = {
            "name": "123-invalid",
            "query": "SELECT 1",
        }
        with pytest.raises(ValueError, match="valid identifier"):
            validate_view(view)

    def test_validate_view_refresh_requires_materialized(self):
        """Refresh setting requires materialized=True."""
        from declaro_persistum.loader import validate_view

        view: View = {
            "name": "test_view",
            "query": "SELECT 1",
            "refresh": "on_demand",
            # materialized not set
        }
        with pytest.raises(ValueError, match="materialized"):
            validate_view(view)


class TestViewIntrospection:
    """Tests for view introspection."""

    async def test_introspect_views_postgresql(self):
        """Introspect views from PostgreSQL."""
        from declaro_persistum.inspector.postgresql import PostgreSQLInspector

        # Mock connection that returns view data
        class MockConn:
            def __init__(self):
                self.call_count = 0

            async def fetch(self, query, *args):
                self.call_count += 1
                if "pg_views" in query:
                    return [
                        {"name": "active_users", "query": "SELECT id FROM users WHERE status = 'active'"},
                    ]
                elif "pg_matviews" in query:
                    return []
                return []

        inspector = PostgreSQLInspector()
        views = await inspector.introspect_views(MockConn())

        assert "active_users" in views
        assert views["active_users"]["name"] == "active_users"
        assert views["active_users"]["materialized"] is False
        assert "SELECT" in views["active_users"]["query"]

    async def test_introspect_materialized_views_postgresql(self):
        """Introspect materialized views from PostgreSQL."""
        from declaro_persistum.inspector.postgresql import PostgreSQLInspector

        class MockConn:
            async def fetch(self, query, *args):
                if "pg_views" in query:
                    return []
                elif "pg_matviews" in query:
                    return [
                        {"name": "user_stats", "query": "SELECT count(*) FROM users"},
                    ]
                return []

        inspector = PostgreSQLInspector()
        views = await inspector.introspect_views(MockConn())

        assert "user_stats" in views
        assert views["user_stats"]["materialized"] is True

    async def test_introspect_views_sqlite(self):
        """Introspect views from SQLite."""
        from declaro_persistum.inspector.sqlite import SQLiteInspector

        class MockCursor:
            def __init__(self, rows, fetchone_result=None):
                self._rows = rows
                self._fetchone_result = fetchone_result

            async def fetchall(self):
                return self._rows

            async def fetchone(self):
                return self._fetchone_result

        class MockConn:
            async def execute(self, query, params=None):
                # Check for metadata table existence query
                if "_dp_materialized_views" in query and "sqlite_master" in query:
                    return MockCursor([], fetchone_result=None)  # Metadata table doesn't exist
                # Regular views query
                return MockCursor([
                    ("active_users", "CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1"),
                ])

        inspector = SQLiteInspector()
        views = await inspector.introspect_views(MockConn())

        assert "active_users" in views
        assert views["active_users"]["materialized"] is False
        assert "SELECT" in views["active_users"]["query"]



class TestViewDiff:
    """Tests for view diff detection."""

    def test_detect_new_view(self):
        """Detect when view is added."""
        from declaro_persistum.differ import diff_views

        old_views: dict[str, View] = {}
        new_views: dict[str, View] = {
            "active_users": {
                "name": "active_users",
                "query": "SELECT * FROM users WHERE status = 'active'",
            }
        }
        operations = diff_views(old_views, new_views)
        assert len(operations) == 1
        assert operations[0]["op"] == "create_view"

    def test_detect_dropped_view(self):
        """Detect when view is removed."""
        from declaro_persistum.differ import diff_views

        old_views: dict[str, View] = {
            "active_users": {
                "name": "active_users",
                "query": "SELECT * FROM users WHERE status = 'active'",
            }
        }
        new_views: dict[str, View] = {}
        operations = diff_views(old_views, new_views)
        assert len(operations) == 1
        assert operations[0]["op"] == "drop_view"

    def test_detect_view_query_changed(self):
        """Detect when view query changes."""
        from declaro_persistum.differ import diff_views

        old_views: dict[str, View] = {
            "active_users": {
                "name": "active_users",
                "query": "SELECT * FROM users WHERE status = 'active'",
            }
        }
        new_views: dict[str, View] = {
            "active_users": {
                "name": "active_users",
                "query": "SELECT id, email FROM users WHERE status = 'active'",  # Changed
            }
        }
        operations = diff_views(old_views, new_views)
        # CREATE OR REPLACE handles this
        assert len(operations) == 1
        assert operations[0]["op"] == "create_view"

    def test_detect_view_materialized_changed(self):
        """Detect when view materialized status changes."""
        from declaro_persistum.differ import diff_views

        old_views: dict[str, View] = {
            "user_stats": {
                "name": "user_stats",
                "query": "SELECT COUNT(*) FROM users",
            }
        }
        new_views: dict[str, View] = {
            "user_stats": {
                "name": "user_stats",
                "query": "SELECT COUNT(*) FROM users",
                "materialized": True,
            }
        }
        operations = diff_views(old_views, new_views)
        # Need to drop and recreate with different type
        assert len(operations) == 2
        assert operations[0]["op"] == "drop_view"
        assert operations[1]["op"] == "create_view"
        assert operations[1]["details"].get("materialized") is True


class TestViewOperationHandlers:
    """Tests for view operation handlers in appliers."""

    def test_postgresql_applier_handles_create_view(self):
        """PostgreSQL applier generates SQL for create_view operation."""
        from declaro_persistum.applier.postgresql import PostgreSQLApplier

        applier = PostgreSQLApplier()
        operation = {
            "op": "create_view",
            "table": "_views",
            "details": {
                "name": "active_users",
                "query": "SELECT * FROM users WHERE active = true",
                "materialized": False,
            },
        }
        sql = applier.generate_operation_sql(operation)
        assert "CREATE" in sql
        assert "VIEW" in sql
        assert "active_users" in sql

    def test_postgresql_applier_handles_create_materialized_view(self):
        """PostgreSQL applier generates SQL for materialized view."""
        from declaro_persistum.applier.postgresql import PostgreSQLApplier

        applier = PostgreSQLApplier()
        operation = {
            "op": "create_view",
            "table": "_views",
            "details": {
                "name": "user_stats",
                "query": "SELECT count(*) FROM users",
                "materialized": True,
            },
        }
        sql = applier.generate_operation_sql(operation)
        assert "MATERIALIZED VIEW" in sql
        assert "user_stats" in sql

    def test_postgresql_applier_handles_drop_view(self):
        """PostgreSQL applier generates SQL for drop_view operation."""
        from declaro_persistum.applier.postgresql import PostgreSQLApplier

        applier = PostgreSQLApplier()
        operation = {
            "op": "drop_view",
            "table": "_views",
            "details": {"name": "old_view", "materialized": False},
        }
        sql = applier.generate_operation_sql(operation)
        assert "DROP VIEW" in sql
        assert "old_view" in sql

    def test_postgresql_applier_handles_drop_materialized_view(self):
        """PostgreSQL applier generates SQL for drop materialized view."""
        from declaro_persistum.applier.postgresql import PostgreSQLApplier

        applier = PostgreSQLApplier()
        operation = {
            "op": "drop_view",
            "table": "_views",
            "details": {"name": "old_stats", "materialized": True},
        }
        sql = applier.generate_operation_sql(operation)
        assert "DROP MATERIALIZED VIEW" in sql
        assert "old_stats" in sql

    def test_sqlite_applier_handles_create_view(self):
        """SQLite applier generates SQL for create_view operation."""
        from declaro_persistum.applier.sqlite import SQLiteApplier

        applier = SQLiteApplier()
        operation = {
            "op": "create_view",
            "table": "_views",
            "details": {
                "name": "active_users",
                "query": "SELECT * FROM users WHERE active = 1",
            },
        }
        sql = applier.generate_operation_sql(operation)
        assert "CREATE VIEW" in sql
        assert "active_users" in sql

    def test_sqlite_applier_handles_drop_view(self):
        """SQLite applier generates SQL for drop_view operation."""
        from declaro_persistum.applier.sqlite import SQLiteApplier

        applier = SQLiteApplier()
        operation = {
            "op": "drop_view",
            "table": "_views",
            "details": {"name": "old_view"},
        }
        sql = applier.generate_operation_sql(operation)
        assert "DROP VIEW" in sql
        assert "old_view" in sql

    def test_turso_applier_handles_create_view(self):
        """Turso applier generates SQL for create_view operation."""
        from declaro_persistum.applier.turso import TursoApplier

        applier = TursoApplier()
        operation = {
            "op": "create_view",
            "table": "_views",
            "details": {
                "name": "active_users",
                "query": "SELECT * FROM users WHERE active = 1",
            },
        }
        sql = applier.generate_operation_sql(operation)
        assert "CREATE VIEW" in sql
        assert "active_users" in sql

    def test_turso_applier_handles_drop_view(self):
        """Turso applier generates SQL for drop_view operation."""
        from declaro_persistum.applier.turso import TursoApplier

        applier = TursoApplier()
        operation = {
            "op": "drop_view",
            "table": "_views",
            "details": {"name": "old_view"},
        }
        sql = applier.generate_operation_sql(operation)
        assert "DROP VIEW" in sql
        assert "old_view" in sql


class TestViewDependencies:
    """Tests for view dependency ordering."""

    def test_view_with_depends_on(self):
        """View with depends_on field."""
        view: View = {
            "name": "user_order_stats",
            "query": "SELECT u.id, COUNT(o.id) FROM users u JOIN orders o ON o.user_id = u.id GROUP BY u.id",
            "depends_on": ["users", "orders"],
        }
        assert view.get("depends_on") == ["users", "orders"]

    def test_parse_view_with_depends_on(self):
        """Parse view with depends_on from TOML data."""
        from declaro_persistum.loader import parse_view

        toml_data = {
            "query": "SELECT * FROM orders WHERE user_id IN (SELECT id FROM users WHERE active)",
            "depends_on": ["users", "orders"],
        }
        view = parse_view("active_orders", toml_data)
        assert view.get("depends_on") == ["users", "orders"]

    def test_view_depends_on_table_in_toposort(self):
        """View depending on table is created after table."""
        from declaro_persistum.differ.toposort import build_dependency_graph, topological_sort

        operations = [
            {
                "op": "create_view",
                "table": "_views",
                "details": {
                    "name": "user_stats",
                    "query": "SELECT count(*) FROM users",
                    "depends_on": ["users"],
                },
            },
            {
                "op": "create_table",
                "table": "users",
                "details": {"columns": {"id": {"type": "integer", "primary_key": True}}},
            },
        ]

        deps = build_dependency_graph(operations)
        order = topological_sort(operations, deps)

        # create_table (users) should come before create_view (user_stats)
        table_idx = next(i for i, idx in enumerate(order) if operations[idx]["op"] == "create_table")
        view_idx = next(i for i, idx in enumerate(order) if operations[idx]["op"] == "create_view")
        assert table_idx < view_idx

    def test_view_depends_on_another_view(self):
        """View depending on another view is created after it."""
        from declaro_persistum.differ.toposort import build_dependency_graph, topological_sort

        operations = [
            {
                "op": "create_view",
                "table": "_views",
                "details": {
                    "name": "summary_stats",
                    "query": "SELECT * FROM user_stats",
                    "depends_on": ["user_stats"],
                },
            },
            {
                "op": "create_view",
                "table": "_views",
                "details": {
                    "name": "user_stats",
                    "query": "SELECT count(*) FROM users",
                    "depends_on": ["users"],
                },
            },
            {
                "op": "create_table",
                "table": "users",
                "details": {"columns": {"id": {"type": "integer", "primary_key": True}}},
            },
        ]

        deps = build_dependency_graph(operations)
        order = topological_sort(operations, deps)

        # Order should be: users table -> user_stats view -> summary_stats view
        ordered_names = []
        for idx in order:
            op = operations[idx]
            if op["op"] == "create_table":
                ordered_names.append(op["table"])
            else:
                ordered_names.append(op["details"]["name"])

        assert ordered_names == ["users", "user_stats", "summary_stats"]

    def test_drop_view_before_drop_table(self):
        """View must be dropped before the table it depends on."""
        from declaro_persistum.differ.toposort import build_dependency_graph, topological_sort

        operations = [
            {
                "op": "drop_table",
                "table": "users",
                "details": {},
            },
            {
                "op": "drop_view",
                "table": "_views",
                "details": {
                    "name": "user_stats",
                    "depends_on": ["users"],
                },
            },
        ]

        deps = build_dependency_graph(operations)
        order = topological_sort(operations, deps)

        # drop_view should come before drop_table
        view_idx = next(i for i, idx in enumerate(order) if operations[idx]["op"] == "drop_view")
        table_idx = next(i for i, idx in enumerate(order) if operations[idx]["op"] == "drop_table")
        assert view_idx < table_idx

    def test_view_operation_priorities(self):
        """View operations have correct priorities."""
        from declaro_persistum.differ.toposort import _operation_priority

        # drop_view should have lower priority number (runs first) than drop_table
        drop_view_op = {"op": "drop_view", "table": "_views", "details": {}}
        drop_table_op = {"op": "drop_table", "table": "users", "details": {}}
        assert _operation_priority(drop_view_op) < _operation_priority(drop_table_op)

        # create_view should have higher priority number (runs last) than add_foreign_key
        create_view_op = {"op": "create_view", "table": "_views", "details": {}}
        add_fk_op = {"op": "add_foreign_key", "table": "orders", "details": {}}
        assert _operation_priority(create_view_op) > _operation_priority(add_fk_op)


class TestConcurrentRefreshValidation:
    """Tests for materialized view concurrent refresh validation."""

    async def test_has_unique_index_true(self):
        """has_unique_index returns True when unique index exists."""
        from declaro_persistum.inspector.postgresql import PostgreSQLInspector

        class MockConn:
            async def fetch(self, query, *args):
                return [
                    {"index_name": "user_stats_id_idx", "is_unique": True, "columns": ["id"]},
                ]

        inspector = PostgreSQLInspector()
        result = await inspector.has_unique_index(MockConn(), "user_stats")
        assert result is True

    async def test_has_unique_index_false(self):
        """has_unique_index returns False when no unique index exists."""
        from declaro_persistum.inspector.postgresql import PostgreSQLInspector

        class MockConn:
            async def fetch(self, query, *args):
                return [
                    {"index_name": "user_stats_created_idx", "is_unique": False, "columns": ["created_at"]},
                ]

        inspector = PostgreSQLInspector()
        result = await inspector.has_unique_index(MockConn(), "user_stats")
        assert result is False

    async def test_has_unique_index_empty(self):
        """has_unique_index returns False when no indexes exist."""
        from declaro_persistum.inspector.postgresql import PostgreSQLInspector

        class MockConn:
            async def fetch(self, query, *args):
                return []

        inspector = PostgreSQLInspector()
        result = await inspector.has_unique_index(MockConn(), "user_stats")
        assert result is False

    async def test_get_materialized_view_indexes(self):
        """get_materialized_view_indexes returns index info."""
        from declaro_persistum.inspector.postgresql import PostgreSQLInspector

        class MockConn:
            async def fetch(self, query, *args):
                return [
                    {"index_name": "user_stats_id_idx", "is_unique": True, "columns": ["id"]},
                    {"index_name": "user_stats_date_idx", "is_unique": False, "columns": ["created_at"]},
                ]

        inspector = PostgreSQLInspector()
        indexes = await inspector.get_materialized_view_indexes(MockConn(), "user_stats")
        assert len(indexes) == 2
        assert indexes[0]["index_name"] == "user_stats_id_idx"
        assert indexes[0]["is_unique"] is True

    async def test_validate_concurrent_refresh_passes(self):
        """validate_concurrent_refresh passes when unique index exists."""
        from declaro_persistum.applier.postgresql import validate_concurrent_refresh

        class MockConn:
            async def fetch(self, query, *args):
                return [
                    {"index_name": "stats_id_idx", "is_unique": True, "columns": ["id"]},
                ]

        # Should not raise
        await validate_concurrent_refresh(MockConn(), "user_stats")

    async def test_validate_concurrent_refresh_fails(self):
        """validate_concurrent_refresh raises when no unique index."""
        from declaro_persistum.applier.postgresql import validate_concurrent_refresh
        from declaro_persistum.exceptions import ValidationError

        class MockConn:
            async def fetch(self, query, *args):
                return []  # No indexes

        with pytest.raises(ValidationError) as exc_info:
            await validate_concurrent_refresh(MockConn(), "user_stats")

        assert "concurrently" in str(exc_info.value).lower()
        assert "unique index" in str(exc_info.value).lower()
        assert "user_stats" in str(exc_info.value)
