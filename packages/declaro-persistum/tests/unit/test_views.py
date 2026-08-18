"""
Unit tests for View support.

Tests the View TypedDict, loader parsing, and applier generation.
"""


import pytest

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
        import warnings

        from declaro_persistum.applier.sqlite import generate_create_view

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


def _inspector_pg():
    from declaro_persistum.inspector import postgresql

    return postgresql


async def _postgres():
    """A connection, or a skip naming the URL that was tried."""
    import os

    import asyncpg

    url = os.environ.get(
        "TEST_POSTGRESQL_URL",
        "postgresql://postgres:postgres@localhost/declaro_probe",
    )
    try:
        return await asyncpg.connect(url)
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"no PostgreSQL at {url}: {type(exc).__name__}: {exc}")


class TestViewIntrospection:
    """Tests for view introspection."""

    @pytest.mark.asyncio
    @pytest.mark.postgresql
    async def test_introspect_views_postgresql(self):
        """Read a view back off a REAL server, not off a `MockConn`.

        This hand-wrote a connection whose `fetch` branched on whether the
        query mentioned `pg_views` or `pg_matviews`, and then asserted the
        rows it had just written. It could not fail for any reason connected
        to PostgreSQL, and it encoded a belief about which catalogue the
        inspector reads.
        """
        conn = await _postgres()
        try:
            await conn.execute("DROP VIEW IF EXISTS active_users CASCADE")
            await conn.execute("DROP TABLE IF EXISTS users CASCADE")
            await conn.execute("CREATE TABLE users (id INT, status TEXT)")
            await conn.execute(
                "CREATE VIEW active_users AS "
                "SELECT id FROM users WHERE status = 'active'"
            )

            views = await _inspector_pg().introspect_views(conn)

            assert "active_users" in views, sorted(views)
            assert views["active_users"]["materialized"] is False
            assert "SELECT" in views["active_users"]["query"].upper()
        finally:
            await conn.execute("DROP VIEW IF EXISTS active_users CASCADE")
            await conn.execute("DROP TABLE IF EXISTS users CASCADE")
            await conn.close()

    @pytest.mark.asyncio
    @pytest.mark.postgresql
    async def test_introspect_materialized_views_postgresql(self):
        """A materialized view comes back marked as one.

        The distinction is the reason this function exists: a plain view is
        recreated freely and a materialized one holds data.
        """
        conn = await _postgres()
        try:
            await conn.execute("DROP MATERIALIZED VIEW IF EXISTS user_stats CASCADE")
            await conn.execute("DROP TABLE IF EXISTS users CASCADE")
            await conn.execute("CREATE TABLE users (id INT)")
            await conn.execute(
                "CREATE MATERIALIZED VIEW user_stats AS SELECT count(*) FROM users"
            )

            views = await _inspector_pg().introspect_views(conn)

            assert views["user_stats"]["materialized"] is True, views["user_stats"]
        finally:
            await conn.execute("DROP MATERIALIZED VIEW IF EXISTS user_stats CASCADE")
            await conn.execute("DROP TABLE IF EXISTS users CASCADE")
            await conn.close()

    async def test_introspect_views_sqlite(self):
        """Views read back off a REAL database, not off two mock classes.

        This test hand-wrote a `MockCursor` and a `MockConn` and asserted
        against what they returned, so it tested the mocks. That shape has
        already manufactured two false bugs in this package: a stand-in that
        got the driver contract wrong invented a pyturso defect, and another
        that was more permissive than the driver would have hidden a
        regression.

        SQLite costs nothing to create, so there is nothing to stand in for.
        """
        import os
        import tempfile

        import aiosqlite

        from declaro_persistum.inspector.sqlite import views_of

        path = tempfile.mktemp(suffix=".db")
        conn = await aiosqlite.connect(path)
        try:
            await conn.execute("CREATE TABLE users (id INT, active INT)")
            await conn.execute(
                "CREATE VIEW active_users AS SELECT * FROM users WHERE active = 1"
            )
            await conn.commit()

            views = await views_of(conn)

            assert "active_users" in views, sorted(views)
            assert "SELECT * FROM users WHERE active = 1" in views["active_users"]["query"]
        finally:
            await conn.close()
            os.unlink(path)

    @pytest.mark.asyncio
    async def test_a_database_with_no_views_reads_back_empty(self):
        """The floor. Returning the whole of sqlite_master would pass above."""
        import os
        import tempfile

        import aiosqlite

        from declaro_persistum.inspector.sqlite import views_of

        path = tempfile.mktemp(suffix=".db")
        conn = await aiosqlite.connect(path)
        try:
            await conn.execute("CREATE TABLE users (id INT)")
            await conn.commit()

            assert await views_of(conn) == {}
        finally:
            await conn.close()
            os.unlink(path)
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

        # DROP THEN CREATE, NOT `CREATE OR REPLACE`. This asserted one
        # operation, with the comment "CREATE OR REPLACE handles this". It
        # does not, on either engine. Measured 2026-08-17:
        #
        #     SQLite      CREATE OR REPLACE VIEW -> near "OR": syntax error
        #     PostgreSQL  changed column list    -> InvalidTableDefinitionError:
        #                                           cannot drop columns from view
        #
        # PostgreSQL accepts CREATE OR REPLACE only when the column list is
        # unchanged, and this test changes `SELECT *` to `SELECT id, email`.
        # SQLite has no such statement at all. Drop-then-create is what works
        # on both, so the differ is right and the belief was wrong.
        assert [o["op"] for o in operations] == ["drop_view", "create_view"]

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
        from declaro_persistum.applier import postgresql as _applier_postgresql

        applier = _applier_postgresql
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
        from declaro_persistum.applier import postgresql as _applier_postgresql

        applier = _applier_postgresql
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
        from declaro_persistum.applier import postgresql as _applier_postgresql

        applier = _applier_postgresql
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
        from declaro_persistum.applier import postgresql as _applier_postgresql

        applier = _applier_postgresql
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
        from declaro_persistum.applier import sqlite as _applier_sqlite

        applier = _applier_sqlite
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
        from declaro_persistum.applier import sqlite as _applier_sqlite

        applier = _applier_sqlite
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
        from declaro_persistum.applier import turso as _applier_turso

        applier = _applier_turso
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
        from declaro_persistum.applier import turso as _applier_turso

        applier = _applier_turso
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


# `TestConcurrentRefreshValidation` stood here: six tests for
# `has_unique_index`, `get_materialized_view_indexes` and
# `validate_concurrent_refresh`, which checked the precondition PostgreSQL
# imposes on `REFRESH MATERIALIZED VIEW CONCURRENTLY`.
#
# That precondition is real. Measured against a live server 2026-08-17:
#
#     REFRESH MATERIALIZED VIEW CONCURRENTLY mv
#     ObjectNotInPrerequisiteStateError: cannot refresh materialized view
#     "public.mv" concurrently
#     HINT: Create a unique index with no WHERE clause ...
#
# and it succeeds once a unique index exists. So the guard was right about
# PostgreSQL.
#
# THE PACKAGE NO LONGER EMITS THAT STATEMENT. `grep CONCURRENTLY src/` finds
# nothing. A materialized view is emulated as a real table now, and
# `refresh_matview_sql` produces DELETE plus INSERT, or DROP plus CREATE TABLE
# AS, on every engine. There is no concurrent refresh to guard.
#
# Deleted rather than ported, for the same reason `test_enums.py` was: porting
# a name onto machinery that is not there resurrects the design it belonged
# to. If native materialized views come back, this is the class to restore and
# the measurement above is why it is worth restoring.
