"""
Unit tests for Materialized View emulation abstraction.

Tests portable materialized view patterns using tables for SQLite/Turso.
These tests should FAIL initially (RED) until implementation is complete.
"""

import pytest
from typing import Any


class TestCreateMatviewSQL:
    """Tests for creating emulated materialized views."""

    def test_create_matview_generates_metadata_table(self):
        """Create matview should ensure metadata table exists."""
        from declaro_persistum.abstractions.materialized_views import create_matview_sql

        statements = create_matview_sql(
            name="monthly_stats",
            query="SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
        )

        # First statement should create metadata table
        assert any("_dp_materialized_views" in s for s in statements)
        assert any("CREATE TABLE IF NOT EXISTS" in s for s in statements)

    def test_create_matview_generates_backing_table(self):
        """Create matview should create backing table from query."""
        from declaro_persistum.abstractions.materialized_views import create_matview_sql

        statements = create_matview_sql(
            name="monthly_stats",
            query="SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
        )

        # Should have CREATE TABLE ... AS SELECT
        assert any('CREATE TABLE "monthly_stats" AS' in s for s in statements)

    def test_create_matview_registers_in_metadata(self):
        """Create matview should register in metadata table."""
        from declaro_persistum.abstractions.materialized_views import create_matview_sql

        statements = create_matview_sql(
            name="monthly_stats",
            query="SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
            refresh_strategy="manual",
        )

        # Should INSERT into metadata
        assert any("INSERT INTO _dp_materialized_views" in s for s in statements)
        assert any("monthly_stats" in s for s in statements)
        assert any("manual" in s for s in statements)

    def test_create_matview_with_depends_on(self):
        """Create matview should store depends_on as JSON."""
        from declaro_persistum.abstractions.materialized_views import create_matview_sql

        statements = create_matview_sql(
            name="monthly_stats",
            query="SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
            depends_on=["orders"],
        )

        # Should include JSON array in INSERT
        joined = " ".join(statements)
        assert '["orders"]' in joined or "orders" in joined

    def test_create_matview_default_strategy_is_manual(self):
        """Default refresh strategy should be 'manual'."""
        from declaro_persistum.abstractions.materialized_views import create_matview_sql

        statements = create_matview_sql(
            name="test_view",
            query="SELECT 1",
        )

        joined = " ".join(statements)
        assert "'manual'" in joined


class TestRefreshMatviewSQL:
    """Tests for refreshing emulated materialized views."""

    def test_refresh_generates_delete_insert(self):
        """Atomic refresh should DELETE then INSERT."""
        from declaro_persistum.abstractions.materialized_views import refresh_matview_sql

        statements = refresh_matview_sql(
            name="monthly_stats",
            query="SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
            atomic=True,
        )

        assert any('DELETE FROM "monthly_stats"' in s for s in statements)
        assert any('INSERT INTO "monthly_stats"' in s for s in statements)

    def test_refresh_updates_timestamp(self):
        """Refresh should update last_refreshed_at in metadata."""
        from declaro_persistum.abstractions.materialized_views import refresh_matview_sql

        statements = refresh_matview_sql(
            name="monthly_stats",
            query="SELECT 1",
            atomic=True,
        )

        assert any("last_refreshed_at" in s for s in statements)
        assert any("UPDATE _dp_materialized_views" in s for s in statements)

    def test_refresh_non_atomic_recreates_table(self):
        """Non-atomic refresh should DROP and CREATE table."""
        from declaro_persistum.abstractions.materialized_views import refresh_matview_sql

        statements = refresh_matview_sql(
            name="monthly_stats",
            query="SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
            atomic=False,
        )

        assert any('DROP TABLE IF EXISTS "monthly_stats"' in s for s in statements)
        assert any('CREATE TABLE "monthly_stats" AS' in s for s in statements)


class TestTriggerGeneration:
    """Tests for auto-refresh trigger generation."""

    def test_generates_insert_trigger(self):
        """Should generate AFTER INSERT trigger."""
        from declaro_persistum.abstractions.materialized_views import (
            generate_refresh_trigger_sql,
        )

        statements = generate_refresh_trigger_sql(
            matview_name="monthly_stats",
            source_table="orders",
            query="SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
        )

        joined = " ".join(statements)
        assert "AFTER INSERT ON" in joined
        assert '"orders"' in joined

    def test_generates_update_trigger(self):
        """Should generate AFTER UPDATE trigger."""
        from declaro_persistum.abstractions.materialized_views import (
            generate_refresh_trigger_sql,
        )

        statements = generate_refresh_trigger_sql(
            matview_name="monthly_stats",
            source_table="orders",
            query="SELECT 1",
        )

        joined = " ".join(statements)
        assert "AFTER UPDATE ON" in joined

    def test_generates_delete_trigger(self):
        """Should generate AFTER DELETE trigger."""
        from declaro_persistum.abstractions.materialized_views import (
            generate_refresh_trigger_sql,
        )

        statements = generate_refresh_trigger_sql(
            matview_name="monthly_stats",
            source_table="orders",
            query="SELECT 1",
        )

        joined = " ".join(statements)
        assert "AFTER DELETE ON" in joined

    def test_trigger_refreshes_matview(self):
        """Trigger body should refresh the matview."""
        from declaro_persistum.abstractions.materialized_views import (
            generate_refresh_trigger_sql,
        )

        statements = generate_refresh_trigger_sql(
            matview_name="monthly_stats",
            source_table="orders",
            query="SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
        )

        joined = " ".join(statements)
        # Trigger should DELETE and INSERT
        assert 'DELETE FROM "monthly_stats"' in joined
        assert 'INSERT INTO "monthly_stats"' in joined

    def test_trigger_naming_convention(self):
        """Triggers should follow naming convention."""
        from declaro_persistum.abstractions.materialized_views import (
            generate_refresh_trigger_sql,
        )

        statements = generate_refresh_trigger_sql(
            matview_name="monthly_stats",
            source_table="orders",
            query="SELECT 1",
        )

        joined = " ".join(statements)
        assert "_dp_refresh_monthly_stats_on_orders_insert" in joined
        assert "_dp_refresh_monthly_stats_on_orders_update" in joined
        assert "_dp_refresh_monthly_stats_on_orders_delete" in joined


class TestDropMatviewSQL:
    """Tests for dropping emulated materialized views."""

    def test_drop_removes_table(self):
        """Drop should remove backing table."""
        from declaro_persistum.abstractions.materialized_views import drop_matview_sql

        statements = drop_matview_sql(name="monthly_stats")

        assert any('DROP TABLE IF EXISTS "monthly_stats"' in s for s in statements)

    def test_drop_removes_metadata(self):
        """Drop should remove metadata row."""
        from declaro_persistum.abstractions.materialized_views import drop_matview_sql

        statements = drop_matview_sql(name="monthly_stats")

        assert any("DELETE FROM _dp_materialized_views" in s for s in statements)
        assert any("monthly_stats" in s for s in statements)

    def test_drop_triggers(self):
        """Should be able to drop triggers."""
        from declaro_persistum.abstractions.materialized_views import (
            drop_refresh_triggers_sql,
        )

        statements = drop_refresh_triggers_sql(
            matview_name="monthly_stats",
            source_tables=["orders"],
        )

        assert len(statements) == 3  # insert, update, delete triggers
        assert any("DROP TRIGGER IF EXISTS" in s for s in statements)


class TestMetadataTableSchema:
    """Tests for metadata table schema generation."""

    def test_generate_metadata_table_schema(self):
        """Should generate proper schema for metadata table."""
        from declaro_persistum.abstractions.materialized_views import (
            generate_metadata_table_schema,
        )

        schema = generate_metadata_table_schema()

        assert "_dp_materialized_views" in schema
        table = schema["_dp_materialized_views"]
        columns = table["columns"]

        assert "name" in columns
        assert columns["name"]["primary_key"] is True
        assert "query" in columns
        assert "refresh_strategy" in columns
        assert "depends_on" in columns
        assert "last_refreshed_at" in columns
        assert "created_at" in columns


class TestMatviewQueryFunctions:
    """Tests for query helper functions."""

    def test_is_matview_sql(self):
        """Should generate SQL to check if table is matview."""
        from declaro_persistum.abstractions.materialized_views import is_matview_sql

        sql = is_matview_sql()
        assert "SELECT" in sql
        assert "_dp_materialized_views" in sql
        assert ":table_name" in sql or "?" in sql

    def test_get_matview_metadata_sql(self):
        """Should generate SQL to get matview metadata."""
        from declaro_persistum.abstractions.materialized_views import (
            get_matview_metadata_sql,
        )

        sql = get_matview_metadata_sql()
        assert "SELECT" in sql
        assert "query" in sql
        assert "refresh_strategy" in sql
        assert "_dp_materialized_views" in sql

    def test_list_matviews_sql(self):
        """Should generate SQL to list all matviews."""
        from declaro_persistum.abstractions.materialized_views import list_matviews_sql

        sql = list_matviews_sql()
        assert "SELECT" in sql
        assert "_dp_materialized_views" in sql
        assert "ORDER BY" in sql


class TestSQLiteApplierIntegration:
    """Tests for SQLite applier matview integration."""

    def test_sqlite_generate_create_view_uses_emulation(self):
        """SQLite should use table emulation for materialized views."""
        from declaro_persistum.applier.sqlite import generate_create_view
        from declaro_persistum.types import View

        view: View = {
            "name": "monthly_stats",
            "query": "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
            "materialized": True,
            "refresh": "manual",
        }

        sql = generate_create_view(view)

        # Should NOT be CREATE VIEW (that's for non-materialized)
        # Should use table-based emulation
        assert "CREATE TABLE" in sql or "_dp_materialized_views" in sql

    def test_sqlite_generate_create_view_regular_view(self):
        """SQLite should create regular view for non-materialized."""
        from declaro_persistum.applier.sqlite import generate_create_view
        from declaro_persistum.types import View

        view: View = {
            "name": "active_users",
            "query": "SELECT * FROM users WHERE status = 'active'",
            "materialized": False,
        }

        sql = generate_create_view(view)

        assert "CREATE VIEW" in sql
        assert "active_users" in sql

    def test_sqlite_no_warning_for_emulated_matview(self):
        """SQLite should NOT emit warning when using emulation."""
        import warnings
        from declaro_persistum.applier.sqlite import generate_create_view
        from declaro_persistum.types import View

        view: View = {
            "name": "monthly_stats",
            "query": "SELECT 1",
            "materialized": True,
        }

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            generate_create_view(view)
            # Should NOT have warning about materialized views not supported
            matview_warnings = [
                x for x in w if "materialized" in str(x.message).lower()
            ]
            assert len(matview_warnings) == 0


class TestTursoApplierIntegration:
    """Tests for Turso applier matview integration."""

    def test_turso_generate_create_view_uses_emulation(self):
        """Turso should use table emulation for materialized views."""
        from declaro_persistum.applier.turso import generate_create_view
        from declaro_persistum.types import View

        view: View = {
            "name": "monthly_stats",
            "query": "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id",
            "materialized": True,
        }

        sql = generate_create_view(view)

        # Should use table-based emulation
        assert "CREATE TABLE" in sql or "_dp_materialized_views" in sql

    def test_turso_sqlite_identical_sql(self):
        """Turso and SQLite should generate identical emulation SQL."""
        from declaro_persistum.applier.sqlite import (
            generate_create_view as sqlite_create,
        )
        from declaro_persistum.applier.turso import (
            generate_create_view as turso_create,
        )
        from declaro_persistum.types import View

        view: View = {
            "name": "monthly_stats",
            "query": "SELECT 1",
            "materialized": True,
        }

        sqlite_sql = sqlite_create(view)
        turso_sql = turso_create(view)

        assert sqlite_sql == turso_sql


class TestViewValidation:
    """Tests for view validation with new refresh strategies."""

    def test_validate_accepts_manual_refresh(self):
        """Should accept refresh='manual'."""
        from declaro_persistum.loader import validate_view
        from declaro_persistum.types import View

        view: View = {
            "name": "test_view",
            "query": "SELECT 1",
            "materialized": True,
            "refresh": "manual",
        }

        # Should not raise
        validate_view(view)

    def test_validate_accepts_trigger_refresh(self):
        """Should accept refresh='trigger' with trigger_sources."""
        from declaro_persistum.loader import validate_view
        from declaro_persistum.types import View

        view: View = {
            "name": "test_view",
            "query": "SELECT 1",
            "materialized": True,
            "refresh": "trigger",
            "trigger_sources": ["orders"],
        }

        # Should not raise
        validate_view(view)

    def test_validate_accepts_hybrid_refresh(self):
        """Should accept refresh='hybrid'."""
        from declaro_persistum.loader import validate_view
        from declaro_persistum.types import View

        view: View = {
            "name": "test_view",
            "query": "SELECT 1",
            "materialized": True,
            "refresh": "hybrid",
        }

        # Should not raise
        validate_view(view)

    def test_validate_trigger_sources_requires_trigger_strategy(self):
        """trigger_sources should require refresh='trigger' or 'hybrid'."""
        from declaro_persistum.loader import validate_view
        from declaro_persistum.types import View

        view: View = {
            "name": "test_view",
            "query": "SELECT 1",
            "materialized": True,
            "refresh": "manual",
            "trigger_sources": ["orders"],
        }

        with pytest.raises(ValueError, match="trigger_sources"):
            validate_view(view)
