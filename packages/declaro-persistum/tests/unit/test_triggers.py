"""
Unit tests for Trigger support.

Tests the Trigger TypedDict, loader parsing, and applier generation.
"""

import pytest
from typing import Any

from declaro_persistum.types import Trigger


class TestTriggerTypedDict:
    """Tests for Trigger TypedDict structure."""

    def test_trigger_basic(self):
        """Basic trigger definition."""
        trigger: Trigger = {
            "name": "update_timestamp",
            "timing": "before",
            "event": "update",
            "for_each": "row",
            "body": "NEW.updated_at = now(); RETURN NEW;",
        }
        assert trigger["name"] == "update_timestamp"
        assert trigger["timing"] == "before"
        assert trigger["event"] == "update"

    def test_trigger_multiple_events(self):
        """Trigger with multiple events."""
        trigger: Trigger = {
            "name": "audit_changes",
            "timing": "after",
            "event": ["insert", "update", "delete"],
            "for_each": "row",
            "body": "INSERT INTO audit_log (table_name, action) VALUES (TG_TABLE_NAME, TG_OP);",
        }
        assert isinstance(trigger["event"], list)
        assert len(trigger["event"]) == 3

    def test_trigger_with_condition(self):
        """Trigger with WHEN condition."""
        trigger: Trigger = {
            "name": "notify_on_status_change",
            "timing": "after",
            "event": "update",
            "for_each": "row",
            "when": "OLD.status IS DISTINCT FROM NEW.status",
            "body": "PERFORM pg_notify('status_changed', NEW.id::text);",
        }
        assert trigger.get("when") is not None

    def test_trigger_execute_procedure(self):
        """Trigger that executes a stored procedure."""
        trigger: Trigger = {
            "name": "validate_data",
            "timing": "before",
            "event": "insert",
            "for_each": "row",
            "execute": "validate_user_data",
        }
        assert trigger.get("execute") == "validate_user_data"
        assert trigger.get("body") is None


class TestTriggerLoading:
    """Tests for loading triggers from TOML."""

    def test_parse_trigger_from_table(self):
        """Parse trigger from table definition."""
        from declaro_persistum.loader import parse_trigger

        toml_data = {
            "timing": "before",
            "event": "update",
            "for_each": "row",
            "body": "NEW.updated_at = now(); RETURN NEW;",
        }
        trigger = parse_trigger("update_timestamp", toml_data)
        assert trigger["name"] == "update_timestamp"
        assert trigger["timing"] == "before"

    def test_parse_trigger_events_list(self):
        """Parse trigger with event list."""
        from declaro_persistum.loader import parse_trigger

        toml_data = {
            "timing": "after",
            "event": ["insert", "update"],
            "for_each": "row",
            "body": "RETURN NULL;",
        }
        trigger = parse_trigger("audit", toml_data)
        assert trigger["event"] == ["insert", "update"]


class TestTriggerApplierPostgreSQL:
    """Tests for PostgreSQL trigger applier."""

    def test_create_trigger_function_sql(self):
        """Generate trigger function SQL."""
        from declaro_persistum.applier.postgresql import generate_trigger_function

        trigger: Trigger = {
            "name": "update_timestamp",
            "timing": "before",
            "event": "update",
            "for_each": "row",
            "body": "NEW.updated_at = now(); RETURN NEW;",
        }
        sql = generate_trigger_function("users", trigger)
        assert "CREATE OR REPLACE FUNCTION" in sql
        assert "users_update_timestamp" in sql
        assert "RETURNS TRIGGER" in sql
        assert "NEW.updated_at = now()" in sql
        assert "LANGUAGE plpgsql" in sql

    def test_create_trigger_sql(self):
        """Generate CREATE TRIGGER SQL."""
        from declaro_persistum.applier.postgresql import generate_create_trigger

        trigger: Trigger = {
            "name": "update_timestamp",
            "timing": "before",
            "event": "update",
            "for_each": "row",
            "body": "NEW.updated_at = now(); RETURN NEW;",
        }
        sql = generate_create_trigger("users", trigger)
        assert "CREATE TRIGGER update_timestamp" in sql
        assert "BEFORE UPDATE" in sql
        assert "ON users" in sql
        assert "FOR EACH ROW" in sql
        assert "EXECUTE FUNCTION" in sql

    def test_create_trigger_multiple_events(self):
        """Generate trigger with multiple events."""
        from declaro_persistum.applier.postgresql import generate_create_trigger

        trigger: Trigger = {
            "name": "audit",
            "timing": "after",
            "event": ["insert", "update", "delete"],
            "for_each": "row",
            "body": "RETURN NULL;",
        }
        sql = generate_create_trigger("users", trigger)
        assert "AFTER INSERT OR UPDATE OR DELETE" in sql

    def test_create_trigger_with_when(self):
        """Generate trigger with WHEN clause."""
        from declaro_persistum.applier.postgresql import generate_create_trigger

        trigger: Trigger = {
            "name": "status_change",
            "timing": "after",
            "event": "update",
            "for_each": "row",
            "when": "OLD.status IS DISTINCT FROM NEW.status",
            "body": "RETURN NULL;",
        }
        sql = generate_create_trigger("orders", trigger)
        assert "WHEN (OLD.status IS DISTINCT FROM NEW.status)" in sql

    def test_drop_trigger_sql(self):
        """Generate DROP TRIGGER SQL."""
        from declaro_persistum.applier.postgresql import generate_drop_trigger

        sql = generate_drop_trigger("users", "update_timestamp")
        assert "DROP TRIGGER IF EXISTS update_timestamp ON users" in sql


class TestTriggerApplierSQLite:
    """Tests for SQLite trigger applier."""

    def test_create_trigger_sql_sqlite(self):
        """Generate SQLite CREATE TRIGGER SQL."""
        from declaro_persistum.applier.sqlite import generate_create_trigger

        trigger: Trigger = {
            "name": "update_timestamp",
            "timing": "before",
            "event": "update",
            "for_each": "row",
            "body": "UPDATE users SET updated_at = datetime('now') WHERE id = NEW.id;",
        }
        sql = generate_create_trigger("users", trigger)
        assert "CREATE TRIGGER" in sql
        assert "users_update_timestamp" in sql
        assert "BEFORE UPDATE" in sql
        assert "ON users" in sql
        assert "FOR EACH ROW" in sql
        assert "BEGIN" in sql
        assert "END" in sql

    def test_create_trigger_sqlite_multiple_events(self):
        """SQLite requires separate triggers for each event."""
        from declaro_persistum.applier.sqlite import generate_create_triggers_for_events

        trigger: Trigger = {
            "name": "audit",
            "timing": "after",
            "event": ["insert", "update"],
            "for_each": "row",
            "body": "INSERT INTO audit_log VALUES (NEW.id);",
        }
        sqls = generate_create_triggers_for_events("users", trigger)
        assert len(sqls) == 2
        assert any("AFTER INSERT" in sql for sql in sqls)
        assert any("AFTER UPDATE" in sql for sql in sqls)

    def test_drop_trigger_sql_sqlite(self):
        """Generate SQLite DROP TRIGGER SQL."""
        from declaro_persistum.applier.sqlite import generate_drop_trigger

        sql = generate_drop_trigger("users", "update_timestamp")
        assert "DROP TRIGGER IF EXISTS users_update_timestamp" in sql


class TestTriggerValidation:
    """Tests for trigger validation."""

    def test_validate_trigger_timing(self):
        """Trigger timing must be valid."""
        from declaro_persistum.loader import validate_trigger

        trigger: Trigger = {
            "name": "test",
            "timing": "invalid",  # type: ignore
            "event": "insert",
            "for_each": "row",
            "body": "RETURN NEW;",
        }
        with pytest.raises(ValueError, match="timing"):
            validate_trigger(trigger)

    def test_validate_trigger_event(self):
        """Trigger event must be valid."""
        from declaro_persistum.loader import validate_trigger

        trigger: Trigger = {
            "name": "test",
            "timing": "before",
            "event": "invalid",
            "for_each": "row",
            "body": "RETURN NEW;",
        }
        with pytest.raises(ValueError, match="event"):
            validate_trigger(trigger)

    def test_validate_trigger_requires_body_or_execute(self):
        """Trigger must have body or execute."""
        from declaro_persistum.loader import validate_trigger

        trigger: Trigger = {
            "name": "test",
            "timing": "before",
            "event": "insert",
            "for_each": "row",
        }
        with pytest.raises(ValueError, match="'body' or 'execute'"):
            validate_trigger(trigger)


class TestTriggerDiff:
    """Tests for trigger diff detection."""

    def test_detect_new_trigger(self):
        """Detect when trigger is added."""
        from declaro_persistum.differ import diff_triggers

        old_triggers: dict[str, Trigger] = {}
        new_triggers: dict[str, Trigger] = {
            "update_timestamp": {
                "name": "update_timestamp",
                "timing": "before",
                "event": "update",
                "for_each": "row",
                "body": "NEW.updated_at = now(); RETURN NEW;",
            }
        }
        operations = diff_triggers("users", old_triggers, new_triggers)
        assert len(operations) == 1
        assert operations[0]["op"] == "create_trigger"

    def test_detect_dropped_trigger(self):
        """Detect when trigger is removed."""
        from declaro_persistum.differ import diff_triggers

        old_triggers: dict[str, Trigger] = {
            "update_timestamp": {
                "name": "update_timestamp",
                "timing": "before",
                "event": "update",
                "for_each": "row",
                "body": "NEW.updated_at = now(); RETURN NEW;",
            }
        }
        new_triggers: dict[str, Trigger] = {}
        operations = diff_triggers("users", old_triggers, new_triggers)
        assert len(operations) == 1
        assert operations[0]["op"] == "drop_trigger"

    def test_detect_trigger_body_changed(self):
        """Detect when trigger body changes."""
        from declaro_persistum.differ import diff_triggers

        old_triggers: dict[str, Trigger] = {
            "update_timestamp": {
                "name": "update_timestamp",
                "timing": "before",
                "event": "update",
                "for_each": "row",
                "body": "NEW.updated_at = now(); RETURN NEW;",
            }
        }
        new_triggers: dict[str, Trigger] = {
            "update_timestamp": {
                "name": "update_timestamp",
                "timing": "before",
                "event": "update",
                "for_each": "row",
                "body": "NEW.updated_at = CURRENT_TIMESTAMP; RETURN NEW;",
            }
        }
        operations = diff_triggers("users", old_triggers, new_triggers)
        # Should drop and recreate
        assert len(operations) == 2
        assert operations[0]["op"] == "drop_trigger"
        assert operations[1]["op"] == "create_trigger"
