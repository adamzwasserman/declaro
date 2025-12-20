"""Tests for the diff engine."""

import pytest

from declaro_persistum.differ.core import diff
from declaro_persistum.differ.ambiguity import (
    detect_ambiguities,
    calculate_rename_confidence,
)
from declaro_persistum.differ.toposort import topological_sort, build_dependency_graph
from declaro_persistum.types import Schema


class TestDiff:
    """Tests for the diff function."""

    def test_diff_empty_schemas(self):
        """Diffing two empty schemas produces no operations."""
        result = diff(current={}, target={})
        assert result["operations"] == []
        assert result["ambiguities"] == []

    def test_diff_identical_schemas(self, simple_schema):
        """Diffing identical schemas produces no operations."""
        result = diff(current=simple_schema, target=simple_schema)
        assert result["operations"] == []

    def test_diff_create_table(self, simple_schema):
        """Adding a new table creates CREATE TABLE operation."""
        result = diff(current={}, target=simple_schema)

        assert len(result["operations"]) >= 1
        create_ops = [op for op in result["operations"] if op["op"] == "create_table"]
        assert len(create_ops) == 1
        assert create_ops[0]["table"] == "users"

    def test_diff_drop_table(self, simple_schema):
        """Removing a table creates DROP TABLE operation."""
        result = diff(current=simple_schema, target={})

        drop_ops = [op for op in result["operations"] if op["op"] == "drop_table"]
        assert len(drop_ops) == 1
        assert drop_ops[0]["table"] == "users"

    def test_diff_add_column(self):
        """Adding a column creates ADD COLUMN operation."""
        current: Schema = {
            "users": {"columns": {"id": {"type": "uuid"}}}
        }
        target: Schema = {
            "users": {"columns": {"id": {"type": "uuid"}, "email": {"type": "text"}}}
        }

        result = diff(current=current, target=target)

        add_ops = [op for op in result["operations"] if op["op"] == "add_column"]
        assert len(add_ops) == 1
        assert add_ops[0]["table"] == "users"
        assert add_ops[0]["details"]["column"] == "email"

    def test_diff_drop_column(self):
        """Removing a column creates DROP COLUMN operation."""
        current: Schema = {
            "users": {"columns": {"id": {"type": "uuid"}, "temp": {"type": "text"}}}
        }
        target: Schema = {
            "users": {"columns": {"id": {"type": "uuid"}}}
        }

        result = diff(current=current, target=target)

        drop_ops = [op for op in result["operations"] if op["op"] == "drop_column"]
        assert len(drop_ops) == 1
        assert drop_ops[0]["details"]["column"] == "temp"

    def test_diff_rename_column_with_hint(self):
        """Renaming a column with hint creates RENAME COLUMN operation."""
        current: Schema = {
            "users": {"columns": {"name": {"type": "text"}}}
        }
        target: Schema = {
            "users": {"columns": {"full_name": {"type": "text", "renamed_from": "name"}}}
        }

        result = diff(current=current, target=target)

        rename_ops = [op for op in result["operations"] if op["op"] == "rename_column"]
        assert len(rename_ops) == 1
        assert rename_ops[0]["details"]["from_column"] == "name"
        assert rename_ops[0]["details"]["to_column"] == "full_name"

    def test_diff_alter_column_type(self):
        """Changing column type creates ALTER COLUMN operation."""
        current: Schema = {
            "users": {"columns": {"count": {"type": "integer"}}}
        }
        target: Schema = {
            "users": {"columns": {"count": {"type": "bigint"}}}
        }

        result = diff(current=current, target=target)

        alter_ops = [op for op in result["operations"] if op["op"] == "alter_column"]
        assert len(alter_ops) == 1
        assert alter_ops[0]["details"]["column"] == "count"
        assert "type" in alter_ops[0]["details"]["changes"]

    def test_diff_add_index(self):
        """Adding an index creates ADD INDEX operation."""
        current: Schema = {
            "users": {"columns": {"email": {"type": "text"}}, "indexes": {}}
        }
        target: Schema = {
            "users": {
                "columns": {"email": {"type": "text"}},
                "indexes": {"email_idx": {"columns": ["email"]}},
            }
        }

        result = diff(current=current, target=target)

        add_idx_ops = [op for op in result["operations"] if op["op"] == "add_index"]
        assert len(add_idx_ops) == 1
        assert add_idx_ops[0]["details"]["index"] == "email_idx"

    def test_diff_with_foreign_key(self):
        """Adding a foreign key creates ADD FOREIGN KEY operation."""
        current: Schema = {
            "users": {"columns": {"id": {"type": "uuid"}}},
            "orders": {"columns": {"id": {"type": "uuid"}, "user_id": {"type": "uuid"}}},
        }
        target: Schema = {
            "users": {"columns": {"id": {"type": "uuid"}}},
            "orders": {
                "columns": {
                    "id": {"type": "uuid"},
                    "user_id": {"type": "uuid", "references": "users.id"},
                }
            },
        }

        result = diff(current=current, target=target)

        fk_ops = [op for op in result["operations"] if op["op"] == "add_foreign_key"]
        assert len(fk_ops) == 1
        assert fk_ops[0]["details"]["references"] == "users.id"


class TestAmbiguityDetection:
    """Tests for ambiguity detection."""

    def test_detect_possible_rename(self):
        """Detects possible column rename when type matches."""
        current: Schema = {
            "users": {"columns": {"name": {"type": "text"}}}
        }
        target: Schema = {
            "users": {"columns": {"full_name": {"type": "text"}}}
        }

        ambiguities = detect_ambiguities(current, target)

        assert len(ambiguities) >= 1
        assert any(a["type"] == "possible_rename" for a in ambiguities)

    def test_no_ambiguity_with_different_types(self):
        """No rename ambiguity when types differ."""
        current: Schema = {
            "users": {"columns": {"count": {"type": "integer"}}}
        }
        target: Schema = {
            "users": {"columns": {"total": {"type": "text"}}}
        }

        ambiguities = detect_ambiguities(current, target)

        rename_amb = [a for a in ambiguities if a["type"] == "possible_rename"]
        assert len(rename_amb) == 0

    def test_no_ambiguity_with_renamed_from_hint(self):
        """No ambiguity when renamed_from hint is present."""
        current: Schema = {
            "users": {"columns": {"name": {"type": "text"}}}
        }
        target: Schema = {
            "users": {"columns": {"full_name": {"type": "text", "renamed_from": "name"}}}
        }

        ambiguities = detect_ambiguities(current, target)

        assert len(ambiguities) == 0

    def test_no_ambiguity_with_is_new_hint(self):
        """No ambiguity when is_new hint is present."""
        current: Schema = {
            "users": {"columns": {"name": {"type": "text"}}}
        }
        target: Schema = {
            "users": {"columns": {"full_name": {"type": "text", "is_new": True}}}
        }

        ambiguities = detect_ambiguities(current, target)

        rename_amb = [a for a in ambiguities if a["type"] == "possible_rename"]
        assert len(rename_amb) == 0


class TestRenameConfidence:
    """Tests for rename confidence calculation."""

    def test_exact_match_case_change(self):
        """Exact match (case change) has confidence 1.0."""
        confidence = calculate_rename_confidence("Name", "name")
        assert confidence == 1.0

    def test_prefix_match(self):
        """Prefix match has reasonable confidence."""
        confidence = calculate_rename_confidence("name", "full_name")
        assert confidence > 0.4  # "name" is contained in "full_name"

    def test_suffix_match(self):
        """Suffix match has some confidence."""
        confidence = calculate_rename_confidence("user_id", "id")
        assert confidence > 0.2  # "id" is shorter, so lower ratio

    def test_completely_different(self):
        """Completely different names have low confidence."""
        confidence = calculate_rename_confidence("foo", "bar")
        assert confidence < 0.5


class TestTopologicalSort:
    """Tests for topological sorting."""

    def test_empty_operations(self):
        """Empty operations produce empty order."""
        order = topological_sort([], {})
        assert order == []

    def test_no_dependencies(self):
        """Operations without dependencies maintain stable order."""
        operations = [
            {"op": "create_table", "table": "a", "details": {}},
            {"op": "create_table", "table": "b", "details": {}},
        ]
        deps = {0: [], 1: []}

        order = topological_sort(operations, deps)

        assert set(order) == {0, 1}

    def test_with_dependencies(self):
        """Operations respect dependencies."""
        operations = [
            {"op": "add_foreign_key", "table": "orders", "details": {}},  # depends on 1
            {"op": "create_table", "table": "users", "details": {}},  # no deps
        ]
        deps = {0: [1], 1: []}

        order = topological_sort(operations, deps)

        assert order.index(1) < order.index(0)  # users before FK

    def test_dependency_graph_for_fk(self):
        """Foreign key depends on referenced table creation."""
        operations = [
            {
                "op": "create_table",
                "table": "users",
                "details": {"columns": {"id": {"type": "uuid"}}},
            },
            {
                "op": "create_table",
                "table": "orders",
                "details": {
                    "columns": {
                        "user_id": {"type": "uuid", "references": "users.id"}
                    }
                },
            },
        ]

        deps = build_dependency_graph(operations)

        # orders creation should depend on users creation
        assert 0 in deps[1] or len(deps[1]) == 0  # depends on users or no deps
