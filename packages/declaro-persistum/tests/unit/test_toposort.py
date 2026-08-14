"""Tests for topological sorting."""

import pytest

from declaro_persistum.differ.toposort import (
    topological_sort,
    build_dependency_graph,
    _operation_priority,
)
from declaro_persistum.exceptions import CycleError


class TestTopologicalSort:
    """Tests for topological_sort function."""

    def test_empty_input(self):
        """Empty input returns empty output."""
        result = topological_sort([], {})
        assert result == []

    def test_single_operation(self):
        """Single operation returns single-element list."""
        operations = [{"op": "create_table", "table": "users", "details": {}}]
        deps = {0: []}

        result = topological_sort(operations, deps)

        assert result == [0]

    def test_independent_operations(self):
        """Independent operations all appear in output."""
        operations = [
            {"op": "create_table", "table": "a", "details": {}},
            {"op": "create_table", "table": "b", "details": {}},
            {"op": "create_table", "table": "c", "details": {}},
        ]
        deps = {0: [], 1: [], 2: []}

        result = topological_sort(operations, deps)

        assert set(result) == {0, 1, 2}

    def test_linear_dependencies(self):
        """Linear dependency chain produces correct order."""
        # c depends on b, b depends on a
        operations = [
            {"op": "create_table", "table": "a", "details": {}},
            {"op": "create_table", "table": "b", "details": {}},
            {"op": "create_table", "table": "c", "details": {}},
        ]
        deps = {0: [], 1: [0], 2: [1]}

        result = topological_sort(operations, deps)

        # a must come before b, b must come before c
        assert result.index(0) < result.index(1)
        assert result.index(1) < result.index(2)

    def test_diamond_dependencies(self):
        """Diamond dependency pattern produces valid order."""
        # d depends on b and c, both b and c depend on a
        operations = [
            {"op": "create_table", "table": "a", "details": {}},
            {"op": "create_table", "table": "b", "details": {}},
            {"op": "create_table", "table": "c", "details": {}},
            {"op": "create_table", "table": "d", "details": {}},
        ]
        deps = {0: [], 1: [0], 2: [0], 3: [1, 2]}

        result = topological_sort(operations, deps)

        # a must come before b and c
        assert result.index(0) < result.index(1)
        assert result.index(0) < result.index(2)
        # b and c must come before d
        assert result.index(1) < result.index(3)
        assert result.index(2) < result.index(3)

    def test_cycle_detection(self):
        """Cycle in dependencies raises CycleError."""
        # a depends on b, b depends on a
        operations = [
            {"op": "create_table", "table": "a", "details": {}},
            {"op": "create_table", "table": "b", "details": {}},
        ]
        deps = {0: [1], 1: [0]}

        with pytest.raises(CycleError):
            topological_sort(operations, deps)

    def test_self_cycle_detection(self):
        """Self-referential dependency raises CycleError."""
        operations = [{"op": "create_table", "table": "a", "details": {}}]
        deps = {0: [0]}

        with pytest.raises(CycleError):
            topological_sort(operations, deps)


class TestBuildDependencyGraph:
    """Tests for build_dependency_graph function."""

    def test_empty_operations(self):
        """Empty operations produce empty graph."""
        result = build_dependency_graph([])
        assert result == {}

    def test_independent_creates(self):
        """Independent CREATE TABLE operations have no dependencies."""
        operations = [
            {"op": "create_table", "table": "a", "details": {"columns": {}}},
            {"op": "create_table", "table": "b", "details": {"columns": {}}},
        ]

        result = build_dependency_graph(operations)

        assert result[0] == []
        assert result[1] == []

    def test_foreign_key_dependency(self):
        """CREATE TABLE with FK depends on referenced table."""
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

        result = build_dependency_graph(operations)

        # orders (index 1) should depend on users (index 0)
        assert 0 in result[1]

    def test_drop_fk_before_drop_table(self):
        """DROP TABLE depends on DROP FOREIGN KEY that references it."""
        operations = [
            {
                "op": "drop_foreign_key",
                "table": "orders",
                "details": {"column": "user_id", "references": "users.id"},
            },
            {"op": "drop_table", "table": "users", "details": {}},
        ]

        result = build_dependency_graph(operations)

        # drop_table (index 1) should depend on drop_fk (index 0)
        assert 0 in result[1]

    def test_add_column_depends_on_create_table(self):
        """ADD COLUMN depends on CREATE TABLE for same table."""
        operations = [
            {
                "op": "create_table",
                "table": "users",
                "details": {"columns": {}},
            },
            {
                "op": "add_column",
                "table": "users",
                "details": {"column": "email", "definition": {}},
            },
        ]

        result = build_dependency_graph(operations)

        # add_column (index 1) should depend on create_table (index 0)
        assert 0 in result[1]


class TestOperationPriority:
    """Tests for operation priority ordering."""

    def test_drops_before_creates(self):
        """DROP operations have lower priority (execute first)."""
        drop_priority = _operation_priority({"op": "drop_table", "table": "", "details": {}})
        create_priority = _operation_priority({"op": "create_table", "table": "", "details": {}})

        assert drop_priority < create_priority

    def test_drop_fk_first(self):
        """DROP FOREIGN KEY has lowest priority."""
        fk_priority = _operation_priority({"op": "drop_foreign_key", "table": "", "details": {}})
        table_priority = _operation_priority({"op": "drop_table", "table": "", "details": {}})

        assert fk_priority < table_priority

    def test_add_fk_last(self):
        """ADD FOREIGN KEY has highest priority (execute last)."""
        fk_priority = _operation_priority({"op": "add_foreign_key", "table": "", "details": {}})
        create_priority = _operation_priority({"op": "create_table", "table": "", "details": {}})

        assert fk_priority > create_priority
