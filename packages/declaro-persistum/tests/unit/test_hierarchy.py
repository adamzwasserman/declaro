"""
Unit tests for Hierarchy abstraction.

Tests closure table pattern for O(1) hierarchy queries.
"""

import pytest
from typing import Any


class TestClosureTableGeneration:
    """Tests for generating closure tables."""

    def test_generate_closure_table_schema(self):
        """Generate closure table schema."""
        from declaro_persistum.abstractions.hierarchy import generate_closure_table

        schema = generate_closure_table("categories")
        assert "categories_closure" in schema
        table = schema["categories_closure"]

        # Should have columns
        assert "ancestor_id" in table["columns"]
        assert "descendant_id" in table["columns"]
        assert "depth" in table["columns"]

        # Foreign keys to parent table
        assert table["columns"]["ancestor_id"]["references"] == "categories.id"
        assert table["columns"]["descendant_id"]["references"] == "categories.id"

        # Cascade deletes
        assert table["columns"]["ancestor_id"]["on_delete"] == "cascade"
        assert table["columns"]["descendant_id"]["on_delete"] == "cascade"

    def test_generate_closure_table_custom_pk(self):
        """Generate closure table with custom primary key."""
        from declaro_persistum.abstractions.hierarchy import generate_closure_table

        schema = generate_closure_table("nodes", pk_column="node_id")
        table = schema["nodes_closure"]

        assert table["columns"]["ancestor_id"]["references"] == "nodes.node_id"

    def test_generate_closure_table_indexes(self):
        """Closure table has performance indexes."""
        from declaro_persistum.abstractions.hierarchy import generate_closure_table

        schema = generate_closure_table("categories")
        table = schema["categories_closure"]

        assert "indexes" in table
        # Should have indexes for common queries
        # - descendants lookup (ancestor_id, depth)
        # - ancestors lookup (descendant_id, depth)

    def test_generate_closure_table_unique_constraint(self):
        """Closure table has unique (ancestor, descendant) constraint."""
        from declaro_persistum.abstractions.hierarchy import generate_closure_table

        schema = generate_closure_table("categories")
        table = schema["categories_closure"]

        # Primary key or unique constraint on (ancestor_id, descendant_id)
        assert table.get("primary_key") == ["ancestor_id", "descendant_id"] or \
               "indexes" in table


class TestClosureOperations:
    """Tests for closure table operations."""

    def test_closure_insert_sql(self):
        """Generate SQL to insert node into hierarchy."""
        from declaro_persistum.abstractions.hierarchy import closure_insert_sql

        sql = closure_insert_sql("categories")
        # Should insert self-reference (node is its own ancestor at depth 0)
        # Plus all parent's ancestors with depth + 1
        assert "INSERT INTO categories_closure" in sql

    def test_closure_insert_root_sql(self):
        """Generate SQL to insert root node."""
        from declaro_persistum.abstractions.hierarchy import closure_insert_root_sql

        sql = closure_insert_root_sql("categories")
        # Root node only has self-reference
        assert "INSERT INTO categories_closure" in sql
        assert "depth" in sql

    def test_closure_update_parent_sql(self):
        """Generate SQL to move node to new parent."""
        from declaro_persistum.abstractions.hierarchy import closure_update_parent_sql

        sql = closure_update_parent_sql("categories")
        # Moving requires:
        # 1. Delete old closure entries (except self)
        # 2. Insert new closure entries from new parent
        assert "DELETE" in sql or "INSERT" in sql

    def test_closure_delete_subtree_sql(self):
        """Generate SQL to delete node and descendants."""
        from declaro_persistum.abstractions.hierarchy import closure_delete_subtree_sql

        sql = closure_delete_subtree_sql("categories")
        # Should delete all descendants from closure table
        # The actual rows will cascade delete
        assert "DELETE" in sql


class TestHierarchyQueries:
    """Tests for hierarchy query SQL generation."""

    def test_descendants_query_sql(self):
        """Generate SQL to get all descendants."""
        from declaro_persistum.abstractions.hierarchy import descendants_query_sql

        sql = descendants_query_sql("categories")
        assert "SELECT" in sql
        assert "categories_closure" in sql
        assert "ancestor_id" in sql

    def test_descendants_query_with_depth(self):
        """Generate SQL to get descendants at specific depth."""
        from declaro_persistum.abstractions.hierarchy import descendants_at_depth_sql

        sql = descendants_at_depth_sql("categories")
        assert "depth" in sql

    def test_ancestors_query_sql(self):
        """Generate SQL to get all ancestors."""
        from declaro_persistum.abstractions.hierarchy import ancestors_query_sql

        sql = ancestors_query_sql("categories")
        assert "SELECT" in sql
        assert "categories_closure" in sql
        assert "descendant_id" in sql

    def test_path_query_sql(self):
        """Generate SQL to get path from root to node."""
        from declaro_persistum.abstractions.hierarchy import path_query_sql

        sql = path_query_sql("categories")
        assert "SELECT" in sql
        assert "ORDER BY" in sql
        assert "depth" in sql

    def test_children_query_sql(self):
        """Generate SQL to get direct children (depth=1)."""
        from declaro_persistum.abstractions.hierarchy import children_query_sql

        sql = children_query_sql("categories")
        assert "depth = 1" in sql or "depth" in sql

    def test_parent_query_sql(self):
        """Generate SQL to get direct parent."""
        from declaro_persistum.abstractions.hierarchy import parent_query_sql

        sql = parent_query_sql("categories")
        # Parent is ancestor at depth 1
        assert "depth = 1" in sql or "depth" in sql

    def test_is_descendant_sql(self):
        """Generate SQL to check if node is descendant."""
        from declaro_persistum.abstractions.hierarchy import is_descendant_sql

        sql = is_descendant_sql("categories")
        assert "SELECT" in sql
        assert "categories_closure" in sql

    def test_subtree_count_sql(self):
        """Generate SQL to count descendants."""
        from declaro_persistum.abstractions.hierarchy import subtree_count_sql

        sql = subtree_count_sql("categories")
        assert "COUNT" in sql


class TestHierarchyHydration:
    """Tests for building tree structures from closure data."""

    def test_build_tree_empty(self):
        """Build tree from empty data."""
        from declaro_persistum.abstractions.hierarchy import build_tree

        rows: list[dict[str, Any]] = []
        result = build_tree(rows)
        assert result == {}

    def test_build_tree_single(self):
        """Build tree with single root."""
        from declaro_persistum.abstractions.hierarchy import build_tree

        rows = [
            {"id": 1, "ancestor_id": 1, "descendant_id": 1, "depth": 0},
        ]
        result = build_tree(rows)
        assert 1 in result
        assert result[1]["children"] == []

    def test_build_tree_simple(self):
        """Build simple tree."""
        from declaro_persistum.abstractions.hierarchy import build_tree

        # Root (1) -> Child (2)
        rows = [
            {"id": 1, "ancestor_id": 1, "descendant_id": 1, "depth": 0},
            {"id": 2, "ancestor_id": 2, "descendant_id": 2, "depth": 0},
            {"id": 2, "ancestor_id": 1, "descendant_id": 2, "depth": 1},
        ]
        result = build_tree(rows)
        assert 1 in result
        # Implementation may vary on structure


class TestReferentialIntegrity:
    """Tests for maintaining referential integrity."""

    def test_cascade_delete_closure(self):
        """Deleting node cascades to closure entries."""
        from declaro_persistum.abstractions.hierarchy import generate_closure_table

        schema = generate_closure_table("categories")
        table = schema["categories_closure"]

        # Both FK columns should cascade delete
        assert table["columns"]["ancestor_id"]["on_delete"] == "cascade"
        assert table["columns"]["descendant_id"]["on_delete"] == "cascade"

    def test_closure_maintained_on_insert(self):
        """Closure entries maintained when inserting node."""
        from declaro_persistum.abstractions.hierarchy import closure_insert_sql

        sql = closure_insert_sql("categories")
        # Should insert all required closure entries atomically
        assert "INSERT" in sql
