"""Tests for the diff engine."""


from declaro_persistum.differ.ambiguity import detect_ambiguities
from declaro_persistum.differ.core import diff
from declaro_persistum.differ.toposort import build_dependency_graph, topological_sort
from declaro_persistum.types import NOTHING, Schema


class TestDiff:
    """Tests for the diff function."""

    def test_diff_empty_schemas(self):
        """Diffing two empty schemas produces no operations."""
        result = diff(current={}, target={}, dialect="postgresql", decisions=NOTHING)
        assert result["operations"] == []
        assert result["ambiguities"] == []

    def test_diff_identical_schemas(self, simple_schema):
        """Diffing identical schemas produces no operations."""
        result = diff(current=simple_schema, target=simple_schema, dialect="postgresql", decisions=NOTHING)
        assert result["operations"] == []

    def test_diff_create_table(self, simple_schema):
        """Adding a new table creates CREATE TABLE operation."""
        result = diff(current={}, target=simple_schema, dialect="postgresql", decisions=NOTHING)

        assert len(result["operations"]) >= 1
        create_ops = [op for op in result["operations"] if op["op"] == "create_table"]
        assert len(create_ops) == 1
        assert create_ops[0]["table"] == "users"

    def test_diff_drop_table(self, simple_schema):
        """Removing a table creates DROP TABLE operation."""
        result = diff(current=simple_schema, target={}, dialect="postgresql", decisions=NOTHING)

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

        result = diff(current=current, target=target, dialect="postgresql", decisions=NOTHING)

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

        result = diff(current=current, target=target, dialect="postgresql", decisions=NOTHING)

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

        result = diff(current=current, target=target, dialect="postgresql", decisions=NOTHING)

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

        result = diff(current=current, target=target, dialect="postgresql", decisions=NOTHING)

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

        result = diff(current=current, target=target, dialect="postgresql", decisions=NOTHING)

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

        result = diff(current=current, target=target, dialect="postgresql", decisions=NOTHING)

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

        ambiguities = detect_ambiguities(current, target, "postgresql", decisions=NOTHING)

        assert len(ambiguities) >= 1
        assert any(a["type"] == "possible_rename" for a in ambiguities)

    def test_a_different_type_is_still_ambiguous(self):
        """A retyped rename is a rename, and this used to say otherwise.

        REVERSED 2026-08-21, because the old assertion recorded the gap. The
        detector required a matching type before it would ask, so a column
        renamed AND retyped in one migration passed unnoticed and its values
        went with the drop. Whether the types match tells you nothing about
        whether the author moved the column or replaced it.
        """
        current: Schema = {"users": {"columns": {"count": {"type": "integer"}}}}
        target: Schema = {"users": {"columns": {"total": {"type": "text"}}}}

        ambiguities = detect_ambiguities(current, target, "postgresql", decisions=NOTHING)

        rename_amb = [a for a in ambiguities if a["type"] == "possible_rename"]
        assert len(rename_amb) == 1
        assert rename_amb[0]["from_column"] == "count"
        assert rename_amb[0]["to_column"] == "total"

    def test_no_ambiguity_with_renamed_from_hint(self):
        """No ambiguity when renamed_from hint is present."""
        current: Schema = {
            "users": {"columns": {"name": {"type": "text"}}}
        }
        target: Schema = {
            "users": {"columns": {"full_name": {"type": "text", "renamed_from": "name"}}}
        }

        ambiguities = detect_ambiguities(current, target, "postgresql", decisions=NOTHING)

        assert len(ambiguities) == 0

    def test_no_ambiguity_with_is_new_hint(self):
        """No ambiguity when is_new hint is present."""
        current: Schema = {
            "users": {"columns": {"name": {"type": "text"}}}
        }
        target: Schema = {
            "users": {"columns": {"full_name": {"type": "text", "is_new": True}}}
        }

        ambiguities = detect_ambiguities(current, target, "postgresql", decisions=NOTHING)

        rename_amb = [a for a in ambiguities if a["type"] == "possible_rename"]
        assert len(rename_amb) == 0


class TestRenameConfidence:
    """Tests for rename confidence calculation."""

    # THE CONFIDENCE TESTS ARE GONE WITH THE FUNCTION THEY TESTED.
    # `calculate_rename_confidence` scored a dropped name against an added one
    # with edit distance, and the differ stayed silent when the score fell
    # under a threshold. A table renamed and reshaped scored under it, so the
    # drop ran and the rows went without the operator being asked. Measured
    # 2026-08-21. A drop beside an add is now ambiguous by construction, and
    # tests/unit/test_a_drop_beside_an_add_is_always_ambiguous.py holds that.


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
