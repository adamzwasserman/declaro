"""Regression tests for the Postgres unique-constraint / drop_index defects.

Reported against 0.1.8 by a downstream app whose startup died in
apply_migrations_async. Two distinct defects, both reachable from any
Postgres model declaring ``unique: True`` on a column:

1. PostgreSQLApplier._drop_index_sql took (self, details) while the
   dispatch in generate_operation_sql calls every generator as
   generator(table, details) -> TypeError on any drop_index op.

2. The Postgres inspector reported constraint-backed indexes (the index
   PostgreSQL builds to implement a UNIQUE constraint) as ordinary
   indexes. The model side declares that as unique: True on the column and
   never as an index, so the differ saw it in current - target and
   scheduled a drop_index against a constraint the model still declares.

A third case here covers the boolean-default churn: the loader emits
"FALSE" for a Python bool default while PostgreSQL introspects the same
default back as "false", so every migration re-emitted a no-op
alter_column.
"""

import pytest

from declaro_persistum.applier.postgresql import PostgreSQLApplier
from declaro_persistum.differ.core import diff
from declaro_persistum.types import Schema


class TestDropIndexSQLGeneration:
    """Defect 1: the drop_index generator's signature."""

    def test_drop_index_sql_is_callable_through_dispatch(self):
        """generate_operation_sql dispatches drop_index without a TypeError."""
        applier = PostgreSQLApplier()

        sql = applier.generate_operation_sql(
            {
                "op": "drop_index",
                "table": "users",
                "details": {"index": "idx_users_created_at"},
            }
        )

        assert sql == 'DROP INDEX "idx_users_created_at"'

    def test_every_generator_accepts_the_dispatch_arity(self):
        """No generator in the dispatch table may take a different arity.

        This is the class of defect, not just the one instance: the dispatch
        calls all of them as generator(table, details), so any generator
        declared without the table parameter is a latent TypeError that only
        fires when a migration happens to emit that op.
        """
        import inspect

        applier = PostgreSQLApplier()
        # Exercise the dispatch table through a representative op so the
        # dict is built exactly as production builds it.
        generators = {
            name: getattr(applier, f"_{name}_sql")
            for name in (
                "create_table",
                "drop_table",
                "rename_table",
                "add_column",
                "drop_column",
                "rename_column",
                "alter_column",
                "add_index",
                "drop_index",
                "add_constraint",
                "drop_constraint",
                "add_foreign_key",
                "drop_foreign_key",
                "create_view",
                "drop_view",
            )
        }

        wrong_arity = {
            name: str(inspect.signature(fn))
            for name, fn in generators.items()
            if len(inspect.signature(fn).parameters) != 2
        }

        assert wrong_arity == {}, f"generators not matching dispatch arity: {wrong_arity}"


class TestConstraintBackedIndexNotDropped:
    """Defect 2: a UNIQUE constraint's backing index is not a declared index."""

    def _schema(self, indexes: dict) -> Schema:
        return {
            "users": {
                "columns": {
                    "id": {"type": "integer", "nullable": False},
                    "email": {"type": "text", "nullable": False, "unique": True},
                },
                "primary_key": ["id"],
                "indexes": indexes,
            }
        }

    def test_no_drop_index_when_inspector_excludes_constraint_backed(self):
        """The fixed inspector omits users_email_key, so no drop is scheduled.

        This is the post-fix shape of introspection: the backing index is
        not reported, the model declares unique on the column, and the two
        sides agree.
        """
        current = self._schema(indexes={})
        target = self._schema(indexes={})

        result = diff(current=current, target=target)

        assert result["operations"] == []

    def test_the_original_failure_shape_is_what_we_fixed(self):
        """Documents the pre-fix input that produced the destructive op.

        If introspection ever regresses and reports the constraint-backed
        index again, the differ still emits drop_index — the differ's set
        logic is correct given its inputs, so this asserts the *inspector*
        contract is the thing holding the line.
        """
        current = self._schema(indexes={"users_email_key": {"columns": ["email"], "unique": True}})
        target = self._schema(indexes={})

        result = diff(current=current, target=target)

        drops = [op for op in result["operations"] if op["op"] == "drop_index"]
        assert len(drops) == 1
        assert drops[0]["details"]["index"] == "users_email_key"

    def test_standalone_unique_index_still_diffed_normally(self):
        """A CREATE UNIQUE INDEX the model drops must still be dropped.

        The fix must not blanket-ignore unique indexes — only ones owned by
        a constraint. A standalone unique index is genuinely declared.
        """
        current = self._schema(indexes={"users_nick_uidx": {"columns": ["nick"], "unique": True}})
        target = self._schema(indexes={})

        result = diff(current=current, target=target)

        drops = [op for op in result["operations"] if op["op"] == "drop_index"]
        assert len(drops) == 1
        assert drops[0]["details"]["index"] == "users_nick_uidx"


class TestBooleanDefaultNormalization:
    """The loader emits TRUE/FALSE; PostgreSQL introspects true/false."""

    def _schema(self, default: str) -> Schema:
        return {
            "flags": {
                "columns": {
                    "enabled": {"type": "boolean", "nullable": False, "default": default},
                },
                "primary_key": ["id"],
                "indexes": {},
            }
        }

    @pytest.mark.parametrize(
        "current_default,target_default",
        [
            ("false", "FALSE"),
            ("true", "TRUE"),
            ("FALSE", "false"),
            ("True", "true"),
        ],
    )
    def test_boolean_default_case_does_not_churn(self, current_default, target_default):
        """Case-only differences in boolean literals emit no alter_column."""
        result = diff(
            current=self._schema(current_default),
            target=self._schema(target_default),
        )

        assert result["operations"] == []

    def test_real_boolean_default_change_still_detected(self):
        """Normalisation must not swallow an actual default change."""
        result = diff(current=self._schema("false"), target=self._schema("TRUE"))

        alters = [op for op in result["operations"] if op["op"] == "alter_column"]
        assert len(alters) == 1
        assert alters[0]["details"]["changes"]["default"] == {"from": "false", "to": "TRUE"}

    def test_non_boolean_defaults_stay_case_sensitive(self):
        """String literals are opaque SQL — case is significant there."""
        result = diff(current=self._schema("'Pending'"), target=self._schema("'pending'"))

        alters = [op for op in result["operations"] if op["op"] == "alter_column"]
        assert len(alters) == 1


class TestVersionConstant:
    """The __version__ constant drifted from pyproject through 0.1.7/0.1.8."""

    def test_version_matches_installed_metadata(self):
        """__version__ is derived from metadata, so it cannot drift again.

        This matters beyond cosmetics: __version__ is mixed into the schema
        hash specifically so a release invalidates the skip-if-clean cache
        and applier/differ fixes reach existing deployments. A stale
        constant silently disables that propagation.
        """
        from importlib.metadata import version

        import declaro_persistum

        assert declaro_persistum.__version__ == version("declaro-persistum")
