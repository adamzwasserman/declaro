"""
Regression tests: executor dispatches write ops on whether SQL has RETURNING.

Bug-class fixed in 0.1.6:
    The executor previously routed every write op on a pool with
    ``acquire_write`` through ``_execute_update`` (cursor rowcount path),
    regardless of whether the SQL had a ``RETURNING`` clause. On Turso /
    MVCC pools this meant prisma ``update_many`` returned an int and
    then called ``len(int)`` -> TypeError, and ``update_one`` / ``create``
    / ``delete`` silently returned ints instead of the documented
    ``dict | None``.

    Fix: the executor now checks ``has_returning_clause(sql)`` for write
    ops on ``acquire_write`` pools. RETURNING -> fetch path (rows). No
    RETURNING -> count path (int).

These tests assert the pure dispatch function directly. The executor's
use of it is by construction — no mocks of the executor needed.
"""

from declaro_persistum.instrumentation import has_returning_clause


class TestHasReturningClauseTrue:
    """SQL with a RETURNING clause is detected."""

    def test_basic_returning_star(self):
        assert has_returning_clause(
            "INSERT INTO t (a) VALUES (1) RETURNING *"
        )

    def test_returning_with_column_list(self):
        assert has_returning_clause(
            "UPDATE t SET a = 1 WHERE b = 2 RETURNING id, a"
        )

    def test_delete_returning(self):
        assert has_returning_clause("DELETE FROM t WHERE id = 1 RETURNING id")

    def test_case_insensitive_lowercase(self):
        assert has_returning_clause("update t set a = 1 returning *")

    def test_case_insensitive_mixed(self):
        assert has_returning_clause("UPDATE t SET a = 1 Returning *")


class TestHasReturningClauseFalse:
    """SQL without a RETURNING clause is correctly rejected."""

    def test_plain_select(self):
        assert not has_returning_clause("SELECT * FROM t")

    def test_plain_update(self):
        assert not has_returning_clause("UPDATE t SET a = 1 WHERE b = 2")

    def test_plain_insert(self):
        assert not has_returning_clause("INSERT INTO t (a) VALUES (1)")

    def test_plain_delete(self):
        assert not has_returning_clause("DELETE FROM t WHERE id = 1")

    def test_empty_string(self):
        assert not has_returning_clause("")


class TestHasReturningClauseFalsePositives:
    """Word-boundary check prevents column names like 'returning_user'
    from matching."""

    def test_column_name_containing_returning(self):
        """A column called ``returning_user`` is not a RETURNING clause."""
        assert not has_returning_clause(
            "UPDATE t SET returning_user = 'alice' WHERE id = 1"
        )

    def test_column_name_returning_suffix(self):
        """A column called ``user_returning`` is not a RETURNING clause."""
        assert not has_returning_clause(
            "INSERT INTO t (user_returning) VALUES ('x')"
        )

    def test_table_name_containing_returning(self):
        """A table named ``returning_log`` is not a RETURNING clause.

        Note: a column or table containing the substring 'returning_' is
        distinct from the keyword. Word boundaries differentiate them.
        """
        assert not has_returning_clause(
            "SELECT * FROM returning_log WHERE id = 1"
        )
