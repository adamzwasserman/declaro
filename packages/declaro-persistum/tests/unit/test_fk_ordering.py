"""Foreign-key ordering: parents before children, children before parents.

Restored. This file held 43 tests and was hollowed to zero; `git show
4c6b106:<path>` has the original. The 36 tests over pure functions come back
unchanged, because `_build_fk_graph`, `_toposort`, `fk_insert_order`,
`fk_delete_order`, `strip_foreign_keys` and `sort_operations` still take a
schema dict and return data.

THE SEVEN I/O TESTS DID NOT COME BACK, AND THAT IS THE POINT. They drove
`execute_fk_ordered` through a `_RecordingPool` and a `_RecordingConn` — a mock
wearing the word "fake" — and then asserted on the SQL string and the parameter
tuple, throwing the rowcount away. The mock existed only to reach a pure
function through an I/O wrapper (Rule 10, and Rule 4 behind it).

So the SQL half is now `STATEMENTS[op_type](table, data)`, a dispatch table of
pure functions, and those seven assertions are `assert f(input) == expected`
with nothing to record. The `if/elif/else` on op_type that they were reaching
through is gone with them (Rule 1).

What remains genuinely of the I/O half — that operations reach the database in
FK-safe order — runs against a real SQLite file and asserts on the rows that
land. A mock could only confirm the order I already believed.
"""

from __future__ import annotations

import pytest

from declaro_persistum.fk_ordering import (
    STATEMENTS,
    _build_fk_graph,
    _toposort,
    execute_fk_ordered,
    fk_delete_order,
    fk_insert_order,
    sort_operations,
    strip_foreign_keys,
)

pytestmark = pytest.mark.precommit


def _t(**columns):
    """A table definition carrying only what FK ordering reads."""
    return {"columns": columns}


def _col(references=None, **extra):
    col = {"type": "text", **extra}
    if references is not None:
        col["references"] = references
    return col


# users <- posts <- comments, plus an unrelated island.
CHAIN = {
    "users": _t(id=_col()),
    "posts": _t(id=_col(), author=_col(references="users.id")),
    "comments": _t(id=_col(), post=_col(references="posts.id")),
    "settings": _t(id=_col()),
}


class TestBuildFkGraph:
    def test_a_table_with_no_references_depends_on_nothing(self):
        assert _build_fk_graph({"users": _t(id=_col())}) == {"users": set()}

    def test_a_reference_becomes_a_dependency_on_the_referenced_table(self):
        graph = _build_fk_graph(CHAIN)
        assert graph["posts"] == {"users"}
        assert graph["comments"] == {"posts"}
        assert graph["users"] == set()
        assert graph["settings"] == set()

    def test_every_table_appears_even_with_no_columns(self):
        assert _build_fk_graph({"empty": {}}) == {"empty": set()}

    def test_a_reference_without_a_dot_is_ignored(self):
        """The parser splits on '.', so a bare table name carries no column."""
        schema = {"a": _t(id=_col()), "b": _t(x=_col(references="a"))}
        assert _build_fk_graph(schema)["b"] == set()

    def test_a_reference_to_an_unknown_table_is_ignored(self):
        """A dangling FK must not invent a graph node and break the sort."""
        schema = {"b": _t(x=_col(references="nowhere.id"))}
        assert _build_fk_graph(schema) == {"b": set()}

    def test_a_self_reference_is_not_a_dependency(self):
        """A tree table referencing its own parent column is orderable."""
        schema = {"nodes": _t(id=_col(), parent=_col(references="nodes.id"))}
        assert _build_fk_graph(schema) == {"nodes": set()}

    def test_an_empty_reference_string_is_ignored(self):
        schema = {"b": _t(x=_col(references=""))}
        assert _build_fk_graph(schema) == {"b": set()}

    def test_two_columns_referencing_one_parent_collapse_to_one_edge(self):
        schema = {
            "users": _t(id=_col()),
            "messages": _t(
                sender=_col(references="users.id"),
                recipient=_col(references="users.id"),
            ),
        }
        assert _build_fk_graph(schema)["messages"] == {"users"}


class TestToposort:
    def test_an_empty_graph_sorts_to_nothing(self):
        assert _toposort({}) == []

    def test_independent_tables_come_back_in_sorted_order(self):
        assert _toposort({"b": set(), "a": set(), "c": set()}) == ["a", "b", "c"]

    def test_a_parent_precedes_its_child(self):
        assert _toposort({"child": {"parent"}, "parent": set()}) == [
            "parent",
            "child",
        ]

    def test_a_chain_is_ordered_end_to_end(self):
        deps = {"c": {"b"}, "b": {"a"}, "a": set()}
        assert _toposort(deps) == ["a", "b", "c"]

    def test_a_direct_cycle_raises_and_names_the_tables(self):
        with pytest.raises(ValueError, match="Circular FK dependencies"):
            _toposort({"a": {"b"}, "b": {"a"}})

    def test_a_three_table_cycle_raises(self):
        with pytest.raises(ValueError, match="Circular FK dependencies"):
            _toposort({"a": {"b"}, "b": {"c"}, "c": {"a"}})

    def test_a_cycle_beside_a_sortable_island_still_raises(self):
        """A partial answer is not an answer: the whole batch is unsafe."""
        with pytest.raises(ValueError, match="Circular FK dependencies") as e:
            _toposort({"a": {"b"}, "b": {"a"}, "island": set()})
        assert "island" not in str(e.value), "the sortable table is not the fault"

    def test_a_dependency_outside_the_graph_is_counted_but_never_satisfied(self):
        """in_degree counts it, nothing decrements it, so it cannot sort."""
        with pytest.raises(ValueError, match="Circular FK dependencies"):
            _toposort({"orphan": {"absent"}})


class TestInsertAndDeleteOrder:
    def test_parents_come_before_children_on_insert(self):
        order = fk_insert_order(CHAIN)
        assert order.index("users") < order.index("posts")
        assert order.index("posts") < order.index("comments")

    def test_delete_order_is_the_exact_reverse(self):
        assert fk_delete_order(CHAIN) == list(reversed(fk_insert_order(CHAIN)))

    def test_children_come_before_parents_on_delete(self):
        order = fk_delete_order(CHAIN)
        assert order.index("comments") < order.index("posts")
        assert order.index("posts") < order.index("users")

    def test_every_table_appears_exactly_once(self):
        order = fk_insert_order(CHAIN)
        assert sorted(order) == sorted(CHAIN)

    def test_an_empty_schema_orders_to_nothing(self):
        assert fk_insert_order({}) == []
        assert fk_delete_order({}) == []

    def test_a_cycle_in_the_schema_reaches_the_caller(self):
        cyclic = {
            "a": _t(x=_col(references="b.id")),
            "b": _t(y=_col(references="a.id")),
        }
        with pytest.raises(ValueError, match="Circular FK dependencies"):
            fk_insert_order(cyclic)


class TestStripForeignKeys:
    def test_references_on_delete_and_on_update_are_removed(self):
        schema = {
            "posts": _t(
                author=_col(references="users.id", on_delete="CASCADE",
                            on_update="RESTRICT")
            )
        }
        col = strip_foreign_keys(schema)["posts"]["columns"]["author"]
        assert "references" not in col
        assert "on_delete" not in col
        assert "on_update" not in col

    def test_every_other_column_attribute_survives(self):
        schema = {
            "posts": _t(
                author=_col(references="users.id", nullable=False, type="integer")
            )
        }
        col = strip_foreign_keys(schema)["posts"]["columns"]["author"]
        assert col == {"type": "integer", "nullable": False}

    def test_table_level_keys_other_than_columns_survive(self):
        schema = {"posts": {"columns": {}, "indexes": ["ix_a"], "comment": "hi"}}
        out = strip_foreign_keys(schema)["posts"]
        assert out["indexes"] == ["ix_a"]
        assert out["comment"] == "hi"

    def test_the_input_schema_is_not_mutated(self):
        """Immutable data: return a new dict rather than editing the caller's."""
        schema = {"posts": _t(author=_col(references="users.id"))}
        strip_foreign_keys(schema)
        assert schema["posts"]["columns"]["author"]["references"] == "users.id"

    def test_a_table_with_no_columns_key_survives(self):
        assert strip_foreign_keys({"t": {}}) == {"t": {"columns": {}}}

    def test_a_stripped_schema_has_no_dependencies_left(self):
        assert all(not d for d in _build_fk_graph(strip_foreign_keys(CHAIN)).values())


class TestSortOperations:
    def test_inserts_are_ordered_parents_first(self):
        ops = [
            ("comments", "insert", {"id": 1}),
            ("users", "insert", {"id": 2}),
            ("posts", "insert", {"id": 3}),
        ]
        assert [o[0] for o in sort_operations(CHAIN, ops)] == [
            "users",
            "posts",
            "comments",
        ]

    def test_deletes_are_ordered_children_first(self):
        ops = [
            ("users", "delete", {"id": 1}),
            ("comments", "delete", {"id": 2}),
            ("posts", "delete", {"id": 3}),
        ]
        assert [o[0] for o in sort_operations(CHAIN, ops)] == [
            "comments",
            "posts",
            "users",
        ]

    def test_deletes_all_run_before_inserts_in_a_mixed_batch(self):
        ops = [
            ("users", "insert", {"id": 1}),
            ("comments", "delete", {"id": 2}),
        ]
        assert [o[1] for o in sort_operations(CHAIN, ops)] == ["delete", "insert"]

    def test_two_operations_on_one_table_keep_their_original_order(self):
        """The sort is stable on the original index, so a row and the row
        that depends on it within the same table do not get swapped."""
        ops = [
            ("users", "insert", {"id": 1}),
            ("users", "insert", {"id": 2}),
            ("users", "insert", {"id": 3}),
        ]
        assert [o[2]["id"] for o in sort_operations(CHAIN, ops)] == [1, 2, 3]

    def test_a_table_absent_from_the_schema_goes_last(self):
        ops = [
            ("unknown", "insert", {"id": 1}),
            ("users", "insert", {"id": 2}),
        ]
        assert [o[0] for o in sort_operations(CHAIN, ops)] == ["users", "unknown"]

    def test_an_update_is_ordered_with_the_inserts(self):
        """Only 'delete' is special-cased; everything else is parents-first."""
        ops = [
            ("comments", "update", {"id": 1}),
            ("users", "update", {"id": 2}),
        ]
        assert [o[0] for o in sort_operations(CHAIN, ops)] == ["users", "comments"]

    def test_an_empty_batch_sorts_to_an_empty_batch(self):
        assert sort_operations(CHAIN, []) == []

    def test_no_operation_is_lost_or_duplicated(self):
        ops = [
            ("comments", "delete", {"id": 1}),
            ("users", "insert", {"id": 2}),
            ("unknown", "insert", {"id": 3}),
            ("posts", "delete", {"id": 4}),
        ]
        out = sort_operations(CHAIN, ops)
        assert sorted(out, key=lambda o: o[2]["id"]) == sorted(
            ops, key=lambda o: o[2]["id"]
        )


class TestStatements:
    """The SQL half, asserted directly. No connection, no recording."""

    def test_an_insert_names_its_columns_and_binds_its_values(self):
        assert STATEMENTS["insert"]("users", {"id": 1, "name": "ada"}) == (
            'INSERT INTO "users" ("id", "name") VALUES (?, ?)',
            (1, "ada"),
        )

    def test_a_delete_filters_on_every_key_it_was_given(self):
        assert STATEMENTS["delete"]("users", {"id": 7}) == (
            'DELETE FROM "users" WHERE "id" = ?',
            (7,),
        )

    def test_a_delete_ands_several_keys_together(self):
        sql, params = STATEMENTS["delete"]("users", {"id": 7, "tenant": "a"})
        assert sql == 'DELETE FROM "users" WHERE "id" = ? AND "tenant" = ?'
        assert params == (7, "a")

    def test_an_update_splits_the_set_clause_from_the_where_clause(self):
        assert STATEMENTS["update"](
            "users", {"name": "ada", "_where": {"id": 7}}
        ) == (
            'UPDATE "users" SET "name" = ? WHERE "id" = ?',
            ("ada", 7),
        )

    def test_update_parameters_are_set_values_then_where_values(self):
        """Order is load-bearing: the placeholders bind positionally."""
        _sql, params = STATEMENTS["update"](
            "users", {"name": "ada", "age": 30, "_where": {"id": 7}}
        )
        assert params == ("ada", 30, 7), "set values must precede where values"

    def test_the_table_and_column_names_are_quoted(self):
        """A column called `order` must not become a keyword."""
        sql, _params = STATEMENTS["insert"]("order", {"select": 1})
        assert sql == 'INSERT INTO "order" ("select") VALUES (?)'

    def test_the_table_covers_exactly_the_operations_sort_operations_emits(self):
        assert set(STATEMENTS) == {"insert", "delete", "update"}


class TestExecuteFkOrdered:
    """The I/O half, against a real SQLite file."""

    @pytest.mark.asyncio
    async def test_rows_land_in_fk_order_not_call_order(self, tmp_path):
        from declaro_persistum import open_sqlite
        from declaro_persistum.database import close, reading

        db = await open_sqlite(
            str(tmp_path / "fk.db"),
            shutdown="exit_immediately",
            busy_timeout_s=5.0,
        )
        async with reading(db) as conn:
            await conn.execute(
                'CREATE TABLE users (id INTEGER PRIMARY KEY)'
            )
            await conn.execute(
                'CREATE TABLE posts (id INTEGER PRIMARY KEY, '
                'author INTEGER REFERENCES users(id))'
            )
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.commit()

        # Deliberately the wrong way round: the child first. With FK
        # enforcement on, an unsorted batch fails here rather than passing.
        counts = await execute_fk_ordered(
            db,
            {
                "users": _t(id=_col()),
                "posts": _t(id=_col(), author=_col(references="users.id")),
            },
            [
                ("posts", "insert", {"id": 1, "author": 10}),
                ("users", "insert", {"id": 10}),
            ],
        )

        assert counts == [1, 1], "one rowcount per operation"
        async with reading(db) as conn:
            cur = await conn.execute("SELECT id, author FROM posts")
            assert await cur.fetchall() == [(1, 10)]
        await close(db)

    @pytest.mark.asyncio
    async def test_an_unknown_op_type_raises_and_names_it(self, tmp_path):
        from declaro_persistum import open_sqlite

        db = await open_sqlite(
            str(tmp_path / "fk.db"),
            shutdown="exit_immediately",
            busy_timeout_s=5.0,
        )
        with pytest.raises(ValueError, match="Unknown op_type: upsert"):
            await execute_fk_ordered(db, CHAIN, [("users", "upsert", {"id": 1})])

    @pytest.mark.asyncio
    async def test_an_empty_batch_touches_nothing(self, tmp_path):
        from declaro_persistum import open_sqlite

        db = await open_sqlite(
            str(tmp_path / "fk.db"),
            shutdown="exit_immediately",
            busy_timeout_s=5.0,
        )
        assert await execute_fk_ordered(db, CHAIN, []) == []
