"""Table ordering for bulk transfer: parents loaded before children.

`transfer.py` sat at 13% coverage with 74 branches — part of the Slop Audit
L1.19 gap (declaro-xu0). The two graph functions are pure, so they are
covered here by assertion; the async transfer machinery around them is not.

This is a SECOND topological sort, separate from the one in
`fk_ordering.py`, and it behaves differently on the case that matters: a
cycle here is RETURNED as `circular_refs` rather than raised. Bulk transfer
wants to load what it can and report the rest; DML ordering cannot proceed
at all. The tests pin that difference so the two are not "unified" by
someone who assumes they are duplicates.

This graph also reads a second schema shape — the introspected
`foreign_keys` list — which `fk_ordering` does not.
"""

from declaro_persistum.transfer import _build_table_fk_graph, _toposort_tables


def _t(columns=None, foreign_keys=None):
    d = {"columns": columns or {}}
    if foreign_keys is not None:
        d["foreign_keys"] = foreign_keys
    return d


def _ref(table_col):
    return {"type": "integer", "references": table_col}


CHAIN = {
    "users": _t({"id": {"type": "integer"}}),
    "posts": _t({"author": _ref("users.id")}),
    "comments": _t({"post": _ref("posts.id")}),
}


class TestBuildTableFkGraph:
    def test_a_table_with_no_references_has_no_parents(self):
        assert _build_table_fk_graph(CHAIN, ["users"]) == {"users": set()}

    def test_a_column_reference_becomes_a_parent(self):
        g = _build_table_fk_graph(CHAIN, ["users", "posts"])
        assert g["posts"] == {"users"}

    def test_only_the_requested_tables_appear(self):
        """The graph is scoped to the transfer set, not the whole schema."""
        g = _build_table_fk_graph(CHAIN, ["posts"])
        assert set(g) == {"posts"}

    def test_a_parent_outside_the_transfer_set_is_not_an_edge(self):
        """Otherwise the sort would wait forever for a table nobody loads."""
        assert _build_table_fk_graph(CHAIN, ["posts"])["posts"] == set()

    def test_a_reference_without_a_dot_is_ignored(self):
        schema = {"a": _t({"id": {}}), "b": _t({"x": _ref("a")})}
        assert _build_table_fk_graph(schema, ["a", "b"])["b"] == set()

    def test_a_self_reference_is_not_a_parent(self):
        schema = {"nodes": _t({"parent": _ref("nodes.id")})}
        assert _build_table_fk_graph(schema, ["nodes"])["nodes"] == set()

    def test_a_table_absent_from_the_schema_still_gets_a_node(self):
        assert _build_table_fk_graph({}, ["ghost"]) == {"ghost": set()}

    def test_the_introspected_foreign_keys_list_is_also_read(self):
        """Introspection produces `foreign_keys`, not column `references`."""
        schema = {
            "users": _t({"id": {}}),
            "posts": _t(foreign_keys=[{"references_table": "users"}]),
        }
        assert _build_table_fk_graph(schema, ["users", "posts"])["posts"] == {"users"}

    def test_a_foreign_key_with_no_references_table_is_ignored(self):
        schema = {"posts": _t(foreign_keys=[{}])}
        assert _build_table_fk_graph(schema, ["posts"])["posts"] == set()

    def test_a_self_referencing_foreign_key_entry_is_ignored(self):
        schema = {"nodes": _t(foreign_keys=[{"references_table": "nodes"}])}
        assert _build_table_fk_graph(schema, ["nodes"])["nodes"] == set()

    def test_both_schema_shapes_can_contribute_to_one_table(self):
        schema = {
            "a": _t({"id": {}}),
            "b": _t({"id": {}}),
            "c": _t({"x": _ref("a.id")}, foreign_keys=[{"references_table": "b"}]),
        }
        assert _build_table_fk_graph(schema, ["a", "b", "c"])["c"] == {"a", "b"}

    def test_an_empty_table_list_gives_an_empty_graph(self):
        assert _build_table_fk_graph(CHAIN, []) == {}


class TestToposortTables:
    def test_no_tables_sorts_to_nothing(self):
        assert _toposort_tables({}, []) == ([], [])

    def test_independent_tables_all_sort_with_no_cycles(self):
        schema = {"a": _t(), "b": _t()}
        sorted_tables, circular = _toposort_tables(schema, ["a", "b"])
        assert sorted(sorted_tables) == ["a", "b"]
        assert circular == []

    def test_a_parent_is_loaded_before_its_child(self):
        sorted_tables, _ = _toposort_tables(CHAIN, ["posts", "users"])
        assert sorted_tables.index("users") < sorted_tables.index("posts")

    def test_a_chain_is_ordered_end_to_end(self):
        sorted_tables, circular = _toposort_tables(
            CHAIN, ["comments", "posts", "users"]
        )
        assert sorted_tables == ["users", "posts", "comments"]
        assert circular == []

    def test_a_cycle_is_RETURNED_not_raised(self):
        """The difference from fk_ordering._toposort, which raises.

        Bulk transfer loads what it can and reports the rest; it does not
        abandon the whole run because two tables reference each other.
        """
        schema = {
            "a": _t({"x": _ref("b.id")}),
            "b": _t({"y": _ref("a.id")}),
        }
        sorted_tables, circular = _toposort_tables(schema, ["a", "b"])
        assert sorted_tables == []
        assert sorted(circular) == ["a", "b"]

    def test_tables_outside_a_cycle_are_still_sorted(self):
        schema = {
            "a": _t({"x": _ref("b.id")}),
            "b": _t({"y": _ref("a.id")}),
            "free": _t(),
        }
        sorted_tables, circular = _toposort_tables(schema, ["a", "b", "free"])
        assert sorted_tables == ["free"]
        assert sorted(circular) == ["a", "b"]

    def test_a_self_reference_does_not_become_a_cycle(self):
        schema = {"nodes": _t({"parent": _ref("nodes.id")})}
        assert _toposort_tables(schema, ["nodes"]) == (["nodes"], [])

    def test_every_table_appears_exactly_once_across_both_lists(self):
        schema = {
            "a": _t({"x": _ref("b.id")}),
            "b": _t({"y": _ref("a.id")}),
            "free": _t(),
        }
        sorted_tables, circular = _toposort_tables(schema, ["a", "b", "free"])
        assert sorted(sorted_tables + circular) == ["a", "b", "free"]

    def test_a_diamond_puts_the_root_first_and_the_join_last(self):
        schema = {
            "root": _t({"id": {}}),
            "left": _t({"r": _ref("root.id")}),
            "right": _t({"r": _ref("root.id")}),
            "join": _t({"l": _ref("left.id"), "r": _ref("right.id")}),
        }
        order, circular = _toposort_tables(schema, ["join", "left", "right", "root"])
        assert circular == []
        assert order[0] == "root"
        assert order[-1] == "join"
