"""What the ordering must do, stated before it is rewritten.

`topological_sort` is being rewritten to remove its conditionals, and a
rewrite of an ordering algorithm needs its contract written down first or the
only thing holding it is whichever cases the existing tests happen to name.

THREE PROPERTIES, and the third is the one a careless rewrite loses.

1. Every dependency comes before the operation that depends on it. That is
   what a topological sort is for.
2. Every operation appears exactly once.
3. Among operations that are ready at the same moment, the one with the
   higher priority runs first: drops before creates, views last. The old loop
   re-sorted its whole queue on every pop, so it always took the
   highest-priority READY node, not the first node of the current layer. A
   layered rewrite is not the same order, and the difference shows when a
   drop becomes ready after a create already has.

The generator builds a DAG by construction, giving each node dependencies
only on lower-numbered nodes, so a cycle can never appear by accident and a
failure is always about ordering rather than about the fixture.
"""

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from declaro_persistum.differ.toposort import (
    _operation_priority,
    topological_sort,
)

_OPS = (
    "drop_foreign_key",
    "drop_index",
    "drop_view",
    "drop_column",
    "drop_table",
    "create_table",
    "add_column",
    "alter_column",
    "add_index",
    "add_foreign_key",
    "create_view",
)


@st.composite
def _acyclic(draw):
    """A dependency graph that cannot contain a cycle.

    Node i may depend only on nodes below i, so the graph is a DAG by
    construction and every failure is about the sort.
    """
    size = draw(st.integers(min_value=0, max_value=12))
    operations = [
        {"op": draw(st.sampled_from(_OPS)), "table": f"t{i}", "details": {}} for i in range(size)
    ]
    dependencies = {
        i: draw(st.lists(st.integers(min_value=0, max_value=i - 1), unique=True)) if i else []
        for i in range(size)
    }
    return operations, dependencies


@given(_acyclic())
@settings(max_examples=300, deadline=None)
def test_every_dependency_comes_first(case) -> None:
    operations, dependencies = case

    order = topological_sort(operations, dependencies)
    position = {index: place for place, index in enumerate(order)}

    for index, needs in dependencies.items():
        for dependency in needs:
            assert position[dependency] < position[index], (
                f"operation {index} runs at {position[index]} but depends on "
                f"{dependency} at {position[dependency]}"
            )


@given(_acyclic())
@settings(max_examples=300, deadline=None)
def test_every_operation_runs_exactly_once(case) -> None:
    operations, dependencies = case

    order = topological_sort(operations, dependencies)

    assert sorted(order) == list(range(len(operations)))


@given(_acyclic())
@settings(max_examples=300, deadline=None)
def test_the_highest_priority_ready_operation_goes_first(case) -> None:
    """Replaying the choice at each step, the sort never passed one over.

    This walks the order it produced and asks, at every position, whether some
    other operation was already ready and carried a higher priority. If one
    did, the sort chose wrongly, and a drop has been ordered after a create.
    """
    operations, dependencies = case

    order = topological_sort(operations, dependencies)
    done: set[int] = set()

    for chosen in order:
        ready = [
            index
            for index in range(len(operations))
            if index not in done and index != chosen and set(dependencies.get(index, ())) <= done
        ]
        best = _operation_priority(operations[chosen])
        for other in ready:
            assert _operation_priority(operations[other]) >= best, (
                f"{operations[other]['op']} was ready and outranks "
                f"{operations[chosen]['op']}, which ran instead"
            )
        done.add(chosen)


def test_an_empty_migration_sorts_to_nothing() -> None:
    """The boundary the recursion and the loop both have to survive."""
    assert topological_sort([], {}) == []


def test_a_cycle_is_refused_and_names_what_is_stuck() -> None:
    """Refusing is the point: a half-run migration is worse than none.

    The message must name the tables, because that is what an operator needs
    to break the loop without reading the whole schema back.
    """
    from declaro_persistum.exceptions import CycleError

    operations = [
        {
            "op": "create_table",
            "table": "a",
            "details": {"columns": {}, "constraints": {}, "indexes": {}, "primary_key": []},
        },
        {
            "op": "create_table",
            "table": "b",
            "details": {"columns": {}, "constraints": {}, "indexes": {}, "primary_key": []},
        },
    ]

    with pytest.raises(CycleError) as raised:
        topological_sort(operations, {0: [1], 1: [0]})

    message = str(raised.value)
    assert "a" in message and "b" in message
