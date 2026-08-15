"""Steps for schema/diff.feature.

THE DIFFER HAD ZERO TESTS. 1,653 lines deciding what DDL runs against somebody
else's database — what is created, what is altered, what is DROPPED, and in
what order — with no unit test and no feature file, while every other subsystem
had something.

NO MOCKS. `diff`, `detect_ambiguities`, `calculate_rename_confidence` and
`topological_sort` are pure functions over dicts. Schemas in, operations out.
Rule 10's easy case, which is exactly why the absence of tests here was a
choice rather than a difficulty.

WHERE THE CODE SURPRISED ME, THE SCENARIO SAYS SO. A specification written from
what the code ought to do, rather than from what it does, produces a suite that
agrees with the bug. Anything asserted here was run first and read second.
"""

from __future__ import annotations

from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.differ import (
    calculate_rename_confidence,
    detect_ambiguities,
    diff,
    diff_views,
    topological_sort,
)
from declaro_persistum.differ.toposort import build_dependency_graph

scenarios("../features/schema/diff.feature")


class Ctx(TypedDict, total=False):
    current: dict[str, Any]
    target: dict[str, Any]
    decisions: dict[str, Any]
    result: Any
    ambiguities: list[Any]
    names: tuple[str, str]
    confidence: float
    operations: list[dict[str, Any]]
    order: list[int]
    error: Exception | None


@pytest.fixture
def ctx() -> Ctx:
    return {"current": {}, "target": {}, "decisions": {}}


def _table(*columns: str) -> dict[str, Any]:
    return {"columns": {c: {"type": "text"} for c in columns}}


def _ops(ctx: Ctx) -> list[dict[str, Any]]:
    return ctx["result"]["operations"]


def _op_names(ctx: Ctx) -> list[str]:
    return [o["op"] for o in _ops(ctx)]


# ------------------------------------------------------------------ set theory


@given("a current schema with no tables")
def _given_no_current(ctx):
    ctx["current"] = {}


@given("a target schema with no tables")
def _given_no_target(ctx):
    ctx["target"] = {}


@given(parsers.parse('a current schema with a table "{name}"'))
def _given_current_table(ctx, name):
    ctx["current"] = {**ctx["current"], name: _table("id")}


@given(parsers.parse('a target schema with a table "{name}"'))
def _given_target_table(ctx, name):
    ctx["target"] = {**ctx["target"], name: _table("id")}


@given(parsers.parse('a current schema with a table "{name}" having columns {cols}'))
def _given_current_table_cols(ctx, name, cols):
    ctx["current"] = {
        **ctx["current"],
        name: _table(*[c.strip() for c in cols.split(",")]),
    }


@given(parsers.parse('a target schema with a table "{name}" having columns {cols}'))
def _given_target_table_cols(ctx, name, cols):
    ctx["target"] = {
        **ctx["target"],
        name: _table(*[c.strip() for c in cols.split(",")]),
    }


@given(parsers.parse('a target schema with a table "{new}" declared as renamed_from "{old}"'))
def _given_renamed_from(ctx, new, old):
    ctx["target"] = {**ctx["target"], new: {**_table("id"), "renamed_from": old}}


@given(parsers.parse('a target schema with a table "{new}" and no rename hint'))
def _given_no_hint(ctx, new):
    ctx["target"] = {**ctx["target"], new: _table("id")}


@given("any current schema and any target schema")
def _given_any(ctx):
    ctx["current"] = {"users": _table("id", "name"), "orders": _table("id")}
    ctx["target"] = {"users": _table("id", "email"), "audit": _table("id")}


@when("the schemas are diffed")
def _when_diff(ctx):
    ctx["result"] = diff(ctx["current"], ctx["target"], decisions=ctx["decisions"], dialect="postgresql")


@when("the schemas are diffed twice")
def _when_diff_twice(ctx):
    ctx["result"] = diff(ctx["current"], ctx["target"], dialect="postgresql")
    ctx["result_again"] = diff(ctx["current"], ctx["target"], dialect="postgresql")


@then(parsers.parse('there is a create_table operation for "{name}"'))
def _then_create_table(ctx, name):
    assert any(
        o["op"] == "create_table" and o["table"] == name for o in _ops(ctx)
    ), f"no create_table for {name}: {_op_names(ctx)}"


@then(parsers.parse('there is a drop_table operation for "{name}"'))
def _then_drop_table(ctx, name):
    assert any(
        o["op"] == "drop_table" and o["table"] == name for o in _ops(ctx)
    ), f"no drop_table for {name}: {_op_names(ctx)}"


@then(parsers.parse('there is no create_table operation for "{name}"'))
def _then_no_create(ctx, name):
    assert not any(
        o["op"] == "create_table" and o["table"] == name for o in _ops(ctx)
    ), f"{name} exists in both schemas and was recreated: {_op_names(ctx)}"


@then(parsers.parse('there is no drop_table operation for "{name}"'))
def _then_no_drop(ctx, name):
    assert not any(
        o["op"] == "drop_table" and o["table"] == name for o in _ops(ctx)
    ), (
        f"{name} exists in both schemas and was DROPPED — every row in it "
        f"would be gone: {_op_names(ctx)}"
    )


@then(parsers.parse('there is an add_column operation for "{col}" on "{table}"'))
def _then_add_column(ctx, col, table):
    assert any(
        o["op"] == "add_column"
        and o["table"] == table
        and o.get("details", {}).get("column") == col
        for o in _ops(ctx)
    ), f"no add_column for {table}.{col}: {_ops(ctx)}"


@then(
    "both results are identical, because the differ performs no I/O, holds no state, and reads no clock — which is what makes a migration reviewable before it runs"
)
def _then_pure(ctx):
    assert ctx["result"] == ctx["result_again"], (
        "two diffs of the same inputs disagreed; a migration you cannot "
        "reproduce is one you cannot review"
    )


# --------------------------------------------------------------------- renames


@then(parsers.parse('there is a rename_table operation from "{old}" to "{new}"'))
def _then_rename(ctx, old, new):
    assert any(
        o["op"] == "rename_table"
        and o["table"] == old
        and o["details"].get("new_name") == new
        for o in _ops(ctx)
    ), f"no rename_table {old} -> {new}: {_ops(ctx)}"


@then(
    "there is NO drop_table operation, because dropping and recreating would discard every row"
)
def _then_no_drop_at_all(ctx):
    drops = [o for o in _ops(ctx) if o["op"] == "drop_table"]
    assert not drops, f"a declared rename still emitted a drop: {drops}"


@then(
    "an ambiguity is also reported, because a drop-and-create that was meant to be a rename is silent data loss and the differ cannot tell the two apart on its own"
)
def _then_rename_ambiguity(ctx):
    ambiguities = ctx["result"]["ambiguities"]
    assert ambiguities, (
        "a table was dropped and another created with no ambiguity raised — "
        "if that pair was a rename, every row is lost with no prompt"
    )


# ------------------------------------------------------------------ ambiguity


@given(parsers.parse('a current schema with a table "{table}" having a column "{col}"'))
def _given_current_col(ctx, table, col):
    ctx["current"] = {**ctx["current"], table: _table(col)}


@given(parsers.parse('a target schema with a table "{table}" having a column "{col}" instead'))
def _given_target_col(ctx, table, col):
    ctx["target"] = {**ctx["target"], table: _table(col)}


@given("a decision has already been recorded for that change")
def _given_decision(ctx):
    table = next(iter(ctx["current"]))
    from_col = next(iter(ctx["current"][table]["columns"]))
    to_col = next(iter(ctx["target"][table]["columns"]))
    # The key format is the code's: "{table}_{column}" for columns and
    # "table_{name}" for tables. Guessing a dotted form made this look like a
    # differ bug when it was mine.
    ctx["decisions"] = {
        f"{table}_{from_col}": {"action": "rename", "from": from_col, "to": to_col},
    }


@when("ambiguities are detected")
def _when_detect(ctx):
    ctx["ambiguities"] = detect_ambiguities(
        ctx["current"], ctx["target"], "postgresql", ctx["decisions"] or None
    )


@then(parsers.parse('a "{kind}" ambiguity is reported from "{frm}" to "{to}"'))
def _then_ambiguity(ctx, kind, frm, to):
    match = [
        a
        for a in ctx["ambiguities"]
        if a["type"] == kind
        and a.get("from_column") == frm
        and a.get("to_column") == to
    ]
    assert match, (
        f"no {kind} from {frm} to {to}; without it a rename is applied as a "
        f"drop plus an add, which loses the column's data: {ctx['ambiguities']}"
    )
    ctx["matched"] = match[0]


@then(
    "it carries a confidence score, so a reviewer can tell a near-certain rename from a guess"
)
def _then_has_confidence(ctx):
    score = ctx["matched"].get("confidence")
    assert isinstance(score, (int, float)), (
        f"the ambiguity has no confidence score: {ctx['matched']}"
    )
    assert 0.0 <= score <= 1.0, f"confidence out of range: {score}"


@then(
    "no ambiguity is reported for it, because re-asking a question the operator has already answered is how a review gets skipped"
)
def _then_no_ambiguity(ctx):
    renames = [a for a in ctx["ambiguities"] if a["type"] == "possible_rename"]
    assert not renames, (
        f"a decided rename was raised again: {renames}"
    )


@given(parsers.parse('the names "{a}" and "{b}"'))
def _given_names(ctx, a, b):
    ctx["names"] = (a, b)


@when("rename confidence is calculated")
def _when_confidence(ctx):
    ctx["confidence"] = calculate_rename_confidence(*ctx["names"])


@then("it is 1.0, because only the case differs")
def _then_confidence_one(ctx):
    assert ctx["confidence"] == 1.0, (
        f"a pure case change scored {ctx['confidence']}, not 1.0"
    )


@then("it is the shorter length over the longer, which is 4 divided by 9")
def _then_confidence_ratio(ctx):
    assert ctx["confidence"] == pytest.approx(4 / 9), (
        f'"name" inside "full_name" scored {ctx["confidence"]}, not 4/9'
    )


@then("it is below 0.5, so a reviewer is not nudged toward a rename that is not one")
def _then_confidence_low(ctx):
    assert ctx["confidence"] < 0.5, (
        f"unrelated names scored {ctx['confidence']}, which would suggest a "
        f"rename that would silently move data between unrelated columns"
    )


# ------------------------------------------------------------------- ordering


def _op(kind: str, table: str = "t") -> dict[str, Any]:
    """A minimal operation of the given kind.

    View operations carry details["name"]; toposort reads it to track which
    views were dropped. Omitting it raised KeyError and looked like a toposort
    defect until I checked what diff_views actually emits.
    """
    details: dict[str, Any] = {}
    if "view" in kind:
        details["name"] = f"{table}_view"
    if "column" in kind:
        details["column"] = "c"
    return {"op": kind, "table": table, "details": details}


@given("operations that drop a foreign key, drop a table, and create a table")
def _given_drop_and_create(ctx):
    ctx["operations"] = [
        _op("create_table", "new"),
        _op("drop_table", "old"),
        _op("drop_foreign_key", "old"),
    ]


@given("one operation of every drop kind")
def _given_every_drop(ctx):
    ctx["operations"] = [
        _op("drop_table"),
        _op("drop_column"),
        _op("drop_view"),
        _op("drop_constraint"),
        _op("drop_index"),
        _op("drop_foreign_key"),
    ]


@given("operations that create a table, add an index, and create a view")
def _given_create_view_last(ctx):
    ctx["operations"] = [
        _op("create_view", "v"),
        _op("add_index", "t"),
        _op("create_table", "t"),
    ]


@given("operations that create a table and add a foreign key")
def _given_create_then_fk(ctx):
    ctx["operations"] = [_op("add_foreign_key", "t"), _op("create_table", "t")]


@given("operations whose dependencies form a cycle")
def _given_cycle(ctx):
    ctx["operations"] = [_op("create_table", "a"), _op("create_table", "b")]
    ctx["cyclic_dependencies"] = {0: [1], 1: [0]}


@when("the operations are ordered")
def _when_order(ctx):
    deps = ctx.get("cyclic_dependencies") or build_dependency_graph(
        ctx["operations"]
    )
    try:
        ctx["order"] = topological_sort(ctx["operations"], deps)
        ctx["error"] = None
    except Exception as e:  # noqa: BLE001 - refusing IS the behaviour
        ctx["error"] = e
        ctx["order"] = []


def _positions(ctx) -> dict[str, int]:
    return {
        ctx["operations"][idx]["op"]: place
        for place, idx in enumerate(ctx["order"])
    }


@then(
    "every drop is ordered before every create, because a create that collides with a name not yet dropped fails, and the failure lands halfway through a migration"
)
def _then_drops_first(ctx):
    pos = _positions(ctx)
    drops = [p for name, p in pos.items() if name.startswith("drop_")]
    creates = [p for name, p in pos.items() if name.startswith(("create_", "add_"))]
    assert max(drops) < min(creates), (
        f"a create was ordered before a drop: {[ctx['operations'][i]['op'] for i in ctx['order']]}"
    )


@then(
    "they run foreign keys, then indexes, then constraints, then views, then columns, then tables — each one removing a thing that depends on the next"
)
def _then_drop_order(ctx):
    actual = [ctx["operations"][i]["op"] for i in ctx["order"]]
    expected = [
        "drop_foreign_key",
        "drop_index",
        "drop_constraint",
        "drop_view",
        "drop_column",
        "drop_table",
    ]
    assert actual == expected, f"drop order is {actual}, expected {expected}"


@then(
    "create_view is last, because a view that references a table or index that does not exist yet fails at creation"
)
def _then_view_last(ctx):
    actual = [ctx["operations"][i]["op"] for i in ctx["order"]]
    assert actual[-1] == "create_view", f"create_view is not last: {actual}"


@then("create_table comes before add_foreign_key")
def _then_table_before_fk(ctx):
    pos = _positions(ctx)
    assert pos["create_table"] < pos["add_foreign_key"], (
        "a foreign key was added before the table it belongs to"
    )


@then(
    "a CycleError is raised naming the cycle, because there is no order that works and running some prefix of it leaves the database in a state neither schema describes"
)
def _then_cycle_error(ctx):
    assert ctx["error"] is not None, (
        "a cyclic dependency graph produced an order; some prefix of it would "
        "run and leave the database in a state neither schema describes"
    )
    assert "cycle" in type(ctx["error"]).__name__.lower() or "cycle" in str(
        ctx["error"]
    ).lower(), f"the error does not name the problem: {ctx['error']!r}"


# ------------------------------------------------------------ extended objects


@given(parsers.parse('an enum "{name}" with values {values}'))
def _given_enum(ctx, name, values):
    ctx["old_enums"] = {
        name: {"name": name, "values": [v.strip() for v in values.split(",")]}
    }


@given(parsers.parse('a view "{name}" with one query'))
def _given_view(ctx, name):
    ctx["old_views"] = {name: {"name": name, "query": "SELECT 1"}}


@given(parsers.parse('a target view "{name}" with a different query'))
def _given_target_view(ctx, name):
    ctx["new_views"] = {name: {"name": name, "query": "SELECT 2"}}


@when("the views are diffed")
def _when_diff_views(ctx):
    ctx["operations"] = diff_views(ctx["old_views"], ctx["new_views"])


@then("an operation is produced for the changed view")
def _then_view_op(ctx):
    assert ctx["operations"], (
        "a view's query changed and no operation was produced, so the "
        "database keeps serving the old definition"
    )


@given("a view that is identical in both schemas")
def _given_all_identical(ctx):
    ctx["identical"] = {"views": {"v": {"name": "v", "query": "SELECT 1"}}}


@when("it is diffed")
def _when_diff_identical(ctx):
    same = ctx["identical"]
    ctx["all_operations"] = diff_views(same["views"], same["views"])


@then(
    "no operations are produced, because a migration that rewrites things nobody changed is indistinguishable from one that had a reason to"
)
def _then_no_ops(ctx):
    assert ctx["all_operations"] == [], (
        f"unchanged objects produced operations: {ctx['all_operations']}"
    )
