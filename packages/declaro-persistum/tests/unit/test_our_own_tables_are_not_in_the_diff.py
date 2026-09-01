"""This package's bookkeeping tables looked like tables the author had dropped.

MEASURED 2026-08-21 on PostgreSQL. A database holding `_dp_transfer_state`,
which `bulk_transfer` creates to record where a transfer got to, produced this
on the next migration:

    Ambiguous changes detected: Table '_dp_transfer_state' removed and
    'orders' added in the same migration.
    Skipping migration - manual intervention required

The migration refused. Nothing was applied. The operator is asked to declare
`renamed_from` between a table they have never heard of and one of their own.

WHY IT WAS INVISIBLE UNTIL TODAY. The differ compares the introspected schema
against the declared one, and an introspected table nobody declared is a
dropped table. That has always been true of every table this package makes for
itself. The rename detector used to score a dropped table against an added one
and stay silent below 50% column overlap, so these scored zero and were never
mentioned. Removing the scoring did not create this; it uncovered it.

`user_tables_sql` already knows the idea, and its docstring says so: a
database holding only `_declaro_meta` is empty, because that table is ours.
The rule was applied to the emptiness check and nowhere else.

WHICH TABLES ARE OURS. `_declaro_meta` holds the schema stamp,
`_declaro_tmp_*` are the temporary tables a SQLite rebuild copies through,
`_dp_materialized_views` is the matview registry and `_dp_transfer_state` is
the transfer log. `_dp_enum_*` is deliberately NOT ours in this sense: enum
lookup tables are put into the target schema by `expand_schema_enums`, so they
appear on both sides and are migrated like anything else.
"""

import pytest

from declaro_persistum.differ.core import diff
from declaro_persistum.types import NOTHING, is_our_table

_ID = {"id": {"type": "integer"}}


@pytest.mark.parametrize(
    "name",
    [
        "_declaro_meta",
        "_declaro_tmp_orders_a1b2c3",
        "_dp_materialized_views",
        "_dp_transfer_state",
    ],
)
def test_our_own_table_is_recognised(name: str) -> None:
    assert is_our_table(name)


@pytest.mark.parametrize(
    "name",
    ["orders", "users", "_dp_enum_orders_status", "_dp_enum_status", "declaro_things"],
)
def test_a_table_that_is_not_ours_is_not_claimed(name: str) -> None:
    """The negative control, and the enum tables are the point of it.

    `expand_schema_enums` puts `_dp_enum_*` into the TARGET schema, so they are
    on both sides of the diff and migrate like any other table. Claiming them
    here would stop them being created at all.
    """
    assert not is_our_table(name)


def test_our_table_is_not_reported_as_dropped() -> None:
    """The defect itself: a bookkeeping table read as a table the author removed."""
    current = {"_dp_transfer_state": {"columns": _ID}}
    target = {"orders": {"columns": _ID}}

    result = diff(current, target, engine="postgresql", decisions=NOTHING)

    assert [op["op"] for op in result["operations"]] == ["create_table"]
    assert result["operations"][0]["table"] == "orders"


def test_our_table_raises_no_ambiguity() -> None:
    """And so the migration is not refused.

    A drop beside an add is ambiguous, which is why the drop must not exist.
    Fixing this by exempting our tables from the ambiguity check instead would
    leave the phantom `drop_table` in the plan.
    """
    current = {"_dp_transfer_state": {"columns": _ID}, "_declaro_meta": {"columns": _ID}}
    target = {"orders": {"columns": _ID}}

    assert diff(current, target, engine="postgresql", decisions=NOTHING)["ambiguities"] == []


def test_a_real_dropped_table_is_still_reported() -> None:
    """The negative control on the whole rule.

    A table the author genuinely removed must still raise the question, or
    this fix has bought silence rather than accuracy.
    """
    current = {"users": {"columns": _ID}}
    target = {"orders": {"columns": _ID}}

    result = diff(current, target, engine="postgresql", decisions=NOTHING)

    assert len(result["ambiguities"]) == 1
