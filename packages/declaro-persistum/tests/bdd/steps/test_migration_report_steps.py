"""Bind `tests/bdd/features/schema/migration_report.feature`.

WHAT THE FEATURE ASKS FOR. Two results about the record a migration leaves
behind. A plan of three operations that all ran gives a report saying three,
against that plan. A database already matching its models gives an empty
report, and an empty report is not a failure.

THE SECOND ONE IS THE ONE WORTH A GATE. "Nothing to do" and "it went wrong"
are the two answers most easily confused, because both look like absence from
a distance. `fn migration_report` has no Fault arm, so the shapes themselves
have to be told apart, and that is asked of `declaro_persistum.types` rather
than of a string: a report carries the fields a `Report` declares and none of
the fields a `Fault` declares.

THE REPORT IS A VALUE, NOT A LINE ON A SCREEN. `boundary_out fn log_write` is
what writes one afterwards, and `orchestrator fn session_connect` is what puts
the report on the handle. This function only builds it.
"""

from __future__ import annotations

import typing

from pytest_bdd import given, parsers, scenarios, then

from declaro_persistum import types as declared_types
from declaro_persistum.migration_report import migration_report

scenarios("../features/schema/migration_report.feature")


def _operation(kind: str, table: str) -> dict:
    """One operation, written out the way `Operation` declares it."""
    return {"kind": kind, "table": table, "detail": {}, "destroys": False}


THREE_OPERATIONS = {
    "operations": [
        _operation("create_table", "people"),
        _operation("add_column", "orders"),
        _operation("create_index", "orders"),
    ],
    "destroys": [],
}

NOTHING_TO_DO = {"operations": [], "destroys": []}

# What each opening of the feature hands the function: the plan, and how many
# of its operations ran. Two `Given` steps differing only in these two values
# would be one decision written twice, so they are one function over the table.
A_MIGRATION = {
    "a plan of three operations, all applied": (THREE_OPERATIONS, 3),
    "a database already matching its models": (NOTHING_TO_DO, 0),
}


@given(
    parsers.re(
        r"(?P<opening>a plan of three operations, all applied"
        r"|a database already matching its models)"
    )
)
def _a_migration_ran(scenario: dict, opening: str) -> None:
    plan, applied = A_MIGRATION[opening]
    scenario["plan"] = plan
    scenario["report"] = migration_report(plan, applied)


@then("the report says three, against that plan")
def _the_report_says_three_against_that_plan(scenario: dict) -> None:
    report = scenario["report"]

    assert report["applied"] == 3, (
        f"three operations ran and the report says {report['applied']!r} ran"
    )
    assert report["plan"] == scenario["plan"], (
        "the report carries a plan that is not the one that ran, so a reader "
        f"cannot tell what the three were: {report['plan']!r}"
    )
    assert len(report["plan"]["operations"]) == report["applied"], (
        f"the report says {report['applied']} of "
        f"{len(report['plan']['operations'])} operations ran, and the migration "
        "was told all of them did"
    )


@then("the report is empty, and that is distinguishable from a failure")
def _an_empty_report_is_not_a_failure(scenario: dict) -> None:
    report = scenario["report"]

    assert report["applied"] == 0, (
        f"there was nothing to do and the report says {report['applied']!r} ran"
    )
    assert report["plan"]["operations"] == [], (
        f"there was nothing to do and the plan carries {report['plan']['operations']!r}"
    )

    # A REPORT AND A FAULT ARE TOLD APART BY THEIR FIELDS, asked of the
    # declaration's own shapes rather than of a name pasted in here.
    a_report = set(typing.get_type_hints(declared_types.Report))
    a_fault = set(typing.get_type_hints(declared_types.Fault))

    assert set(report) == a_report, (
        f"an empty migration came back as {sorted(report)}, and a report is {sorted(a_report)}"
    )
    assert not set(report) & a_fault, (
        f"an empty migration came back carrying {sorted(set(report) & a_fault)}, "
        "which is how this package says something went wrong"
    )
