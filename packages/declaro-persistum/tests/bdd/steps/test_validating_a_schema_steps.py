"""Steps for cli/validating_a_schema.feature: real TOML files on a real disk.

WHAT THIS REPLACES. Ten tests swapped out the schema reader for a fake that
handed back a dictionary, so no file was ever opened and `load_schema` never
ran. A schema is TOML on disk, and every case those tests wanted is something
a real file can say: I checked that a bad reference gives an error and an
unrecognised type gives a warning before writing any of this.

`tmp_path` is a real directory pytest makes and removes. Writing files into it
is not a stand-in for anything.
"""

from __future__ import annotations

import pathlib
from typing import TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum.cli.commands import cmd_validate
from declaro_persistum.cli.parts import (
    _print_no_warnings,
    _print_the_warnings,
    _report_the_errors,
    _schema_passes,
    _warnings_are_fatal,
)

scenarios("../features/cli/validating_a_schema.feature")

_ONE_COLUMN = """
[users]
[users.columns.id]
type = "uuid"
primary_key = true
"""

_BAD_REFERENCE = """
[orders]
[orders.columns.id]
type = "uuid"
primary_key = true
[orders.columns.customer]
type = "uuid"
references = "nowhere.id"
"""

_ODD_TYPE = """
[gadgets]
[gadgets.columns.id]
type = "uuid"
primary_key = true
[gadgets.columns.shape]
type = "wat"
"""


class Ctx(TypedDict, total=False):
    folder: str
    code: int
    printed: str


@pytest.fixture
def ctx() -> Ctx:
    return {"folder": "", "code": -1, "printed": ""}


def _a_folder_holding(tmp_path: pathlib.Path, **files: str) -> str:
    for name, body in files.items():
        (tmp_path / f"{name}.toml").write_text(body)
    return str(tmp_path)


@given("a folder holding a table with one column")
def _given_one_column(ctx, tmp_path):
    ctx["folder"] = _a_folder_holding(tmp_path, users=_ONE_COLUMN)


@given("an empty folder")
def _given_empty(ctx, tmp_path):
    ctx["folder"] = str(tmp_path)


@given("a folder that does not exist")
def _given_absent(ctx, tmp_path):
    ctx["folder"] = str(tmp_path / "no-such-folder")


@given("a folder holding a column that references a table nobody declared")
def _given_bad_ref(ctx, tmp_path):
    ctx["folder"] = _a_folder_holding(tmp_path, orders=_BAD_REFERENCE)


@given("a folder holding a column whose type nobody recognises")
def _given_odd_type(ctx, tmp_path):
    ctx["folder"] = _a_folder_holding(tmp_path, gadgets=_ODD_TYPE)


@given("a folder holding both a bad reference and an unrecognised type")
def _given_both(ctx, tmp_path):
    ctx["folder"] = _a_folder_holding(tmp_path, orders=_BAD_REFERENCE, gadgets=_ODD_TYPE)


def _validate(ctx, capsys, *, strict: bool, verbose: bool) -> None:
    ctx["code"] = cmd_validate(schema_dir=ctx["folder"], strict=strict, verbose=verbose)
    out = capsys.readouterr()
    ctx["printed"] = out.out + out.err


@when("validate runs on it")
def _when_validate(ctx, capsys):
    _validate(ctx, capsys, strict=False, verbose=False)


@when("validate runs on it in strict mode")
def _when_validate_strict(ctx, capsys):
    _validate(ctx, capsys, strict=True, verbose=False)


@when("validate runs on it verbosely")
def _when_validate_verbose(ctx, capsys):
    _validate(ctx, capsys, strict=False, verbose=True)


@when("_report_the_errors is given two errors")
def _when_report_errors(ctx, capsys):
    ctx["code"] = _report_the_errors({}, ["the first thing", "the second thing"], [], False, False)
    ctx["printed"] = capsys.readouterr().out


@when("_print_the_warnings is given one warning")
def _when_print_warnings(ctx, capsys):
    ctx["code"] = _print_the_warnings(["a type nobody knows"])
    ctx["printed"] = capsys.readouterr().out


@when("_print_no_warnings is given no warnings")
def _when_print_no_warnings(ctx, capsys):
    ctx["code"] = _print_no_warnings([])
    ctx["printed"] = capsys.readouterr().out


@when("_warnings_are_fatal answers a strict run")
def _when_fatal(ctx, capsys):
    ctx["code"] = _warnings_are_fatal({}, False)
    ctx["printed"] = capsys.readouterr().out


@when("_schema_passes answers a run that was not strict")
def _when_passes(ctx, capsys):
    ctx["code"] = _schema_passes({}, False)
    ctx["printed"] = capsys.readouterr().out


@then(parsers.parse("the exit code is {expected:d}"))
def _then_code(ctx, expected):
    assert ctx["code"] == expected, f"{ctx['code']}, printed: {ctx['printed']}"


@then("it says the schema is valid")
def _then_valid(ctx):
    assert "valid" in ctx["printed"].lower(), ctx["printed"]


@then("it says the schema would not load")
def _then_would_not_load(ctx):
    assert "Failed to load schema" in ctx["printed"], ctx["printed"]


@then("it names the table that is missing")
def _then_names_missing(ctx):
    assert "nowhere" in ctx["printed"], ctx["printed"]


@then("it warns about the type")
def _then_warns_type(ctx):
    assert "wat" in ctx["printed"], ctx["printed"]


@then("it lists the table and its column count")
def _then_lists_tables(ctx):
    assert "users" in ctx["printed"], ctx["printed"]
    assert "1" in ctx["printed"], ctx["printed"]


@then("both errors were printed")
def _then_both_errors(ctx):
    assert "the first thing" in ctx["printed"], ctx["printed"]
    assert "the second thing" in ctx["printed"], ctx["printed"]


@then("the warning was printed")
def _then_warning_printed(ctx):
    assert "a type nobody knows" in ctx["printed"], ctx["printed"]


@then("nothing was printed")
def _then_nothing_printed(ctx):
    assert ctx["printed"] == "", ctx["printed"]
