"""Steps for applier/spelling.feature: one scenario per function in that module.

EVERY STEP IS CALL IT AND CHECK THE RESULT, and that is the point rather than
a convenience. The Honest Code principles use step length as a readout on the
architecture: thirty lines of setup means the code under test has hidden
dependencies, and when a function is pure the step is two lines. Every
function in `applier/spelling.py` is pure -- a declaration in, text out -- so
every step here is two lines, and if that ever stops being true the module has
grown a dependency nobody declared.

THE EXPECTED VALUES ARE WRITTEN BY HAND, on purpose. Generating them from the
code would pin whatever it does today, including its bugs, which this suite
has done once already.
"""

from __future__ import annotations

from typing import Any, TypedDict

import pytest
from pytest_bdd import parsers, scenarios, then, when

from declaro_persistum.applier import spelling

scenarios("../features/applier/spelling.feature")


class Ctx(TypedDict, total=False):
    answer: Any
    refusal: Exception | None


@pytest.fixture
def ctx() -> Ctx:
    return {"answer": None, "refusal": None}


def _call(ctx: Ctx, name: str, *args: Any) -> None:
    """Run one of the module's functions and keep whatever it did."""
    try:
        ctx["answer"] = getattr(spelling, name)(*args)
        ctx["refusal"] = None
    except Exception as refused:  # noqa: BLE001 - a refusal is an outcome here
        ctx["answer"] = None
        ctx["refusal"] = refused


# ------------------------------------------------------------------ naming a type


@when(parsers.parse('sqlite_type is given "{declared}"'))
def _when_sqlite_type(ctx, declared):
    _call(ctx, "sqlite_type", declared)


@when(parsers.parse('postgresql_type is given "{declared}"'))
def _when_postgresql_type(ctx, declared):
    _call(ctx, "postgresql_type", declared)


@when(parsers.parse('map_type is given "{declared}" for "{dialect}"'))
def _when_map_type(ctx, declared, dialect):
    _call(ctx, "map_type", declared, dialect)


@when(parsers.parse('_the_mappable_dialect is given "{dialect}"'))
def _when_mappable(ctx, dialect):
    _call(ctx, "_the_mappable_dialect", dialect)


@when(parsers.parse('_refuse_unmappable_dialect is given "{dialect}"'))
def _when_unmappable(ctx, dialect):
    _call(ctx, "_refuse_unmappable_dialect", dialect)


# --------------------------------------------------------- comparing two types


@when(parsers.parse('same_type compares "{left}" and "{right}" on "{dialect}"'))
def _when_same_type(ctx, left, right, dialect):
    _call(ctx, "same_type", left, right, dialect)


@when(parsers.parse('_the_same_spelling compares "{left}" and "{right}" on "{dialect}"'))
def _when_same_spelling(ctx, left, right, dialect):
    _call(ctx, "_the_same_spelling", left, right, dialect)


@when(parsers.parse('_one_absence_is_not_the_other compares nothing with "{right}"'))
def _when_one_absence(ctx, right):
    _call(ctx, "_one_absence_is_not_the_other", None, right, "sqlite")


# ------------------------------------------------------------ spelling a default


@when(parsers.parse("sqlite_default is given the number {value:d}"))
def _when_sqlite_default_number(ctx, value):
    _call(ctx, "sqlite_default", value)


@when(parsers.parse('postgresql_default is given "{value}"'))
def _when_postgresql_default(ctx, value):
    _call(ctx, "postgresql_default", value)


@when("_null_default is given nothing")
def _when_null_default(ctx):
    _call(ctx, "_null_default", None)


@when(parsers.parse("_literal_default is given the number {value:d}"))
def _when_literal_default(ctx, value):
    _call(ctx, "_literal_default", value)


@when(parsers.parse('_wrapped_default is given "{value}"'))
def _when_wrapped_default(ctx, value):
    _call(ctx, "_wrapped_default", value)


@when(parsers.parse('_bare_default is given "{value}"'))
def _when_bare_default(ctx, value):
    _call(ctx, "_bare_default", value)


@when(parsers.parse('_text_default is given "{value}"'))
def _when_text_default(ctx, value):
    _call(ctx, "_text_default", value)


@when(parsers.parse("_the_spellable_default is given the number {value:d}"))
def _when_spellable_default(ctx, value):
    _call(ctx, "_the_spellable_default", value)


@when("_refuse_unspellable_default is given an object nothing spells")
def _when_unspellable_default(ctx):
    _call(ctx, "_refuse_unspellable_default", object())


@when(parsers.parse('_the_renderable_dialect is given "{dialect}"'))
def _when_renderable(ctx, dialect):
    _call(ctx, "_the_renderable_dialect", dialect)


@when(parsers.parse('_refuse_unrenderable_dialect is given "{dialect}"'))
def _when_unrenderable(ctx, dialect):
    _call(ctx, "_refuse_unrenderable_dialect", dialect)


# --------------------------------------------------- translating between engines


@when(parsers.parse('_translate_default is given "{value}" for "{dialect}" on column "{column}"'))
def _when_translate_default(ctx, value, dialect, column):
    _call(ctx, "_translate_default", value, dialect, column)


@when(
    parsers.parse('_translate_text_default is given "{value}" for "{dialect}" on column "{column}"')
)
def _when_translate_text(ctx, value, dialect, column):
    _call(ctx, "_translate_text_default", value, dialect, column)


@when(
    parsers.parse(
        '_untranslated_default is given the number {value:d} for "{dialect}" on column "{column}"'
    )
)
def _when_untranslated(ctx, value, dialect, column):
    _call(ctx, "_untranslated_default", value, dialect, column)


@when(
    parsers.parse(
        '_the_spellable_expression is given "{value}" for "{dialect}" on column "{column}"'
    )
)
def _when_spellable_expression(ctx, value, dialect, column):
    _call(ctx, "_the_spellable_expression", value, dialect, column, "unused")


@when(
    parsers.parse(
        '_refuse_unspellable_expression is given "{value}" for "{dialect}" on column "{column}"'
    )
)
def _when_unspellable_expression(ctx, value, dialect, column):
    _call(ctx, "_refuse_unspellable_expression", value, dialect, column, "unused")


@when(parsers.parse('_the_dialects_spelling looks up "{value}" in the sqlite spellings'))
def _when_dialects_spelling(ctx, value):
    _call(ctx, "_the_dialects_spelling", {"now()": "CURRENT_TIMESTAMP"}, value, value)


@when(parsers.parse('_as_declared looks up "{value}" in the sqlite spellings'))
def _when_as_declared(ctx, value):
    _call(ctx, "_as_declared", {"now()": "CURRENT_TIMESTAMP"}, value, value)


# ------------------------------------------------------------- a whole column


@when(parsers.parse('column_definition is given a text column "{name}" for "{dialect}"'))
def _when_column_definition(ctx, name, dialect):
    _call(ctx, "column_definition", name, {"type": "text"}, dialect)


@when(parsers.parse('_foreign_key_clause is given a column referencing "{reference}"'))
def _when_foreign_key(ctx, reference):
    _call(ctx, "_foreign_key_clause", {"references": reference}, reference)


# ------------------------------------------------------------- a constraint


@when(parsers.parse('constraint_clause is given a check "{name}" on "{expression}"'))
def _when_constraint_clause(ctx, name, expression):
    _call(ctx, "constraint_clause", name, {"type": "check", "expression": expression})


@when(parsers.parse('_check_clause is given a check "{name}" on "{expression}"'))
def _when_check_clause(ctx, name, expression):
    _call(ctx, "_check_clause", name, {"type": "check", "expression": expression})


@when(parsers.parse('_unique_clause is given a unique "{name}" over "{column}"'))
def _when_unique_clause(ctx, name, column):
    _call(ctx, "_unique_clause", name, {"type": "unique", "columns": [column]})


@when(parsers.parse('_write_constraint is given a check "{name}" on "{expression}"'))
def _when_write_constraint(ctx, name, expression):
    _call(
        ctx,
        "_write_constraint",
        name,
        "check",
        {"type": "check", "expression": expression},
    )


@when(parsers.parse('_refuse_constraint is given a constraint of kind "{kind}"'))
def _when_refuse_constraint(ctx, kind):
    _call(ctx, "_refuse_constraint", "c", kind, {"type": kind})


# ------------------------------------------------------------------- answers


@then(parsers.parse('it answers "{expected}"'))
def _then_answers(ctx, expected):
    assert ctx["refusal"] is None, ctx["refusal"]
    assert ctx["answer"] == expected, ctx["answer"]


@then(parsers.parse("it answers the number {expected:d}"))
def _then_answers_number(ctx, expected):
    assert ctx["refusal"] is None, ctx["refusal"]
    assert ctx["answer"] == expected, ctx["answer"]


@then("it answers that they are the same")
def _then_same(ctx):
    assert ctx["answer"] is True, ctx["answer"]


@then("it answers that they are not the same")
def _then_not_same(ctx):
    assert ctx["answer"] is False, ctx["answer"]


@then("it refuses")
def _then_refuses(ctx):
    assert ctx["refusal"] is not None, f"it answered {ctx['answer']!r} instead"


@then(parsers.parse('it refuses and names "{named}"'))
def _then_refuses_naming(ctx, named):
    assert ctx["refusal"] is not None, f"it answered {ctx['answer']!r} instead"
    assert named in str(ctx["refusal"]), ctx["refusal"]


@then(parsers.parse('it answers "{name}" {sql_type}'))
def _then_answers_column(ctx, name, sql_type):
    assert ctx["refusal"] is None, ctx["refusal"]
    assert f'"{name}"' in ctx["answer"], ctx["answer"]
    assert sql_type in ctx["answer"].upper(), ctx["answer"]


@then(parsers.parse('it answers a REFERENCES clause naming "{table}"'))
def _then_references(ctx, table):
    assert ctx["refusal"] is None, ctx["refusal"]
    assert "REFERENCES" in ctx["answer"].upper(), ctx["answer"]
    assert table in ctx["answer"], ctx["answer"]


@then(parsers.parse('it answers a CHECK clause naming "{named}"'))
def _then_check(ctx, named):
    assert ctx["refusal"] is None, ctx["refusal"]
    assert "CHECK" in ctx["answer"].upper(), ctx["answer"]
    assert named in ctx["answer"], ctx["answer"]


@then(parsers.parse('it answers a UNIQUE clause naming "{named}"'))
def _then_unique(ctx, named):
    assert ctx["refusal"] is None, ctx["refusal"]
    assert "UNIQUE" in ctx["answer"].upper(), ctx["answer"]
    assert named in ctx["answer"], ctx["answer"]
