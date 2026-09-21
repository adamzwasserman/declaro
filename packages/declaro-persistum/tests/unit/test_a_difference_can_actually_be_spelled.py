"""What `difference` produces, `spell_operation` must be able to spell.

THE FAILURE THIS EXISTS TO STOP, and it had already happened twice.

`difference` compares the live database to your models and writes down what to
change. `spell_operation` reads that and writes the SQL. Both were tested, both
were green, and nothing ran one into the other: every scenario in each feature
built its own operations by hand, so each function was only ever tested against
input its own author wrote.

MEASURED 2026-09-21. An agent blanked every field `difference` fills on a
`create_table` and ran the whole suite. It came back at 413 passed and 11 failed,
unchanged. Not one test anywhere noticed that the comparison had stopped saying
what a new table contains.

WHAT THAT COST, BEFORE ANYBODY LOOKED. `spell_operation` had invented two field
names, `definition` and `keeping`, that `difference` never wrote. It also read
`nullable` and `default` where `difference` sent `declared_type`, `from_type` and
`to_type`. Four disagreements between two functions that are supposed to compose,
none of which any test could see, because the two halves never met.

THIS IS THE SHAPE THIS PACKAGE HAS SHIPPED BEFORE. A command-line flag asked the
operator a question and then crashed writing the answer down: both halves tested,
the seam never run. A test that exercises each side against its own idea of the
contract proves only that each side is consistent with itself.

SO THIS TEST RUNS THE REAL ONE INTO THE REAL OTHER, with no operation written by
hand anywhere in it.
"""

from __future__ import annotations

import pytest

from declaro_persistum.difference import difference
from declaro_persistum.spell_operation import spell_operation
from declaro_persistum.types import (
    Column,
    Constraint,
    Engine,
    Index,
    Operation,
    Schema,
    Table,
)

pytestmark = pytest.mark.precommit

ENGINES: tuple[Engine, ...] = ("sqlite", "postgresql", "turso")

# A Fault carries these three and nothing this package builds carries them too,
# so the shape is how a refusal is told from an answer.
_FAULT_FIELDS = frozenset({"kind", "what_went_wrong", "what_to_do_instead"})


def _column(name: str, declared_type: str, references: str | None) -> Column:
    return {
        "name": name,
        "declared_type": declared_type,
        "renamed_from": None,
        "references": references,
        "facets": {},
    }


def _table(name: str, columns: list[Column], indexes: list[Index]) -> Table:
    constraints: list[Constraint] = []
    return {"name": name, "columns": columns, "indexes": indexes, "constraints": constraints}


def _nothing() -> Schema:
    """An empty database, which is where every deployment starts once."""
    return {"tables": [], "views": []}


def _people() -> Schema:
    """One table, two columns, one index. Deliberately not called `users`.

    This package once reached full branch coverage while emitting SQL no engine
    could parse, because every test named its table `users` and the name never
    varied enough to catch a quoting bug.
    """
    return {
        "tables": [
            _table(
                "people",
                [_column("id", "integer", None), _column("family_name", "string", None)],
                [{"name": "people_by_family_name", "columns": ["family_name"], "unique": False}],
            )
        ],
        "views": [],
    }


def _people_and_orders() -> Schema:
    """A second table pointing at the first, so a foreign key travels too."""
    people = _people()
    return {
        "tables": [
            *people["tables"],
            _table(
                "orders",
                [_column("id", "integer", None), _column("placed_by", "integer", "people.id")],
                [],
            ),
        ],
        "views": [],
    }


def _people_with_an_email() -> Schema:
    grown = _people()
    grown["tables"][0]["columns"].append(_column("email", "string", None))
    return grown


def _people_with_a_wider_id() -> Schema:
    changed = _people()
    changed["tables"][0]["columns"][0]["declared_type"] = "string"
    return changed


# Each row is a real migration: a live schema, a target, and what it is called
# when it fails. No operation is written by hand in any of them.
MIGRATIONS: dict[str, tuple[Schema, Schema]] = {
    "the first deployment, into an empty database": (_nothing(), _people()),
    "a second table pointing at the first": (_people(), _people_and_orders()),
    "a column added to a model": (_people(), _people_with_an_email()),
    "a column whose declared type changed": (_people(), _people_with_a_wider_id()),
}


def _is_a_fault(answer: object) -> bool:
    return isinstance(answer, dict) and frozenset(answer) == _FAULT_FIELDS


def _spelling(operations: list[Operation], engine: Engine) -> list[tuple[Operation, object]]:
    return [(operation, spell_operation(operation, engine)) for operation in operations]


@pytest.mark.parametrize("migration", sorted(MIGRATIONS))
@pytest.mark.parametrize("engine", ENGINES)
def test_every_operation_a_migration_produces_can_be_spelled(migration: str, engine: Engine):
    """The one thing neither function's own tests could ever check."""
    live, target = MIGRATIONS[migration]
    operations = difference(live, target)

    assert operations, (
        f"{migration}: the comparison produced no operations at all, so this "
        "test is proving nothing about either function"
    )

    refused = [
        (operation["kind"], answer)
        for operation, answer in _spelling(operations, engine)
        if _is_a_fault(answer)
    ]

    assert not refused, (
        f"on {engine}, {migration} produced operations the renderer refuses:\n"
        + "\n".join(
            f"    {kind}: {answer['what_went_wrong']}\n        {answer['what_to_do_instead']}"
            for kind, answer in refused
        )
        + "\n\nThese two functions compose in production and nothing else tests "
        "that they do. Either the comparison is not filling a field the renderer "
        "reads, or the renderer is reading one the comparison never writes."
    )


@pytest.mark.parametrize("migration", sorted(MIGRATIONS))
@pytest.mark.parametrize("engine", ENGINES)
def test_a_spelled_migration_carries_the_names_it_was_given(migration: str, engine: Engine):
    """A statement that forgot the table is a statement about nothing.

    This is the assertion that would have caught a blanked `columns` field: a
    CREATE TABLE built from an empty column list still looks like a statement.
    """
    live, target = MIGRATIONS[migration]
    wanted = {table["name"] for table in target["tables"]}

    spelled = [
        statement["text"]
        for _operation, answer in _spelling(difference(live, target), engine)
        if not _is_a_fault(answer)
        for statement in answer
    ]
    together = " ".join(spelled)

    assert spelled, f"on {engine}, {migration} spelled no statements at all"
    missing = sorted(name for name in wanted if name not in together)
    assert not missing, (
        f"on {engine}, {migration} spelled {len(spelled)} statements and none of "
        f"them names {missing}:\n" + "\n".join(f"    {text}" for text in spelled)
    )


@pytest.mark.parametrize("engine", ENGINES)
def test_a_created_table_carries_the_columns_the_models_declared(engine: Engine):
    """The exact defect this file was written for.

    `difference` said "create `people`" and had nowhere to put its columns, so
    the renderer was handed a name and could not write the statement. Blanking
    those fields changed no test in the suite.
    """
    operations = difference(_nothing(), _people())
    created = [operation for operation in operations if operation["kind"] == "create_table"]

    assert created, "the first deployment produced no create_table at all"

    for operation in created:
        answer = spell_operation(operation, engine)
        assert not _is_a_fault(answer), f"on {engine}: {answer}"
        text = " ".join(statement["text"] for statement in answer)
        for column in ("id", "family_name"):
            assert column in text, (
                f"on {engine}, the table was created without {column!r}, which "
                f"the model declares:\n    {text}"
            )
