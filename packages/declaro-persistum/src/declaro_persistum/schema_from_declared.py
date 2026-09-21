"""The target schema, built from the models and from nothing the database says.

WHY THE MODELS AND NOT THE DATABASE. `docs/architecture.md`, under "The one
path": introspection tells you what is, and it cannot tell you what should be. Ask the database which
columns belong and a column somebody added by hand in production becomes part of
the specification, and nothing ever drops it. So this function is handed a
`Declared` and nothing else, which is what `fn schema_from_declared` says. There
is no parameter a live schema could arrive through.

WHAT A TARGET SCHEMA IS FOR. `fn difference` compares it against what the
database holds and works out the operations. Everything downstream of that reads
the target rather than the models, so a promise that does not reach the target
is a promise no engine is ever asked for: an index left out here is never
created, and an index left in here is never dropped.

NO ENGINE APPEARS IN IT. A schema says what must be true. `declared_type` is the
word the model used, such as `text`, and which of `TEXT`, `VARCHAR` and `text`
that becomes belongs to `fn spell_operation`, which is handed an engine. Putting
an engine word here would make one declaration stop serving three engines,
quietly, on the day somebody switched.

WHAT IT REFUSES. An index that names a column its model does not declare. The
index is a promise about a column that will not exist, so the engine would
refuse the CREATE INDEX at open, in a migration nobody is watching, with its own
word for somebody's typo. The refusal instead names the model, says which column
the index asked for, and lists the columns that model does declare.

WHAT IT DOES NOT REFUSE, and where that belongs. A table name the library owns
is `fn refuse_owned_names`, and a migration that would destroy something is
`fn refuse_undeclared_loss`. Each refusal has one place.

`dropping_tables` AND `dropping_views` ARE NOT READ HERE. They name things that
are to go, and a thing that is to go is a thing the target does not hold. Their
reader is `fn refuse_undeclared_loss`, which asks whether a destroying operation
was asked for by name.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import Declared, Fault, Model, Schema, Table

# An index that names a column nobody declared, as the model it sits in, the
# index's own name, the columns that are not there, and the columns that are.
# All four are in the refusal, so all four travel together.
Offence = tuple[str, str, list[str], list[str]]


def _a_table(model: Model) -> Table:
    """The table one model describes.

    THE COLUMNS, INDEXES AND CONSTRAINTS ARE CARRIED, NOT REBUILT. `Column`
    holds `renamed_from`, `references` and `facets`, and `fn difference` reads
    all three to tell an added column from a renamed one. A target that kept
    only the names would make every rename look like a drop and an add, which
    is the one mistake that loses a column's contents.
    """
    return Table(
        name=model["table"],
        columns=model["columns"],
        indexes=model["indexes"],
        constraints=model["constraints"],
    )


def _offences_in(model: Model) -> list[Offence]:
    """Every index of this model that names a column the model does not declare."""
    declares = [column["name"] for column in model["columns"]]
    missing = {
        index["name"]: [name for name in index["columns"] if name not in declares]
        for index in model["indexes"]
    }
    return [
        (model["table"], index, columns, declares) for index, columns in missing.items() if columns
    ]


def _build(declared: Declared, _offences: list[Offence]) -> Schema | Fault:
    """Every index names a column its model declares, so the target is built."""
    return Schema(
        tables=[_a_table(model) for model in declared["models"]],
        views=list(declared["views"]),
    )


def _refuse(_declared: Declared, offences: list[Offence]) -> Schema | Fault:
    """Name the model, name the columns the index asked for, and list the ones it has."""
    went_wrong = " ".join(
        f"{table} declares the index {index} over {', '.join(missing)}, "
        f"and {table} declares no such column."
        for table, index, missing, _declares in offences
    )
    instead = " ".join(
        f"{table} declares {', '.join(declares)}. "
        f"Build {index} over those, or declare the columns it names."
        for table, index, _missing, declares in offences
    )
    return Fault(kind="refused", what_went_wrong=went_wrong, what_to_do_instead=instead)


_ANSWER: dict[bool, Callable[[Declared, list[Offence]], Schema | Fault]] = {
    False: _build,
    True: _refuse,
}


def schema_from_declared(declared: Declared) -> Schema | Fault:
    """Build the target schema the models describe, or refuse an index that cannot stand.

    Reads its one argument and nothing else. It opens no connection, asks no
    clock and names no engine, so the target a migration will be planned against
    can be read without a database anywhere near it.
    """
    offences = [offence for model in declared["models"] for offence in _offences_in(model)]
    return _ANSWER[bool(offences)](declared, offences)
