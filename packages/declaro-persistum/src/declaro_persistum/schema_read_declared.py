"""What the code declares, asked for rather than handed over.

THE FIRST FUNCTION IN THIS PACKAGE THAT READS ANYTHING. Every one written
before it is pure: it takes what it needs and answers from that. This one takes
nothing at all, and the empty parameter list is the point. `docs/design/
persistum.hd` says why on the card for `reader fn schema_read_declared`: a
caller who had to hand the models over could hand a different set to each door,
and nothing would notice. So the program asks the process what it declares, the
same way `reader fn schema_inspect` asks a database what it holds, and the two
answers meet in `fn migration_needed`.

WHAT IT READS IS MODELS, NOT A FILE. The declaration names the input "the
Pydantic models the host declares". Pydantic already keeps the register: every
class that subclasses `BaseModel` is reachable from it, and this walks that
tree. Nothing has to be imported by path, nothing has to be registered twice,
and a model the host never wrote is a model this cannot invent.

HOW A HOST DECLARES ONE. `docs/architecture.md` says the loader reads
`__tablename__` and the columns off `model_fields`, so a model is a Pydantic
model carrying `__tablename__`:

    class Person(BaseModel):
        __tablename__ = "people"
        id: int
        name: str

A view carries `__viewname__` and `__declaration__`, because a view is a
`Declaration` with a name rather than a SELECT. Everything else a `Model`, a
`View` or a `Declared` carries that Pydantic itself cannot say is a class
attribute named for the field it fills: `__dropping__`, `__indexes__`,
`__constraints__`, `__freshness__`, `__dropping_tables__`, `__dropping_views__`.
One rule, no exceptions to remember.

A WHOLE TABLE'S LOSS IS DECLARED BY WHATEVER IS LEFT. `Model.dropping` names a
column a surviving model means to lose, and a table that is gone has no model
left to name it. So `__dropping_tables__` and `__dropping_views__` are read off
every Pydantic model in the process, whether or not that class is itself a
table, and a project with nothing left can still say what goes.

WHAT A COLUMN'S TYPE BECOMES IS A TABLE AND THE TABLE HAS NO FALLBACK ROW. An
annotation with no row comes back as a refusal naming it and listing what this
reads, because a type silently turned into text is a column nobody declared.

WHAT IS NOT READ HERE, AND WHY IT IS NOT A HOLE. `Column.renamed_from` and
`Column.references` are what `field()` declares, and `field()` has no card in
the declaration yet. Both come back as the named absence rather than as a guess.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Literal, Union, get_args, get_origin
from uuid import UUID

from pydantic import BaseModel

from declaro_persistum.types import NOTHING, Column, Declared, Fault, Model, View

# What a column holds and whether it may be absent, which is the pair every
# reader below hands back and the pair a `Column` is built from.
type Held = tuple[str, bool]


# --- what a host writes, and what this package calls it ----------------------
#
# LEFT IS THE ANNOTATION, RIGHT IS THE MEANING. `docs/architecture.md` keeps the
# two apart on purpose: you say the column holds money, and only the renderer
# says whether that is `numeric(10,2)` or `REAL`. Nothing on the right is an
# engine's word for its own storage.

ACCEPTED: dict[Any, str] = {
    str: "text",
    int: "integer",
    float: "float",
    bool: "boolean",
    bytes: "bytes",
    Decimal: "decimal",
    date: "date",
    datetime: "datetime",
    time: "time",
    UUID: "uuid",
}


# --- refusals ----------------------------------------------------------------


def _refused(went_wrong: str, do_instead: str) -> Fault:
    return {
        "kind": "refused",
        "what_went_wrong": went_wrong,
        "what_to_do_instead": do_instead,
    }


def _is_fault(piece: object) -> bool:
    """Whether a piece came back refused rather than read."""
    return isinstance(piece, dict) and set(piece) == {
        "kind",
        "what_went_wrong",
        "what_to_do_instead",
    }


def _the_first_fault_or[Built](pieces: list[Any], build: Callable[[], Built]) -> Built | Fault:
    """A refusal anywhere in the pieces is the answer; otherwise build one."""
    faults = list(filter(_is_fault, pieces))
    answer: dict[bool, Callable[[], Built | Fault]] = {True: lambda: faults[0], False: build}
    return answer[faults != []]()


# THE TWO WAYS AN ANNOTATION SPELLS ITSELF. A bare class has a `__name__` and
# nothing else worth printing; `tuple[int, int]` has a `__name__` of "tuple",
# which drops exactly the part a reader needs to see.
_SPELLING: dict[bool, Callable[[Any], str]] = {True: lambda held: held.__name__, False: repr}


def _spelled(annotation: Any) -> str:
    """The annotation as the host wrote it."""
    return _SPELLING[get_origin(annotation) is None](annotation)


def _the_ones_read() -> str:
    """Every annotation this reads, derived from the table rather than listed."""
    plain = sorted(spelled.__name__ for spelled in ACCEPTED)
    return ", ".join([*plain, "Literal[...]", "list[...]", "dict[...]"])


def _no_such_type(annotation: Any) -> Fault:
    return _refused(
        f"A column is declared {_spelled(annotation)}, and this package reads no such type.",
        f"Declare the column as one of {_the_ones_read()}, or as one of those "
        f"written with None beside it for a column that may be absent.",
    )


def _cannot_be_read(cls: type, named: object) -> Fault:
    return _refused(
        f"The model {cls.__name__} declares its name as {named!r}, which is not "
        f"a table name this package can read.",
        f"Give {cls.__name__} a __tablename__ that is a string, or take the "
        f"class out of the models the host declares.",
    )


def _makes_no_sense(cls: type, _named: object) -> Fault:
    return _refused(
        f"The view {cls.__name__} names itself but declares no query.",
        f"Give {cls.__name__} a __declaration__ holding the declaration the "
        f"view selects, or take the class out of the models the host declares.",
    )


# --- reading one annotation ---------------------------------------------------


def _plain(annotation: Any) -> Held | Fault:
    """A bare class, which is a type this reads or a type it refuses."""
    known: dict[bool, Callable[[], Held | Fault]] = {
        True: lambda: (ACCEPTED[annotation], False),
        False: lambda: _no_such_type(annotation),
    }
    return known[annotation in ACCEPTED]()


def _some_words(_annotation: Any) -> Held:
    """A Literal is a closed set of words, and words are stored as text."""
    return ("text", False)


def _carried_as_json(_annotation: Any) -> Held:
    """A list and a map both travel as one JSON value in one column."""
    return ("json", False)


def _the_one_member(members: list[Any], absent: bool, _annotation: Any) -> Held | Fault:
    """One real type beside None is a column of that type that may be absent."""
    held = _read(members[0])
    return _the_first_fault_or([held], lambda: (held[0], absent))


def _not_one_member(_members: list[Any], _absent: bool, annotation: Any) -> Fault:
    """Two real types in one column is two columns, and this refuses to pick."""
    return _no_such_type(annotation)


_AMONG_MEMBERS: dict[bool, Callable[[list[Any], bool, Any], Held | Fault]] = {
    True: _the_one_member,
    False: _not_one_member,
}


def _may_be_absent(annotation: Any) -> Held | Fault:
    """`str | None` is a column of text that may be absent."""
    members = list(filter(lambda member: member is not type(None), get_args(annotation)))
    return _AMONG_MEMBERS[len(members) == 1](
        members, type(None) in get_args(annotation), annotation
    )


# WHAT THE ANNOTATION IS SHAPED LIKE DECIDES WHO READS IT, and a shape with no
# row is refused by name rather than read as something near it. `None` is what
# `get_origin` answers for a bare class, so it is the row for `str` and `UUID`.
BY_ORIGIN: dict[Any, Callable[[Any], Held | Fault]] = {
    None: _plain,
    Union: _may_be_absent,
    Literal: _some_words,
    list: _carried_as_json,
    dict: _carried_as_json,
}


def _read(annotation: Any) -> Held | Fault:
    """The type a column holds and whether it may be absent, or a refusal."""
    origin = get_origin(annotation)
    shaped: dict[bool, Callable[[], Held | Fault]] = {
        True: lambda: _no_such_type(annotation),
        False: lambda: BY_ORIGIN[origin](annotation),
    }
    return shaped[origin not in BY_ORIGIN]()


# --- reading one class --------------------------------------------------------


def _declared_on[Said](cls: type, attribute: str, nobody_said: Said) -> Said | Any:
    """What the class declares under that name, or what saying nothing means.

    THE ABSENCE IS RESOLVED HERE, AT THE EDGE, WHICH IS THE ONLY PLACE IT MAY
    BE. A class that declares no index has no index, and that is a reading of
    what the host wrote rather than a value invented to fill a hole.
    """
    found = [getattr(cls, name) for name in filter(lambda name: hasattr(cls, name), [attribute])]
    return ([nobody_said, *found])[len(found)]


def _a_column(name: str, annotation: Any) -> Column | Fault:
    """One column, with every field a `Column` names present."""
    held = _read(annotation)
    return _the_first_fault_or(
        [held],
        lambda: Column(
            name=name,
            declared_type=held[0],
            renamed_from=NOTHING,
            references=NOTHING,
            facets={"nullable": held[1]},
        ),
    )


def _the_model(cls: type, named: Any) -> Model | Fault:
    columns = [
        _a_column(name, field.annotation)
        for name, field in cls.model_fields.items()  # type: ignore[attr-defined]
    ]
    return _the_first_fault_or(
        columns,
        lambda: Model(
            # WHAT YOU WROTE, WHICH IS THE CLASS'S OWN NAME. You write `Person`
            # and the table is `people`, so a refusal that could only say
            # `people` was not saying the word you typed.
            name=cls.__name__,
            table=named,
            columns=columns,  # type: ignore[arg-type]
            dropping=_declared_on(cls, "__dropping__", []),
            indexes=_declared_on(cls, "__indexes__", []),
            constraints=_declared_on(cls, "__constraints__", []),
        ),
    )


_A_TABLE: dict[bool, Callable[[type, Any], Model | Fault]] = {
    True: _the_model,
    False: _cannot_be_read,
}


def _a_model(cls: type) -> Model | Fault:
    """One class as the table it declares, or a refusal naming the class."""
    named = cls.__tablename__  # type: ignore[attr-defined]
    return _A_TABLE[isinstance(named, str)](cls, named)


def _the_view(cls: type, named: Any) -> View:
    return View(
        name=named,
        declaration=cls.__declaration__,  # type: ignore[attr-defined]
        freshness=_declared_on(cls, "__freshness__", NOTHING),
    )


_A_VIEW: dict[bool, Callable[[type, Any], View | Fault]] = {
    True: _the_view,
    False: _makes_no_sense,
}


def _a_view(cls: type) -> View | Fault:
    """One class as the view it declares, or a refusal naming the class."""
    named = cls.__viewname__  # type: ignore[attr-defined]
    return _A_VIEW[isinstance(named, str) and hasattr(cls, "__declaration__")](cls, named)


# --- reading the process ------------------------------------------------------


def _descendants(cls: type) -> list[type]:
    """Every class beneath this one, in the order they were written."""
    children = cls.__subclasses__()
    return [found for child in children for found in [child, *_descendants(child)]]


def _declared() -> list[type]:
    """Every Pydantic model alive in this process, each one once."""
    return list(dict.fromkeys(_descendants(BaseModel)))


def _names_on(classes: list[type], attribute: str) -> list[str]:
    """Every name declared under that attribute, across every class, once each."""
    named = [
        name
        for cls in filter(lambda cls: hasattr(cls, attribute), classes)
        for name in getattr(cls, attribute)
    ]
    return list(dict.fromkeys(named))


def schema_read_declared() -> Declared | Fault:
    """The models, the views, and what the code means to lose.

    Takes nothing, because what it reads is not something a caller hands over.
    Reads the Pydantic models the host declared and answers with all four
    lists, or with the first refusal it meets.
    """
    classes = _declared()
    models = [_a_model(cls) for cls in filter(lambda cls: hasattr(cls, "__tablename__"), classes)]
    views = [_a_view(cls) for cls in filter(lambda cls: hasattr(cls, "__viewname__"), classes)]
    return _the_first_fault_or(
        [*models, *views],
        lambda: Declared(
            models=models,  # type: ignore[arg-type]
            views=views,  # type: ignore[arg-type]
            dropping_tables=_names_on(classes, "__dropping_tables__"),
            dropping_views=_names_on(classes, "__dropping_views__"),
        ),
    )
