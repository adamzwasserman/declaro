"""A question asking for some columns and for your own model at once.

    fn refuse_some_columns_as_an_object : (declaration: Declaration) -> NOTHING | Fault

`HOW-IT-SHOULD-WORK.md:57`: you say which shape you want the rows back in,
instances of your model or plain dictionaries. Asking for two columns out of
five AND for your model is asking for something that cannot exist: two fields of
a five-field model is not that model, and handing one over would be handing over
an object that says it is a Person and is not.

REFUSED HERE RATHER THAN LATER. The caller is still holding the question, so the
refusal can name both words they wrote. A caller told later has a half-built
object and nothing to connect it to what they asked for.

NAMING NO COLUMNS MEANS THE WHOLE ROW, so a question that asks for objects and
names no columns is the ordinary case and passes.
"""

from __future__ import annotations

from collections.abc import Callable

from declaro_persistum.types import NOTHING, Declaration, Fault


def _nothing_to_refuse(_declaration: Declaration) -> None | Fault:
    """The two were not asked for together, so there is nothing to say."""
    return NOTHING


def _a_part_cannot_be_the_whole(declaration: Declaration) -> None | Fault:
    """Name both words the caller wrote, so they can drop either one."""
    named = declaration["columns"]
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"You asked {declaration['model']} for {len(named)} of its columns, "
            f"{sorted(named)}, and asked for the rows back as your own model. "
            f"A row carrying {len(named)} fields is not a {declaration['model']}, "
            f"and an object saying it is one would be saying something untrue."
        ),
        what_to_do_instead=(
            f"Ask for the whole row and keep your model, or keep the "
            f"{len(named)} columns and take the rows back as dictionaries."
        ),
    )


# Whether the caller asked for part of a row AND for a whole object. Both rows
# are here, so the ordinary question is answered by a row rather than by a
# check that happens to fall through.
_THE_ANSWER: dict[bool, Callable[[Declaration], None | Fault]] = {
    True: _a_part_cannot_be_the_whole,
    False: _nothing_to_refuse,
}


def refuse_some_columns_as_an_object(declaration: Declaration) -> None | Fault:
    """Refuse a question asking for some columns and for your own model."""
    asked_for_part = bool(declaration["columns"])
    return _THE_ANSWER[asked_for_part and declaration["shaping"] == "objects"](declaration)
