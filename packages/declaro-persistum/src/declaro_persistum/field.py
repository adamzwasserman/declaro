"""What a host says about one column that its annotation cannot carry.

AN ANNOTATION GIVES A COLUMN ITS NAME AND ITS TYPE, AND TWO FACTS DO NOT FIT
THERE. A column may have been called something else before, and a column may
point at another table's column. `docs/design/persistum.hd` declares both on
`type Column`, and until this card existed the only function that could fill
them, `reader fn schema_read_declared`, wrote the named absence into every
column every time.

SO THE RENAME PATH WAS BUILT AND UNREACHABLE. Seven modules read
`Column.renamed_from`. A host who renamed a column got one column dropped and
another added, `fn name_what_is_ambiguous` stopped the open rather than guess
which, and nothing the host could write resolved it. That is what this unblocks.

IT IS SAID BESIDE THE COLUMN, NOT AT THE TOP OF THE CLASS. The six facts a model
declares as class attributes, `__tablename__`, `__indexes__`, `__constraints__`
and the rest, are facts about the table. These two are facts about one column,
and writing them in a dict at the top of the class names that column a second
time. Two namings drift, which is the same argument this package makes against
hand-written migrations, one scope down.

THE ANSWER HAS TO SURVIVE BEING AN ASSIGNMENT, which is what decides its shape.
A host writes `email_address: str = field(...)`, so Pydantic reads whatever comes
back as that attribute. A plain record would be read as the column's default,
which turns a required column optional and declares a schema nobody wrote. So
the pair rides in the field's own extra, where Pydantic keeps what it does not
itself understand, and `schema_read_declared` reads it back from there.

WHERE THE CODE AND THE DECLARATION DISAGREE, AND IT IS NOT RESOLVED HERE. The
card says this answers a `Field`, which is the record of the two facts. What it
returns is Pydantic's own field record carrying that record, because nothing
else can be assigned to a column. The two facts are exactly the declared ones
and nothing else travels, so the disagreement is about the wrapper rather than
about the answer. Adam has not been asked yet which way to close it.

NEITHER ARGUMENT HAS A DEFAULT. A column that forgot to say where it points and
a column that points nowhere are different facts, and only the host knows which
one it meant. A default here would answer for them.
"""

from __future__ import annotations

from typing import Any, cast

from pydantic import Field as _pydantic_field

from declaro_persistum.types import Field, Reference


def field(renamed_from: str | None, references: Reference | None) -> Any:
    """What a host says about one column, in a form Pydantic will hold.

    The answer is annotated `Any` for the same reason Pydantic annotates its own
    `Field` that way: it is assigned to a column of some other declared type, and
    a narrower annotation would make every such assignment a type error at the
    host's own call site.

    THE CAST IS THE TWO TYPE SYSTEMS MEETING, NOT A HOLE. `Reference` is a
    `TypedDict`, so at runtime this is a mapping of strings that Pydantic holds
    and writes out without complaint. Pydantic types the parameter as `JsonDict`,
    a recursive union mypy will not unify a `TypedDict` with, and it does not
    export that name, so naming it here would reach into something Pydantic does
    not publish. The cast says what is already true instead.
    """
    said = Field(renamed_from=renamed_from, references=references)
    return _pydantic_field(json_schema_extra=cast("Any", said))
