"""How to ask one question in each of the four grammars.

WHAT THIS IS FOR. Every scenario about grammars is an outline over the same four
rows, so the four ways of asking belong in one table rather than repeated in
each step file. A scenario says "in the django grammar" and reads the row.

THE ROW CARRIES THE CALL, NOT THE NAME OF THE CALL. `model.objects.all()` sits
in the table as itself. It was four classes with a method each until the audit
pointed out they were one shape; the first fix I reached for was a table of
attribute names walked with `getattr`, which would have turned a checked call
into a string carrying structure, the exact thing this package deleted from
`where`, from `-name` and from `field(check=...)`. A row holding the call keeps
both: one function over a table, and a wrong name that is an AttributeError
naming itself.

THE CALLS ARE WRITTEN AS THEY WILL BE CALLED, not stubbed. Nothing here raises
`NotImplementedError` to stand in for missing code: each row is the expression a
caller will write, so today it fails with `AttributeError: type object 'Person'
has no attribute 'objects'`, which names exactly what is not built. A stub would
have named nothing and would have needed rewriting the day the surface arrived.

THE ROWS ARE THE SPECIFICATION OF THE SURFACE. `HOW-IT-SHOULD-WORK.md` shows one
example per grammar; this is the same four, written so they run.
`tests/unit/test_every_grammar_is_in_the_table.py` reads that document and
asserts these are the four it names, so a fifth grammar written down with no row
here shows up as an absence rather than as a scenario nobody wrote.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypedDict, get_type_hints


class Grammar(TypedDict):
    """What one grammar must be able to ask, in its own phrasing.

    Three entries and not one, because the point of four grammars is that they
    spell these differently. What must NOT differ is the question each one
    builds, and that is what the scenarios assert.
    """

    everything: Callable[[type], Any]
    narrowed: Callable[[type, str, Any], Any]
    only_columns: Callable[[type, list[str]], Any]


# THE FOUR ROWS. The keys are the words the scenarios use, so a scenario outline
# reads `| django |` and this is what it gets.
GRAMMARS: dict[str, Grammar] = {
    # The library's own, reading in SQL's word order without SQL's text.
    "pseudo-sql": {
        "everything": lambda model: model.rows.select(),
        "narrowed": lambda model, column, value: model.rows.select().where({column: value}),
        "only_columns": lambda model, columns: model.rows.select(*columns),
    },
    # For people who think in managers and lookups.
    "django": {
        "everything": lambda model: model.objects.all(),
        "narrowed": lambda model, column, value: model.objects.filter(**{column: value}),
        "only_columns": lambda model, columns: model.objects.all().values(*columns),
    },
    # For people who think in one call carrying named parts.
    "prisma": {
        "everything": lambda model: model.find_many(),
        "narrowed": lambda model, column, value: model.find_many(where={column: value}),
        "only_columns": lambda model, columns: model.find_many(select=columns),
    },
    # For people who think in a query object.
    "sqlalchemy": {
        "everything": lambda model: model.query.all(),
        "narrowed": lambda model, column, value: model.query.filter_by(**{column: value}),
        "only_columns": lambda model, columns: model.query.with_only(*columns),
    },
}

# What a grammar must be able to ask, READ OFF THE TYPE rather than repeated. A
# list written out here would be a second place the same fact lives, and it
# would drift the first time a fourth question was added to `Grammar`.
ASKS = tuple(get_type_hints(Grammar))
