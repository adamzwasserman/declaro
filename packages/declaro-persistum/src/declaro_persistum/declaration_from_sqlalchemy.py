"""SQLAlchemy's words, and nothing else.

    fn declaration_from_sqlalchemy : (asked: Asked) -> Declaration | Fault

A caller who thinks in a query object writes
`Person.query.filter_by(name="bob").with_only("id", "name")`, and this turns
that call into the same `Declaration` the other three grammars produce for the
same question. It borrows the phrasing and nothing else: no session, no unit of
work, and no way back to SQL.

TWO WORDS ARE THIS GRAMMAR'S: `filter_by` for the narrowing and `with_only` for
the columns. They are the whole of the table below, and the keys are the method
names because `Asked` is the call as the caller phrased it.

THE OTHER EIGHT FIELDS ARE REACHED BY THE LIBRARY'S OWN WORD, AND THAT IS A
CORRECTION. Until 2026-09-21 this builder refused `order_by`, `group_by`,
`having` and `page` because the ORM it imitates has no phrasing this surface
copies. That made a caller who picked this spelling a caller with a smaller
library, and made the claim every one of the four feature files opens with --
all four produce the same declaration for the same question -- false. Adam ruled
on it: a field a grammar brings no word for is reached by the library's own
word, so `ordered_by`, `grouped_by`, `having` and `page` reach them here.

Pure: one argument in, one dict out. No clock, no random source, no environment,
no file and no connection. A Fault comes back in the return, never raised and
never logged.
"""

from __future__ import annotations

from declaro_persistum.declaration_from_phrasing import declaration_from_phrasing
from declaro_persistum.types import Asked, Declaration, Fault, Phrasing

PHRASING: Phrasing = {
    "condition": "filter_by",
    "columns": "with_only",
}
"""The two fields this grammar spells its own way. Every other field is reached
by the library's own word."""


def declaration_from_sqlalchemy(asked: Asked) -> Declaration | Fault:
    """A query object's phrasing, as the declaration all four grammars build."""
    return declaration_from_phrasing(asked, PHRASING)
