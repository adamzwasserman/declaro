"""Django's words, and nothing else.

    fn declaration_from_django : (asked: Asked) -> Declaration | Fault

A team that already writes Django writes what it already knows:

    Person.objects.filter(name="bob").values("id", "name")

IT BORROWS THE PHRASING AND NOTHING ELSE. No session holding state, no manager
attached to a class, and no second place where a column can be named. Two words
are Django's: `filter` for the narrowing and `values` for the columns. They are
the whole of the table below.

THE OTHER EIGHT FIELDS ARE REACHED BY THE LIBRARY'S OWN WORD. Django has no
phrasing this surface copies for an order, a grouping, a having or a page, and a
caller who picked this spelling has not picked a smaller library, so `ordered_by`,
`grouped_by`, `having` and `page` reach them here as they do everywhere.

THE WORDS ARE KEYS, NOT A FLATTENED CALL. Flattening `filter(name="bob")` into
the top level would read better until a model declared a column called `model`,
and then a column name and a grammar word would be the same key.

Pure: one argument in, one dict out. No clock, no random source, no environment,
no file and no connection. A Fault comes back in the return, never raised and
never logged.
"""

from __future__ import annotations

from declaro_persistum.declaration_from_phrasing import declaration_from_phrasing
from declaro_persistum.types import Asked, Declaration, Fault, Phrasing

PHRASING: Phrasing = {
    "condition": "filter",
    "columns": "values",
}
"""The two fields Django spells its own way. Every other field is reached by the
library's own word."""


def declaration_from_django(asked: Asked) -> Declaration | Fault:
    """Turn one call written in Django's phrasing into a declaration."""
    return declaration_from_phrasing(asked, PHRASING)
