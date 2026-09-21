"""Prisma's words, and nothing else.

    fn declaration_from_prisma : (asked: Asked) -> Declaration | Fault

Prisma phrases a question as one call carrying named parts, so
`Person.find_many(where={"name": "bob"}, select=["id", "name"])` arrives here as
the model it was made on, the word it asked for, and each named argument under
its own name:

    {"model": "Person", "wanted": "all",
     "where": {"name": "bob"}, "select": ["id", "name"]}

ITS TABLE IS EMPTY, AND THAT IS WHAT THE TWO NAMED PARTS MEAN. `where` and
`select` are the two parts this document writes for Prisma, and both are already
the library's own words, so Prisma spells no field differently and every one of
the ten is reached by the library's word.

WHERE THE VERB IS TRANSLATED, AND WHY NOT HERE. A caller reaches for
`find_many` and a declaration says `all`. A `Phrasing` is a word for each field
and nothing else, so it has nowhere to carry that pair, and whatever builds the
`Asked` puts one of the eighteen asking words in it. Three of the four grammars
already worked that way; this one translated the verb itself until 2026-09-21,
which put the same table in two places.

Pure: one argument in, one dict out. No clock, no random source, no environment,
no file and no connection. A Fault comes back in the return, never raised and
never logged.
"""

from __future__ import annotations

from declaro_persistum.declaration_from_phrasing import declaration_from_phrasing
from declaro_persistum.types import Asked, Declaration, Fault, Phrasing

PHRASING: Phrasing = {}
"""No field is spelled differently here: `where` and `select` are the library's
own words, so every one of the ten is reached by it."""


def declaration_from_prisma(asked: Asked) -> Declaration | Fault:
    """Turn one prisma call into the declaration all four grammars build."""
    return declaration_from_phrasing(asked, PHRASING)
