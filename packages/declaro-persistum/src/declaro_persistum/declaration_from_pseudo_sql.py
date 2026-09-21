"""The library's own words, and nothing else.

    fn declaration_from_pseudo_sql : (asked: Asked) -> Declaration | Fault

`people.select("id", "name").where({"name": "bob"})` reads in SQL's word order
without SQL's text, so the words this grammar speaks are SQL's: `select`,
`where`, `ordered_by`, `grouped_by`, `having` and `page`. Beside them sit the two
words every grammar carries, because neither is anybody's phrasing: `model`, the
table the call was written against, and `wanted`, one of the eighteen asking
words.

ITS TABLE IS EMPTY, AND THAT IS THE TRUTHFUL ENTRY. A `Phrasing` names the
fields a grammar spells its own way, and this grammar spells none of them its own
way: its words are the library's words. They are written down once, as
`THE_LIBRARY_WORDS` in `fn declaration_from_phrasing`, which is also the table
every other grammar is backfilled from. Copying them here would be a second place
for one fact, and a second place drifts.

EVERYTHING THIS USED TO DO NOW HAPPENS ONCE. Reading a caller's operator against
the fifteen, expanding a bare value into an eq comparison, turning `or` and `not`
into `any_of` and `none_of`, deciding what each absent word means: all of it was
written four times, once per grammar, and it is now written once in
`fn declaration_from_phrasing` and `fn condition_from_narrowing`.

Pure: one argument in, one dict out. No clock, no random source, no environment,
no file and no connection. A Fault comes back in the return, never raised and
never logged.
"""

from __future__ import annotations

from declaro_persistum.declaration_from_phrasing import declaration_from_phrasing
from declaro_persistum.types import Asked, Declaration, Fault, Phrasing

PHRASING: Phrasing = {}
"""No field is spelled differently here, so every one of the ten is reached by
the library's own word."""


def declaration_from_pseudo_sql(asked: Asked) -> Declaration | Fault:
    """Turn the library's own phrasing into a declaration."""
    return declaration_from_phrasing(asked, PHRASING)
