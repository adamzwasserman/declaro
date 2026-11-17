"""Which method name one spelling uses for each field, where it differs.

THREE OF THE FOUR SPELLINGS ARE A MODEL, MAYBE AN ATTRIBUTE, THEN A CHAIN OF
CALLS, and only the method names differ. This is the table of those names, read
by the one reader that parses all four, `fn asked_from_written`.

A ROW ONLY EXISTS WHERE A SPELLING DISAGREES WITH THIS LIBRARY. `prisma` already
says `where` and `select`, which are this library's own words, so it renames
nothing and answers with an empty table. `pseudo_sql` is this library, so the
same. Carrying rows that say a word is itself would mean four tables to keep in
step instead of two, and the two that say nothing say it by holding nothing.

AN EMPTY TABLE AND A WORD NOBODY DECLARED ARE DIFFERENT ANSWERS, which is why
this subscripts rather than asking for a default. An empty table means a spelling
that renames nothing, and every method in the question is then read as this
library's own word. A word nobody declared answering the same way would parse a
Django question as though it were written in this library's spelling, so it
raises instead, and the key is the word, so the failure names it.

EVERY SPELLING REACHES EVERY FIELD. A caller who picked Django's spelling has not
picked a smaller library. Django has no method of its own for an order, a
grouping, a having or a page, so those four are reached by this library's own
word in every spelling and appear in no table here.

WRITTEN 2026-10-05, out of `type Phrasing` and four word-lists that are gone.
`Phrasing` claimed to be a spelling's word for every field and held two renamed
keys, because no parser existed and the words were the whole of what a "grammar"
was. Over a real parse, method names are all it ever was.
"""

from __future__ import annotations

from declaro_persistum.types import Grammar, MethodNames

# WHAT EACH SPELLING CALLS A FIELD THAT IT DOES NOT CALL WHAT WE CALL IT. The key
# is this library's word and the value is that spelling's. Every spelling in
# `Grammar` has a row, because a word with no row is a word nobody declared and
# must raise rather than read as renaming nothing.
#
# NO METHOD NAME MAY APPEAR TWICE ACROSS THESE TABLES. `fn asked_from_written`
# reads a name and decides which part of the question it marks, so a name under
# two spellings is a part it cannot place.
WHAT_EACH_SPELLING_CALLS_THEM: dict[Grammar, MethodNames] = {
    # This library's own words, so there is nothing to translate.
    "pseudo_sql": {},
    # Prisma already says `where` and `select`.
    "prisma": {},
    "django": {"where": "filter", "select": "values"},
    "sqlalchemy": {"where": "filter_by", "select": "with_only"},
}


def methods_of(grammar: Grammar) -> MethodNames:
    """The method names this spelling uses, or a raise naming a word nobody uses."""
    return WHAT_EACH_SPELLING_CALLS_THEM[grammar]
