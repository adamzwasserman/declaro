"""Which writes have to land together.

WHAT IT DECIDES. One declaration comes in and one bool goes out: do the writes
this declaration carries have to land as one transaction, or may each one be
deposited on its own?

ONE ROW IS A DEPOSIT AND A TICKET. It goes into the room, a drainer appends it,
and the caller holds a ticket for that one write. SEVERAL ROWS HANDED TO ONE
CALL ARE A TRANSACTION, because the caller asked for all of them or none, and
the room cannot promise that: `docs/architecture.md` line 150 says two deposits
are two independent writes, and the second failing does not undo the first,
because the first is already in the log. So `True` here means hold a connection
instead of depositing.

A READ NEVER RAISES THE QUESTION. It lands nothing, so nothing lands with it,
and the word is checked before the rows are counted. A read carrying rows --
which the shape permits -- still answers `False`, because otherwise it would be
sent down the path that holds a connection for a rollback it has no use for.

THE SIX WRITING WORDS ARE READ OFF `Writing`, not typed again here. Adding a
word to the declaration's vocabulary must not need an edit in this file too,
because the copy would be the one that went stale.

WHAT IT DOES NOT ANSWER. Several separate calls the caller wants to land
together are a group, and a group is not a declaration -- this function is
handed one at a time and never sees the pair. A group's rollback comes from
holding one connection, `writing(db)`.
"""

from typing import get_args

from declaro_persistum.types import Declaration, Writing

A_WRITING_WORD = frozenset(get_args(Writing))


def travels_together(declaration: Declaration) -> bool:
    """True when this declaration's rows have to land together or not at all."""
    return declaration["wanted"] in A_WRITING_WORD and len(declaration["rows"] or ()) > 1
