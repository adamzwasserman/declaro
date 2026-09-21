"""Whether the database still has to be brought up to the models.

`docs/design/persistum.hd:811` declares it:

    fn migration_needed : (live: Schema, target: Schema) -> bool

WHY IT EXISTS. Opening asks this before it hands a handle back. A handle given
out while the database is still behind the models is a handle whose first
question fails on a column the models declare, and it fails in the caller's
request rather than at open, where somebody is watching.

WHAT IT IS ALLOWED TO ASK. Nothing. It is given both states: `schema_inspect`
asked the database what it holds, `schema_from_declared` built what the models
want, and both answers arrive here. HOW-IT-SHOULD-WORK.md:53 says the models
are the desired state and everything else is derived from them, so there is no
third source this could consult and no decision left to make.

WHY IT IS A COMPARISON AND NOT A DIFF. `difference` works out which operations
turn one schema into the other, and that is a longer and more expensive
question. This one answers only whether anything has to run, so opening can
hand the handle straight over in the ordinary case where nothing does.

WHAT IT CANNOT SEE. Two schemas that hold the same tables in a different order
compare as different, so this says a migration is needed when none is. The
cost is one wasted diff, which then emits no operations. The error runs in the
safe direction only: a schema that matches can never read as unequal in a way
that hides a missing column, so this never answers "nothing to do" while the
database is behind. Ordering belongs to whoever decides what a schema's list
of tables means, not here.
"""

from __future__ import annotations

from declaro_persistum.types import Schema


def migration_needed(live: Schema, target: Schema) -> bool:
    """Answer whether the database still has to be brought up to the models."""
    return live != target
