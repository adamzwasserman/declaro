"""What the migration at open did.

NOBODY IS WATCHING A DEPLOY. A migration runs while a process is starting, with
no operator at a terminal, so the only account of it anybody will ever read is
the one it leaves behind. This builds that account: the plan that was going to
run, and how many of its operations ran.

BOTH HALVES, BECAUSE ONE OF THEM ANSWERS NOTHING. A count alone says three and
leaves a reader asking three of what. A plan alone says what was going to
happen and never says whether it did. Held together, a reader can compare them:
three of three ran, or three of eleven did and the migration stopped somewhere.

AN EMPTY REPORT IS AN ANSWER, NOT A FAILURE. A database already matching its
models has nothing to run, so the plan is empty and nothing was applied. That
comes back as an ordinary `Report`, which is how a reader tells it from
something going wrong: this package says a failure with a `Fault`, and a
`Report` is not one. There is no fault arm here and nothing raises, because
"there was nothing to do" is a successful migration.

IT DOES NOT CHECK THE COUNT AGAINST THE PLAN, and that is worth saying out
loud. A caller could hand it a count larger than the plan it ran, and the
report would carry both numbers as given. The declaration gives this function
no way to refuse, so the two numbers sit side by side where a reader can see
them disagree, rather than one of them being quietly corrected here.

IT IS THE WHOLE FUNCTION. Two arguments in, two fields out. The work it saves
is not arithmetic; it is that one place decides what the record of a migration
is, so the log line and the handle carry the same record.
"""

from __future__ import annotations

from declaro_persistum.types import Plan, Report


def migration_report(plan: Plan, applied: int) -> Report:
    """The record of one migration: what was going to run, and how much ran."""
    return Report(plan=plan, applied=applied)
