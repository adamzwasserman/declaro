"""What each applier says about itself, as one table.

SIX FUNCTIONS ACROSS THREE MODULES, each returning a constant. `get_dialect`
named the engine and `get_transaction_mode` said how a batch is scoped, and
the engine and the mode were the only things that differed between them.

THE MODE IS THE PART THAT MATTERS, AND IT IS NOT COSMETIC. SQLite and
PostgreSQL wrap a whole batch, so one failed operation undoes all of it. Turso
commits per operation, so a failure keeps what already landed. Reading the
wrong one for an engine either throws away work that succeeded or reports work
that was rolled back.

THE TABLE IS ALSO WHY A FOURTH ENGINE CANNOT ARRIVE HALF-DESCRIBED. It used to
be possible to add an applier with a `get_dialect` and no
`get_transaction_mode`, and PostgreSQL was exactly that until 2026-08-17: the
other two had declared it all along, `usage.md` documented importing it, and
the import raised. A row here has both columns or it is not a row.
"""

from typing import Literal, TypedDict, cast

TransactionMode = Literal["all_or_nothing", "per_operation"]


class AppliedBy(TypedDict):
    """One engine's own description of itself."""

    dialect: str
    transaction_mode: TransactionMode


FACTS: dict[str, AppliedBy] = {
    "sqlite": {"dialect": "sqlite", "transaction_mode": "all_or_nothing"},
    "turso": {"dialect": "turso", "transaction_mode": "per_operation"},
    "postgresql": {"dialect": "postgresql", "transaction_mode": "all_or_nothing"},
}


# ONE READER, NOT TWO. `dialect_of` and `transaction_mode_of` each read one
# key out of one row, so the key was the only thing separating them. The audit
# reported this file within an hour of my writing it, which is fair: replacing
# six functions with a table and then adding two functions over the table is
# the same shape one layer up.
def fact_of(engine: str, fact: str) -> str:
    """One column of one engine's row.

    Both are read with `[]`. An engine nobody described raises here rather
    than answering, and so does a column no row carries.
    """
    return cast(str, FACTS[engine][fact])
