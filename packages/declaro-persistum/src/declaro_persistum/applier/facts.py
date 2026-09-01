"""What each applier says about itself, as one table.

SIX FUNCTIONS ACROSS THREE MODULES, each returning a constant. `get_engine`
named the engine and `get_transaction_mode` said how a batch is scoped, and
the engine and the mode were the only things that differed between them.

THE MODE IS THE PART THAT MATTERS, AND IT IS NOT COSMETIC. SQLite and
PostgreSQL wrap a whole batch, so one failed operation undoes all of it. Turso
commits per operation, so a failure keeps what already landed. Reading the
wrong one for an engine either throws away work that succeeded or reports work
that was rolled back.

THE TABLE IS ALSO WHY A FOURTH ENGINE CANNOT ARRIVE HALF-DESCRIBED. It used to
be possible to add an applier with a `get_engine` and no
`get_transaction_mode`, and PostgreSQL was exactly that until 2026-08-17: the
other two had declared it all along, `usage.md` documented importing it, and
the import raised. A row here has both columns or it is not a row.
"""

from typing import Literal, TypedDict

TransactionMode = Literal["all_or_nothing", "per_operation"]


class AppliedBy(TypedDict):
    """One engine's own description of itself."""

    engine: str
    transaction_mode: TransactionMode


FACTS: dict[str, AppliedBy] = {
    "sqlite": {"engine": "sqlite", "transaction_mode": "all_or_nothing"},
    "turso": {"engine": "turso", "transaction_mode": "per_operation"},
    "postgresql": {"engine": "postgresql", "transaction_mode": "all_or_nothing"},
}


# ONE READER, NOT TWO. `engine_of` and `transaction_mode_of` each read one
# key out of one row, so the key was the only thing separating them. The audit
# reported this file within an hour of my writing it, which is fair: replacing
# six functions with a table and then adding two functions over the table is
# the same shape one layer up.
# THE TWO COLUMNS A ROW HAS, as a type. `FACTS[engine][fact]` needs a literal
# key, and naming the columns here means a column no row carries is refused
# when the code is checked rather than when it runs.
FactName = Literal["engine", "transaction_mode"]


def fact_of(engine: str, fact: FactName) -> str:
    """One column of one engine's row.

    Both are read with `[]`. An engine nobody described raises here rather
    than answering, and so does a column no row carries.
    """
    return FACTS[engine][fact]
