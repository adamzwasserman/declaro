"""Dialect translation table for SQL function names.

This package once also held `scalars.py` (20 classes) and `aggregates.py`
(8 classes) — a second, parallel implementation of SQL function wrappers.
Nothing imported it. The live query layer builds functions with
`SQLFunction` from `declaro_persistum.query.table`, and defined its own
`count_`, `sum_`, `avg_`, `min_`, `max_` and `now_` with DIFFERENT
signatures, so the two could not have been used interchangeably even by
accident. Only its own tests kept it alive. Deleted 2026-08-12.

`translations.py` survives the deletion but is currently referenced by
nothing except its own tests. `query/update.py` and `query/insert.py` each
carry a hand-written if/elif copy of the same dialect mapping — two copies
of one table, neither of them this one.
"""

from .translations import (
    FUNCTION_TRANSLATIONS,
    translate_function,
)

__all__ = [
    "FUNCTION_TRANSLATIONS",
    "translate_function",
]
