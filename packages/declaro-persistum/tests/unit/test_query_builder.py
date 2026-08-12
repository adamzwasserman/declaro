"""The functional query builder: SQL text and parameter dict, from pure input.

`query/builder.py` carried 56 branches at 7% coverage — one of the six
modules holding the Slop Audit L1.19 gap at 49.7% (declaro-xu0).

Every function here is pure and returns a `Query` TypedDict, so each test
asserts on the exact SQL string and the exact params dict. Asserting the
whole string rather than a substring is the point: a builder that silently
drops a clause still contains the fragment you searched for.

Two safety properties are load-bearing and get their own tests: `update`
and `delete` REFUSE to build without a WHERE clause, and values always
travel as named parameters rather than being interpolated into the SQL.
"""

import pytest

from declaro_persistum.query.builder import (
    _quote_column,
    delete,
    insert,
    raw,
    select,
    update,
    with_limit,
    with_offset,
    with_params,
)














