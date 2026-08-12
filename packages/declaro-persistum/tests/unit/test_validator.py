"""Schema validation: what is fatal, what is only a warning, and why.

`validator.py` carried 56 branches and 0% coverage — one of the six modules
holding the Slop Audit L1.19 gap at 49.7% (declaro-xu0).

The whole module is pure: schema dict in, (warnings, errors) out. So every
test is an assertion on a return value, no mock anywhere.

The distinction the module draws is the thing worth pinning down. An
unresolvable reference is an ERROR — the DDL cannot be emitted. An unknown
type or a circular dependency is a WARNING — a custom type may exist, and a
cycle is legal with deferrable constraints. Tests assert which list a
finding lands in, not merely that something was said.
"""

import pytest

from declaro_persistum.exceptions import ValidationError
from declaro_persistum.validator import (
    KNOWN_TYPES,
    _check_circular_dependencies,
    _validate_column,
    _validate_index,
    _validate_reference,
    validate_schema,
    validate_schema_strict,
)


def _t(columns=None, **rest):
    return {"columns": columns or {}, **rest}


USERS = _t({"id": {"type": "integer"}, "name": {"type": "text"}})
















