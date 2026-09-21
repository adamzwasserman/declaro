"""
BDD test fixtures and configuration.
"""

import pytest

# =============================================================================
# One place for a scenario to leave what it has established
# =============================================================================
#
# WHY ONE NAME. On 2026-09-21 nine step files each invented their own name for
# this: worked_out, spending, deployment, answer, asking, comparison, outcome,
# the_write, decided. Eight of the nine returned an empty dict. One idea under
# nine names is the thing this package deletes on sight in prose, and it is no
# better in test code.
#
# WHAT IT IS NOT. It replaced a fixture carrying `engine`, `sql` and
# `connection`, which are the three things a caller never holds. A bag shaped
# like the design we deleted teaches the shape every time somebody reads a step.


@pytest.fixture
def scenario() -> dict[str, object]:
    """Empty. A Given puts what it established in; a Then reads it out."""
    return {}
