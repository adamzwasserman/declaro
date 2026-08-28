"""A tag declared in a type must not be typed out again in the check.

MEASURED 2026-08-28 by the Slop Audit. `SubqueryExpr` declares its tag

    kind: Literal["subquery"]

and `is_subquery` wrote the same word out again:

    return kind_of(value) == "subquery"

Two copies of one word, and only one of them is enforced by anything. Change
the `Literal` and the check keeps comparing against the old word: every
subquery stops being recognised, `in_` stops rendering a subquery, and nothing
raises, because a value that is not a subquery is a legitimate case.

THIS TEST READS THE DECLARATION, so it cannot pass by holding the same copy.
It asks the type what its tag is and requires the recogniser to agree.
"""

from __future__ import annotations

from typing import get_args, get_type_hints

import pytest

from declaro_persistum.query.expressions import SubqueryExpr, is_subquery

pytestmark = pytest.mark.precommit


def _the_declared_tag() -> str:
    """The one word `Subquery` says its tag is."""
    declared = get_type_hints(SubqueryExpr)["kind"]
    tags = get_args(declared)
    assert len(tags) == 1, f"`kind` declares {tags}, and this reads one tag"
    return tags[0]


def test_the_recogniser_agrees_with_the_declaration() -> None:
    """Build one from the declared tag and require the recogniser to know it."""
    tag = _the_declared_tag()

    assert is_subquery({"kind": tag, "query": None}), (
        f"`SubqueryExpr` declares its tag is {tag!r} and `is_subquery` does not "
        f"recognise a value carrying it. The word is written in two places and "
        f"they have drifted."
    )


def test_a_value_carrying_another_tag_is_not_a_subquery() -> None:
    """The floor. A recogniser that says yes to everything passes the test above."""
    assert not is_subquery({"kind": "case", "query": None})
    assert not is_subquery({"kind": "order_by"})
    assert not is_subquery({"no": "kind at all"})
    assert not is_subquery("a string")
