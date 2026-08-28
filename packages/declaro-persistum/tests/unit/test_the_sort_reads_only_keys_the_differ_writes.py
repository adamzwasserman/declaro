"""What the sort reads out of an operation, the differ has to put in.

`differ/toposort.py` reads operation details with a default at nine places:

    op["details"].get("column", "")
    op["details"].get("index", "")
    op["details"].get("references", "")
    op["details"].get("definition", {})

Each default is silent. An operation missing the key it is asked for does not
fail; it gets an empty string or an empty dict, matches nothing, and receives
no dependency edge. The migration is then ordered as though that operation
depended on nothing.

THAT IS THE DEFECT SHAPE ALREADY FOUND TODAY, one level up. `drop_table`
carried an empty details dict, so nothing connected two drops and the order
was whatever Kahn's algorithm happened to emit. On PostgreSQL, dropping a
parent before its child fails; on SQLite it does not. It was found by a test
that passed alone and failed in the suite.

A DEFAULT IS NOT THE FIX AND NEITHER IS A GUARD. A key the differ always
writes needs no default, and a key it sometimes omits needs the sort to say
so rather than guess. This gate asks which it is, by running every operation
type the differ can emit through the sort's own readers and checking the key
is there.

It is a coarse instrument and says so: it checks presence, not correctness. A
`references` naming the wrong table passes here and is caught by the
convergence gates instead.
"""

from __future__ import annotations

import inspect
import re

import pytest

from declaro_persistum.abstractions.enums import expand_schema_enums
from declaro_persistum.differ import toposort
from declaro_persistum.differ.core import diff
from declaro_persistum.differ.extended import diff_views
from declaro_persistum.types import NOTHING

pytestmark = [pytest.mark.precommit]

USERS = {
    "columns": {
        "id": {"type": "integer", "primary_key": True},
        "name": {"type": "text"},
    }
}


def _every_operation() -> dict[str, dict]:
    """One real operation of each type, from the differ rather than by hand."""
    with_email = {"users": {"columns": {**USERS["columns"], "email": {"type": "text"}}}}
    with_index = {"users": {**USERS, "indexes": {"ix_n": {"columns": ["name"]}}}}
    posts_plain = {
        "users": USERS,
        "posts": {"columns": {"id": {"type": "integer", "primary_key": True}}},
    }
    posts_fk = {
        "users": USERS,
        "posts": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "uid": {"type": "integer", "references": "users.id"},
            }
        },
    }
    posts_no_fk = {
        "users": USERS,
        "posts": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "uid": {"type": "integer"},
            }
        },
    }

    def ops(current, target):
        return diff(current, target, dialect="sqlite", decisions=NOTHING)["operations"]

    produced = [
        *ops({}, {"users": USERS}),
        *ops({"users": USERS}, {}),
        *ops({"users": USERS}, with_email),
        *ops(with_email, {"users": USERS}),
        *ops({"users": USERS}, with_index),
        *ops(with_index, {"users": USERS}),
        *ops(posts_plain, posts_fk),
        *ops(posts_fk, posts_no_fk),
        *ops(
            {"users": USERS},
            {
                "users": {
                    "columns": {
                        "id": {"type": "integer", "primary_key": True},
                        "name": {"type": "integer"},
                    }
                }
            },
        ),
        *ops({"users": USERS}, {"customers": {**USERS, "renamed_from": "users"}}),
        *ops(
            {"users": USERS},
            {
                "users": {
                    "columns": {
                        "id": {"type": "integer", "primary_key": True},
                        "full_name": {"type": "text", "renamed_from": "name"},
                    }
                }
            },
        ),
        *ops(
            {},
            expand_schema_enums(
                {
                    "t": {
                        "columns": {
                            "id": {"type": "integer", "primary_key": True},
                            "status": {"type": "text", "literal_values": ["a", "b"]},
                        }
                    }
                }
            ),
        ),
        *ops(
            {
                "users": {
                    **USERS,
                    "constraints": {"ck": {"type": "check", "expression": "name <> ''"}},
                }
            },
            {
                "users": {
                    **USERS,
                    "constraints": {"ck2": {"type": "check", "expression": "name <> 'x'"}},
                }
            },
        ),
        *diff_views({}, {"v": {"name": "v", "query": "SELECT 1"}}),
        *diff_views({"v": {"name": "v", "query": "SELECT 1"}}, {}),
    ]

    first: dict[str, dict] = {}
    for operation in produced:
        first.setdefault(operation["op"], operation)
    return first


CORPUS = _every_operation()

# Which detail keys the sort reads, in either form. Read from the source, so a
# new read is noticed rather than assumed absent.
#
# BOTH FORMS, because the sort now reads most keys with `[]`. Eight of its nine
# reads used to carry a default and every one of those defaults was for a case
# the differ never produces: measured by running the differ over every kind of
# change, no operation ever omits a key it can carry. The one that survives is
# `depends_on`, which a view genuinely may not declare.
_SOURCE = inspect.getsource(toposort)
READS = sorted(
    set(re.findall(r'\["details"\]\.get\(\s*"([a-z_]+)"\s*,', _SOURCE))
    | set(re.findall(r'\["details"\]\[\s*"([a-z_]+)"\s*\]', _SOURCE))
)


def test_the_sort_still_reads_the_keys_this_file_knows_about() -> None:
    """The floor. A key the sort starts reading would go unchecked otherwise."""
    assert READS == [
        "changes",
        "column",
        "columns",
        "definition",
        "depends_on",
        "index",
        "name",
        "references",
    ], f"the sort reads {READS}; this file was written for a different set"


def test_only_a_key_the_differ_may_omit_is_read_with_a_default() -> None:
    """A default for a case that never arrives is a region nothing exercises.

    The differ never omits a key it can carry, measured by running it over
    every kind of change it handles. So a `.get(key, fallback)` here covers
    nothing real, and it hides the case it was meant to survive: reading
    `definition` off a `drop_index` returned an empty list on every migration
    ever run, so the edge that orders an index drop before the drop of a
    column it covers never once fired.

    `depends_on` is the exception and is genuinely optional: a view that
    selects from nothing declares no dependencies.
    """
    with_default = sorted(set(re.findall(r'\["details"\]\.get\(\s*"([a-z_]+)"\s*,', _SOURCE)))

    assert with_default == ["depends_on"], (
        f"the sort supplies a default for {with_default}. Every key but "
        f"depends_on is one the differ always writes, so a default there "
        f"covers a case that never arrives and hides one that does."
    )


@pytest.mark.parametrize("operation_type", sorted(CORPUS))
def test_no_operation_relies_on_a_default_the_sort_supplies(
    operation_type: str,
) -> None:
    """Whatever the sort reads from an operation, that operation must carry.

    An operation missing the key gets an empty string, matches nothing, and
    is ordered as though it depended on nothing. That is how two related
    tables came to be dropped in an undefined order.
    """
    operation = CORPUS[operation_type]
    details = operation["details"]

    # PRESENT, NOT NON-EMPTY. An empty list is a real answer: `drop_table`
    # carries `references: []` for a table that references nothing, and that
    # is exactly what the sort should read. The defect this guards is a key
    # ABSENT, where the default silently supplies "" and the operation
    # receives no dependency edge at all.
    #
    # The first version asserted non-empty and failed on that correct case,
    # which is the difference between "answered nothing" and "was not asked".
    blank = [key for key in READS if key in details and details[key] == ""]

    assert blank == [], (
        f"{operation_type} carries {blank} as the empty string, which the "
        f"sort reads to build dependency edges: {details}"
    )


def test_every_operation_type_the_differ_declares_is_in_the_corpus() -> None:
    """Without this, an operation dropping out reduces the coverage silently."""
    from declaro_persistum.types import Operation

    declared = set(Operation.__annotations__["op"].__args__)

    assert set(CORPUS) == declared, sorted(declared - set(CORPUS))
