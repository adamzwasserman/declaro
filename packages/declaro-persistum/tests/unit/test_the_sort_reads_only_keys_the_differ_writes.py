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
    with_email = {
        "users": {"columns": {**USERS["columns"], "email": {"type": "text"}}}
    }
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
                    "constraints": {
                        "ck2": {"type": "check", "expression": "name <> 'x'"}
                    },
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

# Which detail keys the sort reads, and for which operation types. Read from
# the source, so a new `.get` with a default is noticed rather than assumed
# absent.
READS = sorted(
    set(
        re.findall(
            r'\["details"\]\.get\(\s*"([a-z_]+)"\s*,',
            inspect.getsource(toposort),
        )
    )
)


def test_the_sort_still_reads_the_keys_this_file_knows_about() -> None:
    """The floor. A key added with a default would go unchecked otherwise."""
    assert READS == [
        "changes",
        "column",
        "columns",
        "definition",
        "depends_on",
        "index",
        "references",
    ], (
        f"the sort reads {READS} with defaults; this file was written for "
        f"a different set"
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
