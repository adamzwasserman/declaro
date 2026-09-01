"""Every operation the differ can emit must render on every engine.

WRITTEN BEFORE ANY FIX, against a vocabulary measured rather than assumed.

The differ emits 16 operation types. Three engines render them, through two
dispatch tables: `postgresql.OPERATION_SQL` and `shared._SQL_GENERATORS`,
the second serving both SQLite and Turso. All three tables hold 16 keys and
the sets are equal, so nothing is missing by name.

WHAT WAS MISSING IS THAT EIGHT OF THE RENDERERS HAD NEVER RUN. Measured
2026-08-18 with `coverage json` over the whole suite, counting a function as
dark when not one line of its body executed:

    add_constraint_sql      drop_constraint_sql
    add_foreign_key_sql     drop_foreign_key_sql
    alter_column_sql        rename_column_sql
    rename_table_sql        drop_index_sql

A key in a dispatch table is a promise that the value behind it works. Eight
of those promises had never been called in, and the same shape had already
shipped one defect: `declaro generate` raised AttributeError on SQLite and on
Turso because only one of the three modules carried the loop.

EVERY OPERATION HERE COMES OUT OF THE DIFFER, NOT OUT OF MY HEAD. Building
the input by hand is how a test comes to assert against a shape production
never produces, and this suite has been bitten by it repeatedly -- an
operation keyed `type` when the key is `op`, an enum table named
`_dp_enum_status(value)` when the format is `table.column`. `_corpus()` below
runs a real schema pair through `diff()` and keeps what comes back.

WHAT THIS DOES NOT COVER: that the rendered SQL is CORRECT, only that it
renders and that SQLite accepts the subset SQLite can be given without
further state. Whether `ALTER TABLE ... RENAME TO` names the right table is
not asked here.
"""

from __future__ import annotations

import sqlite3
from typing import Any

import pytest

from declaro_persistum.abstractions.enums import expand_schema_enums
from declaro_persistum.applier import SQL_GENERATORS, generate_sql
from declaro_persistum.differ.core import diff
from declaro_persistum.differ.extended import diff_views
from declaro_persistum.types import NOTHING, Operation

pytestmark = pytest.mark.precommit

ENGINES = sorted(SQL_GENERATORS)

BASE: dict[str, Any] = {
    "users": {
        "columns": {
            "id": {"type": "integer", "primary_key": True},
            "name": {"type": "text"},
        }
    }
}


def _ops(current: dict[str, Any], target: dict[str, Any]) -> list[Operation]:
    return diff(current, target, engine="sqlite", decisions=NOTHING)["operations"]


def _corpus() -> dict[str, Operation]:
    """One real operation of each type, produced by the differ itself."""
    with_email = {"users": {"columns": {**BASE["users"]["columns"], "email": {"type": "text"}}}}
    with_index = {"users": {**BASE["users"], "indexes": {"ix_n": {"columns": ["name"]}}}}
    posts_plain = {
        "users": BASE["users"],
        "posts": {"columns": {"id": {"type": "integer", "primary_key": True}}},
    }
    posts_fk = {
        "users": BASE["users"],
        "posts": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "uid": {"type": "integer", "references": "users.id"},
            }
        },
    }
    posts_no_fk = {
        "users": BASE["users"],
        "posts": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "uid": {"type": "integer"},
            }
        },
    }
    enum_schema = expand_schema_enums(
        {
            "t": {
                "columns": {
                    "id": {"type": "integer", "primary_key": True},
                    "status": {"type": "text", "literal_values": ["a", "b"]},
                }
            }
        }
    )

    produced: list[Operation] = [
        *_ops({}, BASE),
        *_ops(BASE, {}),
        *_ops(BASE, with_email),
        *_ops(with_email, BASE),
        *_ops(
            BASE,
            {
                "users": {
                    "columns": {
                        "id": {"type": "integer", "primary_key": True},
                        "name": {"type": "integer"},
                    }
                }
            },
        ),
        *_ops(BASE, with_index),
        *_ops(with_index, BASE),
        *_ops({}, enum_schema),
        *_ops(posts_plain, posts_fk),
        *_ops(posts_fk, posts_no_fk),
        *_ops(
            {
                "users": {
                    **BASE["users"],
                    "constraints": {"ck": {"type": "check", "expression": "name <> ''"}},
                }
            },
            {
                "users": {
                    **BASE["users"],
                    "constraints": {"ck2": {"type": "check", "expression": "name <> 'x'"}},
                }
            },
        ),
        *_ops(BASE, {"customers": {**BASE["users"], "renamed_from": "users"}}),
        *_ops(
            BASE,
            {
                "users": {
                    "columns": {
                        "id": {"type": "integer", "primary_key": True},
                        "full_name": {"type": "text", "renamed_from": "name"},
                    }
                }
            },
        ),
        *diff_views({}, {"v": {"name": "v", "query": "SELECT 1"}}),
        *diff_views({"v": {"name": "v", "query": "SELECT 1"}}, {}),
    ]

    first: dict[str, Operation] = {}
    for operation in produced:
        first.setdefault(operation["op"], operation)
    return first


CORPUS = _corpus()


def test_the_corpus_covers_every_operation_the_differ_declares() -> None:
    """The floor under every test below.

    Without this, an operation type dropping out of the corpus would silently
    reduce the coverage of the parametrised tests and every one of them would
    still be green.
    """
    declared = set(Operation.__annotations__["op"].__args__)

    assert set(CORPUS) == declared, (
        f"the corpus is missing {sorted(declared - set(CORPUS))} and carries "
        f"{sorted(set(CORPUS) - declared)} that the type does not declare"
    )


# `apply()` performs these three by rebuilding the table, on SQLite and on
# Turso, and reports success. So the preview must not claim they are
# impossible: whatever it prints, it must not raise.
APPLY_RECONSTRUCTS = ("add_foreign_key", "drop_foreign_key", "alter_column")


@pytest.mark.parametrize("operation_type", sorted(CORPUS))
@pytest.mark.parametrize("engine", ENGINES)
def test_every_operation_renders_or_refuses_in_words_a_reader_can_act_on(
    operation_type: str, engine: str
) -> None:
    """A key in the dispatch table is a promise the value behind it works.

    Some operations genuinely cannot be performed: SQLite has no ADD
    CONSTRAINT. Refusing is the right answer there. Refusing with the string
    `CODE NOT WRITTEN`, which is what five of these did, is not -- it tells the
    reader nothing about what they asked for or what to do instead.
    """
    try:
        rendered = generate_sql([CORPUS[operation_type]], [0], engine)
    except NotImplementedError as refusal:
        assert operation_type not in APPLY_RECONSTRUCTS, (
            f"{operation_type} on {engine} refuses to render, and apply() "
            f"performs it by reconstruction: {refusal}"
        )
        message = str(refusal)
        assert len(message) > 40, f"a refusal a reader cannot act on: {message!r}"
        assert "CODE NOT WRITTEN" not in message, message
        return

    assert rendered and rendered[0].strip(), f"{operation_type} on {engine} rendered {rendered!r}"


@pytest.mark.parametrize("operation_type", APPLY_RECONSTRUCTS)
@pytest.mark.parametrize("engine", ENGINES)
def test_the_preview_does_not_refuse_what_apply_performs(operation_type: str, engine: str) -> None:
    """The measured defect, stated on its own so it cannot be lost.

    `declaro apply` rebuilt the table and reported success. `declaro generate`,
    handed the same operation, died with `NotImplementedError: CODE NOT
    WRITTEN`. Two preview paths asked whether an operation needs
    reconstruction and only `dry_run_preview` knew.
    """
    rendered = generate_sql([CORPUS[operation_type]], [0], engine)

    assert rendered and rendered[0].strip(), f"{operation_type} on {engine} rendered {rendered!r}"


@pytest.mark.parametrize("operation_type", sorted(CORPUS))
@pytest.mark.parametrize("engine", ENGINES)
def test_no_rendered_operation_carries_a_python_repr(operation_type: str, engine: str) -> None:
    """The failure that renders and is still wrong.

    A dict or a list interpolated into SQL produces text that passes every
    substring assertion and no parser. This package has shipped that exact
    defect: the differ's `{"from": x, "to": y}` change record reached a column
    definition and `_map_type` called `.lower()` on it.
    """
    try:
        rendered = generate_sql([CORPUS[operation_type]], [0], engine)[0]
    except NotImplementedError:
        return

    for tell in ("{'", "': ", "[{", "None", "True", "False"):
        assert tell not in rendered, (
            f"{operation_type} on {engine} rendered a Python value: {rendered!r}"
        )


@pytest.mark.parametrize("operation_type", sorted(CORPUS))
def test_a_self_contained_operation_parses_on_sqlite(operation_type: str) -> None:
    """Rendering is not parsing, and this suite has confused the two before.

    Only the operations that need no prior state are run here. The rest need a
    table to alter, which is the applier's job to arrange and is covered by the
    migration tests.
    """
    self_contained = {"create_table", "create_view", "drop_view"}
    if operation_type not in self_contained:
        pytest.skip(f"{operation_type} needs prior state")

    rendered = generate_sql([CORPUS[operation_type]], [0], "sqlite")[0]
    connection = sqlite3.connect(":memory:")
    if operation_type == "drop_view":
        connection.execute("CREATE VIEW v AS SELECT 1")

    connection.execute(rendered)
