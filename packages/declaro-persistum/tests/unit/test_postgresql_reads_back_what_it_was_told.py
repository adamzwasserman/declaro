"""PostgreSQL introspection had never read most of what it can read.

MEASURED 2026-08-18 with `coverage json` over the whole suite. Unrun in
`inspector/postgresql.py`:

    a foreign key's ON DELETE and ON UPDATE actions
    a composite primary key
    an array type, `_text` -> `text[]`
    a length, varchar(n)
    a precision and scale, numeric(p,s)
    an index's uniqueness, its WHERE predicate, its method
    introspect_views

Every one of those is a property the differ compares. A property that does not
survive a write and a read is a migration that runs on every deploy forever,
and this package has shipped that failure five times: a boolean default, a
CHECK read as None, a CHECK rewritten by the engine, a folded table name, and
a folded foreign-key target.

CONVERGENCE IS THE ORACLE. Declare it, apply it, read it back, diff again.
Zero operations is the only pass. Asserting the introspected shape against a
dict written here would pin whatever the inspector does today, including
whatever it does wrong.
"""

from __future__ import annotations

from typing import Any

import pytest

from declaro_persistum.applier import apply
from declaro_persistum.differ.core import diff
from declaro_persistum.inspector import introspect
from declaro_persistum.types import NOTHING
from tests.conftest import probe_url, refuse_without_postgresql

pytestmark = [pytest.mark.precommit, pytest.mark.postgresql, pytest.mark.integration]

PROBE_URL = probe_url()

PARENT: dict[str, Any] = {"columns": {"id": {"type": "integer", "primary_key": True}}}

# Each case is one declaration the engine has to give back unchanged.
CASES: dict[str, dict[str, Any]] = {
    "a foreign key with ON DELETE": {
        "parents": PARENT,
        "children": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "pid": {
                    "type": "integer",
                    "references": "parents.id",
                    "on_delete": "cascade",
                },
            }
        },
    },
    "a foreign key with ON UPDATE": {
        "parents": PARENT,
        "children": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "pid": {
                    "type": "integer",
                    "references": "parents.id",
                    "on_update": "cascade",
                },
            }
        },
    },
    "a composite primary key": {
        "pairs": {
            "columns": {
                "left": {"type": "integer"},
                "right": {"type": "integer"},
            },
            "primary_key": ["left", "right"],
        }
    },
    "a varchar with a length": {
        "labelled": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "code": {"type": "varchar(12)"},
            }
        }
    },
    "a numeric with precision and scale": {
        "priced": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "amount": {"type": "numeric(10,2)"},
            }
        }
    },
    "a unique index": {
        "tagged": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "slug": {"type": "text"},
            },
            "indexes": {"ix_slug": {"columns": ["slug"], "unique": True}},
        }
    },
    "a plain index": {
        "tagged": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "slug": {"type": "text"},
            },
            "indexes": {"ix_slug": {"columns": ["slug"]}},
        }
    },
}


async def _connection() -> Any:
    import asyncpg

    try:
        return await asyncpg.connect(PROBE_URL)
    except (OSError, asyncpg.PostgresError) as unreachable:
        refuse_without_postgresql(PROBE_URL, unreachable)


async def _clean(connection: Any, declared: dict[str, Any]) -> None:
    for name in declared:
        await connection.execute(f'DROP TABLE IF EXISTS "{name}" CASCADE')


async def _deploy(connection: Any, declared: dict[str, Any]) -> None:
    actual = await introspect(connection, "postgresql")
    plan = diff(
        {n: actual[n] for n in declared if n in actual},
        declared,
        dialect="postgresql",
        decisions=NOTHING,
    )
    await apply(
        connection,
        plan["operations"],
        plan["execution_order"],
        "postgresql",
        dry_run=False,
    )


@pytest.mark.parametrize("label", sorted(CASES))
async def test_it_applies(label: str) -> None:
    """The floor. A declaration the applier refuses cannot round-trip at all."""
    declared = CASES[label]
    connection = await _connection()
    try:
        await _clean(connection, declared)

        await _deploy(connection, declared)
    finally:
        await _clean(connection, declared)
        await connection.close()


@pytest.mark.parametrize("label", sorted(CASES))
async def test_it_reads_back_and_converges(label: str) -> None:
    """A second deploy has no work. Anything else migrates forever."""
    declared = CASES[label]
    connection = await _connection()
    try:
        await _clean(connection, declared)
        await _deploy(connection, declared)

        actual = await introspect(connection, "postgresql")
        remaining = diff(
            {n: actual[n] for n in declared if n in actual},
            declared,
            dialect="postgresql",
            decisions=NOTHING,
        )["operations"]

        assert remaining == [], (
            f"{label} migrates on every deploy: "
            f"{[(o['op'], o['table'], o['details']) for o in remaining]}"
        )
    finally:
        await _clean(connection, declared)
        await connection.close()


@pytest.mark.parametrize("label", sorted(CASES))
async def test_the_declared_tables_are_all_there(label: str) -> None:
    """A migration that dropped everything would converge against nothing."""
    declared = CASES[label]
    connection = await _connection()
    try:
        await _clean(connection, declared)
        await _deploy(connection, declared)

        actual = await introspect(connection, "postgresql")

        assert set(declared) <= set(actual), sorted(set(declared) - set(actual))
    finally:
        await _clean(connection, declared)
        await connection.close()
