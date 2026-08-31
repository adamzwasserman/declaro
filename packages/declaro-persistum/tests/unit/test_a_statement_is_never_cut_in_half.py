"""The appliers glued statements together with `;` and cut them apart again.

MEASURED 2026-08-21. Three generators join their statements with a semicolon
and the SQLite and Turso appliers split the string back apart with
`str.split(";")`. A semicolon is not a statement separator. It is an ordinary
character inside a trigger body and inside a string literal, and two legal
declarations break on it.

A materialized view refreshed by trigger generates fifteen fragments. The
split cuts the first trigger between BEGIN and END:

    CREATE TRIGGER ... AFTER INSERT ON "orders" BEGIN DELETE FROM "v_live"

SQLite answers `incomplete input` and the whole migration fails. Turso answers
the same and reports a skipped operation rather than raising, so the two
engines disagree about whether the migration failed at all.

An enum value holding a semicolon breaks the other way. `Literal["ok", "a;b"]`
is ordinary Python and the loader accepts it. The generator escapes
apostrophes, which is the escape SQL has, and no escape exists for a semicolon
inside a literal because none is needed. The split produced:

    INSERT INTO "_dp_enum_status" (value) VALUES ('a
    b')

THE JOIN IS THE BUG, NOT THE SPLIT. Nothing has to be parsed if nothing was
glued together, so `statements_for` returns the list the generator built and
the appliers execute it in order. `generate_operation_sql` still joins, because
a dry run prints for a person to read and nobody parses that back.

This package has three hand-rolled SQL scanners already, each right for one
caller. A fourth, taught about BEGIN and END and string literals, would have
fixed both cases here and left the next generator to find the same trap.
"""

import pathlib

import pytest

from declaro_persistum import close, open_sqlite, open_turso
from declaro_persistum.applier import apply
from declaro_persistum.applier.shared import (
    generate_operation_sql,
    statements_for,
)
from declaro_persistum.applier.sqlite import generate_create_view
from declaro_persistum.database import reading, writing
from declaro_persistum.types import NOTHING

_TRIGGERED = {
    "name": "v_live",
    "query": "SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id",
    "materialized": True,
    "refresh": "trigger",
    "depends_on": ["orders"],
    "trigger_sources": ["orders"],
}


def test_a_trigger_body_is_one_statement() -> None:
    """BEGIN and END wrap semicolons that separate nothing.

    Asserted on the statement list rather than on a database, because this is
    where the cut happened. A trigger that arrives whole here arrives whole
    everywhere.
    """
    statements = statements_for({"op": "create_view", "table": "_views", "details": _TRIGGERED})

    triggers = [s for s in statements if s.lstrip().upper().startswith("CREATE TRIGGER")]
    assert triggers, f"no trigger among {len(statements)} statements"
    for trigger in triggers:
        assert trigger.rstrip().upper().endswith("END"), (
            f"a trigger was cut short: ...{trigger[-60:]!r}"
        )


def test_a_semicolon_inside_a_value_is_not_a_separator() -> None:
    """`a;b` is one enum value, so it is one INSERT."""
    statements = statements_for(
        {
            "op": "insert_enum_values",
            "table": "_dp_enum_status",
            "details": {"values": ["ok", "a;b"]},
        }
    )

    assert len(statements) == 2, f"got {len(statements)}: {statements}"
    assert "'a;b'" in statements[1], statements[1]


def test_a_dry_run_still_reads_as_one_block() -> None:
    """The joined form survives for display, which is all it was ever for.

    A person reading `declaro apply --dry-run` wants the statements together.
    Nothing parses that string back, which is exactly why joining is safe here
    and was not safe on the execution path.
    """
    rendered = generate_operation_sql(
        {
            "op": "insert_enum_values",
            "table": "_dp_enum_status",
            "details": {"values": ["ok", "pending"]},
        }
    )

    assert isinstance(rendered, str)
    assert rendered.count("INSERT INTO") == 2


def test_the_generator_and_the_display_agree() -> None:
    """The joined text holds every statement the list holds, in order.

    Without this the two could drift, and a dry run would print a migration
    that is not the one about to be applied.
    """
    operation = {
        "op": "insert_enum_values",
        "table": "_dp_enum_status",
        "details": {"values": ["ok", "pending", "done"]},
    }

    statements = statements_for(operation)
    rendered = generate_operation_sql(operation)

    assert rendered == ";\n".join(statements)


@pytest.mark.parametrize("engine", ["sqlite", "turso"], ids=["sqlite", "turso"])
async def test_a_triggered_view_reaches_the_database(tmp_path: pathlib.Path, engine: str) -> None:
    """The end of it: the declaration becomes a view the engine holds.

    The unit assertions above say the statements are whole. Only the engine
    can say they are SQL, and only the engine caught this in the first place.
    """
    if engine == "sqlite":
        database = await open_sqlite(
            str(tmp_path / "s.db"), shutdown="exit_immediately", busy_timeout_s=5.0
        )
    else:
        database = await open_turso(
            str(tmp_path / "u.db"), shutdown="exit_immediately", primary=NOTHING, token=NOTHING
        )

    try:
        async with writing(database) as conn:
            await conn.execute(
                'CREATE TABLE "orders" ("id" INTEGER, "user_id" INTEGER, "amount" INTEGER)'
            )
            await conn.commit()

        operations = [{"op": "create_view", "table": "_views", "details": _TRIGGERED}]
        async with writing(database) as conn:
            await apply(conn, operations, [0], engine, dry_run=False)

        async with reading(database) as conn:
            cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'")
            triggers = [row[0] for row in await cursor.fetchall()]

        assert len(triggers) == 3, f"{engine} holds {triggers}"
    finally:
        await close(database)


def test_the_enum_generator_returns_a_list() -> None:
    """The contract, stated where a future caller will look for it.

    `insert_enum_values_sql` used to return a joined string because the
    appliers split. It returns the list now, and nothing between it and the
    engine has to know what a semicolon means.
    """
    values = statements_for(
        {
            "op": "insert_enum_values",
            "table": "_dp_enum_status",
            "details": {"values": ["ok", "a;b"]},
        }
    )

    assert isinstance(values, list)
    assert len(values) == 2


def test_a_create_view_generator_returns_a_list() -> None:
    """The same for the other generator that needs more than one statement."""
    statements = generate_create_view(_TRIGGERED)

    assert isinstance(statements, list)
    assert len(statements) > 1, "a triggered view is more than one statement"
