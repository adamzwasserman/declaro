"""Declaring `on_delete="no action"` proposed a migration on every run.

MEASURED 2026-08-20 against SQLite. A table whose foreign key already carries
`ON DELETE NO ACTION`, compared against a model that declares
`on_delete="no action"`, emitted two operations:

    drop_foreign_key post {'column': 'author_id'}
    add_foreign_key  post {'column': 'author_id', ..., 'on_delete': 'no action'}

Applying them changes nothing, so the next run emits the same two. The schema
never converges and every deploy drops and rebuilds a constraint that was
already correct.

WHY THE INSPECTOR IS NOT THE THING TO FIX. `PRAGMA foreign_key_list` reports
`NO ACTION` for a plain `REFERENCES author(id)` and for an explicit
`ON DELETE NO ACTION`, byte for byte:

    plain  -> (0, 0, 'author', 'a', 'id', 'NO ACTION', 'NO ACTION', 'NONE')
    noact  -> (0, 0, 'author', 'a', 'id', 'NO ACTION', 'NO ACTION', 'NONE')

The two are indistinguishable in the database because they ARE the same
constraint: no action is what SQL does when no clause is written. So an
inspector that reported `"no action"` would put the loop on every plain
foreign key instead, which is the common case rather than the rare one. The
guard that drops it is right.

The defect is that the differ compared the two spellings with `!=`. A
vocabulary that lets a model say `no action` while the database can only ever
say nothing guarantees the two disagree forever. They are now compared as
equal, in both directions, for `on_delete` and `on_update` alike.
"""

import pathlib

import pytest

from declaro_persistum import close, open_sqlite, reading, writing
from declaro_persistum.differ.core import diff
from declaro_persistum.inspector import INSPECTORS
from declaro_persistum.types import NOTHING


async def _live_schema(tmp_path: pathlib.Path, ddl: tuple[str, ...]) -> dict:
    database = await open_sqlite(
        str(tmp_path / "t.db"), shutdown="exit_immediately", busy_timeout_s=5.0
    )
    try:
        async with writing(database) as conn:
            for statement in ddl:
                await conn.execute(statement)
            await conn.commit()
        async with reading(database) as conn:
            return await INSPECTORS["sqlite"](conn, schema_name=NOTHING, include_views=False)
    finally:
        await close(database)


_AUTHOR = 'CREATE TABLE "author" ("id" INTEGER PRIMARY KEY)'


def _declared(**fk: str) -> dict:
    return {
        "author": {"columns": {"id": {"type": "integer", "primary_key": True}}},
        "post": {
            "columns": {
                "id": {"type": "integer", "primary_key": True},
                "author_id": {"type": "integer", "references": "author.id", **fk},
            }
        },
    }


@pytest.mark.parametrize(
    "clause",
    ["", " ON DELETE NO ACTION"],
    ids=["no clause at all", "an explicit ON DELETE NO ACTION"],
)
async def test_declaring_no_action_migrates_nothing(
    tmp_path: pathlib.Path, clause: str
) -> None:
    """Both spellings in the database are the same constraint the model names.

    The parametrisation is the point. SQLite cannot tell the two apart, so a
    fix that only handled the explicit clause would still loop on the plain
    one, and a reader would have no way to see which case was covered.
    """
    live = await _live_schema(
        tmp_path,
        (_AUTHOR, f'CREATE TABLE "post" ("id" INTEGER PRIMARY KEY, '
                  f'"author_id" INTEGER REFERENCES "author"("id"){clause})'),
    )

    result = diff(live, _declared(on_delete="no action"), dialect="sqlite", decisions=NOTHING)

    assert result["operations"] == []


async def test_declaring_nothing_against_the_same_database_migrates_nothing(
    tmp_path: pathlib.Path,
) -> None:
    """The other direction, which is the case that must not regress.

    A model that says nothing about deletion is the overwhelmingly common one.
    Any fix that taught the inspector to report `"no action"` would pass the
    test above and fail this one.
    """
    live = await _live_schema(
        tmp_path,
        (_AUTHOR, 'CREATE TABLE "post" ("id" INTEGER PRIMARY KEY, '
                  '"author_id" INTEGER REFERENCES "author"("id"))'),
    )

    result = diff(live, _declared(), dialect="sqlite", decisions=NOTHING)

    assert result["operations"] == []


async def test_a_real_cascade_is_still_seen_as_a_change(
    tmp_path: pathlib.Path,
) -> None:
    """The negative control, so the equality is not simply switched off.

    Treating absence and `no action` as equal must not make every `on_delete`
    comparison equal. Cascade is the opposite answer to what happens to the
    children, and a schema that changes its mind still has to migrate.
    """
    live = await _live_schema(
        tmp_path,
        (_AUTHOR, 'CREATE TABLE "post" ("id" INTEGER PRIMARY KEY, '
                  '"author_id" INTEGER REFERENCES "author"("id"))'),
    )

    result = diff(live, _declared(on_delete="cascade"), dialect="sqlite", decisions=NOTHING)

    assert [op["op"] for op in result["operations"]] == [
        "drop_foreign_key",
        "add_foreign_key",
    ]
