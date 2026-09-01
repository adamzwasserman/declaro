"""Every path that writes DDL asks the validator, and none of them did.

Three paths in this package apply migration operations:

    apply_migrations_async   the library entry point
    cmd_apply                `declaro apply`
    cmd_migrate_remote       `declaro migrate-remote`

On 2026-08-19 none of them called `validate_schema`. It was called from
`cmd_validate` and nowhere else, so every rule the validator holds fired only
under a command an author has to think to run, and protected none of the
operations they were written for.

Each was fixed separately and each fix was found the same way: by asking where
else the same question goes unasked. That is the pattern this gate exists to
close. Adding a fourth path is easy; noticing that it skipped the validator is
not, and a reviewer will not notice from a diff.

WHAT THIS READS is the source of each function, for a call to the validator.
It is a coarse instrument and it says so: it cannot tell whether the call is
reached, whether it acts on the result, or whether it happens before the DDL.
Those are what the three behavioral files beside it assert. This one catches
the case none of them can, which is a fourth path nobody wrote a file for.
"""

from __future__ import annotations

import inspect

import pytest

import declaro_persistum
from declaro_persistum.cli.commands import cmd_apply, cmd_migrate_remote
from declaro_persistum.migrations import apply_migrations_async

pytestmark = [pytest.mark.precommit]

# Every function that reaches an applier with dry_run False. Named here rather
# than discovered, because a discovery rule loose enough to find them all also
# finds the appliers themselves.
MIGRATES = {
    "apply_migrations_async": apply_migrations_async,
    "cmd_apply": cmd_apply,
    "cmd_migrate_remote": cmd_migrate_remote,
}


@pytest.mark.parametrize("name", sorted(MIGRATES))
def test_it_calls_the_validator(name: str) -> None:
    source = inspect.getsource(MIGRATES[name])

    assert "validate_schema(" in source, (
        f"{name} writes DDL and never asks the validator. Every rule it holds "
        f"would fire only under `declaro validate`."
    )


@pytest.mark.parametrize("name", sorted(MIGRATES))
def test_it_acts_on_the_errors_and_not_the_warnings(name: str) -> None:
    """An unknown type is a warning and must keep migrating.

    A path that stopped on warnings would refuse a custom type or an enum,
    and one that read neither list would have made a call and ignored it.
    """
    source = inspect.getsource(MIGRATES[name])

    assert '["errors"]' in source, f"{name} calls the validator and ignores it"
    assert '["warnings"]' not in source, (
        f"{name} acts on warnings, which would refuse schemas that work"
    )


def test_the_list_still_matches_what_reaches_an_applier() -> None:
    """The floor under the gate.

    A fourth migrating function added without a line here would leave this
    file green while the path it added went unchecked.

    IT USED TO READ TWO NAMED MODULES, `migrations` and `cli.commands`. On
    2026-08-22 both were split for length, one call moved to
    `migration_sql.py`, and the count fell to three while nothing about the
    code had changed. A scan that names its files goes blind the moment a
    file is renamed or divided, and it goes blind quietly: the number falls
    and reads like a path that was removed.

    It reads every module in the package now, so a call cannot escape it by
    moving house.
    """
    import pathlib

    package = pathlib.Path(declaro_persistum.__file__).parent
    calls = sum(
        source.count("await apply_operations(") + source.count("await apply_migrations_async(")
        for module in sorted(package.rglob("*.py"))
        for source in [module.read_text()]
    )

    assert calls == 4, (
        f"{calls} calls reach an applier across the package; the gate above "
        f"covers three functions holding four. A new one needs a line in "
        f"MIGRATES and a file of its own."
    )
