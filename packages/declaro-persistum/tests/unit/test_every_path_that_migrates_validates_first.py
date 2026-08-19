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
Those are what the three behavioural files beside it assert. This one catches
the case none of them can, which is a fourth path nobody wrote a file for.
"""

from __future__ import annotations

import inspect

import pytest

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
    file green while the path it added went unchecked. This counts the
    `apply_operations` and `apply_migrations_async` calls in the two modules
    the list covers, so a new one shows up as a mismatch.
    """
    from declaro_persistum import migrations
    from declaro_persistum.cli import commands

    calls = 0
    for module in (migrations, commands):
        source = inspect.getsource(module)
        calls += source.count("await apply_operations(")
        calls += source.count("await apply_migrations_async(")

    assert calls == 4, (
        f"{calls} calls reach an applier across migrations.py and "
        f"cli/commands.py; the gate above covers three functions holding "
        f"four. A new one needs a line in MIGRATES and a file of its own."
    )
