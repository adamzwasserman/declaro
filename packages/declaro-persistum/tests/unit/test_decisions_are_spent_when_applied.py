"""A decision answers a question once, and is then spent.

`migrations/pending.toml` records how a human resolved an ambiguity: rename
this column, or drop it and add the other. `load_decisions` reads it on every
diff, and NOTHING deleted it after a successful apply.

So a decision keyed on a column name went on answering. Rename `name` to
`full_name`, decide "rename", apply; then months later drop `name` and add
`full_name_2` — the stored decision still matches the key and the engine
resolves an ambiguity nobody was asked about.

`clear_decisions` existed for exactly this and was deleted in `162d967` for
having no caller. Deleting a function nothing calls is right; the defect was
that its JOB had no caller either, and removing the function made the gap
harder to see rather than closing it.
"""

from __future__ import annotations

import pytest

from declaro_persistum.loader import clear_decisions, load_decisions, save_decisions

pytestmark = pytest.mark.precommit


def test_a_saved_decision_can_be_read_back(tmp_path) -> None:
    save_decisions(tmp_path, {"users_name_rename": {"action": "rename"}})
    assert "users_name_rename" in load_decisions(tmp_path)


def test_clearing_leaves_nothing_for_the_next_diff_to_read(tmp_path) -> None:
    save_decisions(tmp_path, {"users_name_rename": {"action": "rename"}})
    clear_decisions(tmp_path)
    assert load_decisions(tmp_path) == {}


def test_clearing_twice_is_not_an_error(tmp_path) -> None:
    """The caller wants there to be no pending decisions, and there are none.

    A second clear raising would make every caller wrap it in a try, which is
    the same defensive shape Rule 13 rejects.
    """
    clear_decisions(tmp_path)
    clear_decisions(tmp_path)
    assert load_decisions(tmp_path) == {}


def test_the_apply_command_spends_them() -> None:
    """Asserted on the source, because the alternative is a full CLI run.

    Named explicitly so that deleting the call fails here rather than silently
    restoring the defect.
    """
    import inspect

    from declaro_persistum.cli import commands

    source = inspect.getsource(commands.cmd_apply)
    assert "clear_decisions(schema_dir)" in source, (
        "a successful apply leaves pending.toml in place, so its decisions "
        "answer the next diff's questions too"
    )
