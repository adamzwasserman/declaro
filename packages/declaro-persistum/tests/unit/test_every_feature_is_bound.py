"""Every .feature file must be bound by exactly one `scenarios()` call.

THE FAILURE THIS EXISTS TO STOP, stated as the shape rather than the instance:

A Gherkin file is prose. It only becomes a test when a step file calls
`scenarios()` on it. Nothing in pytest-bdd, pytest, or CI notices when nobody
does. An unbound feature file is indistinguishable from a bound one by reading
it, by the file tree, or by a green suite — it simply never runs, and the suite
reports success without it.

That is worse than having no file. A missing feature announces itself the first
time someone looks for it. An unbound one asserts that the behavior is
specified AND covered, and both halves are false.

FOUND ON THIS BRANCH, 2026-08-13, by counting rather than by reading:

    schema/materialized_views.feature    20 scenarios    bound by 0
    database/multi_backend.feature        5 scenarios    bound by 0

25 scenarios that have never executed. `database_steps.py` defines steps for
multi_backend and calls `scenarios()` nowhere at all, so it looks like a bound
feature from every angle except the one that matters.

THE OTHER DIRECTION IS ALSO A DEFECT, and it has happened here too: on the
sibling branch `pragma_compat.feature` was bound TWICE, by two different step
files. Every scenario ran twice, which wastes time and — worse — means a step
definition collision resolves silently to whichever module pytest imported
last. Exactly once is the invariant, not at least once.

THIS IS THE SAME CLASS AS test_every_module_is_reachable.py. There, a module
with tests but no production caller looked alive. Here, a feature file with
steps but no `scenarios()` call looks bound. Both are things that are only
verifiable by asking a question nobody thought to ask, so the fix in both cases
is a test that asks it every run.

KNOWN_UNBOUND is a RATCHET, not an allowlist to grow. A name in it is a
decision to ship a specification that does not execute, with the reason
written down. Removing one means the file was bound or deleted.
"""

from __future__ import annotations

import pathlib
import re

import pytest

pytestmark = pytest.mark.precommit

_ROOT = pathlib.Path(__file__).resolve().parents[2]
_FEATURES = _ROOT / "tests" / "bdd" / "features"
_STEPS = _ROOT / "tests" / "bdd"

# name -> why it ships unexecuted. Empty is the goal.
#
# THE `declared/` FILES ARE A SPECIFICATION, NOT COVERAGE. They describe the
# package HOW-IT-SHOULD-WORK.md says this is, and most of what they describe
# does not exist yet: three of the four grammars, declared views, declared
# constraints. Binding them today would turn 52 scenarios red and say nothing
# a reader could act on, because the answer to every one of them is "not
# built". They are here so the requirement is written down in the form the
# tests will take, and they are listed here so nobody mistakes a green suite
# for a package that meets them.
#
# EACH ONE COMES OFF THIS LIST WHEN ITS SECTION IS BUILT, not before.
_SPEC = (
    "the declarative surface HOW-IT-SHOULD-WORK.md describes; the code for it "
    "does not exist yet, and binding it would assert nothing a reader can act on"
)

_ONE_EACH = (
    "One function, one feature, written while the declaration was being "
    "reconciled against the features in September 2026. The function it "
    "describes is declared in docs/design/persistum.hd and not yet built, so "
    "binding it today would turn every scenario red and say only 'not built'. "
    "It comes off this list when its function exists."
)

KNOWN_UNBOUND: dict[str, str] = {
    "answer_from_rows.feature": _ONE_EACH,
    "answer_from_ticket.feature": _ONE_EACH,
    "budget_spent.feature": _ONE_EACH,
    "capability_of.feature": _ONE_EACH,
    "crew_size.feature": _ONE_EACH,
    "declaration_from_django.feature": _ONE_EACH,
    "declaration_from_prisma.feature": _ONE_EACH,
    "declaration_from_pseudo_sql.feature": _ONE_EACH,
    "declaration_from_sqlalchemy.feature": _ONE_EACH,
    "difference.feature": _ONE_EACH,
    "join_between.feature": _ONE_EACH,
    "mechanism_for.feature": _ONE_EACH,
    "migration_needed.feature": _ONE_EACH,
    "migration_report.feature": _ONE_EACH,
    "name_what_destroys.feature": _ONE_EACH,
    "name_what_is_ambiguous.feature": _ONE_EACH,
    "next_in_order.feature": _ONE_EACH,
    "next_wait.feature": _ONE_EACH,
    "order_operations.feature": _ONE_EACH,
    "permitted_connections.feature": _ONE_EACH,
    "progress_line.feature": _ONE_EACH,
    "receipt_for.feature": _ONE_EACH,
    "refusal_reaches_caller.feature": _ONE_EACH,
    "refuse_owned_names.feature": _ONE_EACH,
    "refuse_undeclared_columns.feature": _ONE_EACH,
    "refuse_undeclared_loss.feature": _ONE_EACH,
    "refuse_unusable_settings.feature": _ONE_EACH,
    "replicates.feature": _ONE_EACH,
    "schema_from_declared.feature": _ONE_EACH,
    "schema_inspect.feature": _ONE_EACH,
    "schema_read_declared.feature": _ONE_EACH,
    "spell_operation.feature": _ONE_EACH,
    "spell_view.feature": _ONE_EACH,
    "travels_together.feature": _ONE_EACH,
    "worth_retrying.feature": _ONE_EACH,
    "writer_waiting.feature": _ONE_EACH,
    "query_ask.feature": _ONE_EACH,
    "query_answer.feature": _ONE_EACH,
    "query_run_statement.feature": _ONE_EACH,
    "query_run_together.feature": _ONE_EACH,
    "query_begin_concurrent.feature": _ONE_EACH,
    "query_collect.feature": _ONE_EACH,
    "query_deliver.feature": _ONE_EACH,
    "room_make.feature": _ONE_EACH,
    "room_deposit.feature": _ONE_EACH,
    "room_release_ticket.feature": _ONE_EACH,
    "room_post_receipt.feature": _ONE_EACH,
    "room_run_write.feature": _ONE_EACH,
    "lifecycle_stop.feature": _ONE_EACH,
    "lifecycle_shut_down.feature": _ONE_EACH,
    "lifecycle_turn.feature": _ONE_EACH,
    "lifecycle_catch_up.feature": _ONE_EACH,
    "lifecycle_install_handler.feature": _ONE_EACH,
    "lifecycle_register.feature": _ONE_EACH,
    "lifecycle_read_registered.feature": _ONE_EACH,
    "turso_push.feature": _ONE_EACH,
    "turso_pull.feature": _ONE_EACH,
    "log_write.feature": _ONE_EACH,
    "clock_read.feature": _ONE_EACH,
    "session_connect.feature": _ONE_EACH,
    "session_open_connection.feature": _ONE_EACH,
    "session_open_writer.feature": _ONE_EACH,
    "session_close_connection.feature": _ONE_EACH,
    "a_condition_is_data.feature": _SPEC,
    "absence_is_an_answer.feature": _SPEC,
    "everything_gets_declared.feature": _SPEC,
    # The four grammars: what they mean, and how the library knows
    # which one you used. Three of the four do not exist.
    "the_models_are_the_truth.feature": _SPEC,
    "the_schema_is_declared.feature": _SPEC,
    # The decision the rest of the surface rests on: what an engine produces.
    # A Query today is {sql, params, engine} and carries SQL text, so four
    # engines would be four ways of building a string.
    # The schema axis: what a model may declare, what the library owns, what a
    # change would do before it does it, and what a view and an index are.
    "compile_statement.feature": _SPEC,
    # One door and the pure decision it rests on. Neither is built.
    "session_open.feature": _SPEC,
    "engine_of.feature": _SPEC,
    # Running a question: what is held, what comes back, how many connections,
    # and whose job retrying is.
    # The rules, as gates.
    "absence_survives_the_boundary.feature": _SPEC,
    "a_failure_is_a_value.feature": _SPEC,
    "nothing_reaches_past_the_surface.feature": _SPEC,
    # The enforcement gates. Each describes a check that does not exist yet, and
    # each one names what it would find today: 3 defaulted parameters, 12 modules
    # importing `logging` directly, and a purity check that reads I/O only and
    # lets a clock or a random source through.
    "nothing_takes_a_default.feature": _SPEC,
    "the_middle_is_pure.feature": _SPEC,
    "one_function_says_things.feature": _SPEC,
    "every_engine_is_in_every_table.feature": _SPEC,
    "what_the_library_owns.feature": _SPEC,
}

# `scenarios("../features/x.feature")` and `scenarios("../features")` both bind.
# The second form binds a whole DIRECTORY, which is why the resolver below has
# to compare directories and not only filenames.
_SCENARIOS_CALL = re.compile(r"""scenarios\(\s*['"]([^'"]+)['"]""")


def _feature_files() -> list[pathlib.Path]:
    return sorted(_FEATURES.rglob("*.feature"))


def _bindings() -> dict[pathlib.Path, list[pathlib.Path]]:
    """feature file -> the step files that bind it."""
    bound: dict[pathlib.Path, list[pathlib.Path]] = {f: [] for f in _feature_files()}
    for step_file in _STEPS.rglob("*.py"):
        text = step_file.read_text(encoding="utf-8")
        for raw in _SCENARIOS_CALL.findall(text):
            target = (step_file.parent / raw).resolve()
            for feature in bound:
                if target == feature or (target.is_dir() and target in feature.parents):
                    bound[feature].append(step_file)
    return bound


def _scenario_count(path: pathlib.Path) -> int:
    return len(
        [
            line
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip().startswith("Scenario")
        ]
    )


def test_no_feature_file_is_unbound():
    """A specification nobody runs is prose claiming to be a test."""
    bound = _bindings()
    unbound = [f for f, binders in bound.items() if not binders and f.name not in KNOWN_UNBOUND]

    assert not unbound, (
        "these .feature files are bound by no scenarios() call and have NEVER "
        "executed — they read as specified-and-covered while asserting "
        "nothing:\n"
        + "\n".join(
            f"    {f.relative_to(_FEATURES)}  ({_scenario_count(f)} scenarios)"
            for f in sorted(unbound)
        )
        + "\n\nBind it from a step file, delete it, or add it to KNOWN_UNBOUND "
        "with the reason it ships unexecuted."
    )


def test_no_feature_file_is_bound_twice():
    """Twice is not safer than once; it is a silent step-collision.

    Two step files calling scenarios() on one feature run every scenario
    twice, and a step defined in both resolves to whichever module pytest
    imported last — which is import order, not intent.
    """
    doubled = {f: binders for f, binders in _bindings().items() if len(binders) > 1}

    assert not doubled, (
        "these .feature files are bound more than once, so their scenarios run "
        "repeatedly and any duplicated step resolves by import order:\n"
        + "\n".join(
            f"    {f.relative_to(_FEATURES)}  bound by " + ", ".join(b.name for b in binders)
            for f, binders in sorted(doubled.items())
        )
    )


def test_the_ratchet_can_actually_see_a_feature_file():
    """The guard above is worthless if it finds nothing to guard.

    A discovery bug — wrong directory, wrong glob — would make both tests pass
    by examining an empty set. That is the vacuous-pass shape this package has
    shipped repeatedly, so the ratchet asserts its own reach.
    """
    features = _feature_files()
    assert features, f"no .feature files found under {_FEATURES}"
    assert any(_scenario_count(f) > 0 for f in features), (
        "every feature file parsed as zero scenarios, so the counter is broken "
        "and the reports above would be meaningless"
    )
