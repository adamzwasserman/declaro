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
    "Nothing is built. The source was deleted on 2026-09-21 and every function "
    "comes back one at a time, each binding its own feature as it lands. "
    "Originally: one function, one feature, written while the declaration was being "
    "reconciled against the features in September 2026. The function it "
    "describes is declared in docs/design/persistum.hd and not yet built, so "
    "binding it today would turn every scenario red and say only 'not built'. "
    "It comes off this list when its function exists."
)

_DELETED_2026_09_23 = (
    "Deleted on 2026-09-23, with its code, because the declaration moved under "
    "it. Eleven functions drifted from their card when the crew, the connection "
    "and the channel were reworked, and seven lost their card outright. Adam's "
    "reason for deleting rather than patching: a module left sitting there "
    "invites the next reader to edit it into agreement instead of writing what "
    "the card says. It comes off this list when its function is written back."
)

_NEEDS_A_DELETED_FUNCTION = (
    "Bound until 2026-09-23 by a step file that composed with a function deleted "
    "that day, or that asserted a rule the declaration dropped. `replicates` was "
    "derived from a primary and is now derived from the channel. "
    "`session_open_writer` asserted that a replicated database gets one connection "
    "and one drainer, which was the channel's limit applied to connections. "
    "`schema_read_declared` cross-checks every declared type against "
    "`fn spell_operation`, and `lifecycle_register` needs a `Database`, which only "
    "the opener makes. Each comes back when the function it needs does."
)

_READS_A_RECORD_THAT_MOVED = (
    "Deleted on 2026-09-23. Its arguments still matched its card, so a check on "
    "signatures alone called it fine. What moved was inside a record it reads: "
    "`Declaration` gained the shape a caller asks its rows back in, and "
    "`Database` lost its primary and gained a channel and a crew. "
    "`declaration_from_pseudo_sql` survives untouched, three lines that read no "
    "field of anything, and only its step file went, because the reader it calls "
    "is gone. Each comes back when its function is written against the record as "
    "it now stands."
)

_NEXT_UP = (
    "Written on 2026-09-23 ahead of its function, which is the order: the "
    "feature, then the setup, then the code. It comes off the day the function "
    "lands, and it is here rather than absent so the count of what is specified "
    "and unbuilt is one somebody can read."
)

KNOWN_UNBOUND: dict[str, str] = {
    "lifecycle_register.feature": _NEEDS_A_DELETED_FUNCTION,
    "replicates.feature": _NEEDS_A_DELETED_FUNCTION,
    "schema_read_declared.feature": _NEEDS_A_DELETED_FUNCTION,
    "migration_report.feature": _DELETED_2026_09_23,
    "query_answer.feature": _DELETED_2026_09_23,
    "query_ask.feature": _DELETED_2026_09_23,
    "schema_inspect.feature": _DELETED_2026_09_23,
    "session_connect.feature": _DELETED_2026_09_23,
    "session_open.feature": _DELETED_2026_09_23,
    "turso_pull.feature": _DELETED_2026_09_23,
    "a_condition_is_data.feature": _ONE_EACH,
    "a_failure_is_a_value.feature": _ONE_EACH,
    "a_model_declares_and_does_nothing_else.feature": _ONE_EACH,
    "absence_is_an_answer.feature": _ONE_EACH,
    "absence_survives_the_boundary.feature": _ONE_EACH,
    "add.feature": _ONE_EACH,
    "all.feature": _ONE_EACH,
    "answer_from_ticket.feature": _ONE_EACH,
    "average.feature": _ONE_EACH,
    "change.feature": _ONE_EACH,
    "change_all.feature": _ONE_EACH,
    "command_dispatch.feature": _ONE_EACH,
    "count.feature": _ONE_EACH,
    "count_distinct.feature": _ONE_EACH,
    "distinct.feature": _ONE_EACH,
    "engine_selection.feature": _ONE_EACH,
    "every_engine_is_in_every_table.feature": _ONE_EACH,
    "everything_gets_declared.feature": _ONE_EACH,
    "exists.feature": _ONE_EACH,
    "first.feature": _ONE_EACH,
    "grouped_by.feature": _ONE_EACH,
    "having.feature": _ONE_EACH,
    "largest.feature": _ONE_EACH,
    "lifecycle_catch_up.feature": _ONE_EACH,
    "lifecycle_shut_down.feature": _ONE_EACH,
    "lifecycle_stop.feature": _ONE_EACH,
    "lifecycle_turn.feature": _ONE_EACH,
    "nothing_reaches_past_the_surface.feature": _ONE_EACH,
    "nothing_takes_a_default.feature": _ONE_EACH,
    "of.feature": _ONE_EACH,
    "one.feature": _ONE_EACH,
    "one_function_says_things.feature": _ONE_EACH,
    "ordered_by.feature": _ONE_EACH,
    "overwriting_a_snapshot.feature": _ONE_EACH,
    "page.feature": _ONE_EACH,
    "preparing_the_target.feature": _ONE_EACH,
    "query_begin_concurrent.feature": _ONE_EACH,
    "remove.feature": _ONE_EACH,
    "remove_all.feature": _ONE_EACH,
    "replace.feature": _ONE_EACH,
    "smallest.feature": _ONE_EACH,
    "sum.feature": _ONE_EACH,
    "the_middle_is_pure.feature": _ONE_EACH,
    "the_models_are_the_truth.feature": _ONE_EACH,
    "the_schema_is_declared.feature": _ONE_EACH,
    "the_test_for_any_function.feature": _ONE_EACH,
    "turso_push.feature": _ONE_EACH,
    "validating_a_schema.feature": _ONE_EACH,
    "values.feature": _ONE_EACH,
    "what_the_library_owns.feature": _ONE_EACH,
    "whose_fault_is_a_failed_table.feature": _ONE_EACH,
}

# `scenarios("../features/x.feature")` and `scenarios("../features")` both bind.
# The second form binds a whole DIRECTORY, which is why the resolver below has
# to compare directories and not only filenames.
_SCENARIOS_CALL = re.compile(r"""scenarios\(\s*['"]([^'"]+)['"]""")


# THE FIXTURE IS THE EDGE, AND IT IS THE ONLY THING HERE THAT TOUCHES A DISK.
# This gate is about files, so it cannot avoid reading them. What it can do is
# read them in one place that every test is handed, and let every decision below
# be a function of the text rather than of the filesystem.
@pytest.fixture(scope="module")
def on_disk() -> tuple[dict[pathlib.Path, str], dict[pathlib.Path, str]]:
    """Every feature file and every step file, as path to text."""
    features = {f: f.read_text(encoding="utf-8") for f in sorted(_FEATURES.rglob("*.feature"))}
    steps = {s: s.read_text(encoding="utf-8") for s in sorted(_STEPS.rglob("*.py"))}
    return features, steps


def _bindings(
    features: dict[pathlib.Path, str], steps: dict[pathlib.Path, str]
) -> dict[pathlib.Path, list[pathlib.Path]]:
    """feature file -> the step files that bind it."""
    bound: dict[pathlib.Path, list[pathlib.Path]] = {f: [] for f in features}
    for step_file, text in steps.items():
        for raw in _SCENARIOS_CALL.findall(text):
            target = (step_file.parent / raw).resolve()
            for feature in bound:
                if target == feature or (target.is_dir() and target in feature.parents):
                    bound[feature].append(step_file)
    return bound


def _scenario_count(text: str) -> int:
    return len([line for line in text.splitlines() if line.strip().startswith("Scenario")])


def test_no_feature_file_is_unbound(on_disk):
    """A specification nobody runs is prose claiming to be a test."""
    features, steps = on_disk
    bound = _bindings(features, steps)
    unbound = [f for f, binders in bound.items() if not binders and f.name not in KNOWN_UNBOUND]

    assert not unbound, (
        "these .feature files are bound by no scenarios() call and have NEVER "
        "executed — they read as specified-and-covered while asserting "
        "nothing:\n"
        + "\n".join(
            f"    {f.relative_to(_FEATURES)}  ({_scenario_count(features[f])} scenarios)"
            for f in sorted(unbound)
        )
        + "\n\nBind it from a step file, delete it, or add it to KNOWN_UNBOUND "
        "with the reason it ships unexecuted."
    )


def test_no_feature_file_is_bound_twice(on_disk):
    """Twice is not safer than once; it is a silent step-collision.

    Two step files calling scenarios() on one feature run every scenario
    twice, and a step defined in both resolves to whichever module pytest
    imported last — which is import order, not intent.
    """
    features, steps = on_disk
    doubled = {f: binders for f, binders in _bindings(features, steps).items() if len(binders) > 1}

    assert not doubled, (
        "these .feature files are bound more than once, so their scenarios run "
        "repeatedly and any duplicated step resolves by import order:\n"
        + "\n".join(
            f"    {f.relative_to(_FEATURES)}  bound by " + ", ".join(b.name for b in binders)
            for f, binders in sorted(doubled.items())
        )
    )


def test_the_ratchet_can_actually_see_a_feature_file(on_disk):
    """The guard above is worthless if it finds nothing to guard.

    A discovery bug — wrong directory, wrong glob — would make both tests pass
    by examining an empty set. That is the vacuous-pass shape this package has
    shipped repeatedly, so the ratchet asserts its own reach.
    """
    features, _steps = on_disk
    assert features, f"no .feature files found under {_FEATURES}"
    assert any(_scenario_count(text) > 0 for text in features.values()), (
        "every feature file parsed as zero scenarios, so the counter is broken "
        "and the reports above would be meaningless"
    )
