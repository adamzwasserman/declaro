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

# `_ONE_EACH` WAS HERE AND IT WAS WRONG ABOUT ALL 46 OF THEM. It said "the
# function it describes is declared in docs/design/persistum.hd and not yet
# built", which stopped being true on 2026-09-25, when the last card got its
# module. Not one of the 46 waits for a function: 23 are words in a question, 15
# are rules about this source, 6 describe a command line the declaration never
# names, one contradicted HOW-IT-SHOULD-WORK.md and one was an empty file. A
# reason that covers 46 files and describes none of them is a list pretending to
# be a ratchet, so it is three reasons now and each says what would take its
# files off.

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

_A_WORD_IN_A_QUESTION = (
    "Not a function, and the old reason said it was. `add`, `count`, `ordered_by` "
    "and the twenty others are words a caller writes in a question, and the "
    "declaration carries them as vocabulary rather than as cards: what each one "
    "means is `fn compile_statement` reading a `Declaration`, and what a caller "
    "sees is `boundary_in fn query_ask`. So there is no module to wait for and "
    "never was. Each comes off this list when its scenarios are written against "
    "the door a caller actually calls, which is a rewrite of the feature rather "
    "than a step file for it."
)

_A_RULE_ABOUT_THE_SOURCE = (
    "Not a function either. Each of these asserts something about this "
    "codebase: that the middle is pure, that nothing takes a default, that no "
    "string is interpolated. A scenario cannot be bound to a card because there "
    "is no card; it is bound to a scan of `src/`, which is what every gate in "
    "`tests/unit/` already is. Several are covered there under other names and "
    "nothing ties the two together. Each comes off this list when its gate names "
    "the feature it satisfies."
)

# `_THE_DECLARATION_HAS_NO_CLI` WAS HERE AND ITS SIX FILES ARE GONE. Adam chose
# on 2026-09-25: delete the command line. `src/declaro_persistum/cli/` held no
# Python at all, only stale bytecode that stage 0 left behind, and
# `pyproject.toml` shipped a `declaro` command pointing at a module that raised
# ModuleNotFoundError on import. Seven other folders were in the same state and
# went with it. Git has all of it.

KNOWN_UNBOUND: dict[str, str] = {
    "lifecycle_register.feature": _NEEDS_A_DELETED_FUNCTION,
    "schema_read_declared.feature": _NEEDS_A_DELETED_FUNCTION,
    "migration_report.feature": _DELETED_2026_09_23,
    "turso_pull.feature": _DELETED_2026_09_23,
    "a_condition_is_data.feature": _A_RULE_ABOUT_THE_SOURCE,
    "a_failure_is_a_value.feature": _A_RULE_ABOUT_THE_SOURCE,
    "a_model_declares_and_does_nothing_else.feature": _A_RULE_ABOUT_THE_SOURCE,
    "absence_is_an_answer.feature": _A_RULE_ABOUT_THE_SOURCE,
    "absence_survives_the_boundary.feature": _A_RULE_ABOUT_THE_SOURCE,
    "add.feature": _A_WORD_IN_A_QUESTION,
    "all.feature": _A_WORD_IN_A_QUESTION,
    "average.feature": _A_WORD_IN_A_QUESTION,
    "change.feature": _A_WORD_IN_A_QUESTION,
    "change_all.feature": _A_WORD_IN_A_QUESTION,
    "count.feature": _A_WORD_IN_A_QUESTION,
    "count_distinct.feature": _A_WORD_IN_A_QUESTION,
    "distinct.feature": _A_WORD_IN_A_QUESTION,
    "every_engine_is_in_every_table.feature": _A_RULE_ABOUT_THE_SOURCE,
    "everything_gets_declared.feature": _A_RULE_ABOUT_THE_SOURCE,
    "exists.feature": _A_WORD_IN_A_QUESTION,
    "first.feature": _A_WORD_IN_A_QUESTION,
    "grouped_by.feature": _A_WORD_IN_A_QUESTION,
    "having.feature": _A_WORD_IN_A_QUESTION,
    "largest.feature": _A_WORD_IN_A_QUESTION,
    "nothing_reaches_past_the_surface.feature": _A_RULE_ABOUT_THE_SOURCE,
    "nothing_takes_a_default.feature": _A_RULE_ABOUT_THE_SOURCE,
    "of.feature": _A_WORD_IN_A_QUESTION,
    "one.feature": _A_WORD_IN_A_QUESTION,
    "one_function_says_things.feature": _A_RULE_ABOUT_THE_SOURCE,
    "ordered_by.feature": _A_WORD_IN_A_QUESTION,
    "page.feature": _A_WORD_IN_A_QUESTION,
    "remove.feature": _A_WORD_IN_A_QUESTION,
    "remove_all.feature": _A_WORD_IN_A_QUESTION,
    "replace.feature": _A_WORD_IN_A_QUESTION,
    "smallest.feature": _A_WORD_IN_A_QUESTION,
    "sum.feature": _A_WORD_IN_A_QUESTION,
    "the_models_are_the_truth.feature": _A_RULE_ABOUT_THE_SOURCE,
    "the_schema_is_declared.feature": _A_RULE_ABOUT_THE_SOURCE,
    "the_test_for_any_function.feature": _A_RULE_ABOUT_THE_SOURCE,
    "values.feature": _A_WORD_IN_A_QUESTION,
    "what_the_library_owns.feature": _A_RULE_ABOUT_THE_SOURCE,
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


def test_the_list_only_ever_gets_shorter(on_disk):
    """A name on KNOWN_UNBOUND that is now bound comes off it.

    THE LIST HELD THREE STALE NAMES AND NOTHING NOTICED, found 2026-09-25.
    `query_answer`, `query_ask` and `schema_inspect` had step files binding
    every scenario in them and were still listed as shipping unexecuted. The
    guard above only looks at features with no binder, so a name that stopped
    being true sat there silently, and the count of what is specified and
    unbuilt read three higher than it was.

    A ratchet that does not tighten is a list.
    """
    features, steps = on_disk
    bound = _bindings(features, steps)
    stale = sorted(f.name for f, binders in bound.items() if binders and f.name in KNOWN_UNBOUND)

    assert not stale, (
        f"{stale} are on KNOWN_UNBOUND and are bound by a step file, so the "
        f"list says they ship unexecuted and they do not. Take each one off it."
    )
