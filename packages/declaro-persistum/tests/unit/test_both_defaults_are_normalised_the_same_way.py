"""`same_default` normalised its two arguments in four separate places.

The function asks whether two spellings mean the same default. A default is a
string on one side and often a value on the other, so each side has to be put
into a comparable form first. That was four `if isinstance(x, str)` guards
inline, two for the engine's own spelling and two for whitespace, which is one
rule written four times.

WHY THAT MATTERS MORE THAN IT LOOKS. The whole function exists because
comparing two spellings by identity proposed a migration on every deploy, and
it has been fixed three times: booleans, then numbers, then `now()` against
`(datetime('now'))`. Each fix added a guard. A rule applied to one argument
and not the other is exactly the shape those bugs had.

So the normalisation is one function now, called once per side, and these
tests say the two sides get the same treatment whichever way round they
arrive.
"""

import pytest

from declaro_persistum.differ.core import same_default

_PAIRS = [
    ("now()", "(datetime('now'))", "sqlite"),
    ("now()", "(datetime ('now'))", "turso"),
    ("current_timestamp", "CURRENT_TIMESTAMP", "postgresql"),
    ("0", 0, "postgresql"),
    ("TRUE", "true", "postgresql"),
]


@pytest.mark.parametrize("left,right,engine", _PAIRS)
def test_two_spellings_of_one_default_agree(left, right, engine) -> None:
    """Each of these was a migration loop before it was folded."""
    assert same_default(left, right, engine=engine)


@pytest.mark.parametrize("left,right,engine", _PAIRS)
def test_the_answer_does_not_depend_on_the_order(left, right, engine) -> None:
    """The rule must reach both arguments.

    This is the test the four inline guards could not fail: each was written
    twice, once per side, and a fifth fix adding a guard to only one of them
    would still pass every other test in the suite.
    """
    assert same_default(left, right, engine=engine) == same_default(right, left, engine=engine)


@pytest.mark.parametrize(
    "left,right",
    [("now()", "current_timestamp"), (0, 1), ("true", "false"), ("'a'", "'b'")],
    ids=["two functions", "two numbers", "two flags", "two strings"],
)
def test_two_different_defaults_still_differ(left, right) -> None:
    """The control, so the folding is narrowed and not switched on.

    A change of default has to migrate. Folding these would report a schema
    that changed its mind as unchanged.
    """
    assert not same_default(left, right, engine="postgresql")


def test_absence_is_not_a_value() -> None:
    """None on one side and a default on the other is a real change."""
    assert same_default(None, None, engine="sqlite")
    assert not same_default(None, "0", engine="sqlite")
    assert not same_default("0", None, engine="sqlite")
