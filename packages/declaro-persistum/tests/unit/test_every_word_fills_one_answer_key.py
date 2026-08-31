"""Every word in the vocabulary answers into exactly one declared key.

WHY THIS EXISTS. `asking.FILLS` says which key of an `Answer` each word fills,
and until the words are built nothing else reads it. A table column nothing
reads is a table column nobody has checked, and it would be wrong in silence
until stage 2 went to write the first body against it.

It also holds the two halves of the module together. Words that share a shape
carry their key in the row beside their docstring; the seven that are alone in
their shape carry it in `FILLS` directly. Those two places can drift, and this
is what notices.
"""

from typing import get_args, get_type_hints

from declaro_persistum import asking
from declaro_persistum.types import Answer, AnswerKey, answered, failed


def test_every_declared_word_says_what_it_fills() -> None:
    """The vocabulary and the answer-key table name the same words."""
    assert sorted(asking.FILLS) == sorted(asking.__all__)


def test_every_key_a_word_fills_is_a_key_an_answer_has() -> None:
    """A word cannot answer into a key that does not exist."""
    on_the_answer = set(get_type_hints(Answer))
    assert set(asking.FILLS.values()) <= on_the_answer


def test_the_answer_key_vocabulary_is_the_answer_itself() -> None:
    """`AnswerKey` names every key and no others, so a row is checked, not spelled."""
    assert set(get_args(AnswerKey)) == set(get_type_hints(Answer))


def test_no_word_answers_into_failure() -> None:
    """Failure is what a word fills when it could not do its job, never its answer."""
    assert "failure" not in set(asking.FILLS.values())


def test_an_answer_fills_one_key_and_leaves_the_rest_absent() -> None:
    """Five keys absent is what makes the sixth impossible to walk past."""
    given = answered("number", 3)
    assert given["number"] == 3
    assert [key for key, value in given.items() if value is not ...] == ["number"]


def test_a_failure_is_a_value_in_the_same_shape() -> None:
    """A caller tells failure from success by which key is not absent."""
    given = failed("one", "more than one row matched")
    assert given["failure"] == {"what": "one", "because": "more than one row matched"}
    assert [key for key, value in given.items() if value is not ...] == ["failure"]


def test_a_generated_word_keeps_its_name_and_its_meaning() -> None:
    """A table costs a reader nothing if the row still answers `help()`."""
    assert asking.count.__name__ == "count"
    assert asking.count.__doc__ == "How many rows match. Returns the number, not a row carrying it."
    assert asking.add.__name__ == "add"
