"""Nine places read the differ's change record, and they disagreed.

The differ emits `{"from": old, "to": new}` for a property that changed. Nine
places consume that, and on 2026-08-21 they read it two different ways:

    applier/postgresql.py   changes["type"]["to"]        assumes the record
    applier/shared.py       raw["to"] if isinstance(...)  tests for it
    abstractions/...        value["to"] if isinstance(...) tests for it

So a bare value reaching the PostgreSQL applier raises TypeError, and a record
reaching a site that forgot the test is written into a column definition
whole. That second one has already happened: `_map_type` called `.lower()` on
a dict and the reconstruction renderer produced `CHECK (None)`.

Five sites asking `isinstance(value, dict) and "to" in value` is the same
question written five times, which is five chances to write it differently.
`changed_to` is that question asked once.
"""

import pytest

from declaro_persistum.types import changed_to


@pytest.mark.parametrize(
    "record,expected",
    [
        ({"from": "text", "to": "integer"}, "integer"),
        ({"from": None, "to": "now()"}, "now()"),
        ({"from": True, "to": False}, False),
        ({"from": "x", "to": None}, None),
    ],
    ids=["a type change", "a default arriving", "a flag cleared", "a value removed"],
)
def test_a_record_gives_the_value_it_changed_to(record: dict, expected: object) -> None:
    """`to: None` is a real answer, not an absent one.

    A property removed is written as a change to None, and reading it as
    absence is how a CHECK removed from the models became `CHECK (None)`.
    """
    assert changed_to(record) is expected or changed_to(record) == expected


@pytest.mark.parametrize(
    "value",
    ["integer", 0, False, None, ["a", "b"], {"columns": ["a"]}],
    ids=["a type", "a number", "a flag", "nothing", "a list", "a dict of its own"],
)
def test_a_bare_value_is_returned_as_it_stands(value: object) -> None:
    """Not every value arrives as a record, and one that does not is the answer.

    The last case is the one that makes this more than an isinstance check: an
    index definition is a dict, and it is not a change record.
    """
    assert changed_to(value) == value


def test_a_dict_without_to_is_not_a_record() -> None:
    """`from` alone does not make it one.

    A dict is a change record because it says what the value became. One that
    does not say is some other dict, and returning its contents would put the
    wrong thing in a column definition.
    """
    other = {"from": "text"}

    assert changed_to(other) == other


def test_every_reader_uses_it() -> None:
    """The floor. Nine sites asked this question and five wrote out the test.

    A site that answers it inline can drift from the others, which is exactly
    how the appliers came to disagree about whether a bare value was possible.
    """
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[2] / "src" / "declaro_persistum"
    offenders: list[str] = []
    for path in sorted(root.rglob("*.py")):
        # types.py DEFINES the question, so it is the one place allowed to
        # ask it. Everywhere else calls the answer.
        if path.name == "types.py":
            continue
        for number, line in enumerate(path.read_text().splitlines(), 1):
            if '"to" in ' in line:
                offenders.append(f"{path.relative_to(root)}:{number}")

    assert not offenders, (
        f"{len(offenders)} places still read a change record by hand rather "
        f"than through changed_to: {offenders}"
    )
