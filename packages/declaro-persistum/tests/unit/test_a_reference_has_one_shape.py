"""A foreign key reference was accepted in two shapes and produced in one.

`apply_reconstruction_changes` read the `references` of an add_foreign_key as
either a string or a dict:

    pair = ref.split(".", 1) if isinstance(ref, str) else [ref["table"], ref["column"]]

Measured 2026-08-21: the differ emits `"p.id"`, every producer in `src/`
builds the same string, and nothing in `src`, `tests` or `examples` writes the
dict. Two documents describe it, so it is a documented shape that has never
been executed and no test has ever covered.

AN UNREACHED BRANCH IS NOT SUPPORT, it is a claim. Keeping it means the
package promises a shape it has never run, and the promise is defended by an
isinstance check that reads as though both cases happen. Deleting it makes the
contract what the code actually does.
"""

from declaro_persistum.applier.shared import apply_reconstruction_changes
from declaro_persistum.differ.core import diff
from declaro_persistum.types import NOTHING


def _columns() -> dict:
    return {"id": {"type": "integer"}, "p_id": {"type": "integer"}}


def test_a_reference_is_written_as_table_dot_column() -> None:
    """The one shape, applied."""
    operation = {
        "op": "add_foreign_key",
        "table": "c",
        "details": {"column": "p_id", "references": "p.id"},
    }

    columns = apply_reconstruction_changes(_columns(), operation)

    assert columns["p_id"]["references"] == "p.id"


def test_the_differ_emits_that_shape() -> None:
    """What the applier accepts is what the differ produces.

    The dict form had no producer, so accepting it meant accepting something
    only a hand-written caller could send.
    """
    result = diff(
        {"p": {"columns": {"id": {"type": "integer"}}}, "c": {"columns": _columns()}},
        {
            "p": {"columns": {"id": {"type": "integer"}}},
            "c": {
                "columns": {
                    "id": {"type": "integer"},
                    "p_id": {"type": "integer", "references": "p.id"},
                }
            },
        },
        engine="sqlite",
        decisions=NOTHING,
    )

    keys = [
        op["details"]["references"] for op in result["operations"] if op["op"] == "add_foreign_key"
    ]
    assert keys == ["p.id"]
    assert all(isinstance(k, str) for k in keys)


def test_nothing_is_translated_on_the_way_through() -> None:
    """What the operation declared is what the column carries.

    The branch this replaces turned a dict into `"p.id"`, so an operation
    built in a shape nothing produces came out looking like one that had
    been. Rule 13 says the interior trusts its declared contract rather than
    re-checking it, so there is no guard here: `references` is declared a
    string, and a caller who sends something else has broken a contract the
    type states. What matters is that it is not quietly repaired into
    something that looks right.
    """
    wrong = {"table": "p", "column": "id"}
    operation = {
        "op": "add_foreign_key",
        "table": "c",
        "details": {"column": "p_id", "references": wrong},
    }

    columns = apply_reconstruction_changes(_columns(), operation)

    assert columns["p_id"]["references"] is wrong


def test_no_document_still_shows_the_dict_shape() -> None:
    """The floor. A doc describing a shape the code refuses is worse than none.

    Two documents showed `{"table": ..., "column": ...}` as the contract, and
    a reader following either would have written an operation the applier
    cannot apply.
    """
    import pathlib

    docs = pathlib.Path(__file__).resolve().parents[2] / "docs"
    offenders = [
        str(path.relative_to(docs))
        for path in docs.rglob("*.md")
        if '"references": {"table"' in path.read_text()
    ]

    assert not offenders, f"these still show the dict reference: {offenders}"
