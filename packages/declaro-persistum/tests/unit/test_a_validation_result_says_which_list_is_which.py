"""A pair of same-typed lists tells the caller nothing about their order.

WRITTEN AFTER GETTING IT WRONG TWICE IN ONE SESSION.

    def validate_schema(schema: Schema) -> tuple[list[str], list[str]]:
        ...
        return warnings, errors

Writing a test against that signature, I first subscripted the result as a
dict, then unpacked the pair as `errors, warnings`. Both are wrong and neither
raises: the second produced a test that asserted the absence of warnings by
checking the errors, and it PASSED, because both lists were empty.

That is the failure mode worth naming. A swapped pair of same-typed lists does
not crash. It quietly inverts a check, and a passing test is exactly what you
get. The annotation `tuple[list[str], list[str]]` is honest about the types
and silent about the only thing a caller needs to know.

It is a `ValidationResult` TypedDict now, so the two lists are reached by name
and there is no order to get wrong. Rule 2: data is just data, and a field name
is the cheapest documentation there is.

WHAT THIS DOES NOT CLAIM: that the validator's rules are right. It checks that
its answer can be read correctly, which is a different and smaller thing.
"""

from __future__ import annotations

import inspect
import typing

import pytest

from declaro_persistum.validator import validate_schema

pytestmark = pytest.mark.precommit

NULLABLE_KEY = {
    "t": {"columns": {"id": {"type": "text", "primary_key": True, "nullable": True}}}
}
BROKEN_REFERENCE = {
    "t": {"columns": {"user_id": {"type": "text", "references": "nowhere.id"}}}
}
CLEAN = {"t": {"columns": {"id": {"type": "text", "primary_key": True}}}}


def test_the_result_is_reached_by_name() -> None:
    """The whole point. No order to get wrong."""
    result = validate_schema(CLEAN)

    assert result["warnings"] == []
    assert result["errors"] == []


def test_a_warning_lands_in_warnings() -> None:
    """Swapping the two lists would pass a test that only counted them."""
    result = validate_schema(NULLABLE_KEY)

    assert result["warnings"], result
    assert result["errors"] == [], (
        f"a nullable primary key is a warning, not an error: {result['errors']}"
    )


def test_an_error_lands_in_errors() -> None:
    """The other half, so the two cannot both be satisfied by one list."""
    result = validate_schema(BROKEN_REFERENCE)

    assert result["errors"], result
    assert not any("nowhere" in w for w in result["warnings"]), (
        f"a broken reference was reported as a warning: {result['warnings']}"
    )


def test_the_annotation_says_which_is_which() -> None:
    """A tuple of two identical types is silent about the thing that matters."""
    hints = typing.get_type_hints(validate_schema)
    returned = hints["return"]

    assert typing.get_origin(returned) is not tuple, (
        "validate_schema still returns a bare tuple, so a caller has to guess "
        "the order and a swap does not raise"
    )
    assert set(typing.get_type_hints(returned)) == {"warnings", "errors"}, (
        typing.get_type_hints(returned)
    )


def test_every_producer_of_the_pair_returns_the_same_shape() -> None:
    """One renamed function and one left as a tuple is the worse state.

    Both `validate_schema` and `_validate_column` returned the pair, so a
    caller of either had the same guess to make.
    """
    from declaro_persistum import validator

    for name in ("validate_schema", "_validate_column"):
        fn = getattr(validator, name)
        returned = typing.get_type_hints(fn)["return"]
        assert typing.get_origin(returned) is not tuple, (
            f"{name} still returns a bare tuple"
        )


def test_no_caller_unpacks_it_positionally() -> None:
    """A caller that still unpacks a pair has kept the trap locally.

    Parsed rather than grepped: a comment mentioning the old shape is a record
    of the defect, not an instance of it.
    """
    import ast
    import pathlib

    root = pathlib.Path(inspect.getfile(validate_schema)).parent
    offenders: list[str] = []

    for path in list(root.rglob("*.py")) + list(
        (root.parents[1] / "tests").rglob("*.py")
    ):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign) or len(node.targets) != 1:
                continue
            if not isinstance(node.targets[0], ast.Tuple):
                continue
            called = getattr(node.value, "func", None)
            name = getattr(called, "id", None) or getattr(called, "attr", None)
            if name in ("validate_schema", "_validate_column", "_validate_table"):
                offenders.append(f"{path.name}:{node.lineno}")

    assert not offenders, (
        f"these still unpack the validator's answer by position: {offenders}"
    )


def test_strict_validation_still_raises_on_an_error() -> None:
    """The shape change must not lose the behaviour built on it."""
    from declaro_persistum.exceptions import ValidationError
    from declaro_persistum.validator import validate_schema_strict

    with pytest.raises(ValidationError):
        validate_schema_strict(BROKEN_REFERENCE)


def test_strict_validation_tolerates_a_warning() -> None:
    """A warning is not an error, and strict mode must still know that."""
    from declaro_persistum.validator import validate_schema_strict

    validate_schema_strict(NULLABLE_KEY)
