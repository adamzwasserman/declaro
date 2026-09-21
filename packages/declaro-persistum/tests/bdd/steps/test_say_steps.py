"""Steps for reporting/say.feature. Real LogRecords, read off a real handler.

NO CAPTURED TEXT. Every assertion here reads the `LogRecord` itself, because
the defect this feature exists for was invisible in the formatted line: the
words "exc_info=True" appeared exactly where a reader would expect them, and
the exception they claimed to carry was gone. Only the record shows that.
"""

from __future__ import annotations

import logging
from typing import Any, TypedDict

import pytest
from pytest_bdd import given, parsers, scenarios, then, when

from declaro_persistum import reporting

scenarios("../features/observability/log_write.feature")

MODULE = "declaro_persistum.test_say"


class Ctx(TypedDict, total=False):
    records: list[logging.LogRecord]
    cause: Exception | None
    refusal: Exception | None


@pytest.fixture
def ctx(caplog) -> Ctx:
    caplog.set_level(logging.DEBUG, logger=MODULE)
    return {"records": caplog.records, "cause": None, "refusal": None}


def _run(ctx: Ctx, function: Any, *args: Any, **facts: Any) -> None:
    try:
        function(*args, **facts)
        ctx["refusal"] = None
    except Exception as refused:  # noqa: BLE001 - a refusal is an outcome here
        ctx["refusal"] = refused


@given(parsers.parse('a ValueError reading "{text}"'))
def _given_cause(ctx, text):
    ctx["cause"] = ValueError(text)


@when(parsers.parse('say writes "{message}" at {level}'))
def _when_say(ctx, message, level):
    _run(ctx, reporting.say, MODULE, level, message)


@when(parsers.parse('say writes "{message}" at {level} with the fact path="{path}"'))
def _when_say_with_fact(ctx, message, level, path):
    _run(ctx, reporting.say, MODULE, level, message, path=path)


@when(parsers.parse('say writes "{message}" at {level} with the fact exc_info=True'))
def _when_say_with_exc_info(ctx, message, level):
    _run(ctx, reporting.say, MODULE, level, message, exc_info=True)


@when(parsers.parse('say_because writes "{message}" at {level} with that cause'))
def _when_say_because(ctx, message, level):
    _run(ctx, reporting.say_because, MODULE, level, message, ctx["cause"])


@when(
    parsers.parse(
        'say_because writes "{message}" at {level} with that cause and the fact path="{path}"'
    )
)
def _when_say_because_with_fact(ctx, message, level, path):
    _run(ctx, reporting.say_because, MODULE, level, message, ctx["cause"], path=path)


def _only(ctx: Ctx) -> logging.LogRecord:
    assert ctx["refusal"] is None, ctx["refusal"]
    assert len(ctx["records"]) == 1, ctx["records"]
    return ctx["records"][0]


@then(parsers.parse('a {level} was written saying "{message}"'))
def _then_written(ctx, level, message):
    record = _only(ctx)
    assert record.levelname.lower() == level, record.levelname
    assert record.getMessage() == message, record.getMessage()


@then(parsers.parse('the line written reads "{expected}"'))
def _then_reads(ctx, expected):
    assert _only(ctx).getMessage() == expected, _only(ctx).getMessage()


@then("it refuses")
def _then_refuses(ctx):
    assert ctx["refusal"] is not None, f"it wrote {ctx['records']} instead"


@then(parsers.parse('it refuses and names "{named}"'))
def _then_refuses_naming(ctx, named):
    assert ctx["refusal"] is not None, f"it wrote {ctx['records']} instead"
    assert named in str(ctx["refusal"]), ctx["refusal"]


@then("the record carries that ValueError as the exception")
def _then_carries(ctx):
    record = _only(ctx)
    assert record.exc_info is not None, "the exception was thrown away"
    assert record.exc_info[1] is ctx["cause"], record.exc_info


@then("the record carries no exception")
def _then_no_exception(ctx):
    assert _only(ctx).exc_info is None, _only(ctx).exc_info
