"""
Data factories for generating test data.

Uses Faker for realistic data and Hypothesis for property-based testing.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from faker import Faker
from hypothesis import strategies as st

fake = Faker()


# =============================================================================
# Edge Case Test Data
# =============================================================================

EDGE_CASE_STRINGS = [
    "",  # Empty string
    " ",  # Single space
    "   ",  # Multiple spaces
    "\t\n\r",  # Whitespace chars
    "a" * 10000,  # Very long string
    "Robert'); DROP TABLE users--",  # SQL injection attempt
    "<script>alert(1)</script>",  # XSS attempt
    "emoji: 👍🎉🚀🌟💯",  # Emojis
    "العربية",  # RTL text (Arabic)
    "中文字符测试",  # Chinese characters
    "日本語テスト",  # Japanese characters
    "Ελληνικά",  # Greek
    "\x00\x01\x02",  # Control characters
    "line1\nline2\nline3",  # Newlines
    "tab\there",  # Tab character
    "null\x00byte",  # Null byte in middle
    "'single'quotes'",  # Single quotes
    '"double"quotes"',  # Double quotes
    "back\\slash",  # Backslash
    "percent%sign",  # Percent sign (LIKE wildcard)
    "underscore_char",  # Underscore (LIKE wildcard)
    "mixed'\"\\%_special",  # Mixed special chars
]

EDGE_CASE_INTEGERS = [
    0,
    1,
    -1,
    2147483647,  # INT_MAX (32-bit)
    -2147483648,  # INT_MIN (32-bit)
    9223372036854775807,  # BIGINT_MAX (64-bit)
    -9223372036854775808,  # BIGINT_MIN (64-bit)
]

EDGE_CASE_DATES = [
    datetime(1970, 1, 1, tzinfo=UTC),  # Unix epoch
    datetime(2000, 1, 1, tzinfo=UTC),  # Y2K
    datetime(2038, 1, 19, 3, 14, 7, tzinfo=UTC),  # 32-bit timestamp overflow
    datetime(2099, 12, 31, 23, 59, 59, tzinfo=UTC),  # Far future
    datetime(1900, 1, 1, tzinfo=UTC),  # Very old date
    datetime.now(UTC),  # Current time
    datetime.now(UTC) - timedelta(microseconds=1),  # Microsecond precision
]


# =============================================================================
# Todo Factory
# =============================================================================


# =============================================================================
# User Factory
# =============================================================================


# =============================================================================
# Order Factory
# =============================================================================


# =============================================================================
# Hypothesis Strategies
# =============================================================================

# Strategy for any valid SQL string value
sql_safe_text = st.text(
    min_size=0,
    max_size=1000,
    alphabet=st.characters(
        blacklist_categories=("Cs",),  # Exclude surrogates
        blacklist_characters="\x00",  # Exclude null bytes
    ),
)

# Strategy for valid identifiers
sql_identifier = st.text(
    min_size=1,
    max_size=63,  # PostgreSQL identifier limit
    alphabet=st.characters(
        whitelist_categories=("Ll", "Lu", "Nd"),  # Letters and digits
        whitelist_characters="_",
    ),
).filter(lambda x: x[0].isalpha() or x[0] == "_")  # Must start with letter or underscore

# Strategy for various integer types
sql_integer = st.integers(min_value=-2147483648, max_value=2147483647)
sql_bigint = st.integers(min_value=-9223372036854775808, max_value=9223372036854775807)
sql_smallint = st.integers(min_value=-32768, max_value=32767)

# Strategy for numeric/decimal values
sql_numeric = st.decimals(
    min_value=-99999999.99,
    max_value=99999999.99,
    places=2,
    allow_nan=False,
    allow_infinity=False,
)

# Strategy for boolean values (including NULL representation)
sql_boolean = st.booleans()

# Strategy for UUID values
sql_uuid = st.uuids().map(str)

# Strategy for list of values (for IN clauses)
sql_in_list = st.lists(st.integers(), min_size=0, max_size=100)

# =============================================================================
# Test data, as functions
# =============================================================================
#
# These were `TodoFactory`, `UserFactory` and `OrderFactory`: three classes
# whose every member was a @staticmethod. They held no state, took no self,
# and existed only to namespace functions that were already functions. The
# call sites read `TodoFactory.create(...)`; they now read `todo(...)`.
#
# `create_batch(n)` is kept as `todos(n)` rather than folded into `todo()`
# with a count argument, because a caller asking for one row and a caller
# asking for a thousand want different return types and should say so.


# TWO FUNCTIONS PER ROW, BECAUSE THERE ARE TWO QUESTIONS.
#
# This was one function per row with `**overrides`:
#
#     def todo(**overrides):
#         return {"id": overrides.get("id", str(uuid.uuid4())), ...}
#
# which answered "give me a row with this exact id" and "give me any row" with
# the same call. A misspelled key took the generated branch and said nothing:
# `todo(titel="x")` returned a random title, and the scenario asserting on "x"
# failed somewhere else entirely.
#
# `a_todo(...)` takes every field and generates nothing, so a missing or
# misspelled key is a TypeError at the call site. `some_todo()` generates
# every field and takes nothing, so there is no key to misspell.
#
# The fields are the columns `schema_factory` declares. A test asserts that,
# because a builder and a schema that drift apart produce rows no INSERT can
# take, and every scenario using them fails for the wrong reason.


def a_todo(*, id: str, title: str, completed: Any, created_at: Any) -> dict[str, Any]:
    """A todo with every field specified. Generates nothing."""
    return {"id": id, "title": title, "completed": completed, "created_at": created_at}


def some_todo() -> dict[str, Any]:
    """A todo with every field generated. Takes nothing."""
    return a_todo(
        id=str(uuid.uuid4()),
        title=fake.sentence(nb_words=5),
        completed=fake.boolean(),
        created_at=fake.date_time_this_year(tzinfo=UTC),
    )


def a_user(
    *, id: str, email: str, name: str, status: str, age: int, created_at: Any
) -> dict[str, Any]:
    """A user with every field specified. Generates nothing."""
    return {
        "id": id,
        "email": email,
        "name": name,
        "status": status,
        "age": age,
        "created_at": created_at,
    }


def some_user() -> dict[str, Any]:
    """A user with every field generated. `email` is UNIQUE in the schema."""
    return a_user(
        id=str(uuid.uuid4()),
        email=fake.unique.email(),
        name=fake.name(),
        status=fake.random_element(["active", "inactive", "pending"]),
        age=fake.random_int(min=18, max=100),
        created_at=fake.date_time_this_year(tzinfo=UTC),
    )


def todos(count: int) -> list[dict[str, Any]]:
    """`count` generated todos, every one distinct.

    NO `**overrides`. It used to pass the same overrides to every row, so
    `todos(5, id="x")` returned five rows sharing a primary key and the second
    INSERT failed with a constraint violation that named the table rather than
    the builder.
    """
    return [some_todo() for _ in range(count)]


def todos_with_edge_case_titles() -> list[dict[str, Any]]:
    """One todo per hostile string. The empty one is skipped, title is NOT NULL.

    The title is the field under test, so it is specified and the rest is
    generated. Written out rather than through an override, because that is
    the distinction the two builders exist to keep.
    """
    return [{**some_todo(), "title": t} for t in EDGE_CASE_STRINGS if t]


@st.composite
def any_todo(draw: st.DrawFn) -> dict[str, Any]:
    """Hypothesis strategy: an arbitrary todo."""
    return {
        "id": str(draw(st.uuids())),
        "title": draw(
            st.text(
                min_size=1,
                max_size=500,
                alphabet=st.characters(blacklist_categories=("Cs",), blacklist_characters="\x00"),
            )
        ),
        "completed": draw(st.booleans()),
    }


def users(count: int) -> list[dict[str, Any]]:
    """`count` generated users, every one distinct. See `todos` for why no overrides."""
    fake.unique.clear()  # the unique email generator is per-run state
    return [some_user() for _ in range(count)]


@st.composite
def any_user(draw: st.DrawFn) -> dict[str, Any]:
    return {
        "id": str(draw(st.uuids())),
        "email": draw(st.emails()),
        "name": draw(st.text(min_size=1, max_size=100)),
        "status": draw(st.sampled_from(["active", "inactive", "pending"])),
        "age": draw(st.integers(min_value=0, max_value=150)),
    }


def an_order(*, id: str, user_id: str, total: Any, status: str, created_at: Any) -> dict[str, Any]:
    """An order with every field specified. Generates nothing."""
    return {
        "id": id,
        "user_id": user_id,
        "total": total,
        "status": status,
        "created_at": created_at,
    }


def some_order() -> dict[str, Any]:
    """An order with every field generated. Takes nothing."""
    return an_order(
        id=str(uuid.uuid4()),
        user_id=str(uuid.uuid4()),
        total=round(fake.pyfloat(min_value=0.01, max_value=10000.00), 2),
        status=fake.random_element(["pending", "confirmed", "shipped", "delivered"]),
        created_at=fake.date_time_this_year(tzinfo=UTC),
    )


def orders(count: int) -> list[dict[str, Any]]:
    """`count` generated orders, every one distinct."""
    return [some_order() for _ in range(count)]


def order_with_items(item_count: int) -> dict[str, Any]:
    """An order and its lines. `item_count` is required, see Rule 14."""
    parent = some_order()
    parent["items"] = [
        {
            "id": str(uuid.uuid4()),
            "order_id": parent["id"],
            "quantity": fake.random_int(min=1, max=10),
            "price": round(fake.pyfloat(min_value=0.01, max_value=1000.00), 2),
        }
        for _ in range(item_count)
    ]
    return parent
