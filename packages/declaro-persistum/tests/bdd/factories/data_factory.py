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
    "",                              # Empty string
    " ",                             # Single space
    "   ",                           # Multiple spaces
    "\t\n\r",                        # Whitespace chars
    "a" * 10000,                     # Very long string
    "Robert'); DROP TABLE users--", # SQL injection attempt
    "<script>alert(1)</script>",    # XSS attempt
    "emoji: 👍🎉🚀🌟💯",              # Emojis
    "العربية",                        # RTL text (Arabic)
    "中文字符测试",                     # Chinese characters
    "日本語テスト",                     # Japanese characters
    "Ελληνικά",                       # Greek
    "\x00\x01\x02",                  # Control characters
    "line1\nline2\nline3",          # Newlines
    "tab\there",                    # Tab character
    "null\x00byte",                 # Null byte in middle
    "'single'quotes'",              # Single quotes
    '"double"quotes"',              # Double quotes
    "back\\slash",                  # Backslash
    "percent%sign",                 # Percent sign (LIKE wildcard)
    "underscore_char",              # Underscore (LIKE wildcard)
    "mixed'\"\\%_special",          # Mixed special chars
]

EDGE_CASE_INTEGERS = [
    0,
    1,
    -1,
    2147483647,                     # INT_MAX (32-bit)
    -2147483648,                    # INT_MIN (32-bit)
    9223372036854775807,            # BIGINT_MAX (64-bit)
    -9223372036854775808,           # BIGINT_MIN (64-bit)
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
        blacklist_characters="\x00",   # Exclude null bytes
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


def todo(**overrides: Any) -> dict[str, Any]:
    """One todo. Any field can be pinned; the rest are generated."""
    return {
        "id": overrides.get("id", str(uuid.uuid4())),
        "title": overrides.get("title", fake.sentence(nb_words=5)),
        "completed": overrides.get("completed", fake.boolean()),
        "created_at": overrides.get(
            "created_at", fake.date_time_this_year(tzinfo=UTC)
        ),
    }


def todos(count: int, **overrides: Any) -> list[dict[str, Any]]:
    return [todo(**overrides) for _ in range(count)]


def todos_with_edge_case_titles() -> list[dict[str, Any]]:
    """One todo per hostile string. The empty one is skipped — title is NOT NULL."""
    return [todo(title=t) for t in EDGE_CASE_STRINGS if t]


@st.composite
def any_todo(draw: st.DrawFn) -> dict[str, Any]:
    """Hypothesis strategy: an arbitrary todo."""
    return {
        "id": str(draw(st.uuids())),
        "title": draw(
            st.text(
                min_size=1,
                max_size=500,
                alphabet=st.characters(
                    blacklist_categories=("Cs",), blacklist_characters="\x00"
                ),
            )
        ),
        "completed": draw(st.booleans()),
    }


def user(**overrides: Any) -> dict[str, Any]:
    return {
        "id": overrides.get("id", str(uuid.uuid4())),
        "email": overrides.get("email", fake.unique.email()),
        "name": overrides.get("name", fake.name()),
        "status": overrides.get(
            "status", fake.random_element(["active", "inactive", "pending"])
        ),
        "age": overrides.get("age", fake.random_int(min=18, max=100)),
        "created_at": overrides.get(
            "created_at", fake.date_time_this_year(tzinfo=UTC)
        ),
    }


def users(count: int, **overrides: Any) -> list[dict[str, Any]]:
    fake.unique.clear()  # the unique email generator is per-run state
    return [user(**overrides) for _ in range(count)]


@st.composite
def any_user(draw: st.DrawFn) -> dict[str, Any]:
    return {
        "id": str(draw(st.uuids())),
        "email": draw(st.emails()),
        "name": draw(st.text(min_size=1, max_size=100)),
        "status": draw(st.sampled_from(["active", "inactive", "pending"])),
        "age": draw(st.integers(min_value=0, max_value=150)),
    }


def order(**overrides: Any) -> dict[str, Any]:
    return {
        "id": overrides.get("id", str(uuid.uuid4())),
        "user_id": overrides.get("user_id", str(uuid.uuid4())),
        "total": overrides.get(
            "total", round(fake.pyfloat(min_value=0.01, max_value=10000.00), 2)
        ),
        "status": overrides.get(
            "status",
            fake.random_element(["pending", "confirmed", "shipped", "delivered"]),
        ),
        "created_at": overrides.get(
            "created_at", fake.date_time_this_year(tzinfo=UTC)
        ),
    }


def orders(count: int, **overrides: Any) -> list[dict[str, Any]]:
    return [order(**overrides) for _ in range(count)]


def order_with_items(item_count: int, **overrides: Any) -> dict[str, Any]:
    """An order and its lines. `item_count` is required — see Rule 14."""
    parent = order(**overrides)
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
