"""
Shared pure functions for the SQLite-compatible inspectors: SQLite and Turso.

All functions are pure: no I/O, no side effects, no state.
The SQLite and Turso inspectors both delegate their shared logic here.
"""

import json
import re
from collections.abc import Callable
from functools import partial, reduce
from itertools import accumulate
from typing import Any, Literal

from declaro_persistum.types import (
    Column,
    Index,
    Table,
    View,
    applied_to_text,
)

# Type for FK actions
FKAction = Literal["cascade", "set null", "restrict", "no action"]


def _rewrapped(default: str) -> str:
    """An expression default given back the parens PRAGMA table_info strips.

    A function call comes back bare -- `datetime('now')` for a column declared
    `DEFAULT (datetime('now'))` -- so the round trip loses a layer and the
    differ then sees a change that is not one. Text already parenthesised is
    left as it is.
    """
    return _REWRAP[("(" in default and not default.startswith("("))](default)


def _as_stored(default: str) -> str:
    """Already parenthesised, or not an expression at all."""
    return default


def _wrapped(default: str) -> str:
    """A bare function call, wrapped to match the declared form."""
    return f"({default})"


# WHETHER THE PARENS NEED PUTTING BACK, as a table rather than a three-part
# `and` chain.
_REWRAP: dict[bool, "Callable[[str], str]"] = {
    True: _wrapped,
    False: _as_stored,
}


# THE FK ACTIONS THIS PACKAGE CAN SPELL, keyed by the engine's own wording
# with its spaces turned into underscores. `Column.on_delete` names these four
# and no others, so an action outside the table -- `SET DEFAULT`, which both
# engines accept in a table this package did not create -- has no spelling
# here and reads back as none. That was true before this table existed and it
# is written down rather than buried in a `.get` default.
_FK_ACTIONS: dict[str, FKAction] = {
    "cascade": "cascade",
    "set_null": "set null",
    "restrict": "restrict",
    "no_action": "no action",
}


def normalize_fk_action(action: str | None) -> FKAction | None:
    """Normalize FK action string to proper Literal type.

    `NO ACTION` as both engines write it is the absence of an action, so it
    comes back as none: `test_no_action_is_the_absence_of_an_action.py` holds
    that.
    """
    named = [
        _FK_ACTIONS[spelling]
        for text in [action]
        if text is not None and text != "NO ACTION"
        for spelling in [text.lower().replace(" ", "_")]
        if spelling in _FK_ACTIONS
    ]
    return [*named, None][0]


# THE DECLARED SPELLINGS SQLITE ACCEPTS, EACH BESIDE THE CLASS IT LANDS IN.
# This was an if/elif chain whose order a reader had to hold in their head to
# know which arm answered. The tuple is read in the order it is written and
# the first answer wins, which is the only ordering that remains.
_SQLITE_CLASS_BY_FRAGMENT: tuple[tuple[tuple[str, ...], str], ...] = (
    (("int",), "integer"),
    (("char", "clob", "text"), "text"),
    (("blob",), "blob"),
    (("real", "floa", "doub"), "real"),
)


def normalize_sqlite_type(col_type: str) -> str:
    """Normalize SQLite type to canonical form.

    A column declared with no type at all stores a blob, which is what SQLite
    itself does with `CREATE TABLE t (c)`.
    """
    text = col_type.lower().strip()
    answers = [
        *["blob" for spelling in [text] if not spelling],
        *[
            canonical
            for fragments, canonical in _SQLITE_CLASS_BY_FRAGMENT
            if any(fragment in text for fragment in fragments)
        ],
        "numeric",
    ]
    return answers[0]


def extract_view_query(create_statement: str) -> str:
    """Extract SELECT query from CREATE VIEW statement."""
    match = re.search(r"\bAS\s+(.+)$", create_statement, re.IGNORECASE | re.DOTALL)
    return " ".join(match.group(1).split()) if match else ""


def _column_from_pragma_row(row: tuple) -> Column:
    """One PRAGMA table_info row as a column definition.

    A PK column is implicitly NOT NULL in SQL even when PRAGMA table_info
    reports notnull=0 for it, so both facts set `nullable`.

    PRAGMA table_info strips the outer parens off an expression default, so a
    function call is re-wrapped to match the form the schema declares.
    """
    declared, not_null, default, is_pk = row[2], bool(row[3]), row[4], bool(row[5])
    facts: list[tuple[str, Any]] = [
        ("type", normalize_sqlite_type(declared)),
        *[("nullable", False) for required in [not_null or is_pk] if required],
        *[
            ("default", applied_to_text(_rewrapped, value))
            for value in [default]
            if value is not None
        ],
        *[("primary_key", True) for keyed in [is_pk] if keyed],
    ]
    return dict(facts)  # type: ignore[return-value]


def columns_from_pragma_rows(
    rows: list[tuple],
) -> dict[str, Column]:
    """Convert PRAGMA table_info rows to Column dict (pure)."""
    return {row[1]: _column_from_pragma_row(row) for row in rows}


def _marked_unique(column: Column) -> Column:
    """The column, carrying the flag its single-column unique index states."""
    return {**column, "unique": True}


def apply_unique_columns(columns: dict[str, Column], unique_cols: set[str]) -> None:
    """Mark unique columns (mutates columns in place).

    A PK column is already unique and says so, so the flag is not repeated on
    it.
    """
    columns.update(
        {
            name: _marked_unique(columns[name])
            for name in unique_cols
            if name in columns and not columns[name].get("primary_key")
        }
    )


def unique_cols_from_index_rows(
    index_rows: list[tuple],
    index_info_rows: dict[str, list[tuple]],
) -> set[str]:
    """Extract single-column unique constraint columns from index data (pure)."""
    return {
        idx_cols[0][2]
        for idx_row in index_rows
        if bool(idx_row[2]) and idx_row[3] != "pk"
        for idx_cols in [index_info_rows.get(idx_row[1], [])]
        if len(idx_cols) == 1
    }


def _where_clause_of(sql: str) -> str:
    """The partial-index condition a CREATE INDEX statement carries."""
    return sql[sql.upper().index(" WHERE ") + 7 :].strip()


def _index_from_row(unique: bool, columns: list[str], sql: str | None) -> Index:
    """One index, from its uniqueness, its columns and its own DDL."""
    facts: list[tuple[str, Any]] = [
        ("columns", columns),
        *[("unique", True) for distinct in [unique] if distinct],
        *[
            ("where", _where_clause_of(text))
            for text in [sql]
            if text and " WHERE " in text.upper()
        ],
    ]
    return dict(facts)  # type: ignore[return-value]


def indexes_from_rows(
    index_rows: list[tuple],
    index_info_rows: dict[str, list[tuple]],
    index_sql: dict[str, str | None],
) -> dict[str, Index]:
    """Build Index dict from index list/info/sql data (pure).

    A `pk` INDEX IS ALWAYS A CONSEQUENCE; A `u` INDEX IS ONLY SOMETIMES.
    This skipped both, with the note: "they are consequences of a column
    definition, not objects a caller declared, and reporting them would
    make the differ try to recreate them."

    True of a single-column unique. SQLite creates the index itself for
    `"email" TEXT UNIQUE`, and `apply_unique_columns` folds it back onto
    the column as `unique: True`, so the fact lives on the column and
    the index does not need to carry it.

    A MULTI-COLUMN UNIQUE HAS NO COLUMN TO FOLD ONTO. It is a
    table-level constraint somebody wrote by hand, and skipping it lost
    the fact rather than moving it. Measured 2026-08-19 against a table
    this package did not create:

        CREATE TABLE "t" (..., UNIQUE ("c","d"))
        introspect -> indexes: None, and nothing on either column

    Invisible, so a rebuild wrote the table back without it and the
    duplicates it was refusing started being accepted.
    """
    return {
        name: _index_from_row(unique, columns, index_sql.get(name))
        for idx_row in index_rows
        for name, unique, origin in [(idx_row[1], bool(idx_row[2]), idx_row[3])]
        if origin != "pk"
        for columns in [[row[2] for row in index_info_rows.get(name, [])]]
        if not (origin == "u" and len(columns) <= 1)
    }


def fk_list_from_pragma_rows(rows: list[tuple]) -> list[dict[str, str]]:
    """Convert PRAGMA foreign_key_list rows to FK dicts (pure)."""
    return [
        {
            "from": row[3],
            "table": row[2],
            "to": row[4],
            "on_update": row[5],
            "on_delete": row[6],
        }
        for row in rows
    ]


def as_literal(name: str) -> str:
    """An identifier as a SQL string literal, apostrophes doubled.

    A PRAGMA takes no bound parameters, and neither does the `name = '...'`
    of a `sqlite_master` lookup as it was written, so an identifier reaches
    those queries as text. A legal table named `od\'d` closed the literal and
    the engine reported `near "d": syntax error` for the whole introspection,
    not merely for that table. Measured 2026-08-18 on SQLite and on Turso.

    Doubling the apostrophe is the SQL escape for a string literal and it is
    the whole of the fix. `as_identifier`, in `abstractions/pragma_compat`,
    does the other quoting, which needs the other escape. It lives there
    rather than here because importing the inspector from an abstraction is a
    cycle, and this one is beside its callers for the same reason.
    """
    return name.replace("'", "''")


def _with_foreign_key(columns: dict[str, Column], fk: dict[str, str]) -> dict[str, Column]:
    """The columns, with one key folded onto the column it leaves from.

    A FOLD RATHER THAN A COMPREHENSION, because two keys can leave the same
    column. Each one is merged onto the answer the one before it gave, which
    is what the loop this replaced did; building every column from the
    original would let the second key drop the first key's action.
    """
    name = fk["from"]
    merged = [(name, _pointing_at(columns[name], fk)) for present in [name] if present in columns]
    return {**columns, **dict(merged)}


def _pointing_at(column: Column, fk: dict[str, str]) -> Column:
    """One column, carrying the key that leaves it.

    An action the engine reports as NO ACTION, or in words this package has
    no spelling for, is left off rather than written down as something else.
    """
    facts: list[tuple[str, Any]] = [
        *column.items(),
        ("references", f"{fk['table']}.{fk['to']}"),
        *[("on_delete", action) for action in [normalize_fk_action(fk.get("on_delete"))] if action],
        *[("on_update", action) for action in [normalize_fk_action(fk.get("on_update"))] if action],
    ]
    return dict(facts)  # type: ignore[return-value]


def _without_primary_key(column: Column) -> Column:
    """A member of a composite key, whose flag now sits on the table."""
    return {key: value for key, value in column.items() if key != "primary_key"}


def assemble_table(
    columns: dict[str, Column],
    indexes: dict[str, Index],
    foreign_keys: list[dict[str, str]],
    constraints: dict[str, dict[str, str]],
) -> Table:
    """Merge FK info into columns and build Table dict (pure)."""
    columns.update(reduce(_with_foreign_key, foreign_keys, dict(columns)))

    # A SINGLE PK STAYS ON ITS COLUMN; two or more become the table's key and
    # come off the columns, so the two spellings do not both describe it.
    named = [name for name, col in columns.items() if col.get("primary_key")]
    composite = named if len(named) > 1 else []
    columns.update({name: _without_primary_key(columns[name]) for name in composite})

    # REQUIRED, NOT DEFAULTED. A caller that forgets constraints gets a
    # TypeError rather than a table reported as having none, which is the
    # state that made a declared constraint be proposed on every deploy.
    facts: list[tuple[str, Any]] = [
        ("columns", columns),
        *[("primary_key", composite) for keys in [composite] if keys],
        *[("indexes", indexes) for declared in [indexes] if declared],
        *[("constraints", constraints) for declared in [constraints] if declared],
    ]
    return dict(facts)  # type: ignore[return-value]


def _declared_dependencies(stored: str | None) -> list[str]:
    """The `depends_on` column, back as the list the declaration wrote.

    `create_matview_sql` stores it as a JSON array. NULL and the empty string
    both mean a view that declared none, which is a real answer rather than a
    failure: the column is nullable and a view need not depend on anything.
    """
    return [name for text in [stored] if text for name in json.loads(text)]


def views_from_rows(
    rows: list[tuple], matviews: dict[str, tuple[str, str, list[str]]]
) -> dict[str, View]:
    """Convert sqlite_master view rows and matview metadata to `View` values.

    MATERIALIZED VIEWS ARE NOT IN `sqlite_master` AS VIEWS. SQLite has none,
    so the emulation makes a real TABLE plus a row in
    `_dp_materialized_views`. Reading only `sqlite_master` therefore reported
    `materialized: False` for every view, including the materialized ones.

    That broke idempotence: the declaration says True, introspection said
    False, so the differ saw a change and dropped and recreated the view on
    every migration. Measured 2026-08-14 with one materialized view declared:

        first : 2 operations
        re-run: 3 operations   (want 0)

    Recreating a materialized view discards its contents, so this was not
    merely noisy — it emptied the cache the view exists to be, on every
    deploy.

    `matviews` maps name to (query, refresh_strategy, depends_on), read from
    the metadata table by the caller. I/O stays with the caller; this stays
    pure.

    DEPENDS_ON WAS WRITTEN AND NEVER READ BACK. `create_matview_sql` stores it
    as a JSON array in the registry, and the read selected three columns of
    the four, so a declared `depends_on` could not survive a round trip. The
    feature file has a scenario for exactly this and it passed for months,
    because the step that claimed to introspect built its answer out of the
    row the previous step had written.
    """
    return {
        **{
            name: {
                "name": name,
                "query": extract_view_query(sql),
                "materialized": False,
            }
            for name, sql in rows
        },
        **{
            name: {
                "name": name,
                "query": query,
                "materialized": True,
                "refresh": refresh,
                "depends_on": depends_on,
            }
            for name, (query, refresh, depends_on) in matviews.items()
        },
    }


# =============================================================================
# Reading SQL text: depth, quotes and the two scanners built on them
# =============================================================================

# WHAT ONE CHARACTER DOES TO THE PARENTHESIS DEPTH. Everything else does
# nothing, which the sum over a one-element list says without a branch.
_DEPTH_CHANGE: dict[str, int] = {"(": 1, ")": -1}


def _depth_delta(char: str) -> int:
    """How far this character moves the parenthesis depth: 1, -1 or 0."""
    return sum(_DEPTH_CHANGE[opener] for opener in [char] if opener in _DEPTH_CHANGE)


def _paren_depths(text: str) -> list[int]:
    """The depth after each character, with the depth before any of them first.

    `_paren_depths(t)[i + 1]` is the depth once `t[i]` has been read, so the
    positions where the outermost pair closes are the ones holding zero.
    """
    return list(accumulate((_depth_delta(char) for char in text), initial=0))


def _no_body(_create_statement: str) -> str:
    """No body: the statement has no opening parenthesis.

    An empty string rather than None, because the caller reads the result as
    text and the two callers that once tested for None both meant "nothing
    between the parentheses".
    """
    return ""


def _body_between(create_statement: str) -> str:
    """What the first parenthesis holds, empty when it never closes.

    An unclosed parenthesis is a statement with no readable body, which is the
    same answer as no parenthesis at all. It used to be None here and `""` in
    the sibling, and both callers converted one to the other.
    """
    start = create_statement.find("(")
    depths = _paren_depths(create_statement[start:])
    closes = [offset for offset, depth in enumerate(depths[1:]) if depth == 0]
    bodies = [create_statement[start + 1 : start + offset] for offset in closes[:1]]
    return [*bodies, ""][0]


_TABLE_BODY: dict[bool, Callable[[str], str]] = {
    True: _no_body,
    False: _body_between,
}


def _table_body(create_statement: str) -> str:
    """What sits between the outermost parentheses of a CREATE TABLE.

    Empty when there are none. It used to answer None, and both callers wrote
    `"" if body is None else body` to undo that, so the absence was invented
    here and repaired twice.
    """
    return _TABLE_BODY[create_statement.find("(") == -1](create_statement)


def _keeps_the_text(text: str) -> str:
    """No leading CHECK keyword, so the expression is already alone."""
    return text


def _drops_the_keyword(text: str) -> str:
    """PostgreSQL spells the whole clause, so the keyword comes off."""
    return text[len("CHECK") :].strip()


_CHECK_PREFIX: dict[bool, Callable[[str], str]] = {
    True: _drops_the_keyword,
    False: _keeps_the_text,
}


def _closes_before_the_end(text: str) -> bool:
    """Whether the opening parenthesis is matched before the last character.

    `(a OR b) AND c` closes at position 7 of 14, so its parens wrap a part
    rather than the whole and stripping them would change what it means.
    """
    depths = _paren_depths(text)
    closes = [offset for offset, depth in enumerate(depths[1:]) if depth == 0]
    return closes[:1] not in ([], [len(text) - 1])


def _as_written(text: str) -> str:
    """The parentheses do not wrap the whole expression, so they stay."""
    return text


def _one_layer_off(text: str) -> str:
    """The outer pair wraps everything, so it comes off and the rest is asked."""
    return _unwrapped(text[1:-1].strip())


_UNWRAP: dict[bool, Callable[[str], str]] = {
    True: _one_layer_off,
    False: _as_written,
}


def _unwrapped(text: str) -> str:
    """Outer parens off, as many times as a pair wraps the whole expression."""
    return _UNWRAP[
        text.startswith("(") and text.endswith(")") and not _closes_before_the_end(text)
    ](text)


def normalize_check(expression: str) -> str:
    """A CHECK expression, in one spelling.

    The two engines answer differently for the same declaration:

        SQLite      n > 0
        PostgreSQL  CHECK ((n > 0))

    so both are reduced to the expression alone before anything compares them,
    the same way a declared type is spelled per engine before `same_type`
    asks. Comparing the raw answers would propose a change on every deploy.

    THE OUTER PARENS COME OFF ONLY WHEN THEY WRAP THE WHOLE THING.
    `(a OR b) AND c` opens and closes before the end, so stripping by position
    would turn it into `a OR b) AND c` and change what it means.
    """
    text = " ".join(expression.split())
    return _unwrapped(_CHECK_PREFIX[text.upper().startswith("CHECK")](text))


# SQLite quotes an identifier four ways, and three of them can hold a space
# or a comma: "name", `name`, [name]. Taking `clause.split()[0]` ended the
# name at its first space, and a splitter tracking only ' and " split
# a clause in half at the comma inside `[a,b]`.
_CLOSING = {'"': '"', "`": "`", "[": "]"}

# The same four openers, read the other way: which character ends the name
# this one begins. `'` is here and not in _CLOSING because a string literal
# opens a quote for the splitter but is not an identifier.
_OPENING = {"'": "'", '"': '"', "`": "`", "[": "]"}

# THE CHECK SCANNER READS ONLY STRING LITERALS. A quoted identifier there is
# ordinary text between parens, and treating `[` as a quote inside a column
# clause would swallow `DECIMAL[10]`.
_LITERAL_QUOTES = {"'": "'", '"': '"'}

# (parenthesis depth, the character that would close the open quote).
_ScanState = tuple[int, str]


def _still_open(quote: str, _char: str) -> str:
    """This character is not the one that closes the open quote."""
    return quote


def _now_closed(_quote: str, _char: str) -> str:
    """The character matched the open quote, so nothing is quoted after it."""
    return ""


_QUOTE_AFTER: dict[bool, Callable[[str, str], str]] = {
    True: _now_closed,
    False: _still_open,
}


def _inside_quote(_openers: dict[str, str], depth: int, quote: str, char: str) -> _ScanState:
    """Depth is frozen inside a quoted name or a string literal."""
    return depth, _QUOTE_AFTER[char == quote](quote, char)


def _outside_quote(openers: dict[str, str], depth: int, _quote: str, char: str) -> _ScanState:
    """A parenthesis moves the depth and an opener starts a quote.

    No character does both: `(` and `)` are in neither opener table.
    """
    return depth + _depth_delta(char), "".join(
        openers[opener] for opener in [char] if opener in openers
    )


_SCAN_STEP: dict[bool, Callable[[dict[str, str], int, str, str], _ScanState]] = {
    True: _inside_quote,
    False: _outside_quote,
}


def _step(openers: dict[str, str], state: _ScanState, char: str) -> _ScanState:
    """The state one character further on."""
    depth, quote = state
    return _SCAN_STEP[bool(quote)](openers, depth, quote, char)


def _scanned(text: str, openers: dict[str, str]) -> list[_ScanState]:
    """The state standing before each character of `text`, and after the last."""
    return list(accumulate(text, partial(_step, openers), initial=(0, "")))


def _quoted_identifier(text: str, closing: str) -> tuple[str, bool] | None:
    """The name inside a quote, or None when the quote never closes."""
    end = text.find(closing, 1)
    named = [(text[1:end], True) for position in [end] if position != -1]
    return [*named, None][0]


def _bare_identifier(text: str) -> tuple[str, bool]:
    """The name up to the first space, comma or parenthesis."""
    ends = [position for position, char in enumerate(text) if char.isspace() or char in "(,"]
    stopped = [(text[:position], False) for position in ends[:1]]
    return [*stopped, (text, False)][0]


def _leading_identifier(clause: str) -> tuple[str, bool] | None:
    """The column name a clause opens with, and whether it was quoted.

    Returns None for an empty clause. The QUOTED flag matters: a bare leading
    `CHECK` or `PRIMARY` starts a table-level constraint, and a quoted one is
    a column called that.

    FOUND BY FUZZING, with SQLite choosing the inputs:

        CREATE TABLE t (id TEXT, "CHECK" INTEGER CHECK ("CHECK" > 0))

    is a legal table with a column named CHECK, and treating the name as a
    keyword dropped its constraint -- which means comparing it against None on
    every deploy, the migration loop this scanner exists to avoid. I would not
    have thought of that column name.
    """
    text = clause.lstrip()
    answers = [
        *[_quoted_identifier(text, _CLOSING[char]) for char in text[:1] if char in _CLOSING],
        *[_bare_identifier(text) for char in text[:1] if char not in _CLOSING],
        None,
    ]
    return answers[0]


def _split_top_level(body: str) -> list[str]:
    """Split on commas that are not inside parentheses or a string literal."""
    states = _scanned(body, _OPENING)
    cuts = [
        position
        for position, char in enumerate(body)
        if char == "," and states[position] == (0, "")
    ]
    bounds = [-1, *cuts, len(body)]
    # Each cut paired with the next one. `zip(bounds, bounds[1:], strict=True)`
    # cannot express this: the two always differ in length by one, so strict
    # raises on every call. Indexing says what is meant and needs no flag.
    segments = [body[bounds[i] + 1 : bounds[i + 1]] for i in range(len(bounds) - 1)]
    # EVERY SEGMENT BEFORE A COMMA IS A CLAUSE, even an empty one, so `a,,b`
    # keeps its middle. What follows the last comma is a clause only when
    # something follows it, so a trailing comma adds nothing.
    return [segment.strip() for segment in segments[:-1]] + [
        segment.strip() for segment in segments[-1:] if segment
    ]


def _before(clause: str, position: int) -> str:
    """The character in front of this one, or a space at the start."""
    return clause[position - 1] if position else " "


def _expression_at(clause: str, position: int) -> str:
    """The CHECK expression starting at `position`, normalised.

    Empty when the clause holds none, because `_table_body` answers with an
    empty string rather than None.
    """
    rest = clause[position + 5 :].lstrip()
    normalised: str = applied_to_text(normalize_check, _table_body(rest))
    return normalised


def _check_in(clause: str) -> str | None:
    """The CHECK expression in one column clause, or None.

    Finds the KEYWORD, not the substring: `checksum TEXT` names no constraint,
    and a default of `'CHECK (x > 1)'` is a value.
    """
    states = _scanned(clause, _LITERAL_QUOTES)
    starts = [
        position
        for position in range(len(clause))
        if states[position] == (0, "")
        and clause[position : position + 5].upper() == "CHECK"
        and not _before(clause, position).isalnum()
        and _before(clause, position) != "_"
        and clause[position + 5 : position + 6] in (" ", "(")
        and clause[position + 5 :].lstrip().startswith("(")
    ]
    found: list[str] = [_expression_at(clause, position) for position in starts[:1]]
    return next(iter(found), None)


# The keywords that open a table-level clause. A leading one of these names no
# column, and a QUOTED one does, which is why `_leading_identifier` reports
# both the name and how it was written.
_TABLE_LEVEL_KEYWORDS = frozenset({"CHECK", "PRIMARY", "FOREIGN", "UNIQUE", "CONSTRAINT"})


def table_constraints_from_ddl(create_statement: str) -> dict[str, dict[str, str]]:
    """The NAMED table-level constraints a CREATE TABLE declares.

    `checks_from_ddl` below reads the column-level ones and skips these
    deliberately: a table-level CHECK belongs to no column, and attributing it
    to one would have the differ propose removing a column constraint nobody
    declared. Skipping it left them invisible instead.

    MEASURED 2026-08-19. A declared table-level constraint was not created at
    all -- `create_table_sql` ignored the key -- and once that was fixed it was
    created and still unseen, so every deploy emitted `add_constraint`, which
    SQLite and Turso refuse. Creating it and not reading it back is a loop of
    exactly the same shape as not creating it.

    ONLY NAMED ONES. An anonymous `CHECK (lo < hi)` has nothing to key a
    dict by, and the differ compares constraints by name. A declaration
    written through this package always names them, because
    `constraint_clause` writes `CONSTRAINT "<name>"`.
    """
    body = _table_body(create_statement)
    named = [
        (remainder, leading)
        for clause in _split_top_level(body)
        for stripped in [clause.strip()]
        if stripped.upper().startswith("CONSTRAINT")
        for remainder in [stripped[len("CONSTRAINT") :].strip()]
        for leading in [_leading_identifier(remainder)]
        if leading is not None
    ]
    checked = [
        (name, _check_in(rest))
        for remainder, (name, _quoted) in named
        for rest in [remainder[remainder.index(name) + len(name) :].lstrip('" ').strip()]
        if rest.upper().startswith("CHECK")
    ]
    return {
        name: {"type": "check", "expression": expression}
        for name, expression in checked
        if expression is not None
    }


def checks_from_ddl(create_statement: str) -> dict[str, str]:
    """The column-level CHECK constraints a CREATE TABLE declares.

    SQLite keeps no catalogue of these: the only record is the text of the
    statement in `sqlite_master`, so this reads it. Without it a CHECK is
    created, enforced, and invisible to the differ, which is how comparing
    one produced a migration loop.

    A TABLE-LEVEL CHECK BELONGS TO NO COLUMN and is skipped. `CHECK (n > 0)`
    written as its own clause constrains the row; attributing it to `n` would
    have the differ propose removing a column constraint nobody declared.

    Scanning respects string literals and nested parentheses, because a
    default of `'CHECK (x > 1)'` is a value and `s IN ('x', 'y')` closes twice.
    """
    body = _table_body(create_statement)
    return {
        column: expression
        for clause in _split_top_level(body)
        for leading in [_leading_identifier(clause)]
        if leading is not None
        for column, was_quoted in [leading]
        if was_quoted or column.upper() not in _TABLE_LEVEL_KEYWORDS
        for expression in [_check_in(clause)]
        if expression is not None
    }
