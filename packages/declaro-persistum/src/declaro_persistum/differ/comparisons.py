"""How two schemas are compared, property by property.

differ/core.py reached 1526 lines and could not be cut anywhere: its helpers
named each other in both directions from top to bottom. This is what came out
when the dispatch tables were taken with everything they reach, which is the
one division that leaves the dependency running one way.

Every name is re-exported from `core`, so nothing that imports from there
had to change."""

import re
from collections.abc import Callable
from itertools import accumulate
from typing import Any

from declaro_persistum.boundary import helper
from declaro_persistum.types import (
    Column,
    Dialect,
    Operation,
    Table,
    applied_to_text,
    type_of,
)

# A STRING LITERAL, CLOSED OR NOT. The capture group makes `re.split` hand back
# the literals alongside what sits between them, so a rule that must not reach
# inside quotes is applied to the pieces at even positions only. The trailing
# `?` covers a quote nobody closed: the character scanner this replaced copied
# the rest of the text verbatim in that case, and so does this.
_STRING_LITERAL = re.compile(r"('[^']*'?|\"[^\"]*\"?)")

# The space Turso inserts in front of an opening paren when it stores DDL.
_SPACE_BEFORE_PAREN = re.compile(r" \(")


@helper
def _first_balanced(text: str) -> int:
    """Where the leading paren is closed, or -1 when nothing closes it."""
    closed = [
        position
        for position, depth in enumerate(
            accumulate(int(char == "(") - int(char == ")") for char in text)
        )
        if depth == 0
    ]
    return [*closed, -1][0]


@helper
def _wraps_the_whole(text: str) -> bool:
    """Whether one pair of parens encloses everything between them.

    `(a > 0) AND (b > 0)` does not: its leading paren closes at the seventh
    character rather than at the last one, so the parens are part of the
    expression and taking them off would change what it says.
    """
    return (
        text.startswith("(") and text.endswith(")") and _first_balanced(text) in (-1, len(text) - 1)
    )


@helper
def _peeled(text: str) -> str:
    """The parens carry nothing, so they come off and the rest is asked again."""
    return _without_outer_parens(text[1:-1].strip())


@helper
def _as_it_stands(text: str) -> str:
    """The parens belong to the expression, so the text is already right."""
    return text


_UNWRAP: dict[bool, Callable[[str], str]] = {True: _peeled, False: _as_it_stands}


@helper
def _without_outer_parens(text: str) -> str:
    """An expression with parentheses enclosing the whole of it removed.

    PostgreSQL renders an expression back through `pg_get_expr`, which wraps
    the whole thing, so the declared text and the stored text differ by a pair
    of parens that mean nothing.
    """
    return _UNWRAP[_wraps_the_whole(text)](text)


@helper
def _canonical_spacing(expression: str) -> str:
    """An expression with its cosmetic whitespace and outer parens removed.

    Turso stores `datetime('now')` as `datetime ('now')`. The space carries no
    meaning, and a string literal is left alone because a space inside quotes
    is a character.
    """
    spaced = "".join(
        piece if index % 2 else _SPACE_BEFORE_PAREN.sub("(", piece)
        for index, piece in enumerate(_STRING_LITERAL.split(expression.strip()))
    )
    return _without_outer_parens(spaced)


@helper
def _same_flag(current: Any, target: Any) -> bool:
    """A flag, where absent and False say the same thing."""
    return bool(current) == bool(target)


@helper
def _same_text(current: Any, target: Any) -> bool:
    """An expression, where absent and empty are different declarations."""
    return bool(current == target)


# EACH COMPARED PROPERTY NAMES ITS OWN COMPARISON. Read with `[]`, so a
# property that joins the list without one is refused rather than quietly
# compared the way `unique` is.
_CONSTRAINT_COMPARISON: dict[str, Callable[[Any, Any], bool]] = {
    "unique": _same_flag,
    "check": _same_text,
}


# WHAT A NUMBER LOOKS LIKE WRITTEN DOWN, so that recognising one is a declared
# pattern rather than an exception caught from `float()`. Optional sign, digits
# either side of an optional point, optional exponent.
_NUMERIC = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?\Z")


@helper
def _as_number(value: Any) -> float | None:
    """The number this default denotes, or None if it does not denote one.

    A bare `0` is a number. `'0'` WITH QUOTES is the one-character string and
    must not fold into it, which is why the quotes are not stripped first.

    A BOOL IS NOT A NUMBER HERE even though Python says `True == 1`. A default
    of `true` and a default of `1` are different declarations, and folding
    them would report a changed column as unchanged.
    """
    reading = [_NUMBER_IN[kind] for kind in [type_of(value)] if kind in _NUMBER_IN]
    return [*reading, _not_a_number][0](value)


@helper
def _not_a_number(_value: Any) -> float | None:
    """Nothing here denotes a number, and saying so is the answer."""
    return 0


@helper
def _the_number(value: Any) -> float | None:
    """An int or a float already is one."""
    return float(value)


@helper
def _the_number_in_text(value: Any) -> float | None:
    """Text denotes a number when it is written like one, and not otherwise."""
    return float(value) if _NUMERIC.match(value.strip()) else None


# THE EXACT TYPE, so a bool is not an int. `translated_by` would walk the class
# hierarchy and read `True` as 1.
_NUMBER_IN: dict[type, Callable[[Any], float | None]] = {
    bool: _not_a_number,
    int: _the_number,
    float: _the_number,
    str: _the_number_in_text,
}


@helper
def _comparable(value: Any, spelled: dict[str, str]) -> Any:
    """One default, in the form both sides can be compared in.

    A default is a value on one side of the comparison and an expression on
    the other, so each is put into one form before they meet. Only text needs
    it: a number or a bool is already what it is.

    THE ENGINE'S OWN SPELLING FIRST. A default declared `now()` is WRITTEN to
    SQLite as `(datetime('now'))`, because SQLite cannot parse the first, and
    read back as what was written. Comparing the stored spelling against the
    declared one without knowing the translation happened proposed an
    alter_column on every deploy.

    THEN THE WHITESPACE. Turso inserts a space after a function name when it
    stores DDL, so `(datetime('now'))` came back as `((datetime ('now')))`
    and migrated on every deploy. Whitespace and redundant parentheses only:
    an operator rewrite is not foldable, which is why the CHECK comparison is
    SQLite-only.

    WRITTEN ONCE AND APPLIED TO BOTH SIDES. This was four inline guards, two
    per rule, one per argument. `same_default` has been fixed three times, for
    booleans, then numbers, then `now()`, and each fix added a guard. A rule
    that reaches one argument and not the other is the shape all three had.
    """
    return applied_to_text(
        lambda text: _canonical_spacing(spelled.get(text.strip().lower().replace(" ", ""), text)),
        value,
    )


@helper
def _lowered(text: str) -> str:
    """A bare word, compared without regard to case."""
    return text.lower()


@helper
def _as_written(text: str) -> str:
    """A quoted literal: `'Pending'` and `'pending'` are two different values."""
    return text


# WHETHER THE TEXT IS A BARE WORD, as a table rather than a guard.
_FOLD_KEYWORD: dict[bool, Callable[[str], str]] = {
    True: _lowered,
    False: _as_written,
}


@helper
def _folded_keyword(text: str) -> str:
    """A bare keyword folded to lower case, a quoted literal left alone.

    A BARE KEYWORD IS CASE-INSENSITIVE, and this folded only booleans.
    PostgreSQL stores `current_timestamp` as `CURRENT_TIMESTAMP`, so a schema
    declaring the lowercase form migrated on every deploy:

        alter_column {"default": {"from": "CURRENT_TIMESTAMP",
                                  "to": "current_timestamp"}}
    """
    stripped = text.strip()
    return _FOLD_KEYWORD[stripped.replace("_", "").isalnum()](stripped)


@helper
def _both_absent(current: Any, target: Any, _dialect: Dialect) -> bool:
    """One side declares no default, so they agree only if neither does."""
    return current is target


@helper
def _same_spelling(current: Any, target: Any, dialect: Dialect) -> bool:
    """Both sides declare a default, so the two spellings are compared."""
    # The declared default, spelled the way this engine stores it. The applier
    # uses the same table on the way in, so the two sides are asking one
    # question rather than two.
    from declaro_persistum.applier.shared import EXPRESSION_DEFAULTS

    spelled = EXPRESSION_DEFAULTS.get(dialect, {})
    current = _comparable(current, spelled)
    target = _comparable(target, spelled)

    current_number = _as_number(current)
    target_number = _as_number(target)

    return _WHEN_BOTH_ARE_NUMBERS[current_number is not None and target_number is not None](
        current, target, current_number, target_number
    )


@helper
def _same_number(_current: Any, _target: Any, current_number: Any, target_number: Any) -> bool:
    """Both denote a number, so the numbers decide it."""
    return bool(current_number == target_number)


@helper
def _same_keyword(current: Any, target: Any, _current_number: Any, _target_number: Any) -> bool:
    """At least one is opaque, so the two spellings are compared verbatim.

    EACH SIDE FOLDED ON ITS OWN, THEN COMPARED. This asked whether both were
    text and then whether both were bare words, so a rule that reached one
    argument and not the other was the shape of all three bugs `same_default`
    has been fixed for.
    """
    return bool(
        applied_to_text(_folded_keyword, current) == applied_to_text(_folded_keyword, target)
    )


_WHEN_EITHER_IS_ABSENT: dict[bool, Callable[[Any, Any, Dialect], bool]] = {
    True: _both_absent,
    False: _same_spelling,
}

_WHEN_BOTH_ARE_NUMBERS: dict[bool, Callable[[Any, Any, Any, Any], bool]] = {
    True: _same_number,
    False: _same_keyword,
}


@helper
def _no_key_change(
    _table_name: str,
    _col_name: str,
    _current_col: Column,
    _target_col: Column,
    _changed_action: bool,
) -> list[Operation]:
    """Neither side declares a key, so nothing migrates."""
    return []


@helper
def _dropped_key(
    table_name: str,
    col_name: str,
    current_col: Column,
    _target_col: Column,
    _changed_action: bool,
) -> list[Operation]:
    """The key was removed from the models, so it leaves the database."""
    return [
        {
            "op": "drop_foreign_key",
            "table": table_name,
            "details": {
                "column": col_name,
                "references": current_col.get("references"),
            },
        }
    ]


@helper
def _added_key(
    table_name: str,
    col_name: str,
    _current_col: Column,
    target_col: Column,
    _changed_action: bool,
) -> list[Operation]:
    """The models declare a key the database does not have."""
    return [
        {
            "op": "add_foreign_key",
            "table": table_name,
            "details": {
                "column": col_name,
                "references": target_col.get("references"),
                "on_delete": target_col.get("on_delete"),
                "on_update": target_col.get("on_update"),
            },
        }
    ]


@helper
def _replaced_key(
    table_name: str,
    col_name: str,
    current_col: Column,
    target_col: Column,
    changed_action: bool,
) -> list[Operation]:
    """The key points somewhere else now, so the old one goes and a new one comes."""
    return _dropped_key(table_name, col_name, current_col, target_col, changed_action) + _added_key(
        table_name, col_name, current_col, target_col, changed_action
    )


@helper
def _restated_key(
    table_name: str,
    col_name: str,
    _current_col: Column,
    target_col: Column,
) -> list[Operation]:
    """The key points where it did and does something else on delete or update.

    The declared referential actions are carried across only where the models
    write them, so a key declared without an `ON DELETE` clause is restated
    without one rather than with an explicit NULL.
    """
    return [
        {
            "op": "drop_foreign_key",
            "table": table_name,
            "details": {"column": col_name},
        },
        {
            "op": "add_foreign_key",
            "table": table_name,
            "details": {
                "column": col_name,
                "references": target_col.get("references"),
                **{key: target_col[key] for key in ("on_delete", "on_update") if key in target_col},
            },
        },
    ]


@helper
def _kept_key(
    _table_name: str,
    _col_name: str,
    _current_col: Column,
    _target_col: Column,
) -> list[Operation]:
    """Both sides declare the same key doing the same thing."""
    return []


_WHEN_ACTION_CHANGED: dict[bool, Callable[[str, str, Column, Column], list[Operation]]] = {
    True: _restated_key,
    False: _kept_key,
}


@helper
def _same_reference(
    table_name: str,
    col_name: str,
    current_col: Column,
    target_col: Column,
    changed_action: bool,
) -> list[Operation]:
    """The key points where it did, so only its actions can have changed."""
    return _WHEN_ACTION_CHANGED[changed_action](table_name, col_name, current_col, target_col)


# WHAT THE TWO SIDES SAY ABOUT ONE COLUMN'S FOREIGN KEY, as the three facts
# that decide it: whether each side declares one, and whether the two name the
# same target. Every combination those three can take is a key here, so an
# unlisted one is a state nobody has thought about rather than a silent no-op.
_COLUMN_KEY: dict[
    tuple[bool, bool, bool], Callable[[str, str, Column, Column, bool], list[Operation]]
] = {
    (True, True, True): _same_reference,
    (True, True, False): _replaced_key,
    (True, False, False): _dropped_key,
    (False, True, False): _added_key,
    (False, False, True): _no_key_change,
    (False, False, False): _replaced_key,
}


# A QUOTED NAME THAT NEEDS NO QUOTING: lower case, word characters, and not
# opening with a digit. PostgreSQL renders such a name with the quotes off.
_REDUNDANTLY_QUOTED = re.compile(r'^"[a-z_][a-z0-9_]*"$')


@helper
def _unquoted(piece: str) -> str:
    """The quotes carry nothing, so the bare name means the same column."""
    return piece[1:-1]


@helper
def _quoted_as_written(piece: str) -> str:
    """A string literal, or a name whose quotes are what make it that column."""
    return piece


_QUOTING: dict[bool, Callable[[str], str]] = {
    True: _unquoted,
    False: _quoted_as_written,
}


@helper
def _folded_quoting(piece: str) -> str:
    """One quoted piece of a predicate, spelled the way PostgreSQL renders it."""
    return _QUOTING[bool(_REDUNDANTLY_QUOTED.match(piece))](piece)


@helper
def _fold_predicate(expression: str) -> str:
    """One SQL predicate, in the spelling every engine would agree on.

    PostgreSQL does not keep the text an author wrote. It parses the clause and
    renders it back through `pg_get_expr`, which wraps the whole thing in
    parentheses and drops the quoting from any identifier that does not need
    it, so `WHERE "deleted_at" IS NULL` reads back as `(deleted_at IS NULL)`.
    Comparing those as strings proposed rebuilding the index on every run.

    QUOTES COME OFF ONLY WHERE THEY CARRY NOTHING. A quoted name that is
    already lower case and holds nothing but word characters means the same
    column unquoted, and PostgreSQL will render it that way. `"Email"` and
    `"my col"` keep theirs, because there the quoting is what makes the name
    mean that column: unquoted, `Email` folds to `email` and is a different
    column. Folding those would report a real difference as no change.

    `_canonical_spacing` cannot do this job. It was written for a default like
    `datetime ('now')` and deletes the space before an opening paren, which
    turns `(a > 0) AND (b > 0)` into something no engine emits.

    THE SAME SPLIT `_canonical_spacing` USES, rather than a second scanner. A
    hand-written walk over the characters read quotes here and `_STRING_LITERAL`
    read them one screen up, so one file held two answers to "where does a
    quoted piece start and stop". The pieces at odd positions are the quoted
    ones, closed or not, and a rule that must not reach inside a literal is
    applied to those alone.

    THE OUTER PARENS COME OFF THROUGH `_without_outer_parens`, which is what
    the loop that used to sit here spelled out a second time. Checked against
    the loop over 216,136 inputs, every string up to four characters over the
    alphabet that matters here among them: the two agree everywhere.
    """
    text = " ".join(expression.split())
    folded = "".join(
        _folded_quoting(piece) if index % 2 else piece
        for index, piece in enumerate(_STRING_LITERAL.split(text))
    ).strip()
    return _without_outer_parens(folded)


@helper
def _one_predicate_absent(current: Any, target: Any) -> bool:
    """One side restricts nothing, so the two agree only if neither does."""
    return bool(current == target)


@helper
def _both_predicates_folded(current: Any, target: Any) -> bool:
    """Both sides restrict, so the two spellings fold before they are compared."""
    return _fold_predicate(current) == _fold_predicate(target)


_WHEN_A_PREDICATE_IS_ABSENT: dict[bool, Callable[[Any, Any], bool]] = {
    True: _one_predicate_absent,
    False: _both_predicates_folded,
}


@helper
def _same_constraint_definition(current: Any, target: Any) -> bool:
    """Two constraint definitions, compared as the engine stores them.

    A RAW DICT COMPARISON LOOPED ON TURSO. That engine rewrites `<>` to `!=`
    in the DDL it stores, so a declared `lo <> hi` came back as `lo != hi`,
    the two dicts differed, and every deploy emitted drop_constraint and
    add_constraint -- which Turso then refuses, so every deploy failed.
    Measured 2026-08-19 across four expression shapes; that was the only one
    that differed, and finding it took running all four rather than one.
    #
    The same normalisation the column-level CHECK comparison uses, plus the
    inequality fold. `_canonical_spacing` already strips the space-before-paren
    Turso adds and the redundant outer parens PostgreSQL adds.
    """
    # A DEFINITION THAT IS NOT A MAPPING HAS NO FIELDS TO COMPARE, so the two
    # are the same only if they are equal outright.
    both = {type_of(current), type_of(target)}
    return _WHEN_BOTH_ARE_MAPPINGS[both == {dict}](current, target)


@helper
def _equal_outright(current: Any, target: Any) -> bool:
    """At least one definition is not a mapping, so equality decides it."""
    return bool(current == target)


@helper
def _same_fields(current: Any, target: Any) -> bool:
    """Two mappings, field by field.

    Every field but the expression compares as it stands. `type` is one of
    them: it was compared on its own first and then again in the walk over the
    keys, which asked the same question twice and could only ever give the
    same answer.
    """
    return bool(
        all(
            current.get(key) == target.get(key)
            for key in (set(current) | set(target)) - {"expression"}
        )
        and _canonical_expression(current.get("expression"))
        == _canonical_expression(target.get("expression"))
    )


_WHEN_BOTH_ARE_MAPPINGS: dict[bool, Callable[[Any, Any], bool]] = {
    True: _same_fields,
    False: _equal_outright,
}


@helper
def _canonical_expression(expression: Any) -> Any:
    """One spelling for expressions the engines write differently."""
    return applied_to_text(lambda text: _canonical_spacing(text).replace("<>", "!="), expression)


@helper
def _constraints_not_compared(
    _table_name: str,
    _current: Table,
    _target: Table,
) -> list[Operation]:
    """PostgreSQL rewrites the expression it stores, so nothing is compared.

    The reasoning is in `_diff_constraints` above: the stored text is not the
    written text there, so a comparison proposes a change on every deploy.
    """
    return []


@helper
def _compared_constraints(
    table_name: str,
    current: Table,
    target: Table,
) -> list[Operation]:
    """The constraint operations for an engine that stores what was written."""
    current_constraints = current.get("constraints", {})
    target_constraints = target.get("constraints", {})

    current_names = set(current_constraints.keys())
    target_names = set(target_constraints.keys())

    # A constraint both sides declare, spelled two different ways. It is
    # dropped and added again, the same pair a removal and an addition make.
    redefined = [
        const_name
        for const_name in current_names & target_names
        if not _same_constraint_definition(
            current_constraints[const_name], target_constraints[const_name]
        )
    ]

    # Drop removed constraints
    dropped: list[Operation] = [
        {
            "op": "drop_constraint",
            "table": table_name,
            "details": {"constraint": const_name},
        }
        for const_name in current_names - target_names
    ]
    # Add new constraints
    added: list[Operation] = [
        {
            "op": "add_constraint",
            "table": table_name,
            "details": {
                "constraint": const_name,
                "definition": target_constraints[const_name],
            },
        }
        for const_name in target_names - current_names
    ]
    # Replace the redefined ones
    replaced: list[Operation] = [
        operation
        for const_name in redefined
        for operation in _replaced_constraint(
            table_name, const_name, target_constraints[const_name]
        )
    ]
    return [*dropped, *added, *replaced]


@helper
def _replaced_constraint(table_name: str, const_name: str, definition: Any) -> list[Operation]:
    """A constraint the two sides spell differently: the old goes, the new comes."""
    return [
        {
            "op": "drop_constraint",
            "table": table_name,
            "details": {"constraint": const_name},
        },
        {
            "op": "add_constraint",
            "table": table_name,
            "details": {"constraint": const_name, "definition": definition},
        },
    ]


# WHICH ENGINES A NAMED CONSTRAINT IS COMPARED ON, named one by one. Read with
# `[]`, so an engine that joins the vocabulary without an entry here is refused
# rather than quietly treated like PostgreSQL.
_CONSTRAINTS_ON: dict[Dialect, Callable[[str, Table, Table], list[Operation]]] = {
    "sqlite": _compared_constraints,
    "turso": _compared_constraints,
    "postgresql": _constraints_not_compared,
}
