"""`compile_statement`: a declaration and an engine in, a statement and its values out.

THE LAST STEP, AND THE ONLY ONE THAT KNOWS WHICH ENGINE IT IS TALKING TO. A
model declares what exists, a grammar declares what you want, and neither
carries a word belonging to PostgreSQL, SQLite or Turso. The engine is named
here, it is handed in, and there is no default for it.

THE STATEMENT IS COMPILED AND THE VALUES ARE NOT. They come back beside it,
untouched, for the driver to bind. A value that reached the statement text
would be a value nobody bound, and no earlier check would have seen it happen,
so the values go through `bind` and nothing else. That is why `contains` writes
`name LIKE '%' || $1 || '%'` rather than binding `%bob%`: putting the per-cent
signs around the value would be this function editing a value, and a caller who
wrote a per-cent sign of their own could no longer tell what they had asked.

WHY THERE ARE NO BRANCHES. Three engines and eighteen asking words are two
tables. A branch is a decision made at run time because something upstream was
not decided at declaration time, and both of those were decided in
`docs/design/persistum.hd`. Every choice below is a subscript, so a word nobody
declared raises at the lookup instead of falling through an arm nobody wrote.

WHAT IT REFUSES, RATHER THAN GUESSES. A refusal comes back in the return, never
raised and never written to a log. Four of them: something that is not a
declaration, an engine nobody declared, an asking word nobody declared, and a
declaration missing something its word needs -- an order for `first`, a column
for `sum`, a narrowing for `remove_all`.

TWO GAPS, NAMED HERE BECAUSE A SILENT ONE IS WORSE THAN A LOUD ONE.

`replace` is refused. An upsert has to name the column it conflicts on, and all
three engines require that column in the text of `ON CONFLICT`. A declaration
does not carry a key, and this function is not handed the model that declares
one, so there is nothing here to write. Deciding it from the first column of
the first row would be a guess that worked until somebody's first column was
not the key.

CASE-SENSITIVE TEXT MATCHING IS NOT HONEST ON SQLITE AND TURSO YET. PostgreSQL
reads `LIKE` case-sensitively and `ILIKE` the other way. SQLite reads `LIKE`
case-INsensitively for ASCII, and what Turso does has not been measured on this
machine. So `ignore_case` is true everywhere below, spelled `LOWER(x) LIKE
LOWER(y)`, which is right on all three; `ignore_case` false is spelled plain
`LIKE`, which is right on PostgreSQL and is the silent mismatch this package
has already been bitten by once. The fix belongs in the `sqlite` and `turso`
rows of `_ENGINE` and it needs measuring against both engines first.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import get_args, get_type_hints

from declaro_persistum.how_to_compare import how_to_compare
from declaro_persistum.types import (
    Comparison,
    Condition,
    Declaration,
    Engine,
    Fault,
    Statement,
    Value,
    Wanted,
)

# The ten fields a declaration carries, read off the declaration's own shape so
# that adding a field does not need an edit here to keep this recogniser honest.
_A_DECLARATION_HAS = frozenset(get_type_hints(Declaration))

_THE_THREE_ENGINES = get_args(Engine)
_THE_EIGHTEEN_WORDS = get_args(Wanted)


# --- how each engine spells the two things it spells differently -------------
#
# ONE ROW PER ENGINE, AND THE ROWS ARE READ BY SUBSCRIPT. An engine nobody
# declared never reaches the lookup, because the word is checked against the
# vocabulary first and refused there with all three named.
#
# SQLITE AND TURSO SPELL BOTH OF THESE THE SAME WAY TODAY, and they still have
# a row each. Folding them into one row would make the next difference between
# them an edit to the shape of this table rather than an edit to one row.

_ENGINE: dict[Engine, dict[str, Callable[..., str]]] = {
    "postgresql": {
        # asyncpg numbers its placeholders and reads nothing else.
        "placeholder": lambda position: f"${position}",
        "quote": lambda name: '"' + name.replace('"', '""') + '"',
    },
    "sqlite": {
        # aiosqlite takes a question mark per value, in order.
        "placeholder": lambda _position: "?",
        "quote": lambda name: '"' + name.replace('"', '""') + '"',
    },
    "turso": {
        "placeholder": lambda _position: "?",
        "quote": lambda name: '"' + name.replace('"', '""') + '"',
    },
}


# --- the pieces every shape is built out of ----------------------------------


def _binder(engine: Engine) -> tuple[Callable[[Value], str], dict[str, Value]]:
    """A way to put a value beside the statement and get back its placeholder.

    The values it collects are handed back as they arrived. Nothing here reads
    one, edits one, or writes one into the text: the only thing that reaches
    the text is the placeholder, and the placeholder is built from a count.
    """
    kept: dict[str, Value] = {}
    spell = _ENGINE[engine]["placeholder"]

    def bind(value: Value) -> str:
        kept[f"p{len(kept) + 1}"] = value
        return spell(len(kept))

    return bind, kept


def _said(word: str, clause: str) -> str:
    """`WHERE x = $1`, or nothing at all when there is no clause to introduce."""
    return {True: "", False: f"{word} {clause}"}[clause == ""]


def _joined(word: str, parts: Iterable[str]) -> str:
    """`(a AND b)`, and nothing when there is nothing to join."""
    kept = list(filter(None, parts))
    return {True: "", False: "(" + f" {word} ".join(kept) + ")"}[kept == []]


def _bracketed(clause: str) -> str:
    """`(a AND b)`, so a nested condition cannot bind itself to its neighbour."""
    return {True: "", False: f"({clause})"}[clause == ""]


def _sentence(*pieces: str) -> str:
    """The clauses that have something to say, in order, one space apart."""
    return " ".join(filter(None, pieces))


# --- one comparison, read off the operators table -----------------------------
#
# THE FIFTEEN ROWS USED TO LIVE HERE and now live in `fn how_to_compare`, which
# is one of the seven tables `docs/architecture.md:56-68` names. A copy here
# would be a second place the same fact lives, and it drifted once already: this
# file spelled every ignore_case as `LOWER(x) LIKE LOWER(y)` while PostgreSQL
# has `ILIKE` for exactly that.

_HOW_MANY_VALUES: dict[str, Callable[[str, str, Comparison, Callable[[Value], str], str], str]] = {
    "one": lambda column, symbol, asked, bind, _wraps: (
        f"{column} {symbol} {bind(asked['value'])}"
    ),
    "list": lambda column, symbol, asked, bind, _wraps: (
        f"{column} {symbol} ("
        + ", ".join(bind(one) for one in asked["value"])  # type: ignore[union-attr]
        + ")"
    ),
    "range": lambda column, symbol, asked, bind, _wraps: (
        f"{column} {symbol} "
        + bind(asked["value"][0])  # type: ignore[index]
        + " AND "
        + bind(asked["value"][1])  # type: ignore[index]
    ),
    # THE PATTERN IS BUILT IN THE STATEMENT AND THE VALUE IS STILL THE VALUE.
    # `||` concatenates on all three engines, so the per-cent signs are ours and
    # what the caller wrote arrives at the driver character for character.
    "pattern": lambda column, symbol, asked, bind, wraps: (
        f"{column} {symbol} "
        + " || ".join(
            filter(
                None,
                (
                    f"'{wraps.split('{}')[0]}'" if wraps.split("{}")[0] else "",
                    bind(asked["value"]),
                    f"'{wraps.split('{}')[1]}'" if wraps.split("{}")[1] else "",
                ),
            )
        )
    ),
    "none": lambda column, symbol, _asked, _bind, _wraps: f"{column} {symbol}",
}

# WHICH WORD TO WRITE WHEN CASE DOES NOT MATTER. An engine with a word of its
# own gets that word, and one without gets both sides folded, which is right
# everywhere and is what this file did for every operator before the table
# existed. Both rows are written out, so an operator with no such word is
# answered by a row rather than by a check falling through.
_WHEN_CASE_DOES_NOT_MATTER: dict[
    bool, Callable[[str, str, Callable[[str], str]], tuple[str, Callable[[str], str]]]
] = {
    True: lambda _symbol, instead, _plain: (instead, lambda sql: sql),
    False: lambda symbol, _instead, fold: (symbol, fold),
}

# WHETHER THE CALLER ASKED FOR CASE TO BE IGNORED. Folding the column alone
# would compare a folded column against an unfolded value, which matches
# nothing and says nothing about why, so the fold is applied to both sides.
_CASE: dict[bool, Callable[[str], str]] = {
    False: lambda sql: sql,
    True: lambda sql: f"LOWER({sql})",
}


def _comparer(
    engine: Engine, quote: Callable[[str], str], bind: Callable[[Value], str]
) -> Callable[[str, Comparison], str]:
    """A way to write one comparison on this engine, settled once.

    Handed down beside `quote` and `bind`, which are engine-shaped in the same
    way, so no clause builder below ever sees an engine.
    """

    def compared(column: str, asked: Comparison) -> str:
        return _compared(column, asked, engine, quote, bind)

    return compared


def _compared(
    column: str,
    asked: Comparison,
    engine: Engine,
    quote: Callable[[str], str],
    bind: Callable[[Value], str],
) -> str:
    """One comparison, written the way this engine writes it."""
    how = how_to_compare(asked["operator"], engine)
    symbol, fold = _WHEN_CASE_DOES_NOT_MATTER[
        asked["ignore_case"] and bool(how["ignoring_case"])  # type: ignore[typeddict-item]
    ](
        how["symbol"],  # type: ignore[typeddict-item]
        how["ignoring_case"],  # type: ignore[typeddict-item]
        _CASE[asked["ignore_case"]],
    )
    return _HOW_MANY_VALUES[how["shape"]](  # type: ignore[typeddict-item]
        fold(quote(column)),
        symbol,
        asked,
        lambda value: fold(bind(value)),
        how["wraps"],  # type: ignore[typeddict-item]
    )


def _narrowing(condition: Condition, compare: Callable[[str, Comparison], str]) -> str:
    """One condition, as SQL, with every value bound and no value in the text.

    IT COMES BACK WITHOUT BRACKETS AROUND IT, and each nested condition gets
    its own. Stripping an outer bracket off the finished string instead ate the
    closing bracket of `LOWER($1)` the first time a caller asked to ignore case.
    """
    each = [compare(column, asked) for column, asked in condition["each"].items()]
    any_of = [_joined("OR", [_bracketed(_narrowing(one, compare)) for one in condition["any_of"]])]
    none_of = [
        _said(
            "NOT",
            _joined("AND", [_bracketed(_narrowing(one, compare)) for one in condition["none_of"]]),
        )
    ]
    return " AND ".join(filter(None, each + any_of + none_of))


def _asks_for_nothing(condition: Condition) -> bool:
    """Whether a condition narrows anything at all."""
    return condition["each"] == {} and condition["any_of"] == [] and condition["none_of"] == []


def _across(declaration: Declaration, quote: Callable[[str], str]) -> str:
    """The joins the library worked out, which name columns and bind nothing."""
    return " ".join(
        f"INNER JOIN {quote(join['model'])} ON "
        f"{quote(declaration['model'])}.{quote(join['by_column'])} = "
        f"{quote(join['model'])}.{quote(join['to_column'])}"
        for join in declaration["across"] or ()
    )


def _order(declaration: Declaration, quote: Callable[[str], str]) -> str:
    """The order the caller stated, ties broken left to right."""
    direction = {"ascending": "ASC", "descending": "DESC"}
    return _said(
        "ORDER BY",
        ", ".join(
            f"{quote(column)} {direction[way]}" for column, way in declaration["order"].items()
        ),
    )


def _grouping(declaration: Declaration, quote: Callable[[str], str]) -> str:
    return _said("GROUP BY", ", ".join(quote(column) for column in declaration["grouping"]))


def _columns(declaration: Declaration, quote: Callable[[str], str]) -> str:
    """The columns asked for, or every column when none were named."""
    return ", ".join(quote(column) for column in declaration["columns"]) or "*"


def _measured(declaration: Declaration, quote: Callable[[str], str]) -> str:
    """The one column an aggregate is taken over, or every row when none is named.

    `count` is the row that names none, and a star is what counting every row
    is spelled as. A caller who does name a column gets the count of the rows
    where that column holds something, which is a different question and one
    they are allowed to ask.
    """
    return ", ".join(quote(column) for column in declaration["columns"][:1]) or "*"


def _reading(
    declaration: Declaration,
    quote: Callable[[str], str],
    _bind: Callable[..., str],
    compare: Callable[[str, Comparison], str],
    select_list: str,
    tail: str,
) -> str:
    """One SELECT, with every clause the declaration filled in."""
    return _sentence(
        f"SELECT {select_list}",
        f"FROM {quote(declaration['model'])}",
        _across(declaration, quote),
        _said("WHERE", _narrowing(declaration["condition"], compare)),
        _grouping(declaration, quote),
        _said("HAVING", _narrowing(declaration["having"], compare)),
        _order(declaration, quote),
        tail,
    )


# WHAT EACH MEASURE WRITES AROUND THE COLUMN IT MEASURES. Six words, one shape:
# the group columns come back beside the measure, because a caller who grouped
# by status wants to know which status each number belongs to.
_MEASURE: dict[str, str] = {
    "count": "COUNT({column})",
    "sum": "SUM({column})",
    "average": "AVG({column})",
    "smallest": "MIN({column})",
    "largest": "MAX({column})",
    "count_distinct": "COUNT(DISTINCT {column})",
}


def _measuring(word: str) -> Callable[..., str]:
    """A reading that takes one measure over one column, beside its groups."""

    def built(
        declaration: Declaration,
        quote: Callable[[str], str],
        bind: Callable[..., str],
        compare: Callable[[str, Comparison], str],
    ) -> str:
        groups = [quote(column) for column in declaration["grouping"]]
        measured = _MEASURE[word].format(column=_measured(declaration, quote))
        return _reading(declaration, quote, bind, compare, ", ".join([*groups, measured]), "")

    return built


def _all(
    declaration: Declaration,
    quote: Callable[[str], str],
    bind: Callable[..., str],
    compare: Callable[[str, Comparison], str],
) -> str:
    return _reading(declaration, quote, bind, compare, _columns(declaration, quote), "")


def _one(
    declaration: Declaration,
    quote: Callable[[str], str],
    bind: Callable[..., str],
    compare: Callable[[str, Comparison], str],
) -> str:
    """TWO ROWS, NOT ONE. `one` answers a row, an absence, or a refusal naming
    how many matched, and a statement that took one row could not tell the
    third case from the first."""
    return _reading(declaration, quote, bind, compare, _columns(declaration, quote), "LIMIT 2")


def _first(
    declaration: Declaration,
    quote: Callable[[str], str],
    bind: Callable[..., str],
    compare: Callable[[str, Comparison], str],
) -> str:
    return _reading(declaration, quote, bind, compare, _columns(declaration, quote), "LIMIT 1")


def _page(
    declaration: Declaration,
    quote: Callable[[str], str],
    bind: Callable[..., str],
    compare: Callable[[str, Comparison], str],
) -> str:
    """Pages are counted from one, so page 1 skips nothing."""
    paging = declaration["paging"] or {}
    size = bind(paging["size"])
    skipped = bind(paging["size"] * (paging["number"] - 1))
    return _reading(
        declaration,
        quote,
        bind,
        compare,
        _columns(declaration, quote),
        f"LIMIT {size} OFFSET {skipped}",
    )


def _distinct(
    declaration: Declaration,
    quote: Callable[[str], str],
    bind: Callable[..., str],
    compare: Callable[[str, Comparison], str],
) -> str:
    return _reading(
        declaration, quote, bind, compare, f"DISTINCT {_columns(declaration, quote)}", ""
    )


def _exists(
    declaration: Declaration,
    quote: Callable[[str], str],
    _bind: Callable[..., str],
    compare: Callable[[str, Comparison], str],
) -> str:
    """It reads no rows: the engine answers true or false and stops looking."""
    inner = _sentence(
        "SELECT 1",
        f"FROM {quote(declaration['model'])}",
        _across(declaration, quote),
        _said("WHERE", _narrowing(declaration["condition"], compare)),
        "LIMIT 1",
    )
    return f"SELECT EXISTS({inner})"


def _add(
    declaration: Declaration,
    quote: Callable[[str], str],
    bind: Callable[..., str],
    _compare: Callable[[str, Comparison], str],
) -> str:
    """Every row the caller handed over, in one statement, columns from the first."""
    rows = declaration["rows"] or []
    columns = list(rows[0])
    written = ", ".join(
        "(" + ", ".join(bind(row[column]) for column in columns) + ")" for row in rows
    )
    return (
        f"INSERT INTO {quote(declaration['model'])} "
        + "("
        + ", ".join(quote(column) for column in columns)
        + ") "
        + f"VALUES {written}"
    )


def _change(
    declaration: Declaration,
    quote: Callable[[str], str],
    bind: Callable[..., str],
    compare: Callable[[str, Comparison], str],
) -> str:
    """The new values, and the narrowing that says which rows get them."""
    (row,) = declaration["rows"] or [{}]
    setting = ", ".join(f"{quote(column)} = {bind(value)}" for column, value in row.items())
    return _sentence(
        f"UPDATE {quote(declaration['model'])}",
        f"SET {setting}",
        _said("WHERE", _narrowing(declaration["condition"], compare)),
    )


def _remove(
    declaration: Declaration,
    quote: Callable[[str], str],
    _bind: Callable[..., str],
    compare: Callable[[str, Comparison], str],
) -> str:
    return _sentence(
        f"DELETE FROM {quote(declaration['model'])}",
        _said("WHERE", _narrowing(declaration["condition"], compare)),
    )


# --- the eighteen asking words, one row each ---------------------------------

_SHAPE: dict[Wanted, Callable[..., str]] = {
    "all": _all,
    "one": _one,
    "first": _first,
    "page": _page,
    "distinct": _distinct,
    "exists": _exists,
    "count": _measuring("count"),
    "sum": _measuring("sum"),
    "average": _measuring("average"),
    "smallest": _measuring("smallest"),
    "largest": _measuring("largest"),
    "count_distinct": _measuring("count_distinct"),
    "add": _add,
    "change": _change,
    "change_all": _change,
    "remove": _remove,
    "remove_all": _remove,
    # REFUSED, AND THE MODULE'S OWN NOTE SAYS WHY: an upsert names the column it
    # conflicts on and a declaration carries no key.
    "replace": lambda _declaration, _quote, _bind: "",
}


# --- what each word needs before there is anything to compile ----------------

_MET: dict[str, Callable[[Declaration], bool]] = {
    "an order to be first in": lambda declaration: declaration["order"] != {},
    "a page to fetch": lambda declaration: declaration["paging"] is not None,
    "a page number counted from one": lambda declaration: (declaration["paging"] or {"number": 1})[
        "number"
    ]
    >= 1,
    "a column to measure": lambda declaration: declaration["columns"] != [],
    "the rows it is declaring": lambda declaration: declaration["rows"] not in (None, []),
    "exactly one row to change": lambda declaration: len(declaration["rows"] or []) == 1,
    "a narrowing saying which rows": lambda declaration: not _asks_for_nothing(
        declaration["condition"]
    ),
    "a key to conflict on": lambda _declaration: False,
}

# WHAT TO DO ABOUT EACH ONE, a row per requirement rather than one sentence
# covering all eight. Seven of them the caller fixes by saying one more thing;
# `replace` is the one nobody can fix here, and telling them to say it louder
# would send them looking for a word that does not exist.
_INSTEAD: dict[str, str] = {
    "an order to be first in": "State the order. The first of an unordered set is whichever "
    "row the engine reached first, which is not an answer to anything.",
    "a page to fetch": "State the page: how big it is and which one you want.",
    "a page number counted from one": "Number the pages from one. Page 1 skips nothing.",
    "a column to measure": "Name the one column to measure.",
    "the rows it is declaring": "Hand over the rows you are declaring exist.",
    "exactly one row to change": "Hand over one row of new values. Several rows are several "
    "changes, and each one needs the narrowing that says which row it lands on.",
    "a narrowing saying which rows": "Narrow it. A change or a removal carrying no narrowing "
    "is every row in the table, which nobody means on purpose.",
    "a key to conflict on": "Ask for add, or for change with a narrowing. An upsert has to "
    "name the column it conflicts on, the model is what declares which column that is, and "
    "the renderer is handed the declaration and the engine and no model.",
}

_NEEDS: dict[Wanted, tuple[str, ...]] = {
    "all": (),
    "one": (),
    "first": ("an order to be first in",),
    "page": ("a page to fetch", "a page number counted from one"),
    "distinct": ("a column to measure",),
    "exists": (),
    "count": (),
    "sum": ("a column to measure",),
    "average": ("a column to measure",),
    "smallest": ("a column to measure",),
    "largest": ("a column to measure",),
    "count_distinct": ("a column to measure",),
    "add": ("the rows it is declaring",),
    "change": ("exactly one row to change", "a narrowing saying which rows"),
    "change_all": ("exactly one row to change", "a narrowing saying which rows"),
    "remove": ("a narrowing saying which rows",),
    "remove_all": ("a narrowing saying which rows",),
    "replace": ("a key to conflict on",),
}


# --- the four refusals -------------------------------------------------------


def _not_a_declaration(declaration: object, _engine: Engine) -> Fault:
    return {
        "kind": "refused",
        "what_went_wrong": f"The renderer was handed a {type(declaration).__name__} and it "
        "compiles a declaration. Writing SQL is this function's job and nobody else's, so "
        "SQL handed in is a caller doing the one thing the library owns.",
        "what_to_do_instead": "Ask your question in one of the four grammars and hand over the "
        "declaration it builds. A statement that came back from here is finished: it carries no "
        "model and no columns, and it cannot be compiled a second time.",
    }


def _no_such_engine(_declaration: Declaration, engine: object) -> Fault:
    return {
        "kind": "refused",
        "what_went_wrong": f"No engine here is called {engine!r}. This package compiles for "
        "postgresql, sqlite and turso, and for nothing else.",
        "what_to_do_instead": "Open the database with a target naming one of postgresql, sqlite "
        "or turso; the engine the renderer is handed is the one the open decided.",
    }


def _no_such_word(declaration: Declaration, _engine: Engine) -> Fault:
    return {
        "kind": "refused",
        "what_went_wrong": f"No asking word is called {declaration['wanted']!r}, so there is "
        "nothing for the renderer to compile.",
        "what_to_do_instead": "Ask for one of: " + ", ".join(sorted(_THE_EIGHTEEN_WORDS)) + ".",
    }


def _short_of_what_it_needs(declaration: Declaration, _engine: Engine) -> Fault:
    missing = _unmet(declaration)
    return {
        "kind": "refused",
        "what_went_wrong": f"A {declaration['wanted']} needs "
        + ", and ".join(missing)
        + ", and this declaration carries none of that.",
        "what_to_do_instead": _INSTEAD[missing[0]],
    }


def _unmet(declaration: Declaration) -> list[str]:
    """What this word needs and this declaration does not give."""
    return [need for need in _NEEDS[declaration["wanted"]] if not _MET[need](declaration)]


# --- the four questions, each one a lookup -----------------------------------


def _compiled(declaration: Declaration, engine: Engine) -> Statement:
    """The statement this word spells on this engine, and the values beside it."""
    quote = _ENGINE[engine]["quote"]
    bind, parameters = _binder(engine)
    return {
        "text": _SHAPE[declaration["wanted"]](
            declaration, quote, bind, _comparer(engine, quote, bind)
        ),
        "parameters": parameters,
    }


_WHEN_IT_HAS_WHAT_IT_NEEDS: dict[bool, Callable[[Declaration, Engine], Statement | Fault]] = {
    True: _compiled,
    False: _short_of_what_it_needs,
}


def _with_a_declared_word(declaration: Declaration, engine: Engine) -> Statement | Fault:
    return _WHEN_IT_HAS_WHAT_IT_NEEDS[_unmet(declaration) == []](declaration, engine)


_WHEN_THE_WORD_IS_DECLARED: dict[bool, Callable[[Declaration, Engine], Statement | Fault]] = {
    True: _with_a_declared_word,
    False: _no_such_word,
}


def _with_a_declared_engine(declaration: Declaration, engine: Engine) -> Statement | Fault:
    return _WHEN_THE_WORD_IS_DECLARED[declaration["wanted"] in _SHAPE](declaration, engine)


_WHEN_THE_ENGINE_IS_DECLARED: dict[bool, Callable[..., Statement | Fault]] = {
    True: _with_a_declared_engine,
    False: _no_such_engine,
}


def _with_a_declaration(declaration: Declaration, engine: Engine) -> Statement | Fault:
    return _WHEN_THE_ENGINE_IS_DECLARED[engine in _THE_THREE_ENGINES](declaration, engine)


_WHEN_IT_IS_A_DECLARATION: dict[bool, Callable[..., Statement | Fault]] = {
    True: _with_a_declaration,
    False: _not_a_declaration,
}


def _is_a_declaration(declaration: object) -> bool:
    """Whether this carries the ten fields a declaration carries."""
    return isinstance(declaration, dict) and frozenset(declaration) >= _A_DECLARATION_HAS


def compile_statement(declaration: Declaration, engine: Engine) -> Statement | Fault:
    """Compile a declaration into one engine's statement, values kept beside it.

    It opens nothing, reads no clock, no random source and no environment
    variable, and asks no database what it supports. Handed the same
    declaration and the same engine it writes the same characters, on a machine
    with no database on it.
    """
    return _WHEN_IT_IS_A_DECLARATION[_is_a_declaration(declaration)](declaration, engine)
