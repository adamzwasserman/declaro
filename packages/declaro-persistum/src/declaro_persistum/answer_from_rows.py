"""What a read comes back as.

    fn answer_from_rows : (model: Model, rows: Rows, declaration: Declaration) -> Answer | Fault

Driver rows go in and the caller's answer comes out: instances of the model when
they asked for rows, a number when they asked how many, true or false when they
asked only whether any exist, and `NOTHING` when they asked for exactly one and
none matched.

ABSENCE IS A NAMED VALUE, NOT AN EMPTY CONTAINER. `docs/architecture.md` line
120: a row that is not there is an answer you have to look at, and `None` is
also what a nullable column holds, so the two stop being distinguishable exactly
where it matters. `one` matching nothing answers `NOTHING`, never `[]`.

EIGHTEEN ASKING WORDS AND A GROUPING, SO A TABLE. The asking vocabulary is
closed -- twelve reading words and six writing words -- and the shape of the
answer is a property of the word, not something to work out at runtime. A
grouping changes the shape of four of them, so the table is keyed on the pair.
`count` with no grouping is the number; `count` grouped by status is a row per
group, and a table keyed on the word alone would hand back one number and lose
every group but one.

AN INSTANCE OF THE MODEL IS A ROW CARRYING EVERY COLUMN THE MODEL DECLARES.
There is no class to build, so that set is the whole of the difference between
an instance and a mapping: a caller who named columns gets exactly those keys
and nothing pretending to be more. A row that came back short of a column the
model declares is refused rather than handed over as an instance with a hole in
it, because a caller reading `person["status"]` off a half-built instance gets a
`KeyError` far from the read that caused it.

A ROW ACROSS TWO MODELS ARRIVES QUALIFIED AND LEAVES NESTED. Two models can
declare a column with the same name, and a flat row holds one of them; the
second silently writes over the first. So a row that crosses models carries
`table.column` keys and comes back as one mapping per table. The near model's
columns are checked; the far model's cannot be, because a `Join` carries the far
model's NAME and this function is handed one `Model`.

`exists` READS NO ROWS. The answer is whether the driver handed one back, never
what was in it, which is what `HOW-IT-SHOULD-WORK.md` means by "answers true or
false and reads no rows". That makes the compiled statement's shape part of the
contract: `SELECT 1 ... LIMIT 1`, not `SELECT EXISTS(...)`, which hands back a
row whether or not anything matched.

A WRITING WORD DOES NOT COME BACK THIS WAY. A write is deposited and answers
with a ticket, so the six writing words are refused here rather than left out of
the table, where they would be a `KeyError` at the one moment a caller is least
able to read one.

Pure: three arguments in, one value out. No clock, no random source, no
environment, no file and no connection. Every failure is in the return.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import partial
from typing import cast, get_args

from declaro_persistum.types import (
    NOTHING,
    Answer,
    Declaration,
    Fault,
    Model,
    Row,
    Rows,
    Wanted,
    Writing,
)

# --- which names a row has to carry ----------------------------------------


def _every_column_the_model_declares(model: Model, _declaration: Declaration) -> list[str]:
    """An instance of the model carries all of them, so all of them are required."""
    return [column["name"] for column in model["columns"]]


def _a_list_the_declaration_carries(
    field: str, _model: Model, declaration: Declaration
) -> list[str]:
    """Column names the caller already wrote down, read off the field holding them.

    TWO FIELDS, ONE READING. `columns` is what a caller named, and a mapping
    owes exactly those keys. `grouping` is what a caller grouped by, and a group
    has to say which group it is. Both are a list of column names sitting in the
    declaration, so which field to read is the only thing that differs and it
    belongs in the table rather than in a second copy of this line.
    """
    return cast(list[str], declaration[field])  # type: ignore[literal-required]


def _the_models_own_columns_qualified(model: Model, declaration: Declaration) -> list[str]:
    """Across models, the near model's columns arrive under its table name."""
    return [
        f"{model['table']}.{name}" for name in _every_column_the_model_declares(model, declaration)
    ]


# Keyed on whether the declaration crosses models and whether the caller named
# columns. Naming columns across models is not a shape any scenario states, so
# it takes the nested answer too and the columns are read off the rows.
_REQUIRED: dict[tuple[bool, bool], Callable[[Model, Declaration], list[str]]] = {
    (False, False): _every_column_the_model_declares,
    (False, True): partial(_a_list_the_declaration_carries, "columns"),
    (True, False): _the_models_own_columns_qualified,
    (True, True): _the_models_own_columns_qualified,
}


# --- what one row comes back as --------------------------------------------


def _only_these_keys(required: list[str], row: Row) -> Row:
    """The row narrowed to what was required, so nothing rides along unasked."""
    return {name: row[name] for name in required}


def _nested_by_table(_required: list[str], row: Row) -> Row:
    """One qualified row split into one mapping per table it came from."""
    tables = {key.partition(".")[0] for key in row}
    return cast(
        Row,
        {
            table: {
                key.partition(".")[2]: value
                for key, value in row.items()
                if key.partition(".")[0] == table
            }
            for table in sorted(tables)
        },
    )


def _the_row_as_it_came(_required: list[str], row: Row) -> Row:
    """A group carries the columns it grouped by AND the numbers over them, and
    which numbers those are is the statement's business rather than this one's."""
    return dict(row)


_SHAPE_ROW: dict[bool, Callable[[list[str], Row], Row]] = {
    False: _only_these_keys,
    True: _nested_by_table,
}


# --- every row, or the refusal that says which column did not arrive --------


def _all_of_them(
    _model: Model,
    required: list[str],
    _missing: list[str],
    rows: Rows,
    shape: Callable[[list[str], Row], Row],
) -> list[Row] | Fault:
    return [shape(required, row) for row in rows]


def _short_of_a_column(
    model: Model,
    required: list[str],
    missing: list[str],
    _rows: Rows,
    _shape: Callable[[list[str], Row], Row],
) -> list[Row] | Fault:
    names = ", ".join(missing)
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"A row of {model['table']} came back without {names}. "
            f"The answer has to carry {', '.join(required)}, so handing this over "
            f"would be a {model['table']} row with a hole where {names} belongs."
        ),
        what_to_do_instead=(
            f"Ask the statement for {names} as well, or name the columns you do "
            f"want, which answers with a mapping of those columns instead of a "
            f"whole {model['table']} row."
        ),
    )


_WHOLE_OR_REFUSED: dict[
    bool,
    Callable[
        [Model, list[str], list[str], Rows, Callable[[list[str], Row], Row]],
        list[Row] | Fault,
    ],
] = {
    False: _all_of_them,
    True: _short_of_a_column,
}


def _checked(
    model: Model,
    required: list[str],
    rows: Rows,
    shape: Callable[[list[str], Row], Row],
) -> list[Row] | Fault:
    """Every row shaped, or the refusal naming what the driver did not hand back."""
    missing = sorted({name for row in rows for name in required if name not in row})
    return _WHOLE_OR_REFUSED[bool(missing)](model, required, missing, rows, shape)


def _shaped(model: Model, rows: Rows, declaration: Declaration) -> list[Row] | Fault:
    """The rows as this declaration asked for them."""
    across = bool(declaration["across"])
    required = _REQUIRED[(across, bool(declaration["columns"]))](model, declaration)
    return _checked(model, required, rows, _SHAPE_ROW[across])


# --- one row out of the shaped rows ----------------------------------------


def _the_head(shaped: list[Row]) -> Answer:
    return cast(Answer, shaped[0])


def _the_refusal(shaped: Fault) -> Answer:
    return cast(Answer, shaped)


_HEAD_OR_REFUSAL: dict[bool, Callable] = {True: _the_head, False: _the_refusal}


def _one_shaped_row(model: Model, rows: Rows, declaration: Declaration) -> Answer | Fault:
    """The single row, or the refusal that shaping it produced."""
    shaped = _shaped(model, rows[:1], declaration)
    return _HEAD_OR_REFUSAL[isinstance(shaped, list)](shaped)


def _a_named_absence(_model: Model, _rows: Rows, _declaration: Declaration) -> Answer | Fault:
    """Nothing matched, and that is an answer rather than an empty container."""
    return NOTHING


def _more_than_one_matched(model: Model, rows: Rows, _declaration: Declaration) -> Answer | Fault:
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"You asked for exactly one row of {model['table']} and "
            f"{len(rows)} came back, so there is no one row to hand over."
        ),
        what_to_do_instead=(
            f"Narrow the condition until it can match only one row of "
            f"{model['table']}, or ask for the first in a stated order."
        ),
    )


# Keyed on how many rows matched, counted no further than two because the
# answer for two and for two hundred is the same.
_EXACTLY_ONE: dict[int, Callable[[Model, Rows, Declaration], Answer | Fault]] = {
    0: _a_named_absence,
    1: _one_shaped_row,
    2: _more_than_one_matched,
}
_THE_FIRST: dict[int, Callable[[Model, Rows, Declaration], Answer | Fault]] = {
    0: _a_named_absence,
    1: _one_shaped_row,
    2: _one_shaped_row,
}


# --- the shape each asking word gives the answer ----------------------------


def _rows_of_the_model(model: Model, rows: Rows, declaration: Declaration) -> Answer | Fault:
    """Every matching row, each one an instance of the model."""
    return cast(Answer, _shaped(model, rows, declaration))


def _exactly_one(model: Model, rows: Rows, declaration: Declaration) -> Answer | Fault:
    """One row of the model, or the named absence."""
    return _EXACTLY_ONE[min(len(rows), 2)](model, rows, declaration)


def _the_first(model: Model, rows: Rows, declaration: Declaration) -> Answer | Fault:
    """The first row in the stated order, or the named absence."""
    return _THE_FIRST[min(len(rows), 2)](model, rows, declaration)


def _true_or_false(_model: Model, rows: Rows, _declaration: Declaration) -> Answer | Fault:
    """Whether the driver handed back a row. What was in it is never read."""
    return bool(rows)


def _the_number(_model: Model, _rows: Rows, _declaration: Declaration, values: list) -> Answer:
    """The number itself, not a row carrying it."""
    return cast(Answer, values[0])


def _no_number_came_back(
    model: Model, rows: Rows, declaration: Declaration, values: list
) -> Answer:
    return cast(
        Answer,
        Fault(
            kind="cut_short",
            what_went_wrong=(
                f"You asked {declaration['wanted']} of {model['table']} and the "
                f"driver handed back {len(rows)} rows carrying {len(values)} values, "
                f"so there is no single number to answer with."
            ),
            what_to_do_instead=(
                f"A number over rows is one row carrying one value. Ask "
                f"{declaration['wanted']} on its own, without other columns beside it."
            ),
        ),
    )


_ONE_NUMBER: dict[bool, Callable[[Model, Rows, Declaration, list], Answer]] = {
    True: _the_number,
    False: _no_number_came_back,
}


def _a_number_over_rows(model: Model, rows: Rows, declaration: Declaration) -> Answer | Fault:
    """A count, a total, an average, a smallest, a largest, or a count of
    different values. Each of them comes back as the number."""
    values = [value for row in rows for value in row.values()]
    return _ONE_NUMBER[len(values) == 1](model, rows, declaration, values)


def _the_groups(model: Model, rows: Rows, declaration: Declaration) -> Answer | Fault:
    """A row per group, carrying the columns it grouped by and the numbers over them."""
    required = _a_list_the_declaration_carries("grouping", model, declaration)
    return cast(Answer, _checked(model, required, rows, _the_row_as_it_came))


def _not_a_read(model: Model, _rows: Rows, declaration: Declaration) -> Answer | Fault:
    """A write is deposited and answers with a ticket, so it never arrives here."""
    return Fault(
        kind="refused",
        what_went_wrong=(
            f"`{declaration['wanted']}` writes to {model['table']}, and a write has "
            f"no rows to shape: it is deposited and its caller holds a ticket."
        ),
        what_to_do_instead=(
            "Collect the ticket the deposit handed back. Rows only come back from "
            "the twelve reading words."
        ),
    )


_BY_THE_READING_WORD: dict[str, Callable[[Model, Rows, Declaration], Answer | Fault]] = {
    "all": _rows_of_the_model,
    "distinct": _rows_of_the_model,
    "page": _rows_of_the_model,
    "one": _exactly_one,
    "first": _the_first,
    "exists": _true_or_false,
    "count": _a_number_over_rows,
    "sum": _a_number_over_rows,
    "average": _a_number_over_rows,
    "smallest": _a_number_over_rows,
    "largest": _a_number_over_rows,
    "count_distinct": _a_number_over_rows,
}

# THE WORD AND THE GROUPING TOGETHER. A grouping turns any reading word into a
# row per group, which is why the key is the pair rather than the word. The six
# writing words answer the same way under both, because the grouping is beside
# the point when there are no rows to shape.
ANSWER_SHAPE: dict[tuple[Wanted, bool], Callable[[Model, Rows, Declaration], Answer | Fault]] = (
    {(word, False): shape for word, shape in _BY_THE_READING_WORD.items()}
    | {(word, True): _the_groups for word in _BY_THE_READING_WORD}
    | {(word, grouped): _not_a_read for word in get_args(Writing) for grouped in (False, True)}
)


def answer_from_rows(model: Model, rows: Rows, declaration: Declaration) -> Answer | Fault:
    """Turn driver rows into the answer the caller asked for."""
    return ANSWER_SHAPE[(declaration["wanted"], bool(declaration["grouping"]))](
        model, rows, declaration
    )
