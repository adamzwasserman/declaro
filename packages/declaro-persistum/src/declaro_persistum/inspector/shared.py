"""
Shared pure functions for SQLite-compatible inspectors (SQLite, Turso/libSQL).

All functions are pure: no I/O, no side effects, no state.
Both SQLiteInspector and TursoInspector delegate shared logic here.
"""

import re
from typing import Literal

from declaro_persistum.types import Column, Index, Table, View

# Type for FK actions
FKAction = Literal["cascade", "set null", "restrict", "no action"]


def normalize_fk_action(action: str | None) -> FKAction | None:
    """Normalize FK action string to proper Literal type."""
    if action is None or action == "NO ACTION":
        return None
    normalized = action.lower().replace(" ", "_")
    action_map = {
        "cascade": "cascade",
        "set_null": "set null",
        "restrict": "restrict",
        "no_action": "no action",
    }
    return action_map.get(normalized)  # type: ignore[return-value]


def normalize_sqlite_type(col_type: str) -> str:
    """Normalize SQLite type to canonical form."""
    if not col_type:
        return "blob"

    col_type = col_type.lower().strip()

    if "int" in col_type:
        return "integer"
    elif "char" in col_type or "clob" in col_type or "text" in col_type:
        return "text"
    elif "blob" in col_type or col_type == "":
        return "blob"
    elif "real" in col_type or "floa" in col_type or "doub" in col_type:
        return "real"
    else:
        return "numeric"




def extract_view_query(create_statement: str) -> str:
    """Extract SELECT query from CREATE VIEW statement."""
    match = re.search(r"\bAS\s+(.+)$", create_statement, re.IGNORECASE | re.DOTALL)
    if match:
        return " ".join(match.group(1).split())
    return ""


def columns_from_pragma_rows(
    rows: list[tuple],
) -> dict[str, Column]:
    """Convert PRAGMA table_info rows to Column dict (pure)."""
    columns: dict[str, Column] = {}

    for row in rows:
        col_name = row[1]
        col_type = row[2]
        not_null = bool(row[3])
        default = row[4]
        is_pk = bool(row[5])

        col: Column = {"type": normalize_sqlite_type(col_type)}

        if not_null or is_pk:
            # PK columns are implicitly NOT NULL in SQL even when
            # PRAGMA table_info reports notnull=0 for them.
            col["nullable"] = False

        if default is not None:
            # PRAGMA table_info strips outer parens from expression defaults.
            # Re-wrap function-call expressions so they match the target schema form.
            if isinstance(default, str) and "(" in default and not default.startswith("("):
                default = f"({default})"
            col["default"] = default

        if is_pk:
            col["primary_key"] = True

        columns[col_name] = col

    return columns


def apply_unique_columns(
    columns: dict[str, Column], unique_cols: set[str]
) -> None:
    """Mark unique columns (mutates columns in place)."""
    for col_name in unique_cols:
        if col_name in columns and not columns[col_name].get("primary_key"):
            columns[col_name]["unique"] = True


def unique_cols_from_index_rows(
    index_rows: list[tuple],
    index_info_rows: dict[str, list[tuple]],
) -> set[str]:
    """Extract single-column unique constraint columns from index data (pure)."""
    unique_cols: set[str] = set()

    for idx_row in index_rows:
        idx_name = idx_row[1]
        is_unique = bool(idx_row[2])
        origin = idx_row[3]

        if is_unique and origin != "pk":
            idx_cols = index_info_rows.get(idx_name, [])
            if len(idx_cols) == 1:
                unique_cols.add(idx_cols[0][2])

    return unique_cols


def indexes_from_rows(
    index_rows: list[tuple],
    index_info_rows: dict[str, list[tuple]],
    index_sql: dict[str, str | None],
) -> dict[str, Index]:
    """Build Index dict from index list/info/sql data (pure)."""
    indexes: dict[str, Index] = {}

    for idx_row in index_rows:
        idx_name = idx_row[1]
        is_unique = bool(idx_row[2])
        origin = idx_row[3]

        idx_cols = index_info_rows.get(idx_name, [])
        columns = [row[2] for row in idx_cols]

        # A `pk` INDEX IS ALWAYS A CONSEQUENCE; A `u` INDEX IS ONLY SOMETIMES.
        # This skipped both, with the note: "they are consequences of a column
        # definition, not objects a caller declared, and reporting them would
        # make the differ try to recreate them."
        #
        # True of a single-column unique. SQLite creates the index itself for
        # `"email" TEXT UNIQUE`, and `apply_unique_columns` folds it back onto
        # the column as `unique: True`, so the fact lives on the column and
        # the index does not need to carry it.
        #
        # A MULTI-COLUMN UNIQUE HAS NO COLUMN TO FOLD ONTO. It is a
        # table-level constraint somebody wrote by hand, and skipping it lost
        # the fact rather than moving it. Measured 2026-08-19 against a table
        # this package did not create:
        #
        #     CREATE TABLE "t" (..., UNIQUE ("c","d"))
        #     introspect -> indexes: None, and nothing on either column
        #
        # Invisible, so a rebuild wrote the table back without it and the
        # duplicates it was refusing started being accepted.
        if origin == "pk":
            continue
        if origin == "u" and len(columns) <= 1:
            continue

        index: Index = {"columns": columns}

        if is_unique:
            index["unique"] = True

        sql = index_sql.get(idx_name)
        if sql and " WHERE " in sql.upper():
            where_idx = sql.upper().index(" WHERE ")
            index["where"] = sql[where_idx + 7:].strip()

        indexes[idx_name] = index

    return indexes


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
    return name.replace("\'", "\'\'")


def assemble_table(
    columns: dict[str, Column],
    indexes: dict[str, Index],
    foreign_keys: list[dict[str, str]],
    constraints: dict[str, dict[str, str]],
) -> Table:
    """Merge FK info into columns and build Table dict (pure)."""
    for fk in foreign_keys:
        col_name = fk["from"]
        if col_name in columns:
            columns[col_name]["references"] = f"{fk['table']}.{fk['to']}"
            on_delete = normalize_fk_action(fk.get("on_delete"))
            if on_delete:
                columns[col_name]["on_delete"] = on_delete
            on_update = normalize_fk_action(fk.get("on_update"))
            if on_update:
                columns[col_name]["on_update"] = on_update

    table: Table = {"columns": columns}

    pk_columns = [name for name, col in columns.items() if col.get("primary_key")]
    if len(pk_columns) > 1:
        table["primary_key"] = pk_columns
        for col_name in pk_columns:
            del columns[col_name]["primary_key"]

    if indexes:
        table["indexes"] = indexes

    # REQUIRED, NOT DEFAULTED. A caller that forgets them gets a TypeError
    # rather than a table reported as having none, which is the state that
    # made a declared constraint be proposed on every deploy.
    if constraints:
        table["constraints"] = constraints  # type: ignore[typeddict-item]

    return table


def views_from_rows(
    rows: list[tuple], matviews: dict[str, tuple[str, str]]
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

    `matviews` maps name to (query, refresh_strategy), read from the metadata
    table by the caller. I/O stays with the caller; this stays pure.
    """
    views: dict[str, View] = {}
    for name, sql in rows:
        views[name] = {
            "name": name,
            "query": extract_view_query(sql),
            "materialized": False,
        }
    for name, (query, refresh) in matviews.items():
        views[name] = {
            "name": name,
            "query": query,
            "materialized": True,
            "refresh": refresh,
        }
    return views


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
    if text.upper().startswith("CHECK"):
        text = text[len("CHECK") :].strip()

    while text.startswith("(") and text.endswith(")"):
        depth = 0
        for position, char in enumerate(text):
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
                if depth == 0 and position != len(text) - 1:
                    return text
        text = text[1:-1].strip()
    return text


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
    if body is None:
        return {}

    constraints: dict[str, dict[str, str]] = {}
    for clause in _split_top_level(body):
        stripped = clause.strip()
        if not stripped.upper().startswith("CONSTRAINT"):
            continue
        remainder = stripped[len("CONSTRAINT") :].strip()
        leading = _leading_identifier(remainder)
        if leading is None:
            continue
        name, _quoted = leading
        rest = remainder[remainder.index(name) + len(name) :].lstrip('" ').strip()
        if not rest.upper().startswith("CHECK"):
            continue
        expression = _check_in(rest)
        if expression is not None:
            constraints[name] = {"type": "check", "expression": expression}
    return constraints


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
    if body is None:
        return {}

    checks: dict[str, str] = {}
    for clause in _split_top_level(body):
        leading = _leading_identifier(clause)
        if leading is None:
            continue
        column, was_quoted = leading
        if not was_quoted and column.upper() in (
            "CHECK",
            "PRIMARY",
            "FOREIGN",
            "UNIQUE",
            "CONSTRAINT",
        ):
            continue
        expression = _check_in(clause)
        if expression is not None:
            checks[column] = expression
    return checks


# SQLite quotes an identifier four ways, and three of them can hold a space
# or a comma: "name", `name`, [name]. Taking `clause.split()[0]` ended the
# name at its first space, and `_split_top_level` tracking only ' and " split
# a clause in half at the comma inside `[a,b]`.
_CLOSING = {'"': '"', "`": "`", "[": "]"}

# The same four openers, read the other way: which character ends the name
# this one begins. `'` is here and not in _CLOSING because a string literal
# opens a quote for the splitter but is not an identifier.
_OPENING = {"'": "'", '"': '"', "`": "`", "[": "]"}


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
    if not text:
        return None

    closing = _CLOSING.get(text[0])
    if closing:
        end = text.find(closing, 1)
        return (text[1:end], True) if end != -1 else None

    for position, char in enumerate(text):
        if char.isspace() or char in "(,":
            return text[:position], False
    return text, False


def _table_body(create_statement: str) -> str | None:
    """What sits between the outermost parentheses of a CREATE TABLE."""
    start = create_statement.find("(")
    if start == -1:
        return None
    depth = 0
    for position in range(start, len(create_statement)):
        char = create_statement[position]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return create_statement[start + 1 : position]
    return None


def _split_top_level(body: str) -> list[str]:
    """Split on commas that are not inside parentheses or a string literal."""
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    quote: str | None = None
    for char in body:
        if quote:
            current.append(char)
            if char == quote:
                quote = None
            continue
        # THE QUOTE TABLE ALREADY EXISTS, thirty lines above. This was an
        # if/elif chain re-deciding which characters open a quoted name, so
        # the two could disagree about `[name]` and only one would be fixed.
        if char in _OPENING:
            quote = _OPENING[char]
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    if current:
        parts.append("".join(current).strip())
    return parts


def _check_in(clause: str) -> str | None:
    """The CHECK expression in one column clause, or None.

    Finds the KEYWORD, not the substring: `checksum TEXT` names no constraint,
    and a default of `'CHECK (x > 1)'` is a value.
    """
    depth = 0
    quote: str | None = None
    position = 0
    while position < len(clause):
        char = clause[position]
        if quote:
            if char == quote:
                quote = None
            position += 1
            continue
        if char in "'\"":
            quote = char
            position += 1
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif depth == 0 and clause[position : position + 5].upper() == "CHECK":
            before = clause[position - 1] if position else " "
            after = clause[position + 5 : position + 6]
            if not before.isalnum() and before != "_" and (after in (" ", "(")):
                rest = clause[position + 5 :].lstrip()
                if rest.startswith("("):
                    inner = _table_body(rest)
                    return normalize_check(inner) if inner is not None else None
        position += 1
    return None
