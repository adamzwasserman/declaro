"""Literal types become lookup tables with FK constraints, not CHECK.

`abstractions/enums.py` sat at 18% with 12 branches — part of the Slop
Audit L1.19 gap (declaro-xu0). Every function is pure: names, SQL strings
and schema dicts out of plain input.

The design this pins down: a `Literal[...]` column does NOT become a CHECK
constraint, because CHECK is not portable across the four backends. It
becomes a `_dp_enum_{table}_{column}` lookup table plus a foreign key, so
the constraint is enforced by machinery every backend has.

Values reach SQL by string interpolation here, not as parameters — these
are DDL and seed statements built ahead of execution. `_escape_sql` is
therefore load-bearing and gets its own quote-injection tests.
"""

from declaro_persistum.abstractions.enums import (
    ENUM_TABLE_PREFIX,
    _escape_sql,
    add_enum_value_sql,
    create_enum_table_sql,
    diff_enum_values,
    drop_enum_table_sql,
    enum_table_name,
    expand_schema_enums,
    generate_enum_table_schema,
    get_enum_fk_reference,
    is_enum_table,
    remove_enum_value_sql,
    transform_column_for_enum,
)














