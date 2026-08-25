"""
Portable abstractions for complex data types.

These abstractions provide database-agnostic patterns for:
- Enums (Literal types via lookup tables with FK constraints)
- Arrays (ordered collections via junction tables)
- Maps (key-value pairs via junction tables)
- Ranges (time periods, numeric ranges via start/end columns)
- Hierarchies (tree structures via closure tables)
"""

from .enums import (
    ENUM_TABLE_PREFIX,
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
from .materialized_views import (
    MATVIEW_METADATA_TABLE,
    create_matview_sql,
    drop_matview_sql,
    drop_refresh_triggers_sql,
    generate_metadata_table_schema,
    generate_refresh_trigger_sql,
    get_matview_metadata_sql,
    is_matview_sql,
    list_matviews_sql,
    refresh_matview_sql,
)
from .pragma_compat import (
    pragma_foreign_key_list,
    pragma_index_info,
    pragma_index_list,
    pragma_table_info,
)
from .reconstruction import (
    execute_reconstruction_async,
    generate_create_table_sql,
    generate_data_copy_sql,
    get_reconstruction_columns,
)

__all__ = [
    # Arrays
    # Maps
    # Ranges
    # Hierarchy
    # Materialized Views (SQLite/Turso emulation)
    "MATVIEW_METADATA_TABLE",
    "create_matview_sql",
    "drop_matview_sql",
    "refresh_matview_sql",
    "generate_refresh_trigger_sql",
    "drop_refresh_triggers_sql",
    "generate_metadata_table_schema",
    "is_matview_sql",
    "get_matview_metadata_sql",
    "list_matviews_sql",
    # Enums (Literal type -> lookup table + FK)
    "ENUM_TABLE_PREFIX",
    "enum_table_name",
    "is_enum_table",
    "generate_enum_table_schema",
    "create_enum_table_sql",
    "drop_enum_table_sql",
    "add_enum_value_sql",
    "remove_enum_value_sql",
    "get_enum_fk_reference",
    "transform_column_for_enum",
    "expand_schema_enums",
    "diff_enum_values",
    # PRAGMA Compatibility (Turso Database Rust)
    "pragma_table_info",
    "pragma_index_list",
    "pragma_index_info",
    "pragma_foreign_key_list",
    # CHECK Constraint Emulation (Turso Database Rust)
    # Table Reconstruction (SQLite/Turso ALTER COLUMN workaround)
    "generate_create_table_sql",
    "generate_data_copy_sql",
    "get_reconstruction_columns",
    "execute_reconstruction_async",
]
