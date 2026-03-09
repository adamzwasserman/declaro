"""
Portable abstractions for complex data types.

These abstractions provide database-agnostic patterns for:
- Enums (Literal types via lookup tables with FK constraints)
- Arrays (ordered collections via junction tables)
- Maps (key-value pairs via junction tables)
- Ranges (time periods, numeric ranges via start/end columns)
- Hierarchies (tree structures via closure tables)
"""

from .arrays import (
    array_append_sql,
    array_clear_sql,
    array_delete_sql,
    array_get_sql,
    array_hydrate,
    array_insert_sql,
    array_move_sql,
    array_reindex_sql,
    generate_junction_table,
    parse_array_type,
)
from .hierarchy import (
    ancestors_query_sql,
    build_tree,
    children_query_sql,
    closure_delete_subtree_sql,
    closure_insert_root_sql,
    closure_insert_sql,
    closure_update_parent_sql,
    descendants_at_depth_sql,
    descendants_query_sql,
    generate_closure_table,
    is_descendant_sql,
    parent_query_sql,
    path_query_sql,
    subtree_count_sql,
)
from .maps import (
    generate_junction_table as generate_map_junction_table,
)
from .maps import (
    map_clear_sql,
    map_delete_sql,
    map_get_all_sql,
    map_get_sql,
    map_hydrate,
    map_keys_sql,
    map_set_sql,
    parse_map_type,
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
from .ranges import (
    generate_range_columns,
    parse_range_type,
    range_adjacent_sql,
    range_contains_point_sql,
    range_contains_range_sql,
    range_from_dict,
    range_overlaps_sql,
    range_to_dict,
)
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
from .pragma_compat import (
    pragma_table_info,
    pragma_index_list,
    pragma_index_info,
    pragma_foreign_key_list,
    get_emulation_count,
    get_native_success_count,
    get_affected_tables,
    reset_counters,
)
from .check_compat import (
    CheckAST,
    CheckParseError,
    CheckValidationError,
    ValidatorFn,
    ValidationResult,
    parse_check_expression,
    generate_validator,
    register_check_constraint,
    validate_row,
    process_schema_checks,
    clear_registry,
    get_validation_stats,
    get_affected_tables as get_check_affected_tables,
)
from .reconstruction import (
    generate_create_table_sql,
    generate_data_copy_sql,
    get_reconstruction_columns,
    execute_reconstruction_async,
)

__all__ = [
    # Arrays
    "parse_array_type",
    "generate_junction_table",
    "array_insert_sql",
    "array_append_sql",
    "array_get_sql",
    "array_delete_sql",
    "array_clear_sql",
    "array_hydrate",
    "array_move_sql",
    "array_reindex_sql",
    # Maps
    "parse_map_type",
    "generate_map_junction_table",
    "map_set_sql",
    "map_get_sql",
    "map_get_all_sql",
    "map_delete_sql",
    "map_clear_sql",
    "map_keys_sql",
    "map_hydrate",
    # Ranges
    "parse_range_type",
    "generate_range_columns",
    "range_overlaps_sql",
    "range_contains_point_sql",
    "range_contains_range_sql",
    "range_adjacent_sql",
    "range_to_dict",
    "range_from_dict",
    # Hierarchy
    "generate_closure_table",
    "closure_insert_sql",
    "closure_insert_root_sql",
    "closure_update_parent_sql",
    "closure_delete_subtree_sql",
    "descendants_query_sql",
    "descendants_at_depth_sql",
    "ancestors_query_sql",
    "path_query_sql",
    "children_query_sql",
    "parent_query_sql",
    "is_descendant_sql",
    "subtree_count_sql",
    "build_tree",
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
    "get_emulation_count",
    "get_native_success_count",
    "get_affected_tables",
    "reset_counters",
    # CHECK Constraint Emulation (Turso Database Rust)
    "CheckAST",
    "CheckParseError",
    "CheckValidationError",
    "ValidatorFn",
    "ValidationResult",
    "parse_check_expression",
    "generate_validator",
    "register_check_constraint",
    "validate_row",
    "process_schema_checks",
    "clear_registry",
    "get_validation_stats",
    "get_check_affected_tables",
    # Table Reconstruction (SQLite/Turso ALTER COLUMN workaround)
    "generate_create_table_sql",
    "generate_data_copy_sql",
    "get_reconstruction_columns",
    "execute_reconstruction_async",
]
