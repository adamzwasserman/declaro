"""
Portable abstractions for complex data types.

These abstractions provide database-agnostic patterns for:
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
]
