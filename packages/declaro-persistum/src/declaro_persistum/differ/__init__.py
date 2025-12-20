"""
Schema diff engine.

Computes operations needed to transform current database state to target state.
Uses set theory operations and topological sorting for dependency-aware ordering.
"""

from declaro_persistum.differ.ambiguity import calculate_rename_confidence, detect_ambiguities
from declaro_persistum.differ.core import diff
from declaro_persistum.differ.extended import (
    diff_enums,
    diff_procedures,
    diff_triggers,
    diff_views,
)
from declaro_persistum.differ.toposort import topological_sort

__all__ = [
    "diff",
    "detect_ambiguities",
    "calculate_rename_confidence",
    "topological_sort",
    # Extended schema objects
    "diff_enums",
    "diff_triggers",
    "diff_procedures",
    "diff_views",
]
