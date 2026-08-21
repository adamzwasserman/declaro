"""
Schema diff engine.

Computes operations needed to transform current database state to target state.
Uses set theory operations and topological sorting for dependency-aware ordering.
"""

from declaro_persistum.differ.ambiguity import detect_ambiguities
from declaro_persistum.differ.core import diff
from declaro_persistum.differ.extended import diff_views
from declaro_persistum.differ.toposort import topological_sort

__all__ = [
    "diff",
    "detect_ambiguities",
    "topological_sort",
    "diff_views",
]
