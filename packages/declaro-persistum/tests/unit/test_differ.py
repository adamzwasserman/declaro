"""Tests for the diff engine."""

import pytest

from declaro_persistum.differ.core import diff
from declaro_persistum.differ.ambiguity import (
    detect_ambiguities,
    calculate_rename_confidence,
)
from declaro_persistum.differ.toposort import topological_sort, build_dependency_graph
from declaro_persistum.types import Schema








