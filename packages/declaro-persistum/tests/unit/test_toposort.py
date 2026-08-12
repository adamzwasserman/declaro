"""Tests for topological sorting."""

import pytest

from declaro_persistum.differ.toposort import (
    topological_sort,
    build_dependency_graph,
    _operation_priority,
)
from declaro_persistum.exceptions import CycleError






