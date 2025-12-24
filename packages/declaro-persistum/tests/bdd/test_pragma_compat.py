"""
BDD test runner for PRAGMA compatibility features.

Tests the pragma_compat abstraction layer that provides emulation
for Turso Database (Rust) which has limited PRAGMA support.
"""

import pytest
from pytest_bdd import scenarios

# Import simplified step definitions
from tests.bdd.steps.test_pragma_compat_steps_simple import *  # noqa: F401, F403

# Load all scenarios from the feature file
scenarios("features/pragma_compat.feature")
