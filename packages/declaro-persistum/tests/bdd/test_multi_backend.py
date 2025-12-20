"""
BDD test runner for multi-database backend features.
"""

import pytest
from pytest_bdd import scenarios

# Import all step definitions
from tests.bdd.steps.common_steps import *  # noqa: F401, F403
from tests.bdd.steps.database_steps import *  # noqa: F401, F403

# Load all scenarios from the feature file
scenarios("features/database/multi_backend.feature")
