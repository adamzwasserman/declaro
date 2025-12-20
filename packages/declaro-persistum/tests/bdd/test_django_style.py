"""
BDD test runner for Django-style query features.
"""

import pytest
from pytest_bdd import scenarios

# Import all step definitions
from tests.bdd.steps.common_steps import *  # noqa: F401, F403
from tests.bdd.steps.style_steps import *  # noqa: F401, F403

# Load all scenarios from the feature file
scenarios("features/styles/django_style.feature")
