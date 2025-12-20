"""BDD tests for nested function typing."""

from pytest_bdd import scenarios

from .steps.common_steps import *  # noqa: F401, F403

scenarios("features/nested_functions.feature")
