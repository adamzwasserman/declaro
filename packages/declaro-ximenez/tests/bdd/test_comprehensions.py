"""BDD tests for comprehension variable typing."""

from pytest_bdd import scenarios

from .steps.common_steps import *  # noqa: F401, F403

scenarios("features/comprehensions.feature")
