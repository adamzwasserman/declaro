"""BDD tests for walrus operator typing."""

from pytest_bdd import scenarios

from .steps.common_steps import *  # noqa: F401, F403

scenarios("features/walrus_operator.feature")
