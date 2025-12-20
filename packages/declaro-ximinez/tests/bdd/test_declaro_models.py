"""BDD tests for Declaro model validation."""

from pytest_bdd import scenarios

from .steps.common_steps import *  # noqa: F401, F403

scenarios("features/declaro_models.feature")
