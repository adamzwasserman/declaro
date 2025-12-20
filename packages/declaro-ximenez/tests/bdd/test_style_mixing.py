"""BDD tests for style mixing prevention."""

from pytest_bdd import scenarios

from .steps.common_steps import *  # noqa: F401, F403

scenarios("features/style_mixing.feature")
