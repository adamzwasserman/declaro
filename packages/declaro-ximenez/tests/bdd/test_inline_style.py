"""BDD tests for inline style typing."""

from pytest_bdd import scenarios

from .steps.common_steps import *  # noqa: F401, F403

scenarios("features/inline_style.feature")
