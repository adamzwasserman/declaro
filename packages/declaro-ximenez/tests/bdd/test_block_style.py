"""BDD tests for block style typing."""

from pytest_bdd import scenarios

from .steps.common_steps import *  # noqa: F401, F403

scenarios("features/block_style.feature")
