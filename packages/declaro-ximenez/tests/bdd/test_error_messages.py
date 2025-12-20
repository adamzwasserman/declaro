"""BDD tests for Spanish Inquisition error messages."""

from pytest_bdd import scenarios

from .steps.common_steps import *  # noqa: F401, F403

scenarios("features/error_messages.feature")
