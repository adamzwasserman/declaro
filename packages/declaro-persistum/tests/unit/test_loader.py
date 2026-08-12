"""Tests for schema loading."""

import pytest
from pathlib import Path

from declaro_persistum.loader import (
    load_schema,
    load_snapshot,
    save_snapshot,
    load_decisions,
    save_decisions,
)
from declaro_persistum.exceptions import LoaderError






