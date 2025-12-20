"""
Migration applier layer.

Applies computed operations to the database with proper transaction handling.
Each dialect implements the MigrationApplier protocol.
"""

from declaro_persistum.applier.protocol import MigrationApplier

__all__ = ["MigrationApplier"]
