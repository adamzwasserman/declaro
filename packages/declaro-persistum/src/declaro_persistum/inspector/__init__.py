"""
Database introspection layer.

Provides protocol-based database schema introspection for different dialects.
Each dialect implements the DatabaseInspector protocol.
"""

from declaro_persistum.inspector.protocol import DatabaseInspector

__all__ = ["DatabaseInspector"]
