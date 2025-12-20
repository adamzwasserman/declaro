"""
Command-line interface for declaro_persistum.

Provides the `declaro` command with subcommands:
- diff: Compare target schema to database
- apply: Apply pending migrations
- snapshot: Update schema snapshot
- validate: Validate schema files
- generate: Generate SQL without executing
"""

from declaro_persistum.cli.main import main

__all__ = ["main"]
