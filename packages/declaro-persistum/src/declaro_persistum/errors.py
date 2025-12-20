"""
Custom exceptions for declaro_persistum.
"""


class NotSupportedError(Exception):
    """
    Raised when a feature is not supported by the target database.

    Examples:
        - Stored procedures on SQLite
        - Materialized views on SQLite
        - Array types on SQLite without junction table abstraction
    """

    def __init__(self, message: str, alternatives: list[str] | None = None):
        """
        Args:
            message: Error message describing what's not supported
            alternatives: List of alternative approaches
        """
        self.alternatives = alternatives or []
        if self.alternatives:
            alt_text = "\n\nOptions:\n" + "\n".join(
                f"  {i + 1}. {alt}" for i, alt in enumerate(self.alternatives)
            )
            message = message + alt_text
        super().__init__(message)


class SchemaError(Exception):
    """Raised when there's an error in schema definition."""

    pass


class ValidationError(Exception):
    """Raised when validation fails."""

    pass


class MigrationError(Exception):
    """Raised when migration fails."""

    pass
