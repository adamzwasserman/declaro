"""Writes that ask for RETURNING take the fetch path; the rest take execute.

    A write op that returns rows must be read back, and one that does not must
    report a row count. Routing every write the same way made update_many
    return the wrong kind of answer.
    """

from declaro_persistum.instrumentation import has_returning_clause






