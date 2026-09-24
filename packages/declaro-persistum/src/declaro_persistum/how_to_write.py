"""What one engine needs around a write.

    fn how_to_write : (engine: Engine) -> Transacting

`docs/architecture.md:56`: what differs between engines lives in a table rather
than in branches. This reads the row for writing. Whoever runs the write reads
the answer and never learns an engine's name.

THREE THINGS DIFFER AT ONCE, and they are the three fields. The statement that
opens a transaction, whether the values go in by name or by position, and
whether there is a commit to call.

`architecture.md:255`: `BEGIN CONCURRENT` is mandatory on Turso and is what lets
several drainers overlap inside the engine instead of queueing behind one
writer. SQLite has no concurrent begin, and a deferred transaction takes the
write lock at its first write, so a busy database fails part way through;
`BEGIN IMMEDIATE` fails at the start or not at all. PostgreSQL's driver owns the
transaction, takes its values positionally, and has no commit to call.

NO FALLBACK ON THE READ. `TRANSACTING[engine]`, never `.get`.
"""

from __future__ import annotations

from declaro_persistum.types import Engine, Transacting

# ONE ROW PER ENGINE, AND ADDING AN ENGINE IS ONE MORE LINE. An engine that
# opens nothing says so with an empty statement, which is a real answer here
# rather than an absence: PostgreSQL genuinely writes nothing to begin.
#
# ALL THREE ARE POSITIONAL, AND TWO OF THEM SAID NAMED UNTIL 2026-09-24. SQLite
# and Turso read `?` and PostgreSQL reads `$1`, and `fn compile_statement`
# writes exactly those, so every value travels by its place and no driver here
# reads the names it was given. Nothing in this package read the field, which is
# why nothing caught it: a field nobody reads is a field nobody keeps true. The
# scenario in `how_to_write.feature` reads this against the compiler now.
TRANSACTING: dict[Engine, Transacting] = {
    "turso": {"begin": "BEGIN CONCURRENT", "parameters": "positional", "commits": True},
    "sqlite": {"begin": "BEGIN IMMEDIATE", "parameters": "positional", "commits": True},
    "postgresql": {"begin": "", "parameters": "positional", "commits": False},
}


def how_to_write(engine: Engine) -> Transacting:
    """Say what this engine needs around one write."""
    return TRANSACTING[engine]
