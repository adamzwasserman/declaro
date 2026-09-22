@bdd @specification
Feature: query_collect -- Coming back for a write you deposited
  A ticket belongs to exactly one caller, so a failure reaches that caller and
  nobody else, and a caller draws its own boundary by choosing when to collect.

  Scenario: a collected receipt says the write landed
    Given an empty write queue and a real database
    When a caller deposits a write and the appender drains once
    And the caller collects its ticket
    Then the receipt says the write succeeded
    And the row is in the database

  Scenario: an outright refusal is raised, not put in a receipt
    When a caller deposits a write that violates the primary key and collects it
    Then the engine's own error is raised

  Scenario: a spent retry budget is a receipt, not a raise
    When a write conflicts more often than the budget allows
    Then the receipt says it did not land
    And it says what to do differently

  Scenario: one caller's refusal does not reach another
    When one caller deposits a doomed write and another a good one
    Then the doomed ticket raises and the good one reports success

  Scenario: collecting drops the ticket
    When a caller collects its ticket
    Then the room holds nothing for it any more

  # ------------------------------------------------------------------
  # FOLDED IN FROM `write_queue.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # The WAL is already the queue. A write is durable once it is in the log, so the only
  # job left is to buffer callers who arrive at the same instant and hand their writes to
  # the log in order.
  # What makes this worth describing is not the buffering. It is that a ticket belongs to
  # exactly one caller, so a failure reaches that caller and nobody else, and a caller
  # draws its own atomicity boundary by choosing when to collect.
  # Failure arrives in two shapes and telling them apart is the caller's whole reason to
  # read a receipt. `collect` RAISES the engine's own exception for anything refused
  # outright: a constraint violation, bad SQL, a full disk. A receipt with `ok: False`
  # says only that the engine stayed busy for the whole retry budget. A caller that
  # cannot tell those apart will retry the one that will never succeed and give up on the
  # one that would have.
  # A DEPOSIT IS NOT A TRANSACTION AND NEVER BECOMES ONE. Two deposits are two
  # independent writes, whoever made them. A write that has landed stays landed when a
  # later one fails, because the earlier one is already in the log and nothing in this
  # module has the standing to take it back. A caller who needs several statements to
  # land together holds the connection instead: `query_run_together` is one
  # transaction, and that is the only thing here that is. Decided 2026-08-29; the last
  # scenario below is what holds it.
  # These scenarios run against a real SQLite database. A claim about what an engine
  # refuses can only be settled by asking it.

  Scenario: a deposit returns before the write is done
    Given an empty write queue and a real database
    When a caller deposits a write
    Then it is handed a ticket without waiting for the write

  Scenario: a caller draws its own boundary by choosing when to collect
    Given an empty write queue and a real database
    When a caller deposits three writes before collecting any of them
    And the appender drains once
    Then all three receipts say the writes succeeded
    And all three rows are in the database

  Scenario: an outright refusal is raised to the caller that wrote it
    Given an empty write queue and a real database
    And a row that already holds the only allowed primary key
    When a caller deposits a write that violates the primary key
    And the appender drains once
    Then collecting that ticket raises the engine's own error

  Scenario: one caller's refusal does not reach another caller
    Given an empty write queue and a real database
    And a row that already holds the only allowed primary key
    When one caller deposits a doomed write and another deposits a good one
    And the appender drains once
    Then the doomed ticket raises and the good ticket reports success
    And the good row is in the database

  Scenario: a failure whose caller has gone is handed to the appender
    Given an empty write queue and a real database
    And a row that already holds the only allowed primary key
    When a caller deposits a doomed write and then abandons its ticket
    And the appender drains once
    Then the appender is given that failure to log
    And it is the only place that failure can still be seen

  Scenario: a write with no SQL is refused at the door
    Given an empty write queue and a real database
    When a caller deposits a write with no SQL
    Then it is refused before it takes a ticket
    And the appender is given nothing to do

  # RESTATED 2026-09-21. This asked that the engine was called exactly once,
  # which counts calls rather than saying what a caller can see. Its second line
  # was already the caller-visible form of the same claim, so that line is the
  # scenario now: a write deposited once appends one row, not two.
  Scenario: a deposited write appends one row, not two
    Given an empty write queue and a real database
    When a caller deposits an appending write and the appender drains once
    Then the table holds one row

  Scenario: a collected ticket is dropped, so the room does not grow
    Given an empty write queue and a real database
    When a caller deposits a write and the appender drains once
    And the caller collects its ticket
    Then the room is empty again

  Scenario: a ticket whose write failed is dropped too
    Given an empty write queue and a real database
    And a row that already holds the only allowed primary key
    When a caller deposits a write that violates the primary key
    And the appender drains once
    Then collecting that ticket raises the engine's own error
    And the room is empty again

  Scenario: a write that has landed is not undone when a later one fails
    Given an empty write queue and a real database
    And a row that already holds the only allowed primary key
    When a caller deposits a good write and then a doomed one
    And the appender drains once
    Then the earlier row stands even though the later ticket raises
