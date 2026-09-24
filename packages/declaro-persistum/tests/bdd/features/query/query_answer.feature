@bdd @specification
Feature: query_answer -- Turning a caller's question into an answer
  One orchestrator. It builds the declaration from whichever grammar was used,
  refuses undeclared columns, compiles the statement once for this engine, and
  then either runs it or deposits it.

  Scenario: a read runs now
    When a caller asks for every person
    Then the statement ran against the database
    And the rows came back shaped as the model

  Scenario: a write is deposited
    When a caller declares one new person
    Then the write went to the crew
    And a ticket came back

  Scenario: several rows declared together do not take a ticket
    When a caller declares three people together
    Then they run as one transaction
    And nothing was deposited

  Scenario: the values never reach the statement text
    When a caller asks for people named "bob"
    Then the statement carries a placeholder
    And "bob" came back beside it for the driver to bind

  # ADDED 2026-09-22. A caller writes the class's own name, because that is the
  # only name they have. The rows live under a table name, which is the only
  # name an engine answers to. Nothing here traded one for the other, so every
  # question written the way a caller writes one came back "no such table". No
  # scenario caught it because every model in this file was named after its own
  # table, so the two words were the same word and the trade was invisible.
  Scenario: a caller asks under the class's name and the rows come back
    When a caller asks for every person by the name of the class
    Then the statement ran against the database
    And the rows came back shaped as the model

  # ADDED 2026-09-22, while this was being built. The four spellings are a
  # lookup and the lookup carries no fallback, so a word with no row has to come
  # back refused rather than answered by whichever builder was nearest. Nothing
  # asked for that, and an arm nobody runs is an arm nobody has seen work.
  Scenario: a spelling nothing can build is refused by name
    When a caller asks in a grammar nothing here can build
    Then it is refused naming what arrived and the four spellings there are
    And nothing reached the database
