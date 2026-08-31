@bdd @specification
Feature: One table says what each engine can do
  Engines do not offer the same things. PostgreSQL has a materialized view and SQLite
  does not, so one is built from a table, its triggers and a registry row. One table
  should say which capability is native on which engine and what builds it where it is
  not.

  Split that across a table per operation and native versus built ends up encoded in
  function names, where nobody can read it. Then "does this engine do ranges" is
  answered by running an experiment, and a capability nobody built looks exactly like
  one nobody wrote down.

  Scenario: every capability has a row
    When the capability table is read
    Then each capability the package offers is a row

  Scenario: every engine has a column
    When the capability table is read
    Then every engine the package speaks has a cell in every row

  Scenario: a cell says native or names what builds it
    When any cell is read
    Then it says the engine has it, or names what supplies it

  Scenario: a cell covers writing and reading
    Given a capability that is built rather than native
    When its cell is read
    Then it names what creates it, what removes it, and how the inspector recognises it

  Scenario: a built capability the inspector cannot see is refused
    Given a cell naming what creates a capability and not how to recognise it
    When the table is checked
    Then it fails, because the next difference would propose dropping it

  Scenario: a capability with no cell for an engine is a gap, not a silence
    Given a capability with no cell for turso
    When the table is checked
    Then it fails and names the capability and the engine

  Scenario: native is a claim a test has to support
    Given a cell saying a capability is native on an engine
    Then a test exercises that capability against that engine

  Scenario: adding an engine is adding a column
    When an engine is added
    Then every row needs a cell, and a row without one fails the check

  Scenario: the table is what the documentation reads
    When the documentation says what an engine supports
    Then it says it by reading this table rather than by asserting it
