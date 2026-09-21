@bdd @specification
Feature: capability_of -- What an engine does natively, and what builds it otherwise
  One table says which capability is native on which engine and what builds it
  where it is not. Split across a function per operation and "native or built"
  ends up encoded in function names, where nobody can read it.

  Scenario: PostgreSQL has a materialized view
    Given postgresql and a materialized view
    Then it is native

  Scenario: SQLite builds one
    Given sqlite and a materialized view
    Then it is built from a table, its triggers and a registry row

  Scenario: a capability nobody wrote down is a fault, not a guess
    Given an engine and a feature the table does not cover
    Then a fault comes back naming both

  # ------------------------------------------------------------------
  # FOLDED IN FROM `schema-declared/one_table_of_capabilities.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # Engines do not offer the same things. PostgreSQL has a materialized view and SQLite
  # does not, so one is built from a table, its triggers and a registry row. One table
  # should say which capability is native on which engine and what builds it where it is
  # not.
  # Split that across a table per operation and native versus built ends up encoded in
  # function names, where nobody can read it. Then "does this engine do ranges" is
  # answered by running an experiment, and a capability nobody built looks exactly like
  # one nobody wrote down.

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
    Then it names what creates it, what removes it, and how the inspector recognizes it

  Scenario: a built capability the inspector cannot see is refused
    Given a cell naming what creates a capability and not how to recognize it
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

  # ------------------------------------------------------------------
  # FOLDED IN FROM `pragma_support.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # persistum reads a database's shape through PRAGMA: what columns a table has, what
  # indexes are on it, what those indexes cover, and what foreign keys it declares. The
  # inspector, the differ's inputs and both reconstruction paths rest on these answers.
  # Every supported backend must answer all four natively. A backend that cannot fails at
  # the point of the missing capability, because that is where the problem is.
  # These scenarios run against real engines. A claim about what an engine supports can
  # only be checked by asking it.

  Scenario: every PRAGMA persistum depends on is answered natively
    Given a real SQLite database with a table, an index and a foreign key
    When each PRAGMA persistum depends on is asked of it
    Then every one is answered by the engine

  Scenario: a backend that cannot answer fails loudly
    Given a connection whose PRAGMA support has been withdrawn
    When a PRAGMA is asked of it
    Then the error reaches the caller

  Scenario: table_info reports the columns the engine has
    Given a real SQLite database with a table, an index and a foreign key
    When table_info is read for that table
    Then it lists every column of the table

  Scenario: index_list and index_info agree with each other
    Given a real SQLite database with a table, an index and a foreign key
    When index_list is read and then index_info for one of its indexes
    Then the index named by the first is described by the second

  Scenario: foreign_key_list reports the declared reference
    Given a real SQLite database with a table, an index and a foreign key
    When foreign_key_list is read for the referencing table
    Then it names the referenced table
