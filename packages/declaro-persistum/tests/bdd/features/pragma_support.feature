Feature: PRAGMA support is required of a backend
  persistum reads a database's shape through PRAGMA: what columns a table has, what
  indexes are on it, what those indexes cover, and what foreign keys it declares. The
  inspector, the differ's inputs and both reconstruction paths rest on these answers.

  Every supported backend must answer all four natively. A backend that cannot fails at
  the point of the missing capability, because that is where the problem is.

  These scenarios run against real engines. A claim about what an engine supports can
  only be checked by asking it.

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
