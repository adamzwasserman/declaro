@bdd @specification
Feature: schema_inspect -- Asking a database what it currently holds
  The only place this library asks a database what is there, and it asks so it
  can compare, never so it can decide what belongs. What belongs is in the
  models.

  Scenario: the live schema comes back
    Given a database holding one table with two columns
    When it is inspected
    Then the table and both columns come back

  Scenario: an empty database is an answer
    Given a database with no tables
    When it is inspected
    Then an empty schema comes back, and that is not a failure

  # ADDED 2026-09-24, after opening the same database twice was measured. The
  # first open created the table; the second was refused, saying it would
  # destroy every column in it. Nothing was wrong with either column. The engine
  # calls them INTEGER and TEXT, this package calls them integer and text, and
  # `fn difference` compares the two words and sees every column changed. On
  # SQLite a changed column is a table rebuild, a rebuild destroys data, and no
  # model asked to lose anything, so the open stopped.
  #
  # So no database this library migrated could be opened again, which is every
  # deploy after the first.
  Scenario: the types come back in this package's words, not the engine's
    Given a database whose columns the engine calls INTEGER and TEXT
    When it is inspected
    Then the columns come back holding integer and text
    And nothing in the answer is spelled the way the engine spells it

  Scenario: a type this engine spells in a way nothing here knows is refused
    Given a column the engine describes in a word no table here carries
    When it is inspected
    Then it is refused naming the word and the table it came from
    And no column comes back holding a guess

  Scenario: nothing decides anything from it
    When a database holds a table no model declares
    Then the table is not adopted into the target schema

  # ------------------------------------------------------------------
  # FOLDED IN FROM `inspector/recovering_what_turso_folded.feature` on 2026-09-21,
  # one feature per function. What that file said about why:
  # As the differ, comparing what is declared against what is there
  # I want a column carrying its reference and its CHECK
  # So that a constraint the engine keeps only in the DDL is not read as absent
  # TURSO FOLDS SOME NAMES TO LOWER CASE AND KEEPS OTHERS. What a pragma hands
  # back is not always what was written, and a reference or a CHECK the engine
  # keeps only in the DDL still has to reach the caller.
  #
  # REWRITTEN AS RESULTS on 2026-09-21. Two of its scenarios put a column
  # through one internal step and read the next one back, which fixes the order
  # of two steps inside one function and says nothing a caller can see. A third
  # asked whether a helper copied rather than mutated, and the declaration has
  # no such helper, so it was deleted.

  Scenario: a column comes back carrying where its key points
    Given a database holding a table whose column points at another table
    When it is inspected
    Then the column comes back naming that table, that column, and what a delete does to the child

  Scenario: a rule declared on a table comes back as a condition
    Given a database holding a table with a declared rule on one column
    When it is inspected
    Then the rule comes back as a condition rather than as a string

  # ------------------------------------------------------------------
  # FOLDED IN FROM `pragma/reading_a_pragma.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As the differ, deciding what a migration must do
  # I want the table's columns, indexes and keys read back
  # So that what I compare against the schema is what the engine actually has
  # THE QUOTING IS WHAT BITES. An identifier is quoted on the way to the engine
  # and a double quote inside a name has to be doubled, or half the names come
  # back broken and the differ rebuilds a table on every deploy.
  #
  # REWRITTEN AS RESULTS on 2026-09-21. Four scenarios named four internal
  # readers and attributed the answer to them; one Scenario Outline ran over
  # three pragma names as its examples. A caller sees one answer about one
  # database, so that is what these ask for.
  #
  # ALSO FOLDED IN, from `core/capability_of.feature` on 2026-09-21: four
  # scenarios there asked SQLite whether SQLite answers its own PRAGMAs, which
  # measures the engine rather than this package. What they were reaching for is
  # the last scenario below, which this package can own: what comes back is what
  # the engine really holds, on each of the three engines.
  # @precommit @sqlite

  Scenario: an index comes back naming the columns it covers
    Given a database holding a table with an index over one of its columns
    When it is inspected
    Then the index comes back under its declared name, naming that column

  @precommit @sqlite
  Scenario: a table named with a quote in it is described like any other
    Given a database holding a table whose name carries a double quote
    When it is inspected
    Then that table and its columns come back, and nothing was refused

  @precommit
  Scenario Outline: what comes back is what the engine holds
    Given a table on <engine> with three columns, an index and a key pointing at another table
    When the database is inspected
    Then all three columns come back, with the index and the key the table declares

    Examples:
      | engine     |
      | postgresql |
      | sqlite     |
      | turso      |
