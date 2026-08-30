@bdd @specification
Feature: The test for any function
  Read a public function's arguments. If one is a string of SQL, a live connection, or
  the name of a database engine, the function does not belong here.

  Section "The test for any function" of HOW-IT-SHOULD-WORK.md. This is a gate over
  the whole package rather than a test of one behaviour: it reads every public
  signature and fails on any argument that makes the caller do the library's work.

  Scenario: no public function takes a string of SQL
    When every public function's arguments are read
    Then none of them takes a statement, a query string or an SQL fragment

  Scenario: no public function takes a live connection
    When every public function's arguments are read
    Then none of them takes a connection or a cursor

  Scenario: no public function takes the name of an engine
    When every public function's arguments are read
    Then none of them takes a dialect or an engine name
    And the engine is decided once, where the database is opened

  Scenario: no public function hands a connection back
    When every public function's return is read
    Then none of them yields or returns a connection

  Scenario: the gate names what it found
    Given a public function that takes a string of SQL
    When the gate runs
    Then it fails and names that function and that argument
