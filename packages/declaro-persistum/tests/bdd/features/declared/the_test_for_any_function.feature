@bdd @specification
Feature: Every string is checked or bound, never interpolated
  Follow each argument to what it becomes. A string a caller passes is either a
  declared name, looked up in `model_fields` and refused if the model does not declare
  it, or a value, bound as a parameter. There is no third case, and nothing is pasted
  into a statement.

  THIS IS A TEST ABOUT FATE, NOT APPEARANCE, and that is the whole point. Recognising
  SQL was the obvious approach and it does not work: a scan for the twenty-odd verbs
  finds every statement, finds `created_at` and `update_count` as well, and misses
  every fragment, because `total >= 0` and `name ASC` contain no verb and parse as
  nothing. Those fragments are exactly the escape hatches.

  Sections "The test for any function" of HOW-IT-SHOULD-WORK.md and "Every string is
  checked or bound, never interpolated" of docs/architecture.md.

  # ---------------------------------------------------------------- the two fates

  Scenario: a column name is checked against the declaration
    Given a model Person declaring people with columns id and name
    When a caller passes "name" as a column
    Then it is looked up in the model
    And it never reaches the statement as text

  Scenario: a column the model does not declare is refused
    Given a model Person declaring people with columns id and name
    When a caller passes "nickname" as a column
    Then it is refused in the caller's own process
    And no statement was built

  Scenario: a value is bound as a parameter
    Given a model Person declaring people with columns id and name
    When a caller narrows by the value "bob"
    Then it is bound as a parameter
    And it never appears in the statement text

  Scenario: a value that reads like SQL is still only a value
    Given a model Person declaring people with columns id and name
    When a caller narrows by the value "'; DROP TABLE people; --"
    Then it is bound as a parameter
    And the people table still exists

  Scenario: there is no third fate
    When every string a caller can pass is followed
    Then each one is a declared name or a bound value, and nothing else

  # ------------------------------------------------------- what the gate refuses

  Scenario: a parameter that reaches string concatenation is a hole
    Given a public function whose argument is concatenated into a statement
    When the gate runs
    Then it fails and names that function and that argument

  Scenario: a parameter that reaches an f-string is the same hole
    Given a public function whose argument is interpolated into an f-string statement
    When the gate runs
    Then it fails and names that function and that argument

  Scenario: a declared check taking a SQL fragment is refused
    Given a column declared with a check written as "total >= 0"
    When the gate runs
    Then it fails
    And the fragment contains no SQL verb, so no word scan would have found it

  # ------------------------------------------- the two that look identical

  Scenario: a narrowing given as data is accepted
    Given a model Person declaring people with columns id and name
    When a caller narrows with the mapping name to "bob"
    Then it is accepted, because the key is declared and the value is bound

  Scenario: a narrowing given as text is refused
    Given a model Person declaring people with columns id and name
    When a caller narrows with the text "active = :x"
    Then it is refused
    And the two differ by what becomes of the string, not by how it reads

  # ----------------------------------- the gate does not read what a string says

  Scenario: a column named after a SQL verb is accepted
    Given a model Event declaring events with a column "created_at"
    When a caller passes "created_at" as a column
    Then it is accepted, because the model declares it

  Scenario: the gate never scans for SQL verbs
    When the gate's own source is read
    Then it names no SQL verb
    And it decides by following an argument rather than by matching text

  # --------------------------------------------- the other two the caller must not do

  Scenario: no public function takes a live connection
    When every public function's arguments are read
    Then none of them takes a connection or a cursor

  Scenario: no public function hands a connection back
    When every public function's return is read
    Then none of them yields or returns a connection

  Scenario: no public function takes the name of an engine
    When every public function's arguments are read
    Then none of them takes a dialect or an engine name
    And the engine is decided once, where the database is opened
