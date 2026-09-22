@bdd @specification
Feature: Every string is checked or bound, never interpolated
  Follow each argument to what it becomes. A string a caller passes is either a
  declared name, looked up in `model_fields` and refused if the model does not declare
  it, or a value, bound as a parameter. There is no third case, and nothing is pasted
  into a statement.

  THIS IS A TEST ABOUT FATE, NOT APPEARANCE, and that is the whole point. Recognizing
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

  # --------------------------------------------- the other two the caller must not do

  # A DOOR IS THE CALLER'S SURFACE, AND THERE ARE FIVE OF THEM. Rewritten
  # 2026-09-21: these three said "every public function", which names no set the
  # declaration has. Read as written they were false, because several declared
  # functions behind the doors do take a connection or an engine, on purpose:
  # `fn room_run_write` and `boundary_out fn query_begin_concurrent` are handed a
  # writer, and `fn compile_statement` is handed the engine because it is the one
  # thing that has to know. What the caller may not be given is the claim, and
  # "door" is the declaration's own word for what the caller touches.
  Scenario: no door takes a live connection
    When the arguments of all five doors are read
    Then none of them takes a connection or a cursor

  # A HANDLE CARRIES ONE AND IS NEVER OPENED UP. `fn session_open` hands back a
  # database, and a database holds the connection it reads on. That is the
  # distinction this scenario is about: a caller holds the handle and has no way
  # to run anything on what is inside it.
  Scenario: no door hands a caller a connection to use
    When the return of all five doors is read
    Then no door returns a bare connection
    And the caller never reads or runs anything on the one inside its handle

  Scenario: no door takes the name of an engine
    When the arguments of all five doors are read
    Then none of them takes an engine or an engine name
    And the engine is decided once, where the database is opened
