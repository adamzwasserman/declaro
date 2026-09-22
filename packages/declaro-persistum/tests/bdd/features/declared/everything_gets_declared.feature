@bdd @specification
Feature: Everything gets declared, not just tables
  A view, a materialized view, a constraint and an index are all declared against the
  models. None of them gets an escape hatch back to SQL.

  Section "Everything gets declared, not just tables" of HOW-IT-SHOULD-WORK.md.

  THE DOOR IS `session_open`, AND THERE IS NO OTHER. Written 2026-09-22: these
  scenarios used to say "the library brings the database to the declared state",
  which reads as a call somebody could go looking for. The declaration has one
  caller door, and `orchestrator fn session_connect` behind it is what reaches
  the whole migration. Opening the database is when it happens, and binding
  these scenarios means opening one.

  Background:
    Given a model Order declaring the table "orders" with columns id, user_id and total

  Scenario: a view is declared against the models
    Given a view "order_totals" declared as a question about Order
    When the database is opened
    Then the database holds a view "order_totals"
    And the declaration contained no SELECT

  Scenario: the library writes the view's statement
    Given a view "order_totals" declared as a question about Order
    When the library renders it for postgresql
    Then it writes the SELECT itself

  Scenario: a view naming a column no model declares is refused
    Given a view declared against a column Order does not have
    Then the library refuses and names Order

  Scenario: a materialized view declares how fresh it must be
    Given a view "order_totals" declared materialized and fresh on demand
    When the database is opened
    Then the database holds a materialized view

  Scenario: freshness is stated in plain terms, not engine terms
    Given a view "order_totals" declared materialized and fresh on demand
    Then the declaration names no engine mechanism

  Scenario Outline: the same freshness reaches every engine
    Given a view "order_totals" declared materialized and fresh on demand
    When the library brings a <engine> database to the declared state
    Then reading the view gives the declared answer

    Examples:
      | engine     |
      | postgresql |
      | sqlite     |
      | turso      |

  Scenario: a constraint is a declared rule
    Given a column total declared to hold at least zero
    When the database is opened
    Then the engine refuses a row whose total is below zero
    And the declaration contained no SQL fragment

  Scenario: an index is a declared promise
    Given a declared index over user_id
    When the database is opened
    Then the database holds that index

  Scenario: no declaration accepts a string
    When a view, a constraint or an index is declared with a string of SQL
    Then it is refused
