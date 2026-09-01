@bdd @specification
Feature: A question is data, not a statement
  A grammar produces something. The library turns that into a statement for one
  engine, at the last possible moment. What a grammar produces is the thing this
  feature is about, and it is the decision the rest of the surface rests on.

  IF IT CARRIES SQL TEXT, NOTHING ELSE WORKS. Four grammars producing a string are
  four ways of building a string, and the surface is imperative underneath whatever
  the grammar looks like. A declared view has the same requirement from the other
  direction: "the library writes the SELECT" only means something if a view can be
  declared as an object the library renders.

  So the same object serves both. A question asked in any grammar and a view declared
  against the models are the same kind of value, and rendering is one step that
  happens once, per engine, at the edge.

  # ------------------------------------------------------- what it is, and is not

  Scenario: a question carries no SQL
    Given a model Person declaring people with columns id and name
    When a caller asks for people named "bob" in any grammar
    Then the question it built contains no SQL text
    And nothing in it is a string that would parse as a statement

  Scenario: a question names its model, not a table string
    When a question is built from Person
    Then it carries the model itself
    And it does not carry the word "people"

  Scenario: a question carries declared columns, not identifiers
    When a question asks for the id and name of Person
    Then the columns in it were checked against the model when the question was built
    And a column the model does not declare never reaches the question

  Scenario: a question carries values apart from the question
    When a question narrows by the value "bob"
    Then the value is held separately from what is being asked
    And nothing has to escape or quote it

  Scenario: a question is inspectable before it runs
    When a caller builds a question and does not run it
    Then it can be read, compared and stored
    And building it opened no connection

  Scenario: two questions asked the same way are equal
    When the same question is built twice
    Then the two are equal
    And a dictionary can hold one as a key

  # ------------------------------------------------- rendering happens once, late

  Scenario: rendering is the last step and takes an engine
    Given a question about Person narrowed by name
    When it is rendered for postgresql
    Then it becomes a statement and its parameters
    And the placeholders are the ones postgresql uses

  Scenario Outline: one question renders for every engine
    Given a question about Person narrowed by name
    When it is rendered for <engine>
    Then it becomes that engine's statement
    And the question itself is unchanged

    Examples:
      | engine     |
      | postgresql |
      | sqlite     |
      | turso      |

  Scenario: rendering is pure
    Given a question about Person narrowed by name
    When it is rendered twice for the same engine
    Then both renderings are identical
    And neither opened a connection

  Scenario: nothing but the renderer builds a statement
    When every place a statement is built is read
    Then all of them are in the rendering step
    And no grammar and no vocabulary function builds one

  # ------------------------------------------- the same object serves a view

  Scenario: a view is declared as a question with a name
    Given a view "order_totals" declared as a question about Order
    Then what it holds is the same kind of value a grammar produces

  Scenario: the library renders the view's SELECT
    Given a view "order_totals" declared as a question about Order
    When the schema is brought to the declared state on postgresql
    Then the library wrote the SELECT
    And the declaration contained none of it

  # ------------------------------------------------- what it refuses to carry

  Scenario: a question cannot be built from a string
    When a caller tries to build a question from "SELECT * FROM people"
    Then it is refused

  Scenario: a question cannot be handed a statement to carry
    When a caller tries to put SQL text into a question
    Then it is refused, because the only thing that writes SQL is the renderer

  Scenario: a rendered statement is not a question
    Given a question rendered for sqlite
    Then what comes back is a statement and its parameters
    And it cannot be rendered again, or asked in another grammar
