@bdd @specification
Feature: render -- A declaration and an engine in, a statement and its parameters out
  The last step, and the only one that knows which engine it is talking to.
  Everything before it is engine-free: a model declares what exists, a grammar
  declares what you want, and neither carries a word that belongs to PostgreSQL,
  SQLite or Turso.

  One function, one feature. What it takes is what four grammars produce, and
  `grammars/four_grammars_one_meaning.feature` is why there is only one of those.

  WHY IT TAKES A DECLARATION AND NOT A GRAMMAR. Four grammars times three engines
  is twelve conversions that have to agree. Four grammars into one declaration,
  and one declaration into three engines, is seven. The declaration is what turns
  the multiplication into addition, and it is the only reason a fifth grammar
  costs one path rather than three.

  WHAT IS RENDERED AND WHAT IS NOT. The statement is rendered. The values are not.
  They come back beside it, untouched, and the driver binds them. That is the
  string test at the last place it can still be broken: a value that reached the
  statement text would be a value nobody bound, and no earlier check would have
  seen it happen. See `declared/the_test_for_any_function.feature`.

  IT IS THE SAME WORD THE SCHEMA HALF USES. `applier/rendering.py` takes one
  operation and returns the statements that run it, and calls into
  `applier/spelling.py` for how a single value or type is written. Rendering
  composes; spelling translates a piece. This is rendering, one level over.

  # ------------------------------------------------- what it takes and gives back

  Scenario: it takes a declaration and an engine
    Given a declaration about Person narrowed by name
    When it is rendered for postgresql
    Then it gives back a statement and its parameters
    And the placeholders are the ones postgresql uses

  Scenario Outline: one declaration renders for every engine
    Given a declaration about Person narrowed by name
    When it is rendered for <engine>
    Then it gives back that engine's statement
    And the declaration itself is unchanged

    Examples:
      | engine     |
      | postgresql |
      | sqlite     |
      | turso      |

  Scenario: the same declaration on two engines gives two statements
    Given a declaration about Person narrowed by name
    When it is rendered for postgresql and for sqlite
    Then the two statements differ
    And the two parameter sets are equal, because nothing about a value is engine's

  # ------------------------------------------------- the values are not rendered

  Scenario: a value comes back beside the statement, not inside it
    Given a declaration narrowed by the value "bob"
    When it is rendered for sqlite
    Then "bob" is in the parameters
    And "bob" does not appear anywhere in the statement text

  Scenario: a value that reads like SQL is still not rendered
    Given a declaration narrowed by the value "'; DROP TABLE people; --"
    When it is rendered for sqlite
    Then the value is in the parameters, character for character
    And the statement is the same one the value "bob" produced

  Scenario: a declared column IS rendered, because nothing can bind an identifier
    Given a declaration asking for the id and name of Person
    When it is rendered for postgresql
    Then both column names appear in the statement
    And each was checked against the model before it got here

  # ------------------------------------------------- the engine stops here

  Scenario: the engine is named here and nowhere earlier
    When every step before rendering is read
    Then none of them takes an engine or names one
    And the declaration carries no engine

  Scenario: rendering names no engine of its own
    When the renderer's own arguments are read
    Then the engine is one it was handed, never one it chose
    And it has no default for it

  # ------------------------------------------------- what rendering is

  Scenario: rendering is pure
    Given a declaration about Person narrowed by name
    When it is rendered twice for the same engine
    Then both statements are identical, character for character
    And neither opened a connection

  Scenario: rendering reads nothing but what it was handed
    When the renderer runs
    Then it read no clock, no random source and no environment variable
    And it asked no database what it supports

  Scenario: nothing but the renderer builds a statement
    When every place in the package that builds a statement is read
    Then all of them are in the rendering step
    And no grammar and no vocabulary word builds one

  # ------------------------------------------------- what comes out is not what went in

  Scenario: a rendered statement cannot be rendered again
    Given a declaration rendered for sqlite
    Then what came back is a statement and its parameters
    And it cannot be handed back to the renderer

  Scenario: a rendered statement cannot be asked in another grammar
    Given a declaration rendered for sqlite
    Then what came back carries no model and no columns to narrow
    And the declaration it came from is the only thing that can be asked again

  # ------------------------------------------------- the whole chain, end to end

  Scenario: four grammars render to one statement
    Given the same question written in pseudo-sql, django, prisma and sqlalchemy
    When each is rendered for postgresql
    Then the four statements are identical
    And the four parameter sets are equal

  Scenario: a fifth grammar would add one path, not three
    When the steps between a grammar and a statement are counted
    Then a grammar reaches the engine through the declaration
    And no grammar has a step of its own per engine

  # ------------------------------------------------- what it refuses

  Scenario: it refuses an engine nobody declared
    Given a declaration about Person
    When it is rendered for "mysql"
    Then it is refused
    And the refusal names postgresql, sqlite and turso

  Scenario: it refuses to render a string
    When a caller hands the renderer a string instead of a declaration
    Then it is refused
    And the refusal says the renderer is the only thing that writes SQL
