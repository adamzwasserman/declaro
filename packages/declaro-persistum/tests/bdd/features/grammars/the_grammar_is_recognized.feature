@bdd @specification
Feature: The grammar is recognized from the call, never declared
  Four ways to phrase a question: the library's own pseudo-SQL, Django, Prisma and
  SQLAlchemy. Which one you used is decided by the name you reached for, at the call,
  and nowhere else. There is no mode to set, no session to open in a grammar, and no
  line at the top of a file saying which one that file speaks.

  WHAT THIS BUYS. One file mixes all four, line by line, in any order. A team that
  writes Django and a team that writes Prisma work in the same module without either
  of them configuring anything, and a person reading it does not have to look upward
  to know what they are reading.

  RECOGNITION COSTS NOTHING AT RUNTIME, and that is the design rather than an
  optimization. Nothing inspects a call to work out its shape, because the shape IS
  the entry point: `Person.objects`, `Person.query`, `Person.find_many` and the table
  proxy are four different names on one model. A guess is only needed where the
  information was thrown away, and none is thrown away here.

  IT IS THE SAME MOVE AS THE ENGINE, one axis over. `open_database` reads the target
  rather than asking the caller to name an engine; a grammar is read off the call
  rather than asked for. Both replace a thing the caller had to state and could state
  wrongly. See `tests/bdd/features/opening/engine_of.feature`.

  Background:
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol

  # ------------------------------------------------ nothing is declared anywhere

  Scenario: no mode is set anywhere
    When a caller asks for people in the django grammar
    Then nothing was configured first
    And the call names no grammar, only the way it is written

  Scenario: a file says nothing about which grammar it speaks
    When a file that asks questions is read
    Then it carries no import, setting or declaration naming a grammar
    And removing every comment from it changes nothing about how it runs

  Scenario: nothing on the database says which grammar it expects
    When the database's fields are read
    Then none of them names a grammar
    And two callers on one database may use different ones

  # -------------------------------------------- one file, four grammars, any order

  Scenario: four grammars in one file, one after another
    When a caller asks four questions in pseudo-sql, django, prisma and sqlalchemy
    Then all four are answered
    And nothing between them switched anything

  Scenario: the order they appear in changes nothing
    When the same four questions are asked in a different order
    Then each answer is the one it was before

  Scenario: two grammars inside one function
    When one function asks in django and then in prisma
    Then both are answered
    And neither call had to undo what the other did

  Scenario: the same model answers all four
    When Person is asked in each of the four grammars
    Then it answered every time
    And there is one Person, not one per grammar

  # ------------------------------------------------ what recognition actually is

  Scenario: the entry point IS the recognition
    When each grammar's entry point is read
    Then each is a different name on the model
    And nothing looks at arguments to decide which grammar was used

  Scenario Outline: each grammar has exactly one entry point
    When the <grammar> grammar is entered
    Then there is one name that starts it
    And that name starts no other grammar

    Examples:
      | grammar    |
      | pseudo-sql |
      | django     |
      | prisma     |
      | sqlalchemy |

  Scenario: nothing guesses
    When every function that decides a grammar is read
    Then none of them inspects a value to work out what was meant
    And none of them has a fallback for a shape it does not know

  # ------------------------------------------------ what comes out is the same

  Scenario: four grammars build one kind of value
    When the same question is asked in all four grammars
    Then the four questions are equal
    And a dictionary can hold one as a key

  Scenario: the question does not remember which grammar built it
    When a question built in django is compared to the same question in prisma
    Then they are equal
    And neither carries the name of the grammar that built it

  Scenario: a refusal reads the same in every grammar
    When a caller narrows by "nickname" in each of the four grammars
    Then each refusal names Person and the column
    And none of them mentions a grammar

  # ------------------------------------------------------- what it refuses

  Scenario: two grammars in one expression is refused
    When a caller starts in django and continues in sqlalchemy on the same question
    Then it is refused
    And the refusal says a question is asked in one grammar

  Scenario: a name that starts no grammar is refused by name
    When a caller reaches for a grammar the model does not carry
    Then it is refused
    And the refusal lists the four that exist

  Scenario: no grammar is a way back to SQL
    When a caller passes a string of SQL to any of the four
    Then it is refused
    And the refusal is the same one every grammar gives
