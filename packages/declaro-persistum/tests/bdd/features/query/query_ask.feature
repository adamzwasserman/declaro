@bdd @specification
Feature: query_ask -- The door a caller asks a question or declares a write at
  One door, four spellings. Which grammar was used arrives with the call, because
  the entry point a caller reached for is the shape: nothing inspects a call to
  work it out.

  Scenario Outline: every grammar reaches the same door
    When a caller asks for people named "bob" in the <grammar> grammar
    Then the same declaration was built

    Examples:
      | grammar    |
      | pseudo_sql |
      | django     |
      | prisma     |
      | sqlalchemy |

  Scenario: a read answers with rows
    When a caller asks for every person
    Then rows come back as instances of the model

  Scenario: a write answers with a ticket
    When a caller declares one new person
    Then a ticket comes back and the caller does not wait

  Scenario: a column no model declares is refused here, not by the engine
    When a caller asks for the column "nmae"
    Then it is refused naming the model and the columns it declares
    And nothing reached the database

  # ------------------------------------------------------------------
  # FOLDED IN FROM `grammars/four_grammars_one_meaning.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # There are four ways to ask: the library's own pseudo-SQL, Django, Prisma and
  # SQLAlchemy. All four read the same models, refuse the same undeclared columns, and
  # produce the same rows. A grammar borrows the other tool's phrasing and nothing else.
  # Section "The data is declared too" of HOW-IT-SHOULD-WORK.md.

  Background:
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol

  Scenario Outline: every grammar asks the same question
    When a caller asks for people named "bob" in the <grammar> grammar
    Then it is given one person, and that person is bob

    Examples:
      | grammar    |
      | pseudo-sql |
      | django     |
      | prisma     |
      | sqlalchemy |

  Scenario Outline: every grammar returns model instances
    When a caller asks for every person in the <grammar> grammar
    Then it is given three instances of Person
    And none of them is a dict or a driver row

    Examples:
      | grammar    |
      | pseudo-sql |
      | django     |
      | prisma     |
      | sqlalchemy |

  Scenario Outline: every grammar refuses an undeclared column
    When a caller narrows by "nickname" in the <grammar> grammar
    Then the library refuses in the caller's own process
    And the refusal names Person

    Examples:
      | grammar    |
      | django     |
      | prisma     |
      | pseudo-sql |
      | sqlalchemy |

  Scenario: all four grammars produce the same rows for the same question
    When the same question is asked in all four grammars
    Then all four answers are equal

  Scenario: the four declarations are equal, not merely the rows
    When the same question is asked in all four grammars
    Then the four declarations they built are equal
    And a dictionary can hold one as a key

  Scenario: a declaration is inspectable before it runs
    When a caller writes a question and does not run it
    Then what they hold can be read, compared and stored
    And building it opened no connection

  Scenario: the pseudo-SQL grammar reads in SQL's word order
    When a caller writes select, then from, then where
    Then the library accepts it in that order
    And no part of what the caller wrote is a string of SQL

  Scenario: a grammar brings no session
    When a caller uses the sqlalchemy grammar
    Then nothing holds state between two questions
    And there is no object the caller must open or close

  Scenario: a grammar brings no manager attached to a class
    When a caller uses the django grammar
    Then the model carries no object that owns queries
    And a column is named in one place only

  Scenario Outline: no grammar accepts a string of SQL
    When a caller passes a string of SQL to the <grammar> grammar
    Then it is refused

    Examples:
      | grammar    |
      | pseudo-sql |
      | django     |
      | prisma     |
      | sqlalchemy |

  # ------------------------------------------------------------------
  # FOLDED IN FROM `grammars/the_grammar_is_recognized.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # Four ways to phrase a question: the library's own pseudo-SQL, Django, Prisma and
  # SQLAlchemy. Which one you used is decided by the name you reached for, at the call,
  # and nowhere else. There is no mode to set, no session to open in a grammar, and no
  # line at the top of a file saying which one that file speaks.
  # WHAT THIS BUYS. One file mixes all four, line by line, in any order. A team that
  # writes Django and a team that writes Prisma work in the same module without either
  # of them configuring anything, and a person reading it does not have to look upward
  # to know what they are reading.
  # RECOGNITION COSTS NOTHING AT RUNTIME, and that is the design rather than an
  # optimization. Nothing inspects a call to work out its shape, because the shape IS
  # the entry point: `Person.objects`, `Person.query`, `Person.find_many` and the table
  # proxy are four different names on one model. A guess is only needed where the
  # information was thrown away, and none is thrown away here.
  # IT IS THE SAME MOVE AS THE ENGINE, one axis over. `open_database` reads the target
  # rather than asking the caller to name an engine; a grammar is read off the call
  # rather than asked for. Both replace a thing the caller had to state and could state
  # wrongly. See `tests/bdd/features/opening/engine_of.feature`.

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
