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

  Scenario Outline: every grammar asks the same question
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When a caller asks for people named "bob" in the <grammar> grammar
    Then it is given one person, and that person is bob
    Examples:
      | grammar    |
      | pseudo-sql |
      | django     |
      | prisma     |
      | sqlalchemy |

  Scenario Outline: every grammar returns model instances
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
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
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
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
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When the same question is asked in all four grammars
    Then all four answers are equal

  Scenario: the four declarations are equal, not merely the rows
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When the same question is asked in all four grammars
    Then the four declarations they built are equal
    And a dictionary can hold one as a key

  Scenario: a declaration is inspectable before it runs
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When a caller writes a question and does not run it
    Then what they hold can be read, compared and stored
    And building it opened no connection

  # RESTATED 2026-09-21. This asked that the library accept select, then from,
  # then where, in that order. A question is a dictionary and its keys carry no
  # order that any implementation could get wrong, so the claim was unfailable.
  # Its second line is the one a caller can see, and it is the whole point of
  # the grammar: the words read like SQL and none of them is SQL.
  Scenario: a pseudo-SQL question holds no SQL
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When a caller asks for people named "bob" in the pseudo-sql grammar
    Then it is given one person, and that person is bob
    And no part of what the caller wrote is a string of SQL

  Scenario: a grammar brings no session
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When a caller uses the sqlalchemy grammar
    Then nothing holds state between two questions
    And there is no object the caller must open or close

  Scenario: a grammar brings no manager attached to a class
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When a caller uses the django grammar
    Then the model carries no object that owns queries
    And a column is named in one place only

  Scenario Outline: no grammar accepts a string of SQL
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
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
  # IT IS THE SAME MOVE AS THE ENGINE, one axis over. `session_open` reads the target
  # rather than asking the caller to name an engine; a grammar is read off the call
  # rather than asked for. Both replace a thing the caller had to state and could state
  # wrongly. See `tests/bdd/features/opening/engine_of.feature`.

  Scenario: no mode is set anywhere
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When a caller asks for people in the django grammar
    Then nothing was configured first
    And the call names no grammar, only the way it is written

  # DELETED 2026-09-21: "a file says nothing about which grammar it speaks". It
  # opened a file on disk and asked that stripping its comments changed how it
  # ran, which is a claim about Python rather than about this library, and no
  # caller can see it. What it was reaching for -- a caller never names a
  # grammar anywhere but at the call -- is said by "no mode is set anywhere"
  # above and by the scenario below.

  # RESTATED 2026-09-21. This read the fields of the database record and asked
  # that none of them names a grammar, which inspects the implementation's own
  # data. Its second line is the result that follows from it, and that line is
  # the scenario now.
  Scenario: two callers on one database use different grammars
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When one caller asks in django and another asks in prisma on that one database
    Then both are answered
    And neither caller had to tell the database which grammar it was using
  # -------------------------------------------- one file, four grammars, any order

  Scenario: four grammars in one file, one after another
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When a caller asks four questions in pseudo-sql, django, prisma and sqlalchemy
    Then all four are answered
    And nothing between them switched anything

  Scenario: the order they appear in changes nothing
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When the same four questions are asked in a different order
    Then each answer is the one it was before

  Scenario: two grammars inside one function
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When one function asks in django and then in prisma
    Then both are answered
    And neither call had to undo what the other did

  Scenario: the same model answers all four
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When Person is asked in each of the four grammars
    Then it answered every time
    And there is one Person, not one per grammar
  # ------------------------------------------------ what recognition actually is

  Scenario: the entry point IS the recognition
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When each grammar's entry point is read
    Then each is a different name on the model
    And nothing looks at arguments to decide which grammar was used

  Scenario Outline: each grammar has exactly one entry point
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When the <grammar> grammar is entered
    Then there is one name that starts it
    And that name starts no other grammar
    Examples:
      | grammar    |
      | pseudo-sql |
      | django     |
      | prisma     |
      | sqlalchemy |

  # DELETED 2026-09-21: "nothing guesses". It read the source of every function
  # that decides a grammar and asked that none inspects a value or carries a
  # fallback. Reading the implementation is the mechanism itself, and an honest
  # rewrite that recognized a grammar some other way would fail it while every
  # caller saw the same answers. The result it wanted is already here: each
  # grammar has exactly one entry point, and a name that starts no grammar is
  # refused by name.
  # ------------------------------------------------ what comes out is the same

  Scenario: four grammars build one kind of value
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When the same question is asked in all four grammars
    Then the four questions are equal
    And a dictionary can hold one as a key

  Scenario: the question does not remember which grammar built it
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When a question built in django is compared to the same question in prisma
    Then they are equal
    And neither carries the name of the grammar that built it

  Scenario: a refusal reads the same in every grammar
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When a caller narrows by "nickname" in each of the four grammars
    Then each refusal names Person and the column
    And none of them mentions a grammar
  # ------------------------------------------------------- what it refuses

  Scenario: two grammars in one expression is refused
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When a caller starts in django and continues in sqlalchemy on the same question
    Then it is refused
    And the refusal says a question is asked in one grammar

  Scenario: a name that starts no grammar is refused by name
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When a caller reaches for a grammar the model does not carry
    Then it is refused
    And the refusal lists the four that exist

  Scenario: no grammar is a way back to SQL
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol
    When a caller passes a string of SQL to any of the four
    Then it is refused
    And the refusal is the same one every grammar gives
