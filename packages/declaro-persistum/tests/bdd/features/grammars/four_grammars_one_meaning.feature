@bdd @specification
Feature: Four grammars, one meaning
  There are four ways to ask: the library's own pseudo-SQL, Django, Prisma and
  SQLAlchemy. All four read the same models, refuse the same undeclared columns, and
  produce the same rows. A grammar borrows the other tool's phrasing and nothing else.

  Section "The data is declared too" of HOW-IT-SHOULD-WORK.md.

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
