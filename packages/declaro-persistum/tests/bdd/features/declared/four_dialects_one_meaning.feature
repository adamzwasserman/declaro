@bdd @specification
Feature: Four dialects, one meaning
  There are four ways to ask: the library's own pseudo-SQL, Django, Prisma and
  SQLAlchemy. All four read the same models, refuse the same undeclared columns, and
  produce the same rows. A dialect borrows the other tool's grammar and nothing else.

  Section "The data is declared too" of HOW-IT-SHOULD-WORK.md.

  Background:
    Given a model Person declaring the table "people" with columns id and name
    And a database holding alice, bob and carol

  Scenario Outline: every dialect asks the same question
    When a caller asks for people named "bob" in the <dialect> dialect
    Then it is given one person, and that person is bob

    Examples:
      | dialect    |
      | pseudo-sql |
      | django     |
      | prisma     |
      | sqlalchemy |

  Scenario Outline: every dialect returns model instances
    When a caller asks for every person in the <dialect> dialect
    Then it is given three instances of Person
    And none of them is a dict or a driver row

    Examples:
      | dialect    |
      | pseudo-sql |
      | django     |
      | prisma     |
      | sqlalchemy |

  Scenario Outline: every dialect refuses an undeclared column
    When a caller narrows by "nickname" in the <dialect> dialect
    Then the library refuses in the caller's own process
    And the refusal names Person

    Examples:
      | dialect    |
      | django     |
      | prisma     |
      | pseudo-sql |
      | sqlalchemy |

  Scenario: all four dialects produce the same rows for the same question
    When the same question is asked in all four dialects
    Then all four answers are equal

  Scenario: the pseudo-SQL dialect reads in SQL's word order
    When a caller writes select, then from, then where
    Then the library accepts it in that order
    And no part of what the caller wrote is a string of SQL

  Scenario: a dialect brings no session
    When a caller uses the sqlalchemy dialect
    Then nothing holds state between two questions
    And there is no object the caller must open or close

  Scenario: a dialect brings no manager attached to a class
    When a caller uses the django dialect
    Then the model carries no object that owns queries
    And a column is named in one place only

  Scenario Outline: no dialect accepts a string of SQL
    When a caller passes a string of SQL to the <dialect> dialect
    Then it is refused

    Examples:
      | dialect    |
      | pseudo-sql |
      | django     |
      | prisma     |
      | sqlalchemy |
