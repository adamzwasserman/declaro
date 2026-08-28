@bdd
Feature: The statements a migration runs, spelled for each engine
  As a service booting against Postgres or SQLite
  I want the same question asked in the dialect the engine speaks
  So that a stamp written on one engine can be read on the other

  # TWO BUILDERS, ONE SHAPE. Each looks its SQL up in a per-dialect table and
  # hands it back. The table was the only thing that differed.
  #
  # The SQLite ones are RUN against a real database, because a statement that
  # names a column the table has not got is still a plausible string.

  @precommit @sqlite
  Scenario: the stamp is written, and written again over itself
    Given a stamp table on a real database
    When the same key is stamped twice with different values
    Then the second value is what is stored
    And there is one row, not two

  @precommit @sqlite
  Scenario: the user-tables query does not count our own table
    Given a stamp table on a real database
    And a table "orders" that the user made
    When the user-tables query is run
    Then it names "orders" and not the stamp table

  @precommit
  Scenario Outline: both builders answer for every engine we ship
    When "<builder>" is asked for "<engine>"
    Then it gives back a statement

    Examples:
      | builder         | engine     |
      | store_hash_sql  | sqlite     |
      | store_hash_sql  | postgresql |
      | user_tables_sql | sqlite     |
      | user_tables_sql | postgresql |

  @precommit
  Scenario Outline: an engine nobody wrote a statement for is refused
    When "<builder>" is asked for "mysql"
    Then it refuses and names the engines it has

    Examples:
      | builder         |
      | store_hash_sql  |
      | user_tables_sql |

  # THE PLACEHOLDER IS THE SAME QUESTION, one fragment rather than a whole
  # statement: asyncpg numbers its parameters and SQLite marks them.

  @precommit
  Scenario Outline: each engine marks a parameter its own way
    When "_placeholder" is asked for "<engine>"
    Then it gives back "<mark>"

    Examples:
      | engine     | mark |
      | sqlite     | ?    |
      | postgresql | $1   |

  @precommit
  Scenario: an engine nobody wrote a placeholder for is refused
    When "_placeholder" is asked for "mysql"
    Then it refuses and names the engines it has
