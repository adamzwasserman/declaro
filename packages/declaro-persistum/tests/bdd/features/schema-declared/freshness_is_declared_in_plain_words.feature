@bdd @specification
Feature: How fresh a materialized view must be, in plain words
  A materialized view holds its rows rather than computing them. The one thing you
  have to say is how stale you can stand it being. You say that in plain terms and the
  library picks the mechanism, because PostgreSQL has a real materialized view and
  SQLite does not.

  THE WORDS ARE THE LIBRARY'S, NOT AN ENGINE'S. `on_commit` and `trigger` are
  mechanisms; "always current" is a requirement. A caller who has to know which engine
  they are on has been handed the library's job.

  Scenario Outline: freshness is one of a declared set
    When a materialized view is declared <freshness>
    Then it is accepted

    Examples:
      | freshness              |
      | always current         |
      | current when read      |
      | refreshed when asked   |

  Scenario: a freshness nobody declared is refused
    When a materialized view is declared "eventually-ish"
    Then it is refused, and the refusal lists the ones that exist

  Scenario: no engine word appears in the declaration
    When a materialized view is declared
    Then the declaration names no journal mode, trigger or refresh statement

  Scenario Outline: the same freshness reaches every engine
    Given a materialized view declared always current
    When the schema is brought to the declared state on <engine>
    Then reading it gives the current answer

    Examples:
      | engine     |
      | postgresql |
      | sqlite     |
      | turso      |

  Scenario: an engine that cannot meet the freshness says so
    Given a freshness an engine cannot provide
    When the schema is brought to the declared state there
    Then it refuses and names the engine and the freshness
    And it does not quietly provide a weaker one

  Scenario: changing the freshness changes the database
    Given a materialized view already in the database
    When its declared freshness changes
    Then bringing the schema up to date changes what the engine holds
    And it does not report success while changing nothing
