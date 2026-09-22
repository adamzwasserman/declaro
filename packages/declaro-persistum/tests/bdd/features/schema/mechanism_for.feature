@bdd @specification
Feature: mechanism_for -- Picking what a caller refused to name
  A caller says how fresh a materialized view must be. PostgreSQL has a real
  materialized view and SQLite does not, and bridging that is the library's job
  rather than the caller's.

  Scenario: PostgreSQL uses what it has
    Given a view declared "current when read" on postgresql
    Then a materialized view is used

  Scenario: SQLite is built out of what it has
    Given the same declaration on sqlite
    Then a table, its triggers and a registry row are used

  Scenario: the caller never names a mechanism
    Then no declaration anywhere says on_commit or trigger

  # ------------------------------------------------------------------
  # FOLDED IN FROM `schema-declared/freshness_is_declared_in_plain_words.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # A materialized view holds its rows rather than computing them. The one thing you
  # have to say is how stale you can stand it being. You say that in plain terms and the
  # library picks the mechanism, because PostgreSQL has a real materialized view and
  # SQLite does not.
  # THE WORDS ARE THE LIBRARY'S, NOT AN ENGINE'S. `on_commit` and `trigger` are
  # mechanisms; "always current" is a requirement. A caller who has to know which engine
  # they are on has been handed the library's job.

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

  # ------------------------------------------------------------------
  # FOLDED IN FROM `matview/asking_about_a_matview.feature` on 2026-09-21, and
  # DELETED on 2026-09-21. Its four scenarios ran three hand-written queries
  # against a hand-built registry table and asserted on the rows that came back:
  # is this name a view, what was registered under it, and list them all. Those
  # describe the inside of a table this feature's function never opens -- it is
  # handed a freshness, an engine and a capability, and answers with operations.
  # What they were reaching for survives above: an engine without a materialized
  # view gets a table, its triggers and a registry row, and a caller who changes
  # the freshness changes what the engine holds.
