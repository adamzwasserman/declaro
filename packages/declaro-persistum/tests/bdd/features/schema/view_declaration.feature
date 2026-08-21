@bdd
Feature: Declaring a view in a models file
  As an author writing a schema
  I want every key a view may carry to reach the database
  So that a declared view is the view the engine actually holds

  # WHY THIS SITS BESIDE materialized_views.feature RATHER THAN INSIDE IT.
  # That feature drives the pure SQL generators and asserts the text they
  # return. Its own steps say that asserting against the applier "would add a
  # layer without adding a claim". The layer is the claim: a generator can
  # return text that is correct and still cannot be executed, and on
  # 2026-08-21 that is exactly what it did. Every scenario here runs the
  # declaration through the loader, the differ and the applier into a real
  # database, and then asks the database what it holds.
  #
  # The vocabulary under test is the whole of it, read from the loader:
  #   required  name, query
  #   optional  materialized, refresh, depends_on, trigger_sources

  Background:
    Given a models file declaring a table "orders"

  # ============================================
  # The two required keys
  # ============================================

  @precommit
  Scenario Outline: A plain view reaches every engine
    Given the models file declares a view "v_plain" selecting from "orders"
    When the schema is migrated to <engine>
    Then <engine> holds a view named "v_plain"
    And selecting from "v_plain" returns the rows the query describes

    Examples:
      | engine     |
      | sqlite     |
      | turso      |
      | postgresql |

  @precommit
  Scenario: A dict missing the query key is not a view
    Given the models file declares a dict with a name and no query
    When the views are loaded
    Then no view is found

  @precommit
  Scenario: A dict carrying an unknown key is refused by name
    Given the models file declares a view carrying the key "refersh"
    When the views are loaded
    Then loading fails naming "refersh" and listing the keys a view may carry

  # ============================================
  # materialized, refresh, depends_on
  # ============================================

  @precommit
  Scenario Outline: A materialized view with manual refresh reaches every engine
    Given the models file declares a materialized view "v_totals" refreshed "manual"
    When the schema is migrated to <engine>
    Then <engine> holds a materialized view named "v_totals"
    And "v_totals" holds the aggregate rows at the moment it was built

    Examples:
      | engine     |
      | sqlite     |
      | postgresql |

  # TURSO IS ABSENT FROM THE OUTLINE ABOVE, and the scenario below says why.

  @precommit
  Scenario: Turso reports a materialized view it has not created
    Given the models file declares a materialized view "v_totals" refreshed "manual"
    When the schema is migrated to turso
    Then the migration reports no error
    But turso holds nothing named "v_totals"

  @precommit
  Scenario: depends_on is recorded so a rebuild can order it
    Given the models file declares a materialized view "v_totals" depending on "orders"
    When the schema is migrated to sqlite
    Then the recorded dependencies of "v_totals" are "orders"

  @precommit
  Scenario: A materialized view is native on PostgreSQL and emulated elsewhere
    Given the models file declares a materialized view "v_totals" refreshed "manual"
    When the schema is migrated to postgresql
    Then postgresql holds "v_totals" as a native materialized view
    When the schema is migrated to sqlite
    Then sqlite holds "v_totals" as a table with a row in the matview registry

  # ============================================
  # trigger_sources
  # ============================================
  #
  # MEASURED 2026-08-21 AND RED ON TWO ENGINES. Declaring a trigger-refreshed
  # materialized view fails the whole migration on SQLite and Turso:
  #
  #     OperationalError: incomplete input
  #
  # `generate_create_view` returns a script of fifteen statements for this
  # declaration, and the applier splits it on ";". A trigger body carries
  # semicolons between BEGIN and END, so the split cuts the first trigger in
  # half and hands the engine `CREATE TRIGGER ... BEGIN DELETE FROM "v_live"`.
  # The generated text is correct. It is the executing that is wrong, which is
  # why generator-level tests have passed over this for months.

  @precommit
  Scenario Outline: A trigger-refreshed view is created rather than failing the migration
    Given the models file declares a materialized view "v_live" refreshed "trigger" watching "orders"
    When the schema is migrated to <engine>
    Then the migration reports no error
    And <engine> holds a materialized view named "v_live"

    Examples:
      | engine |
      | sqlite |

  @precommit
  Scenario: A watched insert refreshes the view without another migration
    Given the models file declares a materialized view "v_live" refreshed "trigger" watching "orders"
    And the schema is migrated to sqlite
    When a row is inserted into "orders"
    Then "v_live" reflects the new row

  @precommit
  Scenario: A trigger is declared for each watched table
    Given the models file declares a materialized view "v_live" refreshed "trigger" watching "orders"
    When the schema is migrated to sqlite
    Then sqlite holds a refresh trigger on "orders" for insert, update and delete

  # ============================================
  # Changing and removing a declaration
  # ============================================

  @precommit
  Scenario: Editing the query migrates the view
    Given the models file declares a view "v_plain" selecting from "orders"
    And the schema is migrated to sqlite
    When the declared query is changed
    And the schema is migrated to sqlite again
    Then "v_plain" answers with the new query

  @precommit
  Scenario: Removing the declaration drops the view
    Given the models file declares a view "v_plain" selecting from "orders"
    And the schema is migrated to sqlite
    When the view declaration is deleted from the models file
    And the schema is migrated to sqlite again
    Then sqlite holds no view named "v_plain"

  @precommit
  Scenario: Migrating an unchanged models file a second time changes nothing
    Given the models file declares a view "v_plain" selecting from "orders"
    And the schema is migrated to sqlite
    When the schema is migrated to sqlite again
    Then the second migration applies no operations
