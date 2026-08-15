Feature: Diffing two schemas into an ordered list of operations
  The differ decides what DDL runs against somebody else's database: what is created,
  what is altered, what is DROPPED, and in what order. It is 1,653 lines and it had
  ZERO tests of any kind — no unit test, no feature file — while every other subsystem
  in the package had something.

  That is the wrong place for the gap. A wrong answer here does not raise; it emits
  valid SQL that destroys data, and the applier executes it faithfully. The blast radius
  of a mistaken `drop_table` is the table.

  These scenarios describe what the code DOES, read out of the code and confirmed by
  running it. Where the behaviour looks surprising the scenario says so rather than
  quietly describing what it ought to be — a specification written from intention rather
  than from the code is how a test suite comes to agree with a bug.

  # ============================================================
  # The set theory — what exists where
  # ============================================================

  Scenario: a table only in the target is created
    Given a current schema with no tables
    And a target schema with a table "users"
    When the schemas are diffed
    Then there is a create_table operation for "users"

  Scenario: a table only in the current schema is DROPPED
    Given a current schema with a table "legacy"
    And a target schema with no tables
    When the schemas are diffed
    Then there is a drop_table operation for "legacy"

  Scenario: a table in both is compared rather than recreated
    Given a current schema with a table "users" having columns id, name
    And a target schema with a table "users" having columns id, name, email
    When the schemas are diffed
    Then there is no create_table operation for "users"
    And there is no drop_table operation for "users"
    And there is an add_column operation for "email" on "users"

  Scenario: diff is a pure function of its two inputs
    Given any current schema and any target schema
    When the schemas are diffed twice
    Then both results are identical, because the differ performs no I/O, holds no state, and reads no clock — which is what makes a migration reviewable before it runs

  # ============================================================
  # Renames — the difference between moving data and losing it
  # ============================================================

  Scenario: renamed_from turns a drop-and-create into a rename
    Given a current schema with a table "user"
    And a target schema with a table "users" declared as renamed_from "user"
    When the schemas are diffed
    Then there is a rename_table operation from "user" to "users"
    And there is NO drop_table operation, because dropping and recreating would discard every row

  Scenario: without a rename hint the differ drops and creates
    Given a current schema with a table "user"
    And a target schema with a table "users" and no rename hint
    When the schemas are diffed
    Then there is a drop_table operation for "user"
    And there is a create_table operation for "users"
    But an ambiguity is also reported, because a drop-and-create that was meant to be a rename is silent data loss and the differ cannot tell the two apart on its own

  # ============================================================
  # Ambiguity — what the differ refuses to decide alone
  # ============================================================

  Scenario: a column that disappears and one that appears is reported as a possible rename
    Given a current schema with a table "users" having a column "name"
    And a target schema with a table "users" having a column "full_name" instead
    When ambiguities are detected
    Then a "possible_rename" ambiguity is reported from "name" to "full_name"
    And it carries a confidence score, so a reviewer can tell a near-certain rename from a guess

  Scenario: a decision already made is not asked about again
    Given a current schema with a table "users" having a column "name"
    And a target schema with a table "users" having a column "full_name" instead
    And a decision has already been recorded for that change
    When ambiguities are detected
    Then no ambiguity is reported for it, because re-asking a question the operator has already answered is how a review gets skipped

  Scenario: rename confidence scores identical names as certain
    Given the names "email" and "EMAIL"
    When rename confidence is calculated
    Then it is 1.0, because only the case differs

  Scenario: rename confidence scores a containment by length ratio
    Given the names "name" and "full_name"
    When rename confidence is calculated
    Then it is the shorter length over the longer, which is 4 divided by 9

  Scenario: rename confidence scores unrelated names low
    Given the names "email" and "quantity"
    When rename confidence is calculated
    Then it is below 0.5, so a reviewer is not nudged toward a rename that is not one

  # ============================================================
  # Ordering — the part that makes valid SQL fail
  # ============================================================

  Scenario: drops run before creates
    Given operations that drop a foreign key, drop a table, and create a table
    When the operations are ordered
    Then every drop is ordered before every create, because a create that collides with a name not yet dropped fails, and the failure lands halfway through a migration

  Scenario: the full drop order runs from the most dependent inward
    Given one operation of every drop kind
    When the operations are ordered
    Then they run foreign keys, then indexes, then constraints, then views, then columns, then tables — each one removing a thing that depends on the next

  Scenario: views are created after every table and index
    Given operations that create a table, add an index, and create a view
    When the operations are ordered
    Then create_view is last, because a view that references a table or index that does not exist yet fails at creation

  Scenario: additions run after the tables they attach to
    Given operations that create a table and add a foreign key
    When the operations are ordered
    Then create_table comes before add_foreign_key

  Scenario: a dependency cycle is refused rather than half-run
    Given operations whose dependencies form a cycle
    When the operations are ordered
    Then a CycleError is raised naming the cycle, because there is no order that works and running some prefix of it leaves the database in a state neither schema describes

  # ============================================================
  # Views. Enums, triggers and procedures were here and their diffs are
  # gone: they emitted operations no applier could execute. See
  # differ/extended.py.
  # ============================================================

  Scenario: a view whose query changed is replaced
    Given a view "recent" with one query
    And a target view "recent" with a different query
    When the views are diffed
    Then an operation is produced for the changed view

  Scenario: an unchanged view produces no operation at all
    Given a view that is identical in both schemas
    When it is diffed
    Then no operations are produced, because a migration that rewrites things nobody changed is indistinguishable from one that had a reason to
