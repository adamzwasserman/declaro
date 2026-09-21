@bdd @specification
Feature: difference -- What turns the live schema into the target
  Set theory over two schemas: what is only in the target is created, what is only
  in the live database is dropped, what differs is altered.

  Scenario: a table only in the target is created
    Given a current schema with no tables
    And a target schema with a table "users"
    When the schemas are diffed
    Then there is a create_table operation for "users"

  Scenario: a table only in the live schema is dropped
    Given a live schema with "legacy" and a target without it
    Then there is a drop for "legacy"

  Scenario: a column that differs is altered
    Given a column declared as text and held as an integer
    Then there is an alter for that column

  Scenario: no difference is an answer
    Given two schemas that match
    Then nothing comes back, and that is distinguishable from a failure

  # ------------------------------------------------------------------
  # FOLDED IN FROM `schema/diff.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # The differ decides what DDL runs against somebody else's database: what is created,
  # what is altered, what is DROPPED, and in what order. It is 1,653 lines and it had
  # ZERO tests of any kind — no unit test, no feature file — while every other subsystem
  # in the package had something.
  # That is the wrong place for the gap. A wrong answer here does not raise; it emits
  # valid SQL that destroys data, and the applier executes it faithfully. The blast radius
  # of a mistaken `drop_table` is the table.
  # These scenarios describe what the code DOES, read out of the code and confirmed by
  # running it. Where the behavior looks surprising the scenario says so rather than
  # quietly describing what it ought to be — a specification written from intention rather
  # than from the code is how a test suite comes to agree with a bug.
  # ============================================================
  # The set theory — what exists where
  # ============================================================

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

  # AN EIGHTH LINE WENT WITH THEM, the `But` on the scenario above. It asked
  # that an ambiguity be reported beside the drop and the create.
  # `fn name_what_is_ambiguous` pairs a column gone with a column appeared
  # inside one table and pairs no tables at all, so nothing reports this one,
  # and moving the line into that feature would plant a claim that is false
  # there too. What does stop a table drop nobody declared is
  # `fn refuse_undeclared_loss` reading `dropping_tables`, which is a refusal
  # rather than a question, and `refuse_undeclared_loss.feature` says so.

  # THE THREE CONFIDENCE SCENARIOS ARE GONE WITH THE FUNCTION THEY DESCRIBED.
  # `calculate_rename_confidence` scored a dropped name against an added one,
  # and the detector stayed silent when the score fell under a threshold. A
  # table renamed and reshaped scored under it, so no ambiguity was raised,
  # the drop ran, and the rows went without the operator being asked.
  # Measured 2026-08-21. A drop beside an add is ambiguous by construction now.

  # ------------------------------------------------------------------
  # SEVEN MORE SCENARIOS WENT ON 2026-09-21, when this feature was bound. Two
  # asked about ambiguity and five about ordering, and `difference` returns
  # `list<Operation>` with no ordering and no refusal in it, so nothing it can
  # answer was among them. Both owning functions are written and both features
  # already say these things in the vocabulary the declaration has:
  #
  # THE TWO AMBIGUITY SCENARIOS -> `name_what_is_ambiguous.feature`. They asked
  # for a "possible_rename" ambiguity carrying no confidence score, and for a
  # recorded decision not to be asked about twice. `fn name_what_is_ambiguous`
  # reads `renamed_from` off the column that replaced the old one and refuses
  # when nothing is declared. There is no ambiguity object, no score, and
  # nothing remembers a decision, because remembering one needs somebody at a
  # deploy and nobody is there.
  #
  # THE FIVE ORDERING SCENARIOS -> `order_operations.feature`. Three named
  # `add_foreign_key`, `drop_foreign_key` and a constraint operation, and
  # `OperationKind` has none of the three. One said a cycle raises a
  # `CycleError`; `fn order_operations` returns a `Fault`, because a failure
  # comes back in the return here and is never raised. The two claims that were
  # neither stale nor already written down -- every drop before every create,
  # and `create_view` last -- are answered differently on purpose: that
  # function orders by the tables an operation needs, not by the kind it is.
  #
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
