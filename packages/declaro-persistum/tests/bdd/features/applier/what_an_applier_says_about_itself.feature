@bdd
Feature: What each applier says about itself
  As the migration runner, deciding how to handle a failed operation
  I want the applier to say which engine it is and how it scopes a batch
  So that one bad operation on Turso does not roll back the thirty before it

  # THE TRANSACTION MODE IS THE PART THAT MATTERS. SQLite and PostgreSQL wrap
  # the whole batch, so one failure undoes all of it. Turso commits per
  # operation, so a failure keeps what already landed. Reading the wrong one
  # for an engine either loses work or reports work that was undone.
  #
  # Six functions across three modules, each returning a constant. The engine
  # and the mode were the only things that differed.

  @precommit
  Scenario Outline: each applier names its own engine
    When the "<engine>" applier is asked which engine it is
    Then it says "<engine>"

    Examples:
      | engine     |
      | sqlite     |
      | turso      |
      | postgresql |

  @precommit
  Scenario Outline: each applier says how it scopes a batch
    When the "<engine>" applier is asked how it scopes a batch
    Then it says "<mode>"

    Examples:
      | engine     | mode            |
      | sqlite     | all_or_nothing  |
      | turso      | per_operation   |
      | postgresql | all_or_nothing  |

  @precommit
  Scenario: every applier answers both questions
    When the appliers are listed
    Then each one says which engine it is and how it scopes a batch

  @precommit
  Scenario: the mode is one of the two the runner knows
    When the appliers are listed
    Then no applier names a mode the runner cannot act on

  # WHAT THE RESULT SAYS WHEN NOTHING WENT WRONG. `ApplyResult["error"]` is
  # declared `str | None`, and None is that field's own vocabulary for "no
  # error" rather than a function forgetting to answer. These two scenarios
  # make it a tested answer: the checker cannot tell a declared None from an
  # unwritten one, and only a caller reading the field can.

  @precommit
  Scenario: a batch with nothing skipped reports no error
    When the skip summary is asked about an empty list
    Then the error field is empty

  @precommit
  Scenario: a batch with skips names every one of them
    When the skip summary is asked about two skipped operations
    Then the error field counts them and names both
