@bdd @specification
Feature: name_what_destroys -- Saying which operations lose data
  Dropping a column loses what was in it, dropping a table loses all of it, and
  narrowing a type loses what no longer fits. The library derives these from your
  models without being asked, so it has to say which of them destroy something.

  Scenario Outline: a destructive operation is named as destructive
    When the library works out that it would <operation>
    Then the answer says that operation loses data

    Examples:
      | operation                      |
      | drop a column                  |
      | drop a table                   |
      | narrow a column's type         |
      | drop a table other rows point at |

  Scenario: adding is not destructive
    When the library works out that it would add a column
    Then nothing is named as losing data

  # ------------------------------------------------------------------
  # FOLDED IN FROM `schema-declared/losing_data_is_said_out_loud.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # Dropping a column loses what was in it. Dropping a table loses all of it. Narrowing
  # a type loses what no longer fits. The library derives these from your models without
  # being asked, so it has to say which of them destroy something.

  Scenario: an additive operation is not named as destructive
    When the library works out that it would add a column
    Then the answer does not say it loses data

  # TWO SCENARIOS LEFT HERE ON 2026-09-22, for the function that refuses rather
  # than the one that marks. Refusing an undeclared loss, and one declared loss
  # not carrying the next, are both stated in
  # `features/schema/refuse_undeclared_loss.feature`, which is where that
  # function's own feature is. They arrived here in the September fold and stayed
  # as scenarios this function could never answer.

  # COUNTING THE ROWS A CHANGE WOULD LOSE WAS ASKED FOR HERE AND IS NOT DECLARED
  # ANYWHERE. Deleted 2026-09-22. It needs to read the table, and no function the
  # map names carries a row count into a plan; this one is pure and holds no
  # connection. Left as a scenario it could never pass, in a file it does not
  # belong to.
