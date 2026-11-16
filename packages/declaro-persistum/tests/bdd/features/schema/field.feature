@bdd @specification
Feature: field -- What a host says about one column that its annotation cannot
  An annotation gives a column its name and its type. Two facts about a column
  cannot be said that way: that it used to be called something else, and that it
  points at another table's column. This is where a host says either one, beside
  the column rather than at the top of the class, because a rename written
  anywhere else names that column a second time and two namings drift.

  # WITHOUT THIS THE RENAME PATH IS BUILT AND UNREACHABLE. Seven modules read
  # `Column.renamed_from`, and the only function that could fill it wrote the
  # named absence into every column every time. So a renamed column read as one
  # column dropped and another added, `name_what_is_ambiguous` stopped the open,
  # and nothing the host could write resolved it.

  Scenario: a column says what it was called before
    Given a host declaring a column renamed from "email"
    When the field is read
    Then it carries "email" as the name that column had before
    And it says the column points nowhere

  Scenario: a column says where it points
    Given a host declaring a column referencing "id" on "teams", emptying the column on delete
    When the field is read
    Then it carries that table, that column and that deletion rule
    And it says the column was never renamed

  Scenario: a column says both
    Given a host declaring a column renamed from "team" and referencing "id" on "teams", emptying the column on delete
    When the field is read
    Then it carries both facts

  Scenario: a column says neither
    Given a host declaring a column with neither fact
    When the field is read
    Then both facts come back as the named absence rather than as a guess

  # A HOST WRITES THIS AS THE COLUMN'S ASSIGNMENT, so whatever comes back has to
  # be something Pydantic accepts in that position. A value it reads as a default
  # would make a required column optional, which is a schema this never declared.
  Scenario: declaring a field leaves the column required
    Given a model whose column declares a field and whose column has no default
    When the model is asked what it requires
    Then that column is still required

  Scenario: declaring a field leaves validation alone
    Given a model whose column declares a field
    When a row with the wrong type is validated against it
    Then it is refused exactly as it would be without the field
