@bdd @specification
Feature: what_an_operation_carries -- Which fields one kind of change fills
  An operation used to carry a bag of anything, so an alter_column holding a
  list of enum values was expressible and meant nothing. This says which of the
  named fields each kind of change fills, so an operation can be checked against
  its own kind. Adding a kind of change is one more line here.

  Scenario: creating a table carries its columns, its rules and its indexes
    Given create_table
    Then it carries columns, constraints and indexes
    And it carries no values and no new name

  Scenario: dropping a table carries nothing but its name
    Given drop_table
    Then it carries none of the fields

  Scenario: renaming a table carries the name it is taking
    Given rename_table
    Then it carries a new name and nothing else

  Scenario: filling an enum carries the values
    Given insert_enum_values
    Then it carries values and nothing else

  Scenario: creating a view carries the view
    Given create_view
    Then it carries a view and nothing else

  Scenario: every kind of change answers
    When all thirteen kinds are asked
    Then each one answers about all seven fields

  # KINDS THAT FILL THE SAME FIELDS ARE FINE AND THE COUNT IS THE POINT. Dropping
  # a table and dropping a view both carry nothing but a name, and the three
  # column changes all carry columns. What would be wrong is a table where every
  # row is the same, because that is a table nobody needed.
  Scenario: the kinds do not all fill the same fields
    When all thirteen kinds are asked
    Then more than one set of fields comes back
