@bdd @specification
Feature: refuse_some_columns_as_an_object -- Two columns out of five is not that model
  You say which shape you want your rows back in. Asking for some columns and
  for your own model at once asks for something that cannot exist, so it is
  refused while you are still holding the question.

  Scenario: some columns and your own model is refused
    Given a question naming two columns and asking for objects
    Then it is refused, and the refusal names both

  Scenario: the whole row as your own model is fine
    Given a question naming no columns and asking for objects
    Then nothing is refused

  Scenario: some columns as dictionaries is fine
    Given a question naming two columns and asking for dictionaries
    Then nothing is refused

  Scenario: the whole row as dictionaries is fine
    Given a question naming no columns and asking for dictionaries
    Then nothing is refused
