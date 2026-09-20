@bdd @specification
Feature: schema_read_declared -- Asking the code what it declares
  The models and the views a caller wrote, read rather than passed in. No door
  takes them: a caller that had to hand them over could hand a different set to
  each door and nothing would notice.

  Scenario: models and views come back together
    Given a project declaring two models and one view
    When the declarations are read
    Then both models and the view come back in one answer

  Scenario: what the code means to lose comes with them
    Given a project declaring that the table "legacy" is to be dropped
    When the declarations are read
    Then that permission is part of the answer

  Scenario: a model that cannot be read is a fault
    Given a models file that does not parse
    When the declarations are read
    Then a fault comes back naming the file
