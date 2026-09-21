@bdd @specification
Feature: declaration_from_django -- Django.s phrasing into a declaration
  Managers and lookups, for a team that already writes them. One of four builders; all four produce the
  same declaration for the same question.

  Scenario: columns and a condition become a declaration
    When a caller writes Person.objects.filter(name="bob").values("id", "name")
    Then the declaration names the model, those two columns and that condition

  Scenario: a bare value means eq
    When a caller writes filter(name="bob")
    Then the condition holds an eq comparison, not a bare value
