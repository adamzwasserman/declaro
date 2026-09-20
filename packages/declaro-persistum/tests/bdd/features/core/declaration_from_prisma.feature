@bdd @specification
Feature: declaration_from_prisma -- Prisma.s phrasing into a declaration
  One call carrying named parts. One of four builders; all four produce the
  same declaration for the same question.

  Scenario: columns and a condition become a declaration
    When a caller writes Person.find_many(where={"name": "bob"}, select=["id", "name"])
    Then the declaration names the model, those two columns and that condition

  Scenario: a bare value means eq
    When a caller writes where({"name": "bob"})
    Then the condition holds an eq comparison, not a bare value

  Scenario: no string is ever pasted
    When a caller writes a condition
    Then every key was checked against the model and every value bound
