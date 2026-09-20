@bdd @specification
Feature: declaration_from_sqlalchemy -- SQLAlchemy.s phrasing into a declaration
  A query object, for people who think in one. One of four builders; all four produce the
  same declaration for the same question.

  Scenario: columns and a condition become a declaration
    When a caller writes Person.query.filter_by(name="bob").with_only("id", "name")
    Then the declaration names the model, those two columns and that condition

  Scenario: a bare value means eq
    When a caller writes where({"name": "bob"})
    Then the condition holds an eq comparison, not a bare value

  Scenario: no string is ever pasted
    When a caller writes a condition
    Then every key was checked against the model and every value bound
