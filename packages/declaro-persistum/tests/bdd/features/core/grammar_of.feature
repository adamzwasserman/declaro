@bdd @specification
Feature: grammar_of -- The writing says which way it was written
  A caller writes a question in whichever of the four spellings is familiar, and
  nothing in the call says which one it is. The punctuation and the shape say it.

  # WRITTEN 2026-10-05. Until today the caller passed a word naming the spelling,
  # beside the question, and `boundary_in fn query_ask` took it as an argument.
  # That kept one fact in two places, where they can disagree, and it was never
  # needed: the four look nothing alike. The word is gone from the door and this
  # card reads it off the text instead.

  # WHY THE SHAPE AND NOT THE WORDS. Two spellings can share a word. SQLAlchemy 1.x
  # has `filter` and so does Django, and any library may take the word tomorrow, so
  # a guess from keywords gets weaker with every spelling added. The shape cannot
  # collide: a chain hanging off `.objects.` is Django and nothing else, and `==`
  # inside the text can only be a statement built with operators.

  Scenario: the attribute between the model and the chain names the spelling
    When grammar_of reads Person.objects.filter(name="bob")
    Then it answers django

  Scenario: a different attribute names a different spelling
    When grammar_of reads Person.query.filter_by(name="bob")
    Then it answers sqlalchemy

  Scenario: one call carrying named parts is prisma
    When grammar_of reads Person.find_many(where={"name": "bob"})
    Then it answers prisma

  Scenario: a method straight off the model is the library's own
    When grammar_of reads people.select("id", "name").where({"name": "bob"})
    Then it answers pseudo_sql

  Scenario: the answer does not depend on which method was called
    When grammar_of reads Person.objects.values("id") and Person.objects.filter(name="bob")
    Then it answers django both times
    And it never read a method name to decide

  Scenario: a word two spellings share decides nothing
    When grammar_of reads a question whose only method is filter
    Then it answers by the attribute before it, not by the word filter

  Scenario: writing in no spelling it knows is refused, naming the four
    When grammar_of reads SELECT id FROM people WHERE name = 'bob'
    Then it is refused
    And the refusal names all four spellings it does read

  Scenario: nothing outside the writing is read
    When grammar_of reads Person.objects.filter(name="bob")
    Then reading it twice answers the same
    And no clock, file or connection was reached
