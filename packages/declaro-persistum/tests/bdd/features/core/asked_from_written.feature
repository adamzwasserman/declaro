@bdd @specification
Feature: asked_from_written -- Read the writing, lift the parts out
  One reader, four spellings. It takes what the caller typed and the method names
  that spelling uses, and hands back the parts in the library's own words.

  # WRITTEN 2026-10-05, replacing four cards and the lookup that fanned out to
  # them. `declaration_from_pseudo_sql`, `_django`, `_prisma` and `_sqlalchemy`
  # each took a dict somebody else had already taken apart, and the taking apart
  # happened in a hand-written table inside each step file. The features wrote the
  # real expression and the library never saw it. The four differed by their method
  # names alone, so they were one reader differing by a lookup.

  THE WRITING IS TEXT. `Person.objects.filter(name="bob").values("id", "name")` is
  what a caller types, punctuation and all, and this reads it. That is why four
  spellings can differ in shape rather than only in keys.

  WHAT IT HANDS BACK IS PARTS, NOT A DECLARATION. `fn declaration_from_asked` turns
  parts into a declaration, and it is the one place a missing part is told no. This
  card reads, and nothing else.

  Scenario: a chain with two calls gives both parts
    Given the method names django uses
    When a caller writes Person.objects.filter(name="bob").values("id", "name")
    Then the parts name the model Person
    And the narrowing holds name against bob
    And the columns hold id and name

  Scenario: the same question in another spelling gives the same parts
    Given the method names sqlalchemy uses
    When a caller writes Person.query.filter_by(name="bob").with_only("id", "name")
    Then the parts are the ones django gave for the same question

  Scenario: one call carrying named parts gives the same parts again
    Given the method names prisma uses
    When a caller writes Person.find_many(where={"name": "bob"}, select=["id", "name"])
    Then the parts are the ones django gave for the same question

  Scenario: the library's own spelling gives them too
    Given the method names pseudo_sql uses
    When a caller writes people.select("id", "name").where({"name": "bob"})
    Then the parts are the ones django gave for the same question

  Scenario: a caller who calls one method names one part
    Given the method names django uses
    When a caller writes Person.objects.filter(name="bob")
    Then the parts name the model and the narrowing
    And the parts name no columns

  Scenario: keyword arguments and a dict both read as a narrowing
    When a caller writes filter(name="bob") in django and where({"name": "bob"}) in pseudo_sql
    Then both give one narrowing holding name against bob

  Scenario: a field this spelling has no method for is reached by the library's word
    Given the method names django uses
    When a caller writes Person.objects.filter(name="bob").ordered_by({"name": "asc"})
    Then the parts carry that order
    And a caller who chose this spelling did not choose a smaller library

  Scenario: a method no table names is refused, naming it
    Given the method names django uses
    When a caller writes Person.objects.roughly(name="bob")
    Then it is refused, naming roughly
    And dropping it would answer a question nobody asked

  Scenario: writing that does not parse is refused rather than guessed at
    When a caller writes Person.objects.filter(name=
    Then it is refused
    And the refusal says where the writing stopped making sense

  Scenario: a failure comes back in the return
    When a caller writes the word only
    Then a fault is returned, and nothing was raised

  Scenario: nothing outside the writing is read
    Given the method names django uses
    When a caller writes Person.objects.filter(name="bob").values("id", "name")
    Then reading it twice reads the same thing
    And no clock, file or connection was reached
