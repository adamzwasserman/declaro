@bdd @specification
Feature: declaration_from_asked -- The parts become the one declaration
  Four spellings, four readings, one declaration. This takes the parts a reader
  lifted and builds what every later step reads, and it is the one place a part a
  declaration needs and nobody wrote is told no.

  # WRITTEN 2026-10-05, out of `declaration_from_phrasing` and the shared half of
  # the four grammar features. The old card took a question and a word-list and did
  # two jobs: renaming keys, and checking the result. The renaming is gone with the
  # word-lists. The checking is real, it is the same for all four spellings, and it
  # is what the fourteen scenarios of `declaration_from_phrasing.feature` and six
  # of `declaration_from_sqlalchemy.feature` were about. They are here now.

  A DECLARATION CARRIES EVERY FIELD, HOWEVER FEW THE CALLER NAMED. Eleven fields,
  and the ones nobody named are empty rather than missing, so nothing downstream
  reads a field that may not be there.

  Scenario: the parts of a two-call question become a declaration
    When the parts of Person.objects.filter(name="bob").values("id", "name") arrive
    Then the declaration names the model, those two columns and that condition

  Scenario: every field is there to be read
    When the parts of a question naming two of eleven fields arrive
    Then all eleven fields are present
    And the nine the caller did not name are empty

  Scenario: a narrowing becomes a condition, not a bare value
    When the parts carry a narrowing of name against bob
    Then the declaration holds an eq comparison
    And `fn condition_from_narrowing` is what turned it

  Scenario: the declaration does not remember which spelling built it
    When the parts of the same question arrive from each of the four spellings
    Then the four declarations are one declaration
    And nothing in it names a spelling

  Scenario Outline: a part a declaration needs and nobody wrote is refused
    When the parts arrive naming no <part>
    Then it is refused, naming the <part> it needs

    Examples:
      | part   |
      | model  |
      | wanted |

  Scenario: absence is a refusal only where the shape cannot hold it
    When the parts arrive naming no paging
    Then the declaration carries nothing for paging and is not refused
    And a field whose shape holds nothing is a field a caller may leave out

  Scenario: an asking word nobody declared is refused
    When the parts ask for "roughly"
    Then it is refused, naming that word
    And the refusal names the words it does read

  Scenario: a caller who says nothing about shape gets dictionaries
    When the parts arrive naming no shaping
    Then the declaration says rows come back as dictionaries

  Scenario: a caller who named no columns asked for none
    When the parts arrive naming no columns
    Then the declaration names no columns
    And that is not the same as asking for all of them

  Scenario: what the caller wrote is what comes back
    When the parts name a model and columns
    Then the declaration carries both, and moves when they move

  Scenario: a failure comes back in the return
    When the parts cannot become a declaration
    Then a fault is returned, and nothing was raised

  Scenario: nothing outside the parts is read
    When the parts of Person.objects.filter(name="bob") arrive
    Then reading them twice reads the same thing
    And no clock, file or connection was reached
