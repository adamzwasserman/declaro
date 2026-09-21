@bdd @specification
Feature: declaration_from_sqlalchemy -- SQLAlchemy.s phrasing into a declaration
  A query object, for people who think in one. One of four builders; all four produce the
  same declaration for the same question.

  The call arrives as data, under the four words this grammar spells a question with:
  the model, the asking word, filter_by and with_only. A part no word points at is
  refused by name, because dropping one answers a question nobody asked.

  WHERE THIS GRAMMAR HAS NO WORD, THE LIBRARY SUPPLIES ONE. The ORM this surface
  imitates has no phrasing it copies for an order, a grouping, a having or a page,
  and a caller who picked this spelling must not have picked a smaller library.
  So all ten fields are reached here, four of them by the library's own word.

  Scenario: columns and a condition become a declaration
    When a caller writes Person.query.filter_by(name="bob").with_only("id", "name")
    Then the declaration names the model, those two columns and that condition

  Scenario: a bare value means eq
    When a caller narrows by {"name": "bob"}
    Then the condition holds an eq comparison, not a bare value

  Scenario: a named operator keeps the name the caller gave it
    When a caller narrows by {"age": {"gt": 18}}
    Then the condition compares age with gt

  Scenario: two keys mean both
    When a caller narrows by {"age": {"gt": 18}, "name": "bob"}
    Then the condition holds both comparisons and nothing else

  Scenario: or becomes a list of alternatives
    When a caller narrows by {"or": [{"name": "alice"}, {"name": "carol"}]}
    Then the declaration holds those two as alternatives
    And the word or is gone from it

  Scenario: not becomes a list of exclusions
    When a caller narrows by {"not": [{"name": "bob"}]}
    Then the declaration holds that one as an exclusion
    And the word not is gone from it

  Scenario: a text comparison carries the companion that says case does not matter
    When a caller narrows by {"name": {"like": "bo%", "ignore_case": true}}
    Then the comparison says case does not matter

  Scenario: a caller who names no columns asked for none
    When a caller writes Person.query.filter_by(name="bob")
    Then the declaration names no columns

  Scenario: every field of a declaration is there to be read
    When a caller writes Person.query.filter_by(name="bob").with_only("id", "name")
    Then all ten fields are present
    And the six the caller did not name are empty

  Scenario: the question does not remember which grammar built it
    When a caller writes Person.query.filter_by(name="bob").with_only("id", "name")
    Then nothing in the declaration names sqlalchemy

  Scenario Outline: what the caller wrote is what comes back
    When a caller writes a question naming a <part>
    Then the declaration carries that <part>, and moves when it moves

    Examples:
      | part    |
      | model   |
      | columns |

  # THE BACKFILL. Until 2026-09-21 this scenario asked that ordered_by, grouped_by,
  # having and page be refused here, because the ORM this surface imitates has no
  # phrasing it copies for them. That made a caller who chose this spelling a caller
  # with a smaller library, and made the claim this file opens with -- all four
  # produce the same declaration for the same question -- false. Adam ruled on it.
  Scenario Outline: a field this grammar has no word for is reached by the library's word
    When a caller writes the library's own <word>
    Then the declaration carries what they asked for

    Examples:
      | word       |
      | ordered_by |
      | grouped_by |
      | having     |
      | page       |

  Scenario Outline: a question missing something a declaration needs is refused
    When a caller writes a question naming no <part>
    Then it is refused, naming the <part> it needs

    Examples:
      | part   |
      | model  |
      | wanted |

  Scenario: an asking word nobody declared is refused
    When a caller asks for "roughly"
    Then it is refused, naming that word

  Scenario: an operator nobody declared is refused
    When a caller narrows by {"age": {"roughly": 18}}
    Then it is refused, naming that operator

  Scenario: a failure comes back in the return
    When a caller writes the word only
    Then a fault is returned, and nothing was raised

  Scenario: nothing outside the question is read
    When a caller writes Person.query.filter_by(name="bob").with_only("id", "name")
    Then reading it twice reads the same thing
    And no clock, file or connection was reached
