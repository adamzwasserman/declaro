@bdd @specification
Feature: declaration_from_pseudo_sql -- The library's own phrasing into a declaration
  SQL's word order without SQL's text. One of four builders; all four produce the
  same declaration for the same question.

  The model and the asking word sit beside the phrasing under "model" and "wanted",
  because neither is anybody's phrasing and all four grammars carry them.

  Scenario: columns and a condition become a declaration
    When a caller writes people.select("id", "name").where({"name": "bob"})
    Then the declaration names the model, those two columns and that condition

  Scenario: what the caller named is what comes back
    When a caller writes people.select("id", "name").where({"name": "bob"})
    Then naming another model and other columns moves both

  Scenario: a declaration carries every field however few the caller named
    When a caller writes people.select("id", "name").where({"name": "bob"})
    Then the declaration carries every field

  Scenario Outline: the same question written twice is one declaration
    When a caller writes <phrasing>
    Then it equals the declaration the plain spelling builds

    Examples:
      | phrasing                       |
      | where({"name": "bob"})         |
      | where({"name": {"eq": "bob"}}) |

  Scenario: a bare value means eq
    When a caller writes where({"name": "bob"})
    Then the condition holds an eq comparison, not a bare value

  Scenario Outline: every comparison is the one the caller wrote
    When a caller writes <phrasing>
    Then the comparison is the one the caller wrote

    Examples:
      | phrasing                                              |
      | where({"name": "bob"})                                |
      | where({"age": 30})                                    |
      | where({"rate": 1.5})                                  |
      | where({"paid": true})                                 |
      | where({"note": nothing})                              |
      | where({"age": {"gt": 18}})                            |
      | where({"name": {"like": "bo%", "ignore_case": true}}) |
      | where({"name": {"like": "bo%"}})                      |

  Scenario Outline: or and not are held as any_of and none_of
    When a caller writes <phrasing>
    Then the condition holds what the caller combined

    Examples:
      | phrasing                                            |
      | where({"or": [{"name": "bob"}, {"name": "alice"}]}) |
      | where({"not": {"name": "bob"}})                     |

  Scenario: a caller who narrowed by nothing gets an empty condition
    When a caller writes select("id", "name") and nothing else
    Then the condition is empty rather than absent

  Scenario: no string is ever pasted
    When a caller writes a condition
    Then every key was checked against the model and every value bound

  Scenario: a value that reads like SQL is still only a value
    When a caller writes a condition
    Then moving a dangerous value moves that value and nothing else

  Scenario Outline: what a declaration cannot be built from is refused, not carried
    When a caller writes <phrasing>
    Then it is refused, and the refusal says what to write instead

    Examples:
      | phrasing                                |
      | select("id", "name") naming no model    |
      | select("id", "name") asking for nothing |
      | where("active = :x")                    |
      | where({"age": {"roughly": 18}})         |
      | where({"age": {"gt": 18, "lt": 30}})    |
      | where({"age": a set of two numbers})    |
      | where({"not": "active = :x"})           |
      | where({"or": "active = :x"})            |

  Scenario Outline: a refusal names what was written and lists what it could have been
    When a caller writes <phrasing>
    Then the refusal names what was written and lists what it could have been

    Examples:
      | phrasing                             |
      | where({"age": {"roughly": 18}})      |
      | asking for "fetch"                   |
      | where({"age": a set of two numbers}) |
