@bdd @specification
Feature: declaration_from_phrasing -- One reader, and a grammar is only its words
  A caller's question arrives in the words their grammar gave them, beside a
  table saying which word fills which field. This reads the one through the other
  and builds the declaration all four grammars produce.

  WHY THE WORDS ARRIVE AS DATA. Django says `filter`, Prisma and the library's
  own phrasing say `where`, SQLAlchemy says `filter_by`, and all four mean
  `condition`. That difference is a table, so each grammar is a table and this is
  the reader they share.

  WHERE A GRAMMAR HAS NO WORD, THE LIBRARY SUPPLIES ONE. A caller who picked a
  spelling must not have picked a smaller library. So every grammar reaches all
  ten fields of a declaration, including the fields whose imitated ORM has no
  phrasing this surface copies.

  # ------------------------------------------------- what the caller said travels

  Scenario: what the caller named is what comes back
    Given a grammar whose word for the condition is "where"
    When a caller asks Person for id and name, narrowed by name being "bob"
    Then the declaration names Person, carries those two columns and that condition

  Scenario: a caller who named no columns asked for none
    Given a grammar whose word for the columns is "select"
    When a caller names no columns
    Then the declaration asks for none, and not for the last ones anybody named

  Scenario: every field of a declaration is there to be read
    When any question is built
    Then the declaration carries all ten fields

  Scenario: the declaration does not remember which grammar built it
    When four callers ask one question in four grammars
    Then the four declarations are equal
    And none of them names a grammar

  # ------------------------------------- the words a grammar did not bring

  # THE BACKFILL, WHICH IS WHY THE FOUR ARE PEERS. On 2026-09-21 one builder
  # refused these four words because the ORM it imitates has no phrasing this
  # surface copies. That made a caller who chose that spelling a caller with a
  # smaller library, and made the claim in all four feature files false.
  Scenario Outline: a field the grammar has no word for is reached by the library's word
    Given a grammar that brings no word for <field>
    When a caller uses the library's own word for it
    Then the declaration carries what they asked for

    Examples:
      | field    |
      | order    |
      | grouping |
      | having   |
      | paging   |

  Scenario: a grammar's own word wins over the library's where it has one
    Given a grammar whose word for the condition is "filter_by"
    When a caller narrows using that word
    Then the declaration carries the narrowing

  # ------------------------------------------------------------ what is refused

  Scenario Outline: a question missing something a declaration needs is refused
    When a caller leaves out <part>
    Then the refusal names <part> and says what to write

    Examples:
      | part   |
      | model  |
      | wanted |

  # `columns`, `order` and `grouping` have an empty value meaning the caller named
  # none, and three more fields are written so absence can be represented. `model`
  # and `wanted` are not: a table name and one of eighteen words, with no member
  # meaning absence. A caller who named neither would otherwise get a table and a
  # verb nobody chose and nobody can tell from a choice.
  Scenario: absence is a refusal only where the type cannot hold it
    When a caller leaves out the paging
    Then the declaration says so without refusing

  Scenario: an asking word nobody declared is refused
    When a caller asks for something that is not one of the eighteen words
    Then the refusal names the word and lists the ones that exist

  Scenario: a named part this grammar does not read is refused by name
    When a caller names a part no word in the table points at
    Then the refusal names that part rather than dropping it

  Scenario: a failure comes back in the return
    When any question is refused
    Then the refusal was returned, not raised
    And it says what went wrong and what to write instead

  # ---------------------------------------------------- what it does not do

  Scenario: no column is checked against a model here
    When a caller asks for a column no model declares
    Then the declaration carries the name and refuses nothing
    And the name sits where a column name is looked up, never where a value binds

  Scenario: nothing outside the question is read
    When one question is asked twice
    Then both answers are the same
