@bdd @specification
Feature: methods_of -- Which method name means which part, one row per spelling
  Three of the four spellings are a model, maybe an attribute, then a chain of
  calls. Only the method names differ. This is the table of those names, read by
  the one reader that parses all four.

  # WRITTEN 2026-10-05, out of `type Phrasing` and the four word-lists that are
  # gone. `Phrasing` claimed to be a grammar's word for every field and held two
  # renamed keys, because no parser existed and the words were the whole of what
  # a "grammar" was. Over a real parse, method names are all it ever was, so this
  # says that and nothing more.

  # EVERY SPELLING REACHES EVERY FIELD. A caller who picked Django's spelling has
  # not picked a smaller library. Django has no method of its own for an order, a
  # grouping, a having or a page, so those four are reached by the library's own
  # word in every spelling, and the table carries only where a spelling differs.

  Scenario: the library's own spelling renames nothing
    When methods_of is asked for pseudo_sql
    Then every field is reached by the library's own word
    And the table holds no row

  Scenario: Django renames two
    When methods_of is asked for django
    Then filter means the narrowing and values means the columns

  Scenario: SQLAlchemy renames the same two, differently
    When methods_of is asked for sqlalchemy
    Then filter_by means the narrowing and with_only means the columns

  Scenario: Prisma renames nothing, because its words are already the library's
    When methods_of is asked for prisma
    Then where and select are reached by the library's own word
    And the table holds no row

  Scenario: no two spellings claim one method name
    When every table is read together
    Then no method name appears under two spellings
    And a name read back gives one spelling

  Scenario: a spelling nobody declared raises at the lookup
    When methods_of is asked for a word no spelling uses
    Then it raises naming the word, rather than answering an empty table
    And an empty table would have read every method as the library's own

  # CORRECTED 2026-11-16. This scenario said "only fields a declaration has", and
  # a table's keys are not declaration fields. They are this library's own method
  # names, which is the only reading under which pseudo_sql and prisma hold no
  # row, as the two scenarios above require. A declaration calls the narrowing
  # `condition` and the columns `columns`; this library's methods for them are
  # `where` and `select`, and Django's are `filter` and `values`.
  Scenario: the four tables agree on which methods exist
    When every table is read together
    Then each one renames only a method this library has
    And a method renamed by one is renamed by all that rename anything
