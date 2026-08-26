@bdd
Feature: How each engine spells a column
  As an author declaring one schema for three databases
  I want a declared type, default and constraint written the way each engine wants
  So that the same declaration produces the same column everywhere

  # ONE SCENARIO PER FUNCTION IN `applier/spelling.py`, which is the
  # bijection the Honest Code principles ask for: a function with no scenario
  # is code nothing describes, and a scenario with no function describes code
  # that does not exist. The counts reconcile mechanically, and that is what
  # makes a missing test visible one at a time.
  #
  # Every function here is pure: a declaration in, text out. So every step is
  # call it and check the result, which is the principle's own test for
  # whether the code underneath is honest.

  # ============================================
  # Naming a type
  # ============================================

  @precommit
  Scenario: sqlite_type maps a declared type to a storage class
    When sqlite_type is given "uuid"
    Then it answers "TEXT"

  @precommit
  Scenario: postgresql_type keeps the type the author declared
    When postgresql_type is given "uuid"
    Then it answers "uuid"

  @precommit
  Scenario: map_type spells a type for the engine it is given
    When map_type is given "boolean" for "postgresql"
    Then it answers "boolean"

  @precommit
  Scenario: _the_mappable_dialect hands back an engine that has a mapper
    When _the_mappable_dialect is given "sqlite"
    Then it answers "sqlite"

  @precommit
  Scenario: _refuse_unmappable_dialect names the engines that do exist
    When _refuse_unmappable_dialect is given "mysql"
    Then it refuses and names "sqlite"

  # ============================================
  # Comparing two declared types
  # ============================================

  @precommit
  Scenario: same_type finds two spellings of one type equal on an engine
    When same_type compares "uuid" and "TEXT" on "sqlite"
    Then it answers that they are the same

  @precommit
  Scenario: _the_same_spelling compares two declared types as one engine writes them
    When _the_same_spelling compares "int" and "integer" on "sqlite"
    Then it answers that they are the same

  @precommit
  Scenario: _one_absence_is_not_the_other answers only when both sides are absent
    When _one_absence_is_not_the_other compares nothing with "text"
    Then it answers that they are not the same

  # ============================================
  # Spelling a default
  # ============================================

  @precommit
  Scenario: sqlite_default writes the text that follows DEFAULT
    When sqlite_default is given the number 0
    Then it answers "DEFAULT 0"

  @precommit
  Scenario: postgresql_default returns an expression as it was written
    When postgresql_default is given "(now())"
    Then it answers "DEFAULT (now())"

  @precommit
  Scenario: _null_default writes NULL for a default nobody declared
    When _null_default is given nothing
    Then it answers "DEFAULT NULL"

  @precommit
  Scenario: _literal_default writes a number as it stands
    When _literal_default is given the number 42
    Then it answers "DEFAULT 42"

  @precommit
  Scenario: _wrapped_default wraps an expression a second time
    When _wrapped_default is given "(now())"
    Then it answers "DEFAULT ((now()))"

  @precommit
  Scenario: _bare_default leaves text that is not parenthesised alone
    When _bare_default is given "'active'"
    Then it answers "DEFAULT 'active'"

  @precommit
  Scenario: _text_default decides by whether the text is already an expression
    When _text_default is given "(now())"
    Then it answers "DEFAULT ((now()))"

  @precommit
  Scenario: _the_spellable_default hands back a default the table can write
    When _the_spellable_default is given the number 1
    Then it answers the number 1

  @precommit
  Scenario: _refuse_unspellable_default names the default nobody can write
    When _refuse_unspellable_default is given an object nothing spells
    Then it refuses

  @precommit
  Scenario: _the_renderable_dialect hands back an engine that has a renderer
    When _the_renderable_dialect is given "postgresql"
    Then it answers "postgresql"

  @precommit
  Scenario: _refuse_unrenderable_dialect names the engines that do exist
    When _refuse_unrenderable_dialect is given "mysql"
    Then it refuses and names "sqlite"

  # ============================================
  # Translating a default between engines
  # ============================================

  @precommit
  Scenario: _translate_default spells a declared default for one engine
    When _translate_default is given "now()" for "sqlite" on column "created"
    Then it answers "(datetime('now'))"

  @precommit
  Scenario: _translate_text_default matches text against what the dialect spells
    When _translate_text_default is given "now()" for "sqlite" on column "created"
    Then it answers "(datetime('now'))"

  @precommit
  Scenario: _untranslated_default leaves a number the same in every dialect
    When _untranslated_default is given the number 7 for "sqlite" on column "n"
    Then it answers the number 7

  @precommit
  Scenario: _the_spellable_expression hands back an expression the engine writes
    When _the_spellable_expression is given "now()" for "sqlite" on column "created"
    Then it answers "now()"

  @precommit
  Scenario: _refuse_unspellable_expression names the column it could not spell
    When _refuse_unspellable_expression is given "gen_random_uuid()" for "sqlite" on column "id"
    Then it refuses and names "id"

  @precommit
  Scenario: _the_dialects_spelling writes the expression this engine's own way
    When _the_dialects_spelling looks up "now()" in the sqlite spellings
    Then it answers "CURRENT_TIMESTAMP"

  @precommit
  Scenario: _as_declared keeps what the author wrote when no engine differs
    When _as_declared looks up "'x'" in the sqlite spellings
    Then it answers "'x'"

  # ============================================
  # Writing a whole column
  # ============================================

  @precommit
  Scenario: column_definition spells one column for one engine
    When column_definition is given a text column "name" for "sqlite"
    Then it answers "name" TEXT

  @precommit
  Scenario: _foreign_key_clause writes REFERENCES with the cascades declared
    When _foreign_key_clause is given a column referencing "users.id"
    Then it answers a REFERENCES clause naming "users"

  # ============================================
  # Writing a table-level constraint
  # ============================================

  @precommit
  Scenario: constraint_clause writes one constraint as a CREATE TABLE carries it
    When constraint_clause is given a check "ck_positive" on "id > 0"
    Then it answers a CHECK clause naming "ck_positive"

  @precommit
  Scenario: _check_clause writes the expression the declaration carries
    When _check_clause is given a check "ck_positive" on "id > 0"
    Then it answers a CHECK clause naming "ck_positive"

  @precommit
  Scenario: _unique_clause writes a UNIQUE over the columns named
    When _unique_clause is given a unique "uq_email" over "email"
    Then it answers a UNIQUE clause naming "email"

  @precommit
  Scenario: _write_constraint hands a kind this renderer writes to its writer
    When _write_constraint is given a check "ck_positive" on "id > 0"
    Then it answers a CHECK clause naming "ck_positive"

  @precommit
  Scenario: _refuse_constraint names a kind this renderer does not write
    When _refuse_constraint is given a constraint of kind "exclusion"
    Then it refuses and names "exclusion"
