@bdd @specification
Feature: spell_operation -- One schema change into the statements that carry it out
  The schema half's compiler. One operation in, and the statements for this
  engine out, which may be several: a SQLite table rebuild is one operation and
  many statements.

  Scenario: an added column is one statement on PostgreSQL
    When adding a column is spelled for postgresql
    Then one statement comes back

  Scenario: the same change is a rebuild on SQLite
    When dropping a column is spelled for sqlite
    Then several statements come back, and they rebuild the table

  Scenario: an operation this engine cannot express is refused
    When an operation no engine table covers is spelled
    Then a fault comes back naming the operation and the engine

  # ------------------------------------------------------------------
  # FOLDED IN FROM `applier/spelling.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As an author declaring one schema for three databases
  # I want a declared type, default and constraint written the way each engine wants
  # So that the same declaration produces the same column everywhere
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
  # @precommit

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
  Scenario: _the_mappable_engine hands back an engine that has a mapper
    When _the_mappable_engine is given "sqlite"
    Then it answers "sqlite"

  @precommit
  Scenario: _refuse_unmappable_engine names the engines that do exist
    When _refuse_unmappable_engine is given "mysql"
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
  Scenario: _the_renderable_engine hands back an engine that has a renderer
    When _the_renderable_engine is given "postgresql"
    Then it answers "postgresql"

  @precommit
  Scenario: _refuse_unrenderable_engine names the engines that do exist
    When _refuse_unrenderable_engine is given "mysql"
    Then it refuses and names "sqlite"

  # ============================================
  # Translating a default between engines
  # ============================================

  @precommit
  Scenario: _translate_default spells a declared default for one engine
    When _translate_default is given "now()" for "sqlite" on column "created"
    Then it answers "(datetime('now'))"

  @precommit
  Scenario: _translate_text_default matches text against what the engine spells
    When _translate_text_default is given "now()" for "sqlite" on column "created"
    Then it answers "(datetime('now'))"

  @precommit
  Scenario: _untranslated_default leaves a number the same in every engine
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
  Scenario: _the_engines_spelling writes the expression this engine's own way
    When _the_engines_spelling looks up "now()" in the sqlite spellings
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

  # ------------------------------------------------------------------
  # FOLDED IN FROM `enums/changing_an_enum_value.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As a schema author widening a Literal type
  # I want the new value to reach the lookup table the column points at
  # So that the column accepts it instead of refusing every row that uses it
  # A `Literal["a", "b"]` becomes a lookup table plus a foreign key, because a
  # CHECK constraint is not portable. Widening the Literal means inserting a
  # row; narrowing it means deleting one.
  #
  # BOTH RUN AGAINST A REAL DATABASE with real foreign key enforcement, because
  # the interesting half is what happens when a row still points at the value
  # being removed. Reading the SQL string cannot see that.
  # @precommit @sqlite

  Scenario: a value added to the lookup is a value the column accepts
    Given a table "orders" whose status is one of "new" and "paid"
    When "shipped" is added to the statuses
    Then a row with status "shipped" is accepted

  @precommit @sqlite
  Scenario: a value the column never had is refused before it is added
    Given a table "orders" whose status is one of "new" and "paid"
    Then a row with status "shipped" is refused

  @precommit @sqlite
  Scenario: a value taken away is a value the column stops accepting
    Given a table "orders" whose status is one of "new" and "paid"
    When "paid" is taken out of the statuses
    Then a row with status "paid" is refused

  @precommit @sqlite
  Scenario: a value still in use cannot be taken away
    Given a table "orders" whose status is one of "new" and "paid"
    And an order whose status is "paid"
    When "paid" is taken out of the statuses
    Then the engine refuses, because a row still points at it

  @precommit
  Scenario Outline: a quote in a value does not end the statement early
    When "<value>" is <change> the statuses
    Then the statement carries the value with its quote doubled

    Examples:
      | value    | change        |
      | it's new | added to      |
      | it's new | taken out of  |

  # ------------------------------------------------------------------
  # FOLDED IN FROM `migration/statements_per_engine.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As a service booting against Postgres or SQLite
  # I want the same question asked in the engine the engine speaks
  # So that a stamp written on one engine can be read on the other
  # TWO BUILDERS, ONE SHAPE. Each looks its SQL up in a per-engine table and
  # hands it back. The table was the only thing that differed.
  #
  # The SQLite ones are RUN against a real database, because a statement that
  # names a column the table has not got is still a plausible string.
  # @precommit @sqlite

  Scenario: the stamp is written, and written again over itself
    Given a stamp table on a real database
    When the same key is stamped twice with different values
    Then the second value is what is stored
    And there is one row, not two

  @precommit @sqlite
  Scenario: the user-tables query does not count our own table
    Given a stamp table on a real database
    And a table "orders" that the user made
    When the user-tables query is run
    Then it names "orders" and not the stamp table

  @precommit
  Scenario Outline: both builders answer for every engine we ship
    When "<builder>" is asked for "<engine>"
    Then it gives back a statement

    Examples:
      | builder         | engine     |
      | store_hash_sql  | sqlite     |
      | store_hash_sql  | postgresql |
      | user_tables_sql | sqlite     |
      | user_tables_sql | postgresql |

  @precommit
  Scenario Outline: an engine nobody wrote a statement for is refused
    When "<builder>" is asked for "mysql"
    Then it refuses and names the engines it has

    Examples:
      | builder         |
      | store_hash_sql  |
      | user_tables_sql |

  # THE PLACEHOLDER IS THE SAME QUESTION, one fragment rather than a whole
  # statement: asyncpg numbers its parameters and SQLite marks them.

  @precommit
  Scenario Outline: each engine marks a parameter its own way
    When "_placeholder" is asked for "<engine>"
    Then it gives back "<mark>"

    Examples:
      | engine     | mark |
      | sqlite     | ?    |
      | postgresql | $1   |

  @precommit
  Scenario: an engine nobody wrote a placeholder for is refused
    When "_placeholder" is asked for "mysql"
    Then it refuses and names the engines it has

  # THE NAME A REBUILD COPIES ROWS INTO. It carries a random tail so a retry
  # does not collide with a table the previous attempt left behind, and the
  # recovery rule strips that tail BY POSITION -- which is right only while the
  # width stays what the constant says.

  @precommit
  Scenario: a temporary name carries the prefix orphan recovery looks for
    When a temporary name is made for "users"
    Then it begins with the temporary prefix
    And it carries the table's own name

  @precommit
  Scenario: two temporary names for one table differ
    When two temporary names are made for "users"
    Then they are not the same name

  @precommit
  Scenario: the random tail is the width the recovery rule strips
    When a temporary name is made for "users"
    Then the tail is exactly as wide as the constant says

  # ------------------------------------------------------------------
  # FOLDED IN FROM `table_reconstruction.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As a developer using declaro-persistum with SQLite/Turso
  # I want ALTER COLUMN operations to work via table reconstruction
  # So that I can modify column properties despite SQLite's limitations

  Scenario: Change column from nullable to NOT NULL
    Given a database connection
    Given a table "users" with columns:
      | name  | type    | nullable |
      | id    | INTEGER | false    |
      | email | TEXT    | true     |
      | name  | TEXT    | true     |
    And the table contains data:
      | id | email          | name  |
      | 1  | alice@test.com | Alice |
      | 2  | bob@test.com   | Bob   |
    When I alter column "email" to be NOT NULL
    Then the table schema shows "email" as NOT NULL
    And all existing data is preserved
    And the table has 2 rows

  Scenario: Change column from NOT NULL to nullable
    Given a database connection
    Given a table "products" with columns:
      | name        | type    | nullable |
      | id          | INTEGER | false    |
      | name        | TEXT    | false    |
      | description | TEXT    | false    |
    And the table contains data:
      | id | name       | description    |
      | 1  | Product 1  | Description 1  |
      | 2  | Product 2  | Description 2  |
    When I alter column "description" to be nullable
    Then the table schema shows "description" as nullable
    And all existing data is preserved
  # ============================================
  # Change Column Type
  # ============================================

  Scenario: Change column type from INTEGER to TEXT
    Given a database connection
    Given a table "orders" with columns:
      | name      | type    | nullable |
      | id        | INTEGER | false    |
      | status    | INTEGER | true     |
      | amount    | INTEGER | true     |
    And the table contains data:
      | id | status | amount |
      | 1  | 1      | 100    |
      | 2  | 2      | 200    |
    When I alter column "status" to type "TEXT"
    Then the table schema shows "status" as type "TEXT"
    And all existing data is preserved
    And the data values are converted to TEXT

  Scenario: Change column type from TEXT to INTEGER
    Given a database connection
    Given a table "metrics" with columns:
      | name  | type | nullable |
      | id    | TEXT | false    |
      | value | TEXT | true     |
    And the table contains data:
      | id  | value |
      | 1   | 42    |
      | 2   | 99    |
    When I alter column "value" to type "INTEGER"
    Then the table schema shows "value" as type "INTEGER"
    And the data values are converted to INTEGER
  # ============================================
  # Change Default Value
  # ============================================

  Scenario: Add default value to column
    Given a database connection
    Given a table "settings" with columns:
      | name   | type | nullable | default |
      | id     | TEXT | false    |         |
      | status | TEXT | true     |         |
    When I alter column "status" to have default "'active'"
    Then the table schema shows "status" with default "'active'"
    And new inserts use the default value

  Scenario: Change existing default value
    Given a database connection
    Given a table "accounts" with columns:
      | name    | type | nullable | default  |
      | id      | TEXT | false    |          |
      | balance | TEXT | true     | '0'      |
    When I alter column "balance" to have default "'100'"
    Then the table schema shows "balance" with default "'100'"

  Scenario: Remove default value from column
    Given a database connection
    Given a table "configs" with columns:
      | name  | type | nullable | default   |
      | id    | TEXT | false    |           |
      | value | TEXT | true     | 'default' |
    When I alter column "value" to have no default
    Then the table schema shows "value" with no default
  # ============================================
  # Data Preservation
  # ============================================

  Scenario: Preserve data during complex reconstruction
    Given a database connection
    Given a table "customers" with columns:
      | name       | type    | nullable |
      | id         | INTEGER | false    |
      | email      | TEXT    | true     |
      | created_at | TEXT    | true     |
    And the table contains 100 rows of test data
    When I alter column "email" to be NOT NULL
    Then all 100 rows are preserved
    And the column "email" is now NOT NULL

  Scenario: Handle NULL values when changing to NOT NULL
    Given a database connection
    Given a table "posts" with columns:
      | name  | type | nullable |
      | id    | TEXT | false    |
      | title | TEXT | true     |
    And the table contains data:
      | id | title |
      | 1  | Post1 |
      | 2  | NULL  |
    When I alter column "title" to be NOT NULL
    Then the operation fails with constraint violation
    And the transaction is rolled back
  # ============================================
  # Foreign Key Relationships
  # ============================================

  Scenario: Preserve foreign key relationships during reconstruction
    Given a database connection
    Given a table "authors" with columns:
      | name | type    | nullable |
      | id   | INTEGER | false    |
      | name | TEXT    | false    |
    And a table "books" with columns:
      | name      | type    | nullable | references  |
      | id        | INTEGER | false    |            |
      | title     | TEXT    | false    |            |
      | author_id | INTEGER | true     | authors.id |
    And the tables contain related data
    When I alter column "title" in "books" to be nullable
    Then the foreign key relationship is preserved
    And foreign key constraints are still enforced

  Scenario: Verify foreign keys after reconstruction
    Given a database connection
    Given tables with foreign key relationships
    And data that satisfies the foreign key constraints
    When I reconstruct a table with column changes
    Then foreign key checks pass after reconstruction
    And the foreign key relationships still work
  # ============================================
  # Index Preservation
  # ============================================

  Scenario: Preserve indexes during reconstruction
    Given a database connection
    Given a table "users" with columns:
      | name  | type | nullable |
      | id    | TEXT | false    |
      | email | TEXT | false    |
      | name  | TEXT | true     |
    And an index "idx_email" on column "email"
    And a unique index "idx_email_unique" on column "email"
    When I alter column "name" to be NOT NULL
    Then the index "idx_email" still exists
    And the unique index "idx_email_unique" still exists
    And both indexes are functional

  Scenario: Skip auto-generated indexes during reconstruction
    Given a database connection
    Given a table "items" with columns:
      | name | type | nullable | unique |
      | id   | TEXT | false    | false  |
      | code | TEXT | false    | true   |
    When I alter column "id" nullability
    Then the UNIQUE constraint on "code" is preserved
    And only explicit CREATE INDEX statements are recreated
  # ============================================
  # Transaction Safety
  # ============================================

  Scenario: Rollback on reconstruction failure
    Given a database connection
    Given a table "data" with columns:
      | name  | type | nullable |
      | id    | TEXT | false    |
      | value | TEXT | true     |
    And the table contains data
    When reconstruction fails during data copy
    Then the transaction is rolled back
    And the original table is unchanged
    And the original data is intact
  # RENAMED, BECAUSE THE OLD TITLE CLAIMED SOMETHING THIS UNIT CANNOT DO.
  # `reconstruct_table` cannot re-enable the pragma: `PRAGMA foreign_keys` is a
  # no-op inside a transaction, and the row copy has opened one by the time the
  # restore runs. Its own comment records `ENTER fk=1 / EXIT fk=0` and says the
  # restore belongs to whoever owns the COMMIT — `applier/sqlite.py`, covered by
  # `tests/unit/test_a_failed_turso_migration_leaves_the_keys_on.py`.
  #
  # The step under the old title read the pragma and discarded the answer, so
  # the scenario passed for eleven months while asserting nothing. Made to
  # assert, it fails: enforcement reads 0.

  Scenario: A reconstruction leaves no foreign key violations
    Given a database connection
    Given foreign keys are enabled
    And a table requiring reconstruction
    When I perform table reconstruction
    Then foreign keys are temporarily disabled
    And no foreign key violations remain after reconstruction
    And foreign key checks are performed
  # ============================================
  # Edge Cases
  # ============================================

  Scenario: Reconstruct table with no common columns
    Given a database connection
    Given a table "legacy" with columns:
      | name    | type | nullable |
      | old_id  | TEXT | false    |
      | old_val | TEXT | true     |
    When I reconstruct with entirely new column names
    Then the table is recreated
    And no data is copied (no common columns)
    And a warning is logged

  Scenario: Reconstruct empty table
    Given a database connection
    Given a table "empty_table" with columns:
      | name | type | nullable |
      | id   | TEXT | false    |
      | data | TEXT | true     |
    And the table has 0 rows
    When I alter column "data" to be NOT NULL
    Then the table is reconstructed successfully
    And the table remains empty

  Scenario: Reconstruct table with composite primary key
    Given a database connection
    Given a table "composite_pk" with columns:
      | name | type    | nullable | primary_key |
      | a    | INTEGER | false    | true        |
      | b    | INTEGER | false    | true        |
      | data | TEXT    | true     | false       |
    When I alter column "data" to be NOT NULL
    Then the composite primary key is preserved
    And both columns remain part of primary key
  # ============================================
  # Foreign Key Operations via Reconstruction
  # ============================================

  Scenario: Add foreign key via reconstruction
    Given a database connection
    Given a table "authors" with columns:
      | name | type    | nullable |
      | id   | INTEGER | false    |
      | name | TEXT    | false    |
    And a table "books" with columns:
      | name      | type    | nullable |
      | id        | INTEGER | false    |
      | title     | TEXT    | false    |
      | author_id | INTEGER | true     |
    And the tables contain related data:
      | table   | id | name              | title         | author_id |
      | authors | 1  | Alice Author      |               |           |
      | authors | 2  | Bob Writer        |               |           |
      | books   | 1  |                   | Book One      | 1         |
      | books   | 2  |                   | Book Two      | 2         |
    When I add foreign key on "books.author_id" referencing "authors.id"
    Then the foreign key relationship exists
    And foreign key constraints are enforced
    And all existing data is preserved
    And inserting invalid author_id raises foreign key error

  Scenario: Drop foreign key via reconstruction
    Given a database connection
    Given a table "authors" with columns:
      | name | type    | nullable |
      | id   | INTEGER | false    |
      | name | TEXT    | false    |
    And a table "books" with columns:
      | name      | type    | nullable | references  |
      | id        | INTEGER | false    |            |
      | title     | TEXT    | false    |            |
      | author_id | INTEGER | true     | authors.id |
    And the tables contain related data:
      | table   | id | name              | title         | author_id |
      | authors | 1  | Alice Author      |               |           |
      | authors | 2  | Bob Writer        |               |           |
      | books   | 1  |                   | Book One      | 1         |
      | books   | 2  |                   | Book Two      | 2         |
    When I drop foreign key on "books.author_id"
    Then the foreign key relationship does not exist
    And all existing data is preserved
    And inserting invalid author_id succeeds (no FK check)

  Scenario: Add foreign key with ON DELETE CASCADE
    Given a database connection
    Given a table "categories" with columns:
      | name | type    | nullable |
      | id   | INTEGER | false    |
      | name | TEXT    | false    |
    And a table "products" with columns:
      | name        | type    | nullable |
      | id          | INTEGER | false    |
      | name        | TEXT    | false    |
      | category_id | INTEGER | true     |
    And the tables contain related data:
      | table      | id | name        | category_id |
      | categories | 1  | Electronics |             |
      | products   | 1  | Laptop      | 1           |
    When I add foreign key on "products.category_id" referencing "categories.id" with ON DELETE CASCADE
    Then the foreign key relationship exists
    And deleting category cascades to products

  Scenario: Add foreign key fails with invalid data
    Given a database connection
    Given a table "authors" with columns:
      | name | type    | nullable |
      | id   | INTEGER | false    |
      | name | TEXT    | false    |
    And a table "books" with columns:
      | name      | type    | nullable |
      | id        | INTEGER | false    |
      | title     | TEXT    | false    |
      | author_id | INTEGER | true     |
    And the books table contains invalid foreign key data:
      | id | title     | author_id |
      | 1  | Orphan    | 999       |
    When I add foreign key on "books.author_id" referencing "authors.id"
    Then the operation fails with foreign key violation
    And the transaction is rolled back
    And the original schema is unchanged
  # ============================================
  # Drop Column via Reconstruction
  # ============================================

  Scenario: Drop column with UNIQUE constraint via reconstruction
    Given a database connection
    Given a table "users" with columns:
      | name  | type | nullable | unique |
      | id    | TEXT | false    | false  |
      | email | TEXT | false    | true   |
      | phone | TEXT | true     | true   |
      | name  | TEXT | true     | false  |
    And the table contains data:
      | id | email          | phone      | name  |
      | 1  | alice@test.com | 5551234567 | Alice |
      | 2  | bob@test.com   | 5559876543 | Bob   |
    When I drop column "phone"
    Then the column "phone" does not exist
    And all other data is preserved
    And the UNIQUE constraint on "email" is preserved

  Scenario: Drop column with foreign key via reconstruction
    Given a database connection
    Given a table "departments" with columns:
      | name | type    | nullable |
      | id   | INTEGER | false    |
      | name | TEXT    | false    |
    And a table "employees" with columns:
      | name          | type    | nullable | references      |
      | id            | INTEGER | false    |                 |
      | name          | TEXT    | false    |                 |
      | department_id | INTEGER | true     | departments.id  |
      | manager_id    | INTEGER | true     |                 |
    When I drop column "manager_id"
    Then the column "manager_id" does not exist
    And the foreign key on "department_id" is preserved

  Scenario: Drop column that is referenced by foreign key fails
    Given a database connection
    Given a table "authors" with columns:
      | name | type    | nullable |
      | id   | INTEGER | false    |
      | name | TEXT    | false    |
    And a table "books" with columns:
      | name      | type    | nullable | references  |
      | id        | INTEGER | false    |            |
      | title     | TEXT    | false    |            |
      | author_id | INTEGER | true     | authors.id |
    When I drop column "authors.id"
    Then the operation fails with constraint violation
    And the transaction is rolled back

  Scenario: Drop simple column without constraints (direct DROP)
    Given a database connection
    Given a table "products" with columns:
      | name        | type | nullable |
      | id          | TEXT | false    |
      | name        | TEXT | false    |
      | description | TEXT | true     |
      | notes       | TEXT | true     |
    When I drop column "notes"
    Then the column "notes" does not exist
    And the operation uses direct ALTER TABLE DROP COLUMN
    And all other data is preserved
  # ============================================
  # Complex Foreign Key Scenarios
  # ============================================

  Scenario: Reconstruct table that has incoming foreign keys
    Given a database connection
    Given a table "authors" with columns:
      | name | type    | nullable |
      | id   | INTEGER | false    |
      | name | TEXT    | false    |
      | bio  | TEXT    | true     |
    And a table "books" with columns:
      | name      | type    | nullable | references  |
      | id        | INTEGER | false    |            |
      | title     | TEXT    | false    |            |
      | author_id | INTEGER | true     | authors.id |
    When I alter column "authors.bio" to be NOT NULL
    Then the incoming foreign key from "books" is preserved
    And the foreign key constraint still works

  Scenario: Reconstruct table with self-referential foreign key
    Given a database connection
    Given a table "employees" with columns:
      | name       | type    | nullable | references    |
      | id         | INTEGER | false    |               |
      | name       | TEXT    | false    |               |
      | manager_id | INTEGER | true     | employees.id  |
      | title      | TEXT    | true     |               |
    And the table contains hierarchical data:
      | id | name    | manager_id | title    |
      | 1  | CEO     | NULL       | Chief    |
      | 2  | Manager | 1          | Manager  |
      | 3  | Worker  | 2          | Employee |
    When I alter column "title" to be NOT NULL with default "'Staff'"
    Then the self-referential foreign key is preserved
    And all hierarchical relationships are intact
  # ============================================
  # Advanced Error Handling
  # ============================================

  Scenario: Reconstruction fails during data copy
    Given a database connection
    Given a table "users" with columns:
      | name  | type    | nullable |
      | id    | INTEGER | false    |
      | email | TEXT    | true     |
      | age   | TEXT    | true     |
    And the table contains data with incompatible types:
      | id | email          | age      |
      | 1  | alice@test.com | 30       |
      | 2  | bob@test.com   | invalid  |
    When I alter column "age" to type "INTEGER"
    Then the operation fails during data copy
    And the transaction is rolled back
    And the original table is unchanged
    And the temp table is cleaned up

  Scenario: Foreign key violation detected after reconstruction
    Given a database connection
    Given a table "authors" with columns:
      | name | type    | nullable |
      | id   | INTEGER | false    |
      | name | TEXT    | false    |
    And a table "books" with columns:
      | name      | type    | nullable | references  |
      | id        | INTEGER | false    |            |
      | title     | TEXT    | false    |            |
      | author_id | INTEGER | true     | authors.id |
    And the books table has orphaned foreign keys (data corruption):
      | id | title        | author_id |
      | 1  | Valid Book   | 1         |
      | 2  | Orphan Book  | 999       |
    When I alter column "books.title" to be NOT NULL
    Then foreign key check fails after reconstruction
    And the transaction is rolled back
    And the original table is unchanged
  # ============================================
