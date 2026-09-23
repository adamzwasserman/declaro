@bdd @specification
Feature: spell_operation -- One schema change into the statements that carry it out
  The schema half's compiler. One operation in, and the statements for this
  engine out, which may be several: a SQLite table rebuild is one operation and
  many statements.

  # A SCENARIO HERE SAYS WHAT COMES BACK, NEVER HOW IT WAS BUILT. Twenty-four
  # went on 2026-09-21 because each named a private helper of the spelling code.
  # Twenty-six more were rewritten or deleted the same day, for the same reason
  # wearing other clothes: they named a helper, counted the statements, pinned
  # the shape of a name no caller sees, demanded a session setting, or asked an
  # engine to enforce something SQLite has never enforced. What each was
  # reaching for is below, said as a result a caller could see.
  # ============================================
  # One column, spelled for one engine
  # ============================================

  @precommit
  Scenario Outline: a declared column reaches the statement in the engine's own words
    When a column "<column>" declared "<type>" defaulting to <default> is added on <engine>
    Then the statement carries the text <fragment>

    Examples:
      | column | type     | default      | engine     | fragment        |
      | id     | uuid     | nothing      | sqlite     | "id" TEXT       |
      | id     | uuid     | nothing      | postgresql | "id" uuid       |
      | name   | text     | nothing      | sqlite     | "name" TEXT     |
      | hits   | integer  | the number 0 | sqlite     | DEFAULT 0       |
      | made   | datetime | (now())      | postgresql | DEFAULT (now()) |

  @precommit
  Scenario: two declared types one engine stores alike become one column
    When a column "id" declared "uuid" defaulting to nothing is added on sqlite
    And a column "id" declared "text" defaulting to nothing is added on sqlite
    Then the two statements are the same

  @precommit
  Scenario: a declared type is spelled the way one engine writes it
    When a column "settled" declared "boolean" defaulting to nothing is added on postgresql
    Then the statement writes it as that engine's own boolean

  @precommit
  Scenario: a column added on PostgreSQL lands on the table already there
    When a column "settled_on" declared "date" defaulting to nothing is added on postgresql
    Then the column arrives, and the table it lands on is never dropped

  @precommit
  Scenario: a column for an engine this package does not ship is refused
    When a column "settled_on" declared "date" defaulting to nothing is added on an engine this package does not ship
    Then it is refused, naming the engines it has

  @precommit
  Scenario: an operation this engine cannot express is refused
    When an operation no engine table covers is spelled
    Then a fault comes back naming the operation and the engine

  @precommit @sqlite
  Scenario: a declared check reaches the CREATE TABLE as a rule with a name
    When a table is created with a check "ck_positive" that "id" must be greater than 0
    Then the CREATE TABLE names the check "ck_positive"
    And the engine refuses a row whose "id" is 0
  # ============================================
  # Every kind of rule a model may declare
  # ============================================
  #
  # A RULE IS A CONSTRAINT AND NOTHING ELSE. Until 2026-09-23 a column carried
  # a bag of facets keyed by strings, so a key nobody wrote a clause for was
  # spelled as silence, and `spell_operation` had to check for unknown keys
  # before writing a definition. The kinds are a declared set now, `Constraint`
  # carries the one value a kind may need, and a kind with no clause is refused
  # by name.

  @precommit
  Scenario Outline: every declared kind reaches the statement in the engine's own words
    When a table is created with a <kind> rule on "code" allowing <value>
    Then the statement carries the text <fragment>

    Examples:
      | kind        | value  | fragment     |
      | not_null    | -      | NOT NULL     |
      | unique      | -      | UNIQUE       |
      | primary_key | -      | PRIMARY KEY  |
      | length      | 8      | length       |

  @precommit
  Scenario: a declared rule of a kind this package writes no clause for is refused
    When a table is created with a rule of a kind no clause covers
    Then it is refused, naming the kinds it writes

  @precommit @sqlite
  Scenario: the engine holds a row to the length that was declared
    Given a table "parts" whose "code" may hold 8 characters
    Then the engine accepts a row whose "code" is 8 characters
    And the engine refuses a row whose "code" is 9 characters

  @precommit @sqlite
  Scenario: the engine holds a row to a column that must hold something
    Given a table "parts" whose "code" must hold something
    Then the engine refuses a row that leaves "code" empty
  # ============================================
  # What goes in when a write names no value
  # ============================================
  #
  # A DEFAULT IS THE ONE RULE THAT RULES NOTHING OUT. Every value the column's
  # type permits is still permitted with the default in place. What it does is
  # supply a value in the one case where the writer supplied none, which is why
  # it is the kind that needed `Constraint` to carry a value at all.

  @precommit @sqlite
  Scenario: a row written without the column gets the value that was declared
    Given a table "parts" whose "status" starts at "new"
    When a row is written naming only "code"
    Then the row's "status" reads "new"

  @precommit @sqlite
  Scenario: a row that names the column keeps what it named
    Given a table "parts" whose "status" starts at "new"
    When a row is written naming "code" and a "status" of "shipped"
    Then the row's "status" reads "shipped"

  @precommit
  Scenario: a default this package cannot write into a clause is refused
    When a table is created whose "status" starts at a value no engine can be told
    Then it is refused, saying what a default may be
  # ============================================
  # A value the statement does not carry
  # ============================================
  #
  # EVERY VALUE IS BOUND. The text carries the mark its engine reads and the
  # value travels beside it, so a value carrying a quote never reaches the text
  # at all and there is nothing in it to escape.

  @precommit
  Scenario Outline: a value travels beside the statement, marked the way its engine reads
    When "shipped" is added to the statuses on <engine>
    Then the value is marked "<mark>" and travels beside the statement

    Examples:
      | engine     | mark |
      | sqlite     | ?    |
      | postgresql | $1   |

  @precommit
  Scenario: a value for an engine this package does not ship is refused
    When "shipped" is added to the statuses on an engine this package does not ship
    Then it is refused, naming the engines it has

  @precommit @sqlite
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
  # ------------------------------------------------------------------
  # FOLDED IN FROM `table_reconstruction.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # As a developer using declaro-persistum with SQLite/Turso
  # I want ALTER COLUMN operations to work via table reconstruction
  # So that I can modify column properties despite SQLite's limitations

  Scenario: Dropping a column on SQLite keeps the rows and the indexes
    Given a database connection
    Given a table "invoices" with columns:
      | name       | type    | nullable |
      | id         | INTEGER | false    |
      | memo       | TEXT    | true     |
      | settled_on | TEXT    | true     |
    And an index "idx_settled" on column "settled_on"
    And the table contains data:
      | id | memo   | settled_on |
      | 1  | first  | 2026-01-01 |
      | 2  | second | 2026-02-02 |
    When I drop column "memo"
    Then the column "memo" does not exist
    And all other data is preserved
    And the index "idx_settled" still exists

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
    And the original table is unchanged
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

  Scenario: A UNIQUE column stays unique through a rebuild
    Given a database connection
    Given a table "items" with columns:
      | name | type | nullable | unique |
      | id   | TEXT | false    | false  |
      | code | TEXT | false    | true   |
    When I alter column "id" nullability
    Then the UNIQUE constraint on "code" is preserved
  # ============================================
  # Transaction Safety
  # ============================================

  Scenario: A rebuild that cannot copy the rows leaves the table as it was
    Given a database connection
    Given a table "data" with columns:
      | name  | type | nullable |
      | id    | TEXT | false    |
      | value | TEXT | true     |
    And the table contains data
    When reconstruction fails during data copy
    Then the original table is unchanged
    And the original data is intact
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
    And the change came back spelled, not refused

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
    And the original table is unchanged

  Scenario: Drop simple column without constraints
    Given a database connection
    Given a table "products" with columns:
      | name        | type | nullable |
      | id          | TEXT | false    |
      | name        | TEXT | false    |
      | description | TEXT | true     |
      | notes       | TEXT | true     |
    When I drop column "notes"
    Then the column "notes" does not exist
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
    And the original table is unchanged
  # ============================================
