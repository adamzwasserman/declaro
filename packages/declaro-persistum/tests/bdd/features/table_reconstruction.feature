Feature: Table Reconstruction for SQLite/Turso ALTER COLUMN
  As a developer using declaro-persistum with SQLite/Turso
  I want ALTER COLUMN operations to work via table reconstruction
  So that I can modify column properties despite SQLite's limitations

  Background:
    Given a database connection

  # ============================================
  # Core Functionality - Change Column Nullability
  # ============================================

  Scenario: Change column from nullable to NOT NULL
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
    Given a table "settings" with columns:
      | name   | type | nullable | default |
      | id     | TEXT | false    |         |
      | status | TEXT | true     |         |
    When I alter column "status" to have default "'active'"
    Then the table schema shows "status" with default "'active'"
    And new inserts use the default value

  Scenario: Change existing default value
    Given a table "accounts" with columns:
      | name    | type | nullable | default  |
      | id      | TEXT | false    |          |
      | balance | TEXT | true     | '0'      |
    When I alter column "balance" to have default "'100'"
    Then the table schema shows "balance" with default "'100'"

  Scenario: Remove default value from column
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
    Given tables with foreign key relationships
    And data that satisfies the foreign key constraints
    When I reconstruct a table with column changes
    Then foreign key checks pass after reconstruction
    And the foreign key relationships still work

  # ============================================
  # Index Preservation
  # ============================================

  Scenario: Preserve indexes during reconstruction
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
    Given a table "data" with columns:
      | name  | type | nullable |
      | id    | TEXT | false    |
      | value | TEXT | true     |
    And the table contains data
    When reconstruction fails during data copy
    Then the transaction is rolled back
    And the original table is unchanged
    And the original data is intact

  Scenario: Re-enable foreign keys after reconstruction
    Given foreign keys are enabled
    And a table requiring reconstruction
    When I perform table reconstruction
    Then foreign keys are temporarily disabled
    And foreign keys are re-enabled after reconstruction
    And foreign key checks are performed

  # ============================================
  # Edge Cases
  # ============================================

  Scenario: Reconstruct table with no common columns
    Given a table "legacy" with columns:
      | name    | type | nullable |
      | old_id  | TEXT | false    |
      | old_val | TEXT | true     |
    When I reconstruct with entirely new column names
    Then the table is recreated
    And no data is copied (no common columns)
    And a warning is logged

  Scenario: Reconstruct empty table
    Given a table "empty_table" with columns:
      | name | type | nullable |
      | id   | TEXT | false    |
      | data | TEXT | true     |
    And the table has 0 rows
    When I alter column "data" to be NOT NULL
    Then the table is reconstructed successfully
    And the table remains empty

  Scenario: Reconstruct table with composite primary key
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
  # Sequential Reconstructions
  # ============================================

  Scenario: Multiple sequential reconstructions of same table
    Given a table "config" with columns:
      | name  | type | nullable |
      | id    | TEXT | false    |
      | key   | TEXT | false    |
      | value | TEXT | true     |
    And the table contains data:
      | id | key      | value   |
      | 1  | setting1 | value1  |
      | 2  | setting2 | value2  |
    When I perform the following operations in sequence:
      | operation     | column | detail           |
      | alter_column  | value  | nullable=false   |
      | alter_column  | value  | type=INTEGER     |
      | alter_column  | key    | unique=true      |
    Then each operation uses fresh introspection
    And all data is preserved after all operations
    And the final schema matches expected state

  Scenario: Reconstruction followed by FK add
    Given a table "users" with columns:
      | name  | type | nullable |
      | id    | TEXT | false    |
      | email | TEXT | true     |
    And a table "posts" with columns:
      | name    | type | nullable |
      | id      | TEXT | false    |
      | title   | TEXT | false    |
      | user_id | TEXT | true     |
    When I alter column "users.email" to be NOT NULL
    And I add foreign key on "posts.user_id" referencing "users.id"
    Then both operations complete successfully
    And all constraints are enforced

  # ============================================
  # Complex Foreign Key Scenarios
  # ============================================

  Scenario: Reconstruct table that has incoming foreign keys
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
  # Dialect-Specific Behavior
  # ============================================

  Scenario: SQLite applier chooses reconstruction vs direct operation
    Given a SQLite database connection
    And a table "users" with columns:
      | name  | type | nullable |
      | id    | TEXT | false    |
      | email | TEXT | true     |
      | notes | TEXT | true     |
    When I request to drop column "notes"
    Then the SQLite applier uses direct ALTER TABLE DROP COLUMN
    But when I request to add a foreign key
    Then the SQLite applier uses table reconstruction

  Scenario: Turso applier uses reconstruction for all constrained operations
    Given a Turso database connection
    And a table "users" with columns:
      | name  | type | nullable |
      | id    | TEXT | false    |
      | email | TEXT | true     |
    When I request any ALTER COLUMN operation
    Then the Turso applier uses table reconstruction
    And the operation completes successfully
