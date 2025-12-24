Feature: PRAGMA Compatibility for Turso Database (Rust)
  As a developer using declaro-persistum with Turso Database
  I want PRAGMA commands to work even when not natively supported
  So that schema introspection works reliably

  Background:
    Given a database connection

  # ============================================
  # Core Functionality - Native Pass-through
  # ============================================

  Scenario: table_info works natively on Turso
    Given a Turso Database connection
    And a table "users" with columns "id INTEGER PRIMARY KEY, email TEXT, name TEXT"
    When I call pragma_table_info for table "users"
    Then I receive 3 column definitions
    And no emulation was triggered

  Scenario: table_info works natively on SQLite
    Given a SQLite connection
    And a table "users" with columns "id INTEGER PRIMARY KEY, email TEXT"
    When I call pragma_table_info for table "users"
    Then I receive 2 column definitions
    And no emulation was triggered

  # ============================================
  # index_list Emulation
  # ============================================

  Scenario: index_list falls back to emulation on Turso
    Given a Turso Database connection
    And the native PRAGMA index_list is not supported
    And a table "users" with an index "idx_email" on column "email"
    When I call pragma_index_list for table "users"
    Then I receive index list from sqlite_master parsing
    And emulation was logged for monitoring
    And the result contains index "idx_email"

  Scenario: index_list excludes sqlite_autoindex entries
    Given a table with a UNIQUE constraint creating sqlite_autoindex
    When I call pragma_index_list
    Then sqlite_autoindex entries have origin "u" not "c"

  Scenario: index_list identifies partial indexes
    Given a table "events" with columns "id INTEGER, status TEXT, created_at TEXT"
    And an index "idx_active" on "created_at" with WHERE clause "status = 'active'"
    When I call pragma_index_list for table "events"
    Then the index "idx_active" has partial flag set to 1

  Scenario: index_list distinguishes unique constraint from unique index
    Given a table with column "email TEXT UNIQUE"
    And a separate "CREATE UNIQUE INDEX idx_name ON t(name)"
    When I call pragma_index_list
    Then the constraint index has origin "u"
    And the explicit index has origin "c"

  Scenario: index_list identifies primary key indexes
    Given a table "items" with "a INTEGER, b INTEGER, PRIMARY KEY(a, b)"
    When I call pragma_index_list for table "items"
    Then there is an index with origin "pk"

  # ============================================
  # index_info Emulation
  # ============================================

  Scenario: index_info falls back to emulation on Turso
    Given a Turso Database connection
    And the native PRAGMA index_info is not supported
    And a table "users" with an index "idx_email" on column "email"
    When I call pragma_index_info for index "idx_email"
    Then I receive index column info from sqlite_master parsing
    And the result shows column "email" at seqno 0

  Scenario: index_info handles multi-column indexes
    Given a table "orders" with columns "id, user_id, status, created_at"
    And an index "idx_user_status" on columns "user_id, status"
    When I call pragma_index_info for index "idx_user_status"
    Then I receive 2 rows
    And row 0 has seqno 0 and name "user_id"
    And row 1 has seqno 1 and name "status"

  Scenario: index_info handles expression indexes
    Given a table "users" with column "email TEXT"
    And an index "idx_email_lower" defined as "CREATE INDEX idx_email_lower ON users(lower(email))"
    When I call pragma_index_info for index "idx_email_lower"
    Then the column name indicates an expression

  Scenario: index_info handles DESC ordering in index
    Given a table "events" with column "created_at TEXT"
    And an index "idx_recent" defined as "CREATE INDEX idx_recent ON events(created_at DESC)"
    When I call pragma_index_info for index "idx_recent"
    Then the result captures the column "created_at"

  Scenario: index_info handles COLLATE in index
    Given a table "names" with column "name TEXT"
    And an index "idx_nocase" defined as "CREATE INDEX idx_nocase ON names(name COLLATE NOCASE)"
    When I call pragma_index_info for index "idx_nocase"
    Then the result captures the column "name"

  # ============================================
  # foreign_key_list Emulation
  # ============================================

  Scenario: foreign_key_list falls back to emulation on Turso
    Given a Turso Database connection
    And the native PRAGMA foreign_key_list is not supported
    And a table "orders" with "user_id INTEGER REFERENCES users(id)"
    When I call pragma_foreign_key_list for table "orders"
    Then I receive FK info parsed from CREATE TABLE statement
    And the result shows from="user_id", table="users", to="id"

  Scenario: foreign_key_list parses inline FK syntax
    Given a table defined as "CREATE TABLE orders(id INTEGER, user_id INTEGER REFERENCES users(id))"
    When I call pragma_foreign_key_list for table "orders"
    Then I receive 1 foreign key
    And it has from="user_id", table="users", to="id"

  Scenario: foreign_key_list parses table-level FK syntax
    Given a table defined as:
      """
      CREATE TABLE order_items(
        order_id INTEGER,
        product_id INTEGER,
        FOREIGN KEY(order_id, product_id) REFERENCES catalog(order_ref, product_ref)
      )
      """
    When I call pragma_foreign_key_list for table "order_items"
    Then I receive a composite FK with 2 column pairs
    And seq 0 has from="order_id", to="order_ref"
    And seq 1 has from="product_id", to="product_ref"

  Scenario: foreign_key_list handles ON DELETE CASCADE
    Given a table "posts" with "author_id INTEGER REFERENCES users(id) ON DELETE CASCADE"
    When I call pragma_foreign_key_list for table "posts"
    Then the FK has on_delete="CASCADE"

  Scenario: foreign_key_list handles ON UPDATE SET NULL
    Given a table "posts" with "author_id INTEGER REFERENCES users(id) ON UPDATE SET NULL"
    When I call pragma_foreign_key_list for table "posts"
    Then the FK has on_update="SET NULL"

  Scenario: foreign_key_list handles ON DELETE SET DEFAULT
    Given a table "posts" with "status_id INTEGER REFERENCES statuses(id) ON DELETE SET DEFAULT"
    When I call pragma_foreign_key_list for table "posts"
    Then the FK has on_delete="SET DEFAULT"

  Scenario: foreign_key_list handles ON DELETE RESTRICT
    Given a table "posts" with "cat_id INTEGER REFERENCES categories(id) ON DELETE RESTRICT"
    When I call pragma_foreign_key_list for table "posts"
    Then the FK has on_delete="RESTRICT"

  Scenario: foreign_key_list defaults missing actions to NO ACTION
    Given a table "posts" with "author_id INTEGER REFERENCES users(id)"
    When I call pragma_foreign_key_list for table "posts"
    Then the FK has on_delete="NO ACTION"
    And the FK has on_update="NO ACTION"

  Scenario: foreign_key_list handles both ON DELETE and ON UPDATE
    Given a table with "fk INTEGER REFERENCES t(id) ON DELETE CASCADE ON UPDATE SET NULL"
    When I call pragma_foreign_key_list
    Then the FK has on_delete="CASCADE"
    And the FK has on_update="SET NULL"

  Scenario: foreign_key_list handles quoted identifiers
    Given a table defined as:
      """
      CREATE TABLE "My Orders"(
        "order id" INTEGER,
        "user id" INTEGER REFERENCES "My Users"("user id")
      )
      """
    When I call pragma_foreign_key_list for table "My Orders"
    Then identifiers are correctly unquoted in the result
    And from="user id", table="My Users", to="user id"

  Scenario: foreign_key_list parsing is case insensitive for SQL keywords
    Given a table defined as "CREATE TABLE t(fk integer references Other(Id) on delete cascade)"
    When I call pragma_foreign_key_list for table "t"
    Then parsing succeeds
    And the FK has on_delete="CASCADE"

  Scenario: foreign_key_list handles multiple FKs in one table
    Given a table defined as:
      """
      CREATE TABLE posts(
        id INTEGER PRIMARY KEY,
        author_id INTEGER REFERENCES users(id),
        category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL
      )
      """
    When I call pragma_foreign_key_list for table "posts"
    Then I receive 2 foreign keys
    And one has from="author_id", table="users"
    And one has from="category_id", table="categories", on_delete="SET NULL"

  # ============================================
  # Format Compatibility with Native SQLite
  # ============================================

  Scenario: Emulated index_list matches native SQLite format
    Given a SQLite connection with native PRAGMA support
    And a table "test" with various indexes
    When I get native PRAGMA index_list output
    And I get emulated PRAGMA index_list output for same table
    Then both outputs have same number of rows
    And each row has columns: seq, name, unique, origin, partial
    And values match for each row

  Scenario: Emulated index_info matches native SQLite format
    Given a SQLite connection with native PRAGMA support
    And an index "idx_test" on multiple columns
    When I get native PRAGMA index_info output
    And I get emulated PRAGMA index_info output for same index
    Then both outputs have same number of rows
    And each row has columns: seqno, cid, name
    And values match for each row

  Scenario: Emulated foreign_key_list matches native SQLite format
    Given a SQLite connection with native PRAGMA support
    And a table with foreign keys
    When I get native PRAGMA foreign_key_list output
    And I get emulated PRAGMA foreign_key_list output for same table
    Then both outputs have same number of rows
    And each row has columns: id, seq, table, from, to, on_update, on_delete, match
    And values match for each row

  # ============================================
  # Monitoring and Observability
  # ============================================

  Scenario: Emulation usage is logged at INFO level
    Given logging is configured to capture INFO
    When pragma_index_list falls back to emulation for table "users"
    Then a log entry is created at INFO level
    And the log includes "index_list" and "users"
    And the log indicates emulation was used

  Scenario: Emulation counter increments on each use
    Given emulation counters are reset
    When I call pragma_index_list with emulation 3 times
    Then the emulation_count for "index_list" is 3

  Scenario: Native success is detected and logged as WARNING
    Given pragma_index_list previously required emulation
    When native PRAGMA index_list succeeds unexpectedly
    Then a log entry is created at WARNING level
    And the log indicates "native PRAGMA now supported"
    And the native_success counter increments

  Scenario: Monitoring tracks affected tables
    When pragma_foreign_key_list emulates for tables "orders", "posts", "comments"
    Then monitoring shows 3 distinct tables affected

  # ============================================
  # Error Handling
  # ============================================

  Scenario: Emulation handles table not found gracefully
    Given a Turso Database connection
    When I call pragma_index_list for non-existent table "ghost"
    Then I receive an empty result
    And no exception is raised

  Scenario: Emulation handles index not found gracefully
    Given a Turso Database connection
    When I call pragma_index_info for non-existent index "ghost_idx"
    Then I receive an empty result
    And no exception is raised

  Scenario: Emulation handles malformed CREATE TABLE gracefully
    Given a table with unusual but valid SQL syntax
    When I call pragma_foreign_key_list
    Then parsing attempts best-effort extraction
    And any unparseable FKs are logged as warnings

  # ============================================
  # Connection Type Detection
  # ============================================

  Scenario: Abstraction auto-detects Turso vs SQLite connection
    Given both Turso and SQLite connections available
    When I call pragma_index_list on Turso connection
    Then emulation is used
    When I call pragma_index_list on SQLite connection
    Then native PRAGMA is used
