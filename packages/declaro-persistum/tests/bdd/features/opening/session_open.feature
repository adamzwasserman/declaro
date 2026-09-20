@bdd @specification
Feature: open_database -- One door, and the target says what is behind it
  You hand it where the database is. It works out what the database is, opens it,
  and gives you back a `Database` carrying the engine it found. Nothing in the
  call names an engine.

  One function, one feature. The decision it rests on is `engine_of`, and its own
  feature is beside this one.

  WHAT THIS BUYS. A file that opens SQLite, PostgreSQL and Turso in any order,
  interleaved, with no engine named anywhere in it. A test across three engines
  stops being three code paths. Moving a program from a local file to a primary
  changes one string.

  WHAT IT REPLACES. `open_sqlite`, `open_postgresql` and `open_turso` each stamped
  a constant, so the caller decided the engine before writing a line and that
  decision then lived in their code, in the one place it could go stale against the
  target beside it.

  A SETTING THAT MEANS NOTHING TO THE ENGINE IS REFUSED, NOT IGNORED. One door has
  to accept the union of what three engines need, and `busy_timeout_s` means
  nothing to PostgreSQL while `primary` means nothing to SQLite. Ignoring one is a
  silent default wearing a different hat: the caller asked for something, nothing
  happened, and nothing said so.

  # -------------------------------------------------- the engine comes back with it

  Scenario: the database carries the engine that was detected
    When a caller opens "postgresql://localhost/todo"
    Then the database carries postgresql as its engine
    And that is the only place the engine is written down

  Scenario: nothing downstream works the engine out again
    Given a database opened from "app.db"
    When a caller asks it for every Person
    Then the engine came from the database, not from the connection it used

  Scenario: a replicated Turso is told apart by its primary
    When a caller opens "data.db" with the primary "libsql://team-app.turso.io"
    Then it is on turso
    And it holds one sync connection for its life

  Scenario: a local Turso names no primary
    When a caller opens "data.db" as turso with no primary
    Then it is on turso
    And it holds no sync connection

  # -------------------------------------------- one file, every engine, any order

  Scenario: one file opens all three engines
    When a caller opens "app.db", then "postgresql://localhost/todo", then "libsql://team-app.turso.io"
    Then three databases come back, on sqlite, postgresql and turso
    And the file that opened them names no engine anywhere

  Scenario: the order they are opened in changes nothing
    When the same three targets are opened in a different order
    Then each database is on the same engine it was before
    And no database's engine depends on which was opened first

  Scenario: two databases on different engines are used in one function
    Given a database opened from "app.db" and a database opened from "postgresql://localhost/todo"
    When a caller asks each of them for every Person
    Then each answers from its own database
    And neither call named an engine

  Scenario: the same target opened twice gives two independent databases
    When a caller opens "app.db" twice
    Then it gets two databases
    And closing one does not close the other

  # ------------------------------------------------------- what the door takes

  Scenario: it takes where the database is, not what it is
    When the door's arguments are read
    Then none of them is an engine or a scheme
    And the only thing that says which engine to use is the target

  Scenario: a setting the detected engine has no use for is refused
    When a caller opens "postgresql://localhost/todo" with a busy timeout
    Then it is refused
    And the refusal says postgresql has no busy timeout

  Scenario: a primary given for an engine that cannot replicate is refused
    When a caller opens "app.db" with a primary
    Then it is refused
    And the refusal says sqlite has no primary to replicate to

  Scenario: a setting every engine needs is still required
    When a caller opens "app.db" and says nothing about shutdown
    Then it is refused
    And the refusal says shutdown has no default, on any engine

  # ------------------------------------------------------- what it refuses to open

  Scenario: a target no engine claims is refused before anything is opened
    When a caller opens "mysql://localhost/todo"
    Then it is refused
    And no file was created and no connection was attempted

  Scenario: a target that detects but cannot be reached fails at opening, not at detecting
    When a caller opens "postgresql://unreachable.invalid/todo"
    Then the engine was decided first, and postgresql
    And what failed is the connection, and the failure says so
