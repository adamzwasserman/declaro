@bdd @specification
Feature: The engine is detected from the target, never named by the caller
  One door. You hand it where the database is, and it works out what the database
  is. Three named openers made the caller decide the engine before writing a line,
  and that decision then sat in their code in the one place it could go stale.

  WHAT THIS BUYS: a file that opens SQLite, PostgreSQL and Turso in any order,
  interleaved, with nothing in it naming an engine. A test that runs against three
  engines stops being three code paths. A program moved from a local file to a
  primary changes one string.

  DETECTION READS THE TARGET AND NOTHING ELSE. It opens no connection, sends no
  packet and asks no server. Deciding what to open by first opening it is circular,
  and it would make choosing an engine cost a round trip.

  IT MUST NEVER READ A CONNECTION. `docs/architecture.md` refuses that in its own
  words: a driver connection does not say which engine it belongs to, and the
  closest you can get is where its class happens to live, which makes the SQL you
  generate depend on somebody else's file layout. `query/executor.py` holds a
  `detect_engine` that does exactly this, and one scenario here is about that.

  Sections "Dialects are data, not branches" and "Where the outside world gets
  touched" of docs/architecture.md.

  # ------------------------------------------------ the target decides the engine

  Scenario Outline: the target says which engine it is
    When a caller opens the target <target>
    Then the database it gets back is on <engine>
    And nothing in the call named an engine

    Examples:
      | target                             | engine     |
      | "app.db"                           | sqlite     |
      | "/var/lib/app/data.db"             | sqlite     |
      | "sqlite:///app.db"                 | sqlite     |
      | "postgresql://localhost/todo"      | postgresql |
      | "postgres://localhost/todo"        | postgresql |
      | "libsql://team-app.turso.io"       | turso      |

  Scenario: a database carries the engine that was detected
    When a caller opens the target "postgresql://localhost/todo"
    Then the database carries "postgresql" as its engine
    And that is the only place the engine is written down

  Scenario: a bare path is SQLite because nothing else is a bare path
    When a caller opens the target "app.db"
    Then it is on sqlite
    And it is decided by the absence of a scheme, not by the file extension

  Scenario: a replicated Turso is told apart by its primary
    When a caller opens the target "data.db" with the primary "libsql://team-app.turso.io"
    Then it is on turso
    And it holds one sync connection for its life

  Scenario: a local Turso names no primary
    When a caller opens the target "data.db" as turso with no primary
    Then it is on turso
    And it holds no sync connection

  # -------------------------------------------- one file, every engine, any order

  Scenario: one file opens all three engines in any order
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

  # ------------------------------------------------ what detection must not do

  Scenario: detecting the engine opens no connection
    When a caller opens the target "postgresql://unreachable.invalid/todo"
    Then the engine was decided before anything was opened
    And a target pointing at a host that does not exist still detects postgresql

  Scenario: detection sends nothing over the network
    When the engine is detected for every target in the table above
    Then no socket was opened and no packet was sent

  Scenario: detection is pure
    When the same target is detected twice
    Then both answers are the same
    And neither reads a clock, a random source or the environment

  Scenario: the engine is never worked out from a connection
    When every function that decides an engine is read
    Then none of them reads a connection object
    And none of them matches on the module a driver's class happens to live in

  Scenario: no public function takes an engine, detection included
    When the door's arguments are read
    Then it takes where the database is, and not what it is

  # ----------------------------------------------------- what it refuses, by name

  Scenario: a target nobody recognises is refused
    When a caller opens the target "mysql://localhost/todo"
    Then it is refused
    And the refusal names sqlite, postgresql and turso

  Scenario: a refusal names the schemes, not just the engines
    When a caller opens the target "mysql://localhost/todo"
    Then the refusal lists the schemes that would have worked

  Scenario: an ambiguous scheme is refused rather than guessed
    When a caller opens the target "https://team-app.turso.io"
    Then it is refused as ambiguous
    And the refusal says an https URL is not enough to name an engine

  Scenario: an empty target is refused
    When a caller opens the target ""
    Then it is refused
    And the refusal says a target is required

  # ------------------------------ a setting that means nothing to the detected engine

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
