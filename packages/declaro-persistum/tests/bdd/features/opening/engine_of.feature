@bdd @specification
Feature: engine_of -- Which engine a target names
  One pure function. A string in, one of three words out, or a refusal naming the
  three. It opens nothing, sends nothing and asks nobody, so it can run in a test,
  in a loop, or on a target that points at a host which does not exist.

  One function, one feature. The door that uses it is `open_database`, and its own
  feature is beside this one.

  WHY IT IS SEPARATE FROM THE DOOR. Opening a database does I/O and detecting an
  engine does not. Kept in one function, every test of the decision would need a
  database, and the decision is exactly the part worth testing cheaply.

  IT MUST NEVER READ A CONNECTION, and this is not a style preference.
  `docs/architecture.md` refuses it in its own words: a driver connection does not
  say which engine it belongs to, and the closest you can get is where its class
  happens to live, which makes the SQL this package generates depend on somebody
  else's file layout. The driver reorganises its own package and your placeholders
  change, silently, with nothing in your code having moved.

  # ------------------------------------------------ what a target says

  Scenario Outline: the target names the engine
    When engine_of is given <target>
    Then it answers <engine>

    Examples:
      | target                          | engine     |
      | "app.db"                        | sqlite     |
      | "/var/lib/app/data.db"          | sqlite     |
      | "sqlite:///app.db"              | sqlite     |
      | "postgresql://localhost/todo"   | postgresql |
      | "postgres://localhost/todo"     | postgresql |
      | "libsql://team-app.turso.io"    | turso      |

  Scenario: a bare path is SQLite because nothing else is a bare path
    When engine_of is given "app.db"
    Then it answers sqlite
    And it decided on the absence of a scheme, not on the file extension

  Scenario: the extension is not what decides
    When engine_of is given "notes.sqlite3" and "notes.txt"
    Then it answers sqlite to both
    And a file that does not exist yet answers the same as one that does

  # ------------------------------------------------ what it must not do

  Scenario: it opens no connection
    When engine_of is given "postgresql://unreachable.invalid/todo"
    Then it answers postgresql
    And nothing was opened, so a host that does not exist changes no answer

  Scenario: it sends nothing over the network
    When engine_of is given every target in the table above
    Then no socket was opened and no packet was sent

  Scenario: it is pure
    When engine_of is given the same target twice
    Then both answers are the same
    And it read no clock, no random source and no environment variable

  Scenario: no function decides an engine by reading a connection
    When every function in this package that decides an engine is read
    Then none of them takes a connection object
    And none of them matches on the module a driver's class happens to live in

  # ------------------------------------------------ what it refuses, by name

  Scenario: a scheme nobody supports is refused
    When engine_of is given "mysql://localhost/todo"
    Then it is refused
    And the refusal names sqlite, postgresql and turso

  Scenario: the refusal lists the schemes, not only the engines
    When engine_of is given "mysql://localhost/todo"
    Then the refusal lists the schemes that would have worked

  Scenario: an ambiguous scheme is refused rather than guessed
    When engine_of is given "https://team-app.turso.io"
    Then it is refused as ambiguous
    And the refusal says an https URL is not enough to name an engine

  Scenario: an empty target is refused
    When engine_of is given ""
    Then it is refused
    And the refusal says a target is required

  Scenario: a refusal is a value, not a raise
    When engine_of is given "mysql://localhost/todo"
    Then what comes back is a failure a caller can read and hand on
    And nothing was written to a log
