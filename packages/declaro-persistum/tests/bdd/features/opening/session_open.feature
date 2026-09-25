@bdd @specification
Feature: session_open -- One door, and the target says what is behind it
  You hand it where the database is. It works out what the database is, opens it,
  brings it up to your models, and gives you back a `Database` carrying the engine
  it found. Nothing in the call names an engine.

  One function, one feature. The decision it rests on is `engine_of`, and its own
  feature is beside this one. Everything after that decision is
  `orchestrator fn session_connect`, which has its own feature too. What is here
  is what a caller hands over and what comes back.

  WHAT THIS BUYS. A file that opens SQLite, PostgreSQL and Turso in any order,
  interleaved, with no engine named anywhere in it. A test across three engines
  stops being three code paths. Moving a program from a local file to a primary
  changes one string.

  WHAT IT REPLACES. `open_sqlite`, `open_postgresql` and `open_turso` each stamped
  a constant, so the caller decided the engine before writing a line and that
  decision then lived in their code, in the one place it could go stale against the
  target beside it.

  # REWRITTEN 2026-09-25 against `boundary_in fn session_open`, which takes a
  # target, a shutdown, a primary and a token, and reads DECLARO_CONNECTIONS and
  # DECLARO_CREW_SIZE off the environment. Four scenarios named arguments the
  # door does not have and one named a door that does not exist. What each of
  # them said, and what happened to it, is written beside the scenario that
  # replaced it.

  # -------------------------------------------------- the engine comes back with it

  Scenario: the database carries the engine that was detected
    When a caller opens "postgresql://localhost/todo"
    Then the database carries postgresql as its engine
    And that is the only place the engine is written down

  # REWORDED 2026-09-25. It asked the database for every Person, which is
  # `boundary_in fn query_ask` rather than this door. What this scenario is
  # about is where the engine is written down, and that is readable off the
  # handle without asking a question.
  Scenario: nothing reads the engine back off a connection
    Given a database opened from "app.db"
    Then the handle names sqlite
    And no connection it holds was asked what engine it is

  Scenario: a replicated Turso is told apart by its primary
    When a caller opens "libsql://team-app.turso.io" with a primary
    Then it is on turso
    And it holds one channel for its life

  # REWORDED 2026-09-25, and it names a gap rather than a behaviour. `fn
  # engine_of` reads a bare path as sqlite, because what makes a path SQLite is
  # that it names no scheme. So no target spells "a Turso database on this disk
  # with no primary", although `docs/architecture.md` says that deployment
  # exists. Until a target can say it, this scenario cannot pass, and it is here
  # rather than deleted so the gap is one somebody can read.
  Scenario: a local Turso names no primary
    When a caller opens a local Turso with no primary
    Then it is on turso
    And it holds no channel

  # -------------------------------------------- one file, every engine, any order

  Scenario: one file opens all three engines
    When a caller opens "app.db", then "postgresql://localhost/todo", then "libsql://team-app.turso.io"
    Then three databases come back, on sqlite, postgresql and turso
    And the file that opened them names no engine anywhere

  Scenario: the order they are opened in changes nothing
    When the same three targets are opened in a different order
    Then each database is on the same engine it was before
    And no database's engine depends on which was opened first

  # REWORDED 2026-09-25. It asked each database for every Person, which is
  # another door's work. Two handles carrying two engines is what this door can
  # be held to.
  Scenario: two databases on different engines come back from one door
    When a caller opens "app.db" and "postgresql://localhost/todo"
    Then each handle carries its own engine
    And neither call named an engine

  # REWORDED 2026-09-25. It said closing one does not close the other, and there
  # is no door in the declaration a caller closes a database at. What a caller
  # can see is that the two handles share nothing: their own connections, their
  # own writers, their own crew.
  Scenario: the same target opened twice gives two independent databases
    When a caller opens "app.db" twice
    Then it gets two databases
    And they share no connection, no writer and no crew

  # ------------------------------------------------------- what the door takes

  Scenario: it takes where the database is, not what it is
    When the door's arguments are read
    Then none of them is an engine or a scheme
    And the only thing that says which engine to use is the target

  # REWRITTEN 2026-09-25. It handed PostgreSQL a busy timeout, and the door
  # takes no busy timeout: its four arguments are the target, the shutdown, the
  # primary and the token. The primary is the one setting a caller can hand over
  # that an engine may have no use for, so that is what this asks about now.
  Scenario: a primary given for an engine that cannot replicate is refused
    When a caller opens "app.db" with a primary
    Then it is refused
    And the refusal names sqlite and says it has nothing to replicate to
    And no connection was opened

  # REWORDED 2026-09-25. It said "it is refused", and what refuses a call with
  # no shutdown is Python itself: the argument has no default, so the call never
  # runs. That is the mechanism rather than a check somebody wrote, and it is
  # stronger for it, so the scenario says what actually happens.
  Scenario: a setting every engine needs is still required
    When a caller opens "app.db" and says nothing about shutdown
    Then the call never runs
    And what stopped it names shutdown

  # ------------------------------------------------------- what it refuses to open

  Scenario: a target no engine claims is refused before anything is opened
    When a caller opens "mysql://localhost/todo"
    Then it is refused
    And no file was created and no connection was attempted

  Scenario: a target that detects but cannot be reached fails at opening, not at detecting
    When a caller opens "postgresql://unreachable.invalid/todo"
    Then the engine was decided first, and postgresql
    And what failed is the connection, and the failure says so

  # DELETED 2026-09-25: "the budget has no default". It said the retry budget is
  # part of the declaration that opens a database, and the door has no budget
  # argument. `fn budget_spent` is reached from the crew, which reads how many
  # attempts a write has had rather than a number a caller handed the opener.

  # REWRITTEN 2026-09-25. It said the connection bound is stated where the
  # database is opened. It is stated by the deployment instead:
  # `boundary_in fn session_open` declares that it reads DECLARO_CONNECTIONS and
  # DECLARO_CREW_SIZE, because the right number is a property of the machine and
  # not of the call. What survives from the old scenario is the part that
  # matters, which is that nothing fills it in.
  Scenario: the deployment states how many connections, and nothing fills it in
    When a database is opened on a machine that says nothing about connections
    Then it is refused
    And the refusal says the number is a property of the machine

  Scenario: the deployment states how big the crew is, and nothing fills that in either
    When a database is opened on a machine that says nothing about the crew
    Then it is refused
    And the refusal names the crew rather than the connections
