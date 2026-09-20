@bdd @specification
Feature: refuse_unusable_settings -- A setting this engine cannot use is refused
  One door accepts the union of what three engines need, and ignoring a setting
  the detected engine has no use for is a silent default wearing a different hat:
  the caller asked for something, nothing happened, and nothing said so.

  Scenario: a busy timeout means nothing to PostgreSQL
    When a caller opens a PostgreSQL target with a busy timeout
    Then it is refused

  Scenario: a primary means nothing to SQLite
    When a caller opens a SQLite target naming a primary
    Then it is refused

  Scenario: a setting the engine uses is kept
    When a caller opens a Turso target naming a primary
    Then it is kept
