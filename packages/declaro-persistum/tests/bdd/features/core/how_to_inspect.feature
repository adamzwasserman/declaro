@bdd @specification
Feature: how_to_inspect -- The five questions an engine answers about itself
  Reading a live schema back means asking a database what it holds, and each
  engine is asked differently. Those differences live in a table, so the reader
  takes five questions and a connection and names no engine.

  It asks so the two can be compared, never so it can decide what belongs. The
  models say what should be there.

  # ADDED 2026-10-10 AFTER A REAL OPEN REFUSED. A replicated open against a Turso
  # primary came back with "the column in 'sqlite_sequence' is a '', and this
  # package has no word of its own for that", because the tables question asked
  # for every table in the catalogue and the engine keeps four of its own there:
  # `sqlite_sequence`, `turso_cdc`, `turso_cdc_version` and an internal sequence.
  # A plain sqlite database refuses the same way as soon as one column is
  # autoincrement, which is what puts `sqlite_sequence` there.
  #
  # THE MODELS ARE NOT ALLOWED TO DECLARE ONE, which is what makes this the
  # question's business and not the differ's. `fn refuse_owned_names` already
  # refuses a model named after one of this package's own tables, so a live schema
  # carrying a table no model may declare plans a drop nobody can allow, and the
  # open stops on a table the engine put there.
  Scenario Outline: a database whose key counts up for itself still reads back
    Given a "<engine>" database whose table has a key that counts up by itself
    When its tables question is asked
    Then the answer holds the table the models declare and nothing the engine owns

    Examples:
      | engine |
      | sqlite |
      | turso  |

  # THE LOG TURSO KEEPS OF ITS OWN CHANGES. A replica carries `turso_cdc`,
  # `turso_cdc_version` and an internal sequence beside them, and they are the
  # engine's bookkeeping. This asks a local database holding one by that name,
  # because a replica is the only place the engine makes them itself.
  Scenario: the log an engine keeps of its own changes is left out
    Given a "turso" database holding a table named "turso_cdc"
    When its tables question is asked
    Then the answer holds the table the models declare and nothing the engine owns

  # POSTGRESQL KEEPS ITS OWN TABLES IN ANOTHER SCHEMA, so naming the schema is how
  # it leaves them out, and it has asked that way all along.
  Scenario: postgresql asks about one schema and not the whole server
    Given the engine "postgresql"
    Then its tables question names the schema it asks about

  Scenario: an engine gives five questions
    Given any engine
    Then the answer carries a question about its tables, its columns, its indexes, its rules and its views

  Scenario: every engine answers
    When every engine the package names is asked
    Then each one gives all five

  # SQLITE AND TURSO GIVE THE SAME FIVE, AND THAT IS A ROW RATHER THAN AN ALIAS.
  # Turso is a Rust rewrite aiming at SQLite compatibility, not a fork of it, so
  # the day its catalogue differs one row changes and nothing else does. What
  # would be wrong is all three being alike, because then the table says nothing.
  Scenario: the engines do not all ask the same way
    When every engine the package names is asked
    Then more than one set of five comes back

  # ADDED 2026-09-24, with `types`. `Inspecting` grew a sixth field that is not
  # a question: this engine's own word for each thing a column can hold, which
  # `reader fn schema_inspect` reads so the live schema comes back in this
  # package's words. Every check below walked the record field by field and
  # treated the new table as a question, and all three engines read as broken.
  # A seventh field has to be accounted for here before anything else sees it.
  Scenario: nothing is on the record that this file cannot account for
    When every engine the package names is asked
    Then the record holds the five questions and nothing else unaccounted for

  Scenario: an engine's own word for a type is on the record
    When every engine the package names is asked
    Then each one says what it calls the things a column can hold
    And every word it hands back is one this package uses

  Scenario: every question keeps its values out of its text
    When every engine the package names is asked
    Then each question carries its text and its values apart

  Scenario: a question about one table binds the table's name
    When every engine the package names is asked
    Then the questions asked per table bind the name rather than writing it in

  # THE ANSWERS HAVE TO SHARE A SHAPE OR THE READER CANNOT BE ENGINE-FREE, and
  # until 2026-09-24 they did not. Measured that day against real databases: the
  # columns question answered under `cid, name, type, notnull, dflt_value, pk`
  # on SQLite and `column_name, data_type, is_nullable, column_default` on
  # PostgreSQL, and the nullability came back as 0 and 1 on one and as 'YES' and
  # 'NO' on the other. A reader handed those would need to know which engine it
  # was talking to, which is the one thing `Inspecting` exists to prevent.
  #
  # SO EACH QUESTION NAMES ITS OWN ANSWER. The engine differences stay in this
  # table, in the SQL, where adding an engine is one more row.

  @precommit @sqlite @postgresql @turso
  Scenario: every engine answers under the same names
    When each engine is asked its five questions against a real database
    Then the answers come back under the same names on every engine

  @precommit @sqlite @postgresql @turso
  Scenario: every engine answers in the same words
    When each engine is asked its five questions against a real database
    Then a column that must hold something reads the same on every engine
    And a column that is the key reads the same on every engine
