@bdd @specification
Feature: log_write -- What an operator reads afterwards
  `docs/architecture.md:114`: a failure comes back as a value, never as a log
  line. A function that logs an error and hands you something you cannot tell
  from success has two outputs and only one of them is in the signature.

  Scenario: a failure is returned, not logged
    When a function cannot do its job
    Then the fault came back to its caller

  Scenario: what is logged is what nobody is waiting on
    Given a write whose caller abandoned its ticket
    When it fails
    Then the failure is logged, because there is nobody left to return it to

  Scenario: a pure function never reaches this
    When the pure core runs
    Then nothing in it wrote a line

  # ------------------------------------------------------------------
  # FOLDED IN FROM `reporting/say.feature` on 2026-09-21, one feature
  # per function. What that file said about why:
  # `say` is the only function in this package that writes a line. Everything a
  # caller hands it has to arrive at the standard library intact, and anything it
  # cannot carry has to be refused rather than quietly turned into text.
  # THE DEFECT THIS FEATURE EXISTS FOR. `say` rendered every keyword as
  # "name=value" and appended it to the message. `exc_info=True` is not a fact,
  # it is the standard library's own keyword for "attach the exception being
  # handled". So a push failure that meant to record its cause wrote the literal
  # words "exc_info=True" and threw the exception away. The comment above the
  # call site said the opposite: "exc_info=True keeps the type and the
  # traceback." Two call sites did this and neither ever carried a cause.

  Scenario: a line is written at the level the caller named
    When say writes "the disk filled" at warning
    Then a warning was written saying "the disk filled"

  Scenario: named facts are written beside the message
    When say writes "the disk filled" at warning with the fact path="/tmp/a.db"
    Then the line written reads "the disk filled path=/tmp/a.db"

  Scenario: a level nobody declared is refused
    When say writes "the disk filled" at catastrophe
    Then it refuses

  Scenario: a keyword belonging to the standard library is refused as a fact
    When say writes "the push failed" at warning with the fact exc_info=True
    Then it refuses and names "exc_info"

  Scenario: say_because carries the exception, not a rendering of it
    Given a ValueError reading "no such column"
    When say_because writes "the push failed" at warning with that cause
    Then the record carries that ValueError as the exception

  Scenario: say_because still writes the message and its facts
    Given a ValueError reading "no such column"
    When say_because writes "the push failed" at warning with that cause and the fact path="/tmp/a.db"
    Then the line written reads "the push failed path=/tmp/a.db"

  Scenario: say attaches no exception at all
    When say writes "the disk filled" at warning
    Then the record carries no exception
