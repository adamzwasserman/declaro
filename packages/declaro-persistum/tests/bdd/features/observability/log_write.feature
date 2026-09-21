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
  # FOLDED IN FROM `reporting/say.feature` on 2026-09-21, rewritten as results
  # on the same day. What that file said about why:
  #
  # This is the only place in the package that writes a line, so everything a
  # caller hands it has to arrive at the standard library intact, and anything
  # it cannot carry has to be refused rather than quietly turned into text.
  #
  # THE DEFECT IT EXISTS FOR. The old writer rendered every keyword as
  # "name=value" and appended it to the message. The standard library's own
  # keyword for "attach the exception being handled" is not a fact, so a push
  # failure meaning to record its cause wrote those words as text and threw the
  # exception away. Two call sites did it and neither ever carried a cause. The
  # comment above them said the opposite.

  Scenario: a line is written at the level the caller named
    When a warning is written saying "the disk filled"
    Then the line reads "the disk filled" and its level is warning

  Scenario: a level nobody declared is refused
    When a line is written at a level the vocabulary does not hold
    Then it is refused, naming the levels there are

  Scenario: a keyword belonging to the standard library is refused as a fact
    When a line is written with a fact named for the standard library's own keyword
    Then it is refused, naming that keyword

  Scenario: a cause is carried as an exception, not as words
    Given a failure carrying a cause
    When a line is written with that cause
    Then the record carries the cause itself, and the message does not mention it

  Scenario: a line carrying a cause still carries its facts
    Given a failure carrying a cause
    When a line is written with that cause and one fact
    Then the line reads the message and the fact, and the record carries the cause

  Scenario: a line with no cause carries none
    When a line is written with no cause
    Then the record carries no exception
