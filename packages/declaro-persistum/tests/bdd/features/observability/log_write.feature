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
  #
  # FOUR OF THOSE SCENARIOS CHANGED ON 2026-09-21: three were restated and are
  # below, and one was deleted. They described Python's logging module: a
  # reserved keyword, an
  # exception attached to a record, an empty exception slot. This function is
  # declared to take one event whose values are a string, a number, a bool or
  # nothing, so an exception cannot arrive here at all and there is no keyword
  # for one to collide with. A failure travels as a fault, and a fault is three
  # strings: which kind it is, what went wrong, and what to do instead. So the
  # scenarios now ask what an operator finds on the line. The fourth, "a line
  # with no cause carries none", asked that an absent exception left an empty
  # slot; with no exception in the vocabulary there is no slot to be empty.

  Scenario: a line is written at the level the caller named
    When a warning is written saying "the disk filled"
    Then the line reads "the disk filled" and its level is warning

  Scenario: a level nobody declared is refused
    When a line is written at a level the vocabulary does not hold
    Then it is refused, naming the levels there are

  Scenario: every fact the caller named is on the line, whatever it is called
    When a line is written with a fact whose name is one the logging vocabulary uses
    Then the line reads that name and its value, like any other fact

  Scenario: a failure reaches the line as its own three words
    Given a failure carrying its kind, what went wrong and what to do instead
    When that failure is written
    Then the line names all three

  Scenario: a line about a failure still carries the facts beside it
    Given a failure carrying its kind, what went wrong and what to do instead
    When that failure is written beside the name of the database it happened on
    Then the line names all three and the database
