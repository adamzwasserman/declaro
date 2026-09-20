@bdd @specification
Feature: next_wait -- How long to wait before trying again
  Each wait is longer than the last, and two callers retrying in the same instant
  must not wait in step. The spread comes from the ticket, because the time they
  read is the same.

  Scenario: the wait grows with the attempt
    When the first three attempts are worked out
    Then each wait is longer than the one before it

  Scenario: two callers at the same instant wait differently
    Given two writes retrying at the same time with different tickets
    Then their waits differ

  Scenario: the same three inputs always give the same wait
    Then a test can check an exact number
