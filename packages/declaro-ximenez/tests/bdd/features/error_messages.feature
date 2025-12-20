Feature: Spanish Inquisition Error Messages
  As a Python developer
  I want ximenez to report errors in Spanish Inquisition style
  So that type violations are memorable and impossible to ignore

  Scenario: No violations shows dismissal
    Given a Python file with content:
      """
      def add(x: int, y: int) -> int:
          result: int = x + y
          return result
      """
    When ximenez checks the file
    Then no violations are reported
    And the output contains "Dismissed! The accused is free to go."

  Scenario: One violation - chief violation
    Given a Python file with content:
      """
      def add(x: int, y: int) -> int:
          result = x + y
          return result
      """
    When ximenez checks the file
    Then 1 violation is reported
    And the output contains "NOBODY expects a type violation!"
    And the output contains "Our chief violation is:"

  Scenario: Two violations - announces one, lists two, then TWO!
    Given a Python file with content:
      """
      def add(x: int, y) -> int:
          result = x + y
          return result
      """
    When ximenez checks the file
    Then 2 violations are reported
    And the output contains "NOBODY expects a type violation!"
    And the output contains "Our chief violation is:"
    And the output contains "...TWO! Our TWO chief violations are fear and surprise!"

  Scenario: Three violations - announces two, lists three, then THREE!
    Given a Python file with content:
      """
      def add(x, y) -> int:
          result = x + y
          return result
      """
    When ximenez checks the file
    Then 3 violations are reported
    And the output contains "Our TWO chief violations are:"
    And the output contains "...THREE! Our THREE chief violations are fear, surprise, and ruthless efficiency!"

  Scenario: Four violations - classic restart
    Given a Python file with content:
      """
      def add(x, y):
          result = x + y
          return result
      """
    When ximenez checks the file
    Then 4 violations are reported
    And the output contains "Our THREE chief violations are:"
    And the output contains "...FOUR! Amongst our violations..."
    And the output contains "I'll come in again."
    And the output contains "Our FOUR chief violations are:"
    And the output contains "...and a fanatical devotion to the Pope."

  Scenario: Five or more violations - escalating chaos
    Given a Python file with content:
      """
      def terrible(a, b, c):
          x = a + b
          y = b + c
          z = a + c
          return x + y + z
      """
    When ximenez checks the file
    Then the violation count is greater than 4
    And the output contains "Our FOUR chief violations are:"
    And the output contains "...FIVE! Our FIVE... no..."
    And the output contains "Amongst our violations are such diverse elements as:"
    And the output contains "Cardinal Biggles, read the charges."
    And the output contains "...and an almost fanatical devotion to the Pope."

  Scenario: Quiet mode suppresses comedy
    Given a Python file with content:
      """
      def add(x: int, y) -> int:
          result = x + y
          return result
      """
    When ximenez checks the file with --quiet flag
    Then 2 violations are reported
    And the output does not contain "NOBODY expects"
    And the output does not contain "Spanish Inquisition"
    And the output contains the violation count and locations only

  Scenario: Machine mode for CI
    Given a Python file with content:
      """
      def add(x: int, y) -> int:
          result = x + y
          return result
      """
    When ximenez checks the file with --machine flag
    Then 2 violations are reported
    And the output matches pattern "{file}:{line}:{col}: error: {message} [XI\d+]"
    And the output does not contain "NOBODY expects"

  Scenario: Comfy chair mode shows warnings only
    Given a Python file with content:
      """
      def add(x: int, y) -> int:
          result = x + y
          return result
      """
    When ximenez checks the file with --comfy-chair flag
    Then the exit code is 0
    And the output contains "warning:" instead of "error:"
    And the output contains "The Comfy Chair has been applied. You may go... for now."

  Scenario: Rack mode promotes warnings to errors
    Given a Python file with content:
      """
      def add(x: int, y: int) -> int:
          # Normally a warning: unused import
          result: int = x + y
          return result
      """
    And the file has an unused import
    When ximenez checks the file with --rack flag
    Then the exit code is 1
    And the output contains "The Rack has been applied!"

  Scenario: Parse error shows Cardinal Biggles message
    Given a Python file with content:
      """
      def broken(x: int -> int:
          return x
      """
    When ximenez checks the file
    Then the exit code is 2
    And the output contains "Cardinal Biggles! Fetch... THE DOCUMENTATION!"

  Scenario: Model violations have their own header
    Given ximenez is configured with declaro schema path "schema/"
    And a Python file with content:
      """
      def get_username(user: User) -> str:
          types:
              username: str

          username = user["username"]
          return username
      """
    When ximenez checks the file
    Then 1 violation is reported
    And the output contains "NOBODY expects a model violation!"
    And the output contains "The Inquisition has examined your models and found them... heretical."

  Scenario: Schema not found shows Cardinal Fang message
    Given ximenez is configured with declaro schema path "nonexistent/"
    And a Python file with content:
      """
      def get_user(user: User) -> str:
          return user["name"]
      """
    When ximenez checks the file
    Then the exit code is 2
    And the output contains "Cardinal Fang! The sacred texts are missing!"
    And the output contains "Could not load schema from: nonexistent/"
