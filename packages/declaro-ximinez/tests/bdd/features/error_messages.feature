Feature: Error Message Formatting
  As a Python developer
  I want ximinez to report errors clearly
  So that type violations are easy to understand and fix

  # ==========================================================================
  # Standard (default) output mode
  # ==========================================================================

  Scenario: No violations shows clean success message
    Given a Python file with content:
      """
      def add(x: int, y: int) -> int:
          result: int = x + y
          return result
      """
    When ximinez checks the file
    Then no violations are reported
    And the output contains "No violations found."

  Scenario: Standard output shows professional format
    Given a Python file with content:
      """
      def add(x: int, y: int) -> int:
          result = x + y
          return result
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the output contains "Found 1 violation(s):"
    And the output does not contain "NOBODY"

  # ==========================================================================
  # Full output mode (--full flag)
  # ==========================================================================

  Scenario: Full mode - no violations shows dismissal
    Given a Python file with content:
      """
      def add(x: int, y: int) -> int:
          result: int = x + y
          return result
      """
    When ximinez checks the file with --full flag
    Then no violations are reported
    And the output contains "Dismissed!"

  Scenario: Full mode - one violation
    Given a Python file with content:
      """
      def add(x: int, y: int) -> int:
          result = x + y
          return result
      """
    When ximinez checks the file with --full flag
    Then 1 violation is reported
    And the output contains "NOBODY expects a type violation!"
    And the output contains "Our CHIEF weapon:"

  Scenario: Full mode - two violations (off by one)
    Given a Python file with content:
      """
      def add(x: int, y) -> int:
          result = x + y
          return result
      """
    When ximinez checks the file with --full flag
    Then 2 violations are reported
    And the output contains "NOBODY expects a type violation!"
    And the output contains "Our CHIEF weapon:"
    And the output contains "Our TWO"

  Scenario: Full mode - three violations
    Given a Python file with content:
      """
      def add(x, y) -> int:
          result = x + y
          return result
      """
    When ximinez checks the file with --full flag
    Then 3 violations are reported
    And the output contains "Our TWO chief weapons are:"
    And the output contains "Our THREE"

  Scenario: Full mode - four violations (classic restart)
    Given a Python file with content:
      """
      def add(x, y):
          result = x + y
          return result
      """
    When ximinez checks the file with --full flag
    Then 4 violations are reported
    And the output contains "Our THREE chief weapons are:"
    And the output contains "I'll come again!"
    And the output contains "Our FOUR chief weapons:"

  Scenario: Full mode - five or more violations
    Given a Python file with content:
      """
      def terrible(a, b, c):
          x = a + b
          y = b + c
          z = a + c
          return x + y + z
      """
    When ximinez checks the file with --full flag
    Then the violation count is greater than 4
    And the output contains "Our FOUR chief weapons:"
    And the output contains "No, wait, FIVE!"

  # ==========================================================================
  # Other output modes
  # ==========================================================================

  Scenario: Quiet mode - minimal output
    Given a Python file with content:
      """
      def add(x: int, y) -> int:
          result = x + y
          return result
      """
    When ximinez checks the file with --quiet flag
    Then 2 violations are reported
    And the output does not contain "NOBODY"
    And the output does not contain "Found"
    And the output contains the violation count and locations only

  Scenario: Machine mode for CI
    Given a Python file with content:
      """
      def add(x: int, y) -> int:
          result = x + y
          return result
      """
    When ximinez checks the file with --machine flag
    Then 2 violations are reported
    And the output matches pattern ".*:\d+:\d+: error: .* \[XI\d+\]"
    And the output does not contain "NOBODY"

  Scenario: Comfy chair mode shows warnings
    Given a Python file with content:
      """
      def add(x: int, y) -> int:
          result = x + y
          return result
      """
    When ximinez checks the file with --comfy-chair flag
    Then the exit code is 0
    And the output contains "warning:" instead of "error:"

  # ==========================================================================
  # Error conditions
  # ==========================================================================

  Scenario: Parse error returns exit code 2
    Given a Python file with content:
      """
      def broken(x: int -> int:
          return x
      """
    When ximinez checks the file
    Then the exit code is 2

  Scenario: Model violations in full mode
    Given a TOML schema file "schema/user.toml" with content:
      """
      [user]
      table = "users"

      [user.fields]
      id = { type = "uuid" }
      email = { type = "str" }
      name = { type = "str", nullable = true }
      """
    And ximinez is configured with declaro schema path "schema/"
    And a Python file with content:
      """
      def get_username(user: User) -> str:
          types:
              username: str

          username = user["username"]
          return username
      """
    When ximinez checks the file with --full flag
    Then 1 violation is reported
    And the output contains "NOBODY expects a model violation!"

  Scenario: Schema not found returns exit code 2
    Given ximinez is configured with declaro schema path "nonexistent/"
    And a Python file with content:
      """
      def get_user(user: User) -> str:
          return user["name"]
      """
    When ximinez checks the file
    Then the exit code is 2
    And the output contains "Could not load schema from:"
    And the output contains "nonexistent"
