Feature: Inline Type Annotations
  As a Python developer
  I want to annotate local variables inline on first use
  So that every variable has an explicit type

  Background:
    Given ximinez is configured with inline style allowed

  Scenario: Valid inline annotations pass
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          total: int = x + y
          multiplier: float = 1.5
          result: int = int(total * multiplier)
          return result
      """
    When ximinez checks the file
    Then no violations are reported
    And the output contains "No violations found."

  Scenario: Missing parameter type annotation
    Given a Python file with content:
      """
      def calculate(x, y: int) -> int:
          return x + y
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "missing type annotation for parameter 'x'"

  Scenario: Missing return type annotation
    Given a Python file with content:
      """
      def calculate(x: int, y: int):
          return x + y
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "missing return type annotation"

  Scenario: Local variable used without type declaration
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          total = x + y
          return total
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "local variable 'total' used without type declaration"

  Scenario: Type mismatch on assignment
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          total: str = x + y
          return total
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "expected 'str', got 'int'"

  Scenario: Variable used before declaration
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          result = total * 2
          total: int = x + y
          return result
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "'total' used before declaration"

  Scenario: Variable declared twice
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          total: int = x + y
          total: int = total * 2
          return total
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "'total' already declared"

  Scenario: Multiple violations trigger off-by-one pattern
    Given a Python file with content:
      """
      def calculate(x, y) -> int:
          total = x + y
          return total
      """
    When ximinez checks the file
    Then 3 violations are reported
    And the output contains "Our TWO chief violations are:"
    And the output contains "...THREE!"
