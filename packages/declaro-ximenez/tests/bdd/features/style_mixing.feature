Feature: Style Mixing Prevention
  As a Python developer
  I want ximenez to enforce a single typing style per function
  So that code remains consistent and readable

  Scenario: Inline style in function with types: block is rejected
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          types:
              total: int

          total = x + y
          extra: int = 5
          return total + extra
      """
    When ximenez checks the file
    Then 1 violation is reported
    And the violation message contains "inline annotation not allowed when 'types:' block is present"

  Scenario: Different functions can use different styles
    Given a Python file with content:
      """
      def inline_style(x: int) -> int:
          result: int = x * 2
          return result

      def block_style(x: int) -> int:
          types:
              result: int

          result = x * 2
          return result
      """
    When ximenez checks the file
    Then no violations are reported

  Scenario: Module-level can enforce single style
    Given ximenez is configured with style enforcement at module level
    And a Python file with content:
      """
      # ximenez: style=block

      def first(x: int) -> int:
          types:
              result: int

          result = x * 2
          return result

      def second(x: int) -> int:
          result: int = x * 2
          return result
      """
    When ximenez checks the file
    Then 1 violation is reported
    And the violation message contains "'second' uses inline style but module enforces block style"
