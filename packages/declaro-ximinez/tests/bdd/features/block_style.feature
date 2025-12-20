Feature: Block Type Declarations
  As a Python developer
  I want to declare all local variable types upfront in a types: block
  So that type information is centralized and explicit

  Background:
    Given ximinez is configured with block style allowed

  Scenario: Valid types: block passes
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          types:
              total: int
              multiplier: float = 1.5
              result: int

          total = x + y
          result = int(total * multiplier)
          return result
      """
    When ximinez checks the file
    Then no violations are reported
    And the output contains "Dismissed! The accused is free to go."

  Scenario: types: block with initializers
    Given a Python file with content:
      """
      def confess(x: int, y: str) -> float:
          types:
              sins: int = 0
              penance: float = 0.0
              fear: bool = True

          sins = sins + x
          penance = float(sins) * 0.1
          return penance
      """
    When ximinez checks the file
    Then no violations are reported

  Scenario: types: block must be first statement
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          total = x + y
          types:
              result: int

          result = total * 2
          return result
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "'types:' block must be first statement"

  Scenario: Docstring allowed before types: block
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          \"\"\"Calculate the sum and double it.\"\"\"
          types:
              total: int
              result: int

          total = x + y
          result = total * 2
          return result
      """
    When ximinez checks the file
    Then no violations are reported

  Scenario: Only one types: block allowed
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          types:
              total: int

          types:
              result: int

          total = x + y
          result = total * 2
          return result
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "only one 'types:' block allowed per function"

  Scenario: Variable not declared in types: block
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          types:
              total: int

          total = x + y
          result = total * 2
          return result
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "'result' not declared in types: block"

  Scenario: Declared variable never used
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          types:
              total: int
              unused: str

          total = x + y
          return total
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "'unused' declared but never used"

  Scenario: Inline annotation not allowed with types: block
    Given a Python file with content:
      """
      def calculate(x: int, y: int) -> int:
          types:
              total: int

          total = x + y
          extra: int = 5
          return total + extra
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "inline annotation not allowed when 'types:' block is present"
