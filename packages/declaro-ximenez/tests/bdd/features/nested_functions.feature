Feature: Nested Function Typing
  As a Python developer
  I want nested functions to have their own typing scope
  So that each function maintains its own type discipline

  Scenario: Nested function with its own inline style
    Given a Python file with content:
      """
      def outer(x: int) -> int:
          result: int = x * 2

          def inner(y: int) -> int:
              local: int = y + 1
              return local

          return inner(result)
      """
    When ximenez checks the file
    Then no violations are reported

  Scenario: Nested function with its own types: block
    Given a Python file with content:
      """
      def outer(x: int) -> int:
          types:
              result: int

          def inner(y: int) -> int:
              types:
                  local: int

              local = y + 1
              return local

          result = inner(x) * 2
          return result
      """
    When ximenez checks the file
    Then no violations are reported

  Scenario: Outer uses block, inner uses inline
    Given a Python file with content:
      """
      def outer(x: int) -> int:
          types:
              result: int

          def inner(y: int) -> int:
              local: int = y + 1
              return local

          result = inner(x) * 2
          return result
      """
    When ximenez checks the file
    Then no violations are reported

  Scenario: Nested function must still type all locals
    Given a Python file with content:
      """
      def outer(x: int) -> int:
          result: int = x * 2

          def inner(y: int) -> int:
              local = y + 1
              return local

          return inner(result)
      """
    When ximenez checks the file
    Then 1 violation is reported
    And the violation message contains "local variable 'local' used without type declaration"

  Scenario: Closure variable access from outer scope
    Given a Python file with content:
      """
      def outer(x: int) -> int:
          types:
              multiplier: int

          multiplier = 2

          def inner(y: int) -> int:
              types:
                  result: int

              result = y * multiplier
              return result

          return inner(x)
      """
    When ximenez checks the file
    Then no violations are reported

  Scenario: Lambda expressions require typed context
    Given a Python file with content:
      """
      def process(items: list[int]) -> list[int]:
          types:
              fn: Callable[[int], int]
              result: list[int]

          fn = lambda x: x * 2
          result = list(map(fn, items))
          return result
      """
    When ximenez checks the file
    Then no violations are reported
