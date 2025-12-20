Feature: Comprehension Variable Typing
  As a Python developer
  I want loop variables in comprehensions to be explicitly typed
  So that transformations and slices are type-safe

  Scenario: List comprehension loop variable must be declared
    Given a Python file with content:
      """
      def double_items(items: list[int]) -> list[int]:
          types:
              result: list[int]

          result = [x * 2 for x in items]
          return result
      """
    When ximenez checks the file
    Then 1 violation is reported
    And the violation message contains "comprehension variable 'x' must be declared in types: block"

  Scenario: List comprehension with declared loop variable passes
    Given a Python file with content:
      """
      def double_items(items: list[int]) -> list[int]:
          types:
              x: int
              result: list[int]

          result = [x * 2 for x in items]
          return result
      """
    When ximenez checks the file
    Then no violations are reported

  Scenario: Dict comprehension variables must be declared
    Given a Python file with content:
      """
      def invert_dict(d: dict[str, int]) -> dict[int, str]:
          types:
              k: str
              v: int
              result: dict[int, str]

          result = {v: k for k, v in d.items()}
          return result
      """
    When ximenez checks the file
    Then no violations are reported

  Scenario: Nested comprehension variables must all be declared
    Given a Python file with content:
      """
      def flatten(matrix: list[list[int]]) -> list[int]:
          types:
              row: list[int]
              x: int
              result: list[int]

          result = [x for row in matrix for x in row]
          return result
      """
    When ximenez checks the file
    Then no violations are reported

  Scenario: Missing nested comprehension variable is violation
    Given a Python file with content:
      """
      def flatten(matrix: list[list[int]]) -> list[int]:
          types:
              row: list[int]
              result: list[int]

          result = [x for row in matrix for x in row]
          return result
      """
    When ximenez checks the file
    Then 1 violation is reported
    And the violation message contains "comprehension variable 'x' must be declared in types: block"

  Scenario: Generator expression variables must be declared
    Given a Python file with content:
      """
      def sum_squares(items: list[int]) -> int:
          types:
              x: int
              result: int

          result = sum(x * x for x in items)
          return result
      """
    When ximenez checks the file
    Then no violations are reported

  Scenario: Set comprehension variables must be declared
    Given a Python file with content:
      """
      def unique_lengths(strings: list[str]) -> set[int]:
          types:
              s: str
              result: set[int]

          result = {len(s) for s in strings}
          return result
      """
    When ximenez checks the file
    Then no violations are reported

  Scenario: Comprehension with conditional still requires declaration
    Given a Python file with content:
      """
      def even_squares(items: list[int]) -> list[int]:
          types:
              x: int
              result: list[int]

          result = [x * x for x in items if x % 2 == 0]
          return result
      """
    When ximenez checks the file
    Then no violations are reported

  Scenario: Inline style comprehension variables
    Given a Python file with content:
      """
      def double_items(items: list[int]) -> list[int]:
          x: int
          result: list[int] = [x * 2 for x in items]
          return result
      """
    When ximenez checks the file
    Then no violations are reported
