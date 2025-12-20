Feature: Walrus Operator Typing
  As a Python developer
  I want walrus operator assignments to require type declarations
  So that inline assignments maintain type safety

  Scenario: Walrus operator variable must be pre-declared in types: block
    Given a Python file with content:
      """
      def check_length(items: list[int]) -> bool:
          types:
              result: bool

          if (n := len(items)) > 10:
              result = True
          else:
              result = False
          return result
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "walrus operator variable 'n' must be declared in types: block"

  Scenario: Walrus operator with declared variable passes
    Given a Python file with content:
      """
      def check_length(items: list[int]) -> bool:
          types:
              n: int
              result: bool

          if (n := len(items)) > 10:
              result = True
          else:
              result = False
          return result
      """
    When ximinez checks the file
    Then no violations are reported

  Scenario: Walrus operator in while loop
    Given a Python file with content:
      """
      def read_chunks(reader: Reader) -> list[bytes]:
          types:
              chunk: bytes
              chunks: list[bytes]

          chunks = []
          while (chunk := reader.read(1024)):
              chunks.append(chunk)
          return chunks
      """
    When ximinez checks the file
    Then no violations are reported

  Scenario: Walrus operator in list comprehension filter
    Given a Python file with content:
      """
      def valid_results(items: list[str]) -> list[int]:
          types:
              item: str
              parsed: int
              result: list[int]

          result = [parsed for item in items if (parsed := try_parse(item)) is not None]
          return result
      """
    When ximinez checks the file
    Then no violations are reported

  Scenario: Walrus operator without declaration in inline style
    Given a Python file with content:
      """
      def check_length(items: list[int]) -> bool:
          if (n := len(items)) > 10:
              result: bool = True
          else:
              result: bool = False
          return result
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "walrus operator variable 'n' used without type declaration"

  Scenario: Walrus operator with inline pre-declaration
    Given a Python file with content:
      """
      def check_length(items: list[int]) -> bool:
          n: int
          result: bool
          if (n := len(items)) > 10:
              result = True
          else:
              result = False
          return result
      """
    When ximinez checks the file
    Then no violations are reported

  Scenario: Multiple walrus operators all need declarations
    Given a Python file with content:
      """
      def complex_check(a: list[int], b: list[int]) -> bool:
          types:
              len_a: int
              len_b: int
              result: bool

          if (len_a := len(a)) > 0 and (len_b := len(b)) > 0:
              result = len_a == len_b
          else:
              result = False
          return result
      """
    When ximinez checks the file
    Then no violations are reported
