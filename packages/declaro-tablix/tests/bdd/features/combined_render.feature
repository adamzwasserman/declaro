Feature: Combined Table Rendering
  As a developer using tablix
  I want a single function to render header, filters, and table together
  So that I can easily create complete table UIs

  Background:
    Given the rendering module is available

  Scenario: render_table_ui function exists
    When I import the rendering module
    Then render_table_ui function is available

  Scenario: Render complete table UI with all sections
    Given a TableConfig with table name "holdings"
    And a TableHeaderConfig with title "Portfolio Holdings"
    And a FilterLayoutConfig with 2 filter controls
    And TableData with 10 rows
    When I call render_table_ui with all sections
    Then the output contains "Portfolio Holdings"
    And the output contains filter controls
    And the output contains table with 10 rows

  Scenario: Render without header
    Given a TableConfig with table name "holdings"
    And a FilterLayoutConfig with 2 filter controls
    And TableData with 5 rows
    And no header configuration
    When I call render_table_ui
    Then the output does not contain header section
    And the output contains filter controls
    And the output contains table with 5 rows

  Scenario: Render without filters
    Given a TableConfig with table name "holdings"
    And a TableHeaderConfig with title "Portfolio"
    And TableData with 3 rows
    And no filter configuration
    When I call render_table_ui
    Then the output contains "Portfolio"
    And the output does not contain filter controls
    And the output contains table with 3 rows

  Scenario: Render table only
    Given a TableConfig with table name "holdings"
    And TableData with 7 rows
    And no header configuration
    And no filter configuration
    When I call render_table_ui
    Then the output does not contain header section
    And the output does not contain filter controls
    And the output contains table with 7 rows
