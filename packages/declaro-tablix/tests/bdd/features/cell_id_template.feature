Feature: Cell ID Template
  As a developer building accessible tables
  I want to specify ID templates for table cells
  So that each cell has a unique, predictable ID

  Background:
    Given the tablix domain module is available

  Scenario: ColumnDefinition accepts cell_id_template field
    Given a ColumnDefinition model
    When I set cell_id_template to "cell-aum-{row_idx}"
    Then the config is valid
    And config.cell_id_template equals "cell-aum-{row_idx}"

  Scenario: cell_id_template defaults to None
    Given a ColumnDefinition without cell_id_template specified
    Then config.cell_id_template is None

  Scenario: Table renders cell IDs when template provided
    Given a TableConfig with a column having cell_id_template="cell-value-{row_idx}"
    And table data with 3 rows
    When the table is rendered
    Then cell in row 0 has id="cell-value-0"
    And cell in row 1 has id="cell-value-1"
    And cell in row 2 has id="cell-value-2"

  Scenario: No cell IDs when template not provided
    Given a TableConfig with a column without cell_id_template
    And table data with 2 rows
    When the table is rendered
    Then cells have no id attribute
