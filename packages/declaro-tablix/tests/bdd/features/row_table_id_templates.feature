Feature: Row and Table ID Templates
  As a developer building accessible tables
  I want to specify ID templates for table rows and override table IDs
  So that each row has a unique ID and tables can be targeted by specific IDs

  Background:
    Given the tablix domain module is available

  Scenario: TableConfig accepts row_id_template field
    Given a TableConfig model
    When I set row_id_template to "holding-row-{row_idx}"
    Then the config is valid
    And config.row_id_template equals "holding-row-{row_idx}"

  Scenario: TableConfig accepts table_id_override field
    Given a TableConfig model
    When I set table_id_override to "my-custom-table"
    Then the config is valid
    And config.table_id_override equals "my-custom-table"

  Scenario: row_id_template defaults to None
    Given a TableConfig without row_id_template specified
    Then config.row_id_template is None

  Scenario: Table renders row IDs when template provided
    Given a TableConfig with row_id_template="row-{row_idx}"
    And table data with 3 rows
    When the table is rendered
    Then row 0 has id="row-0"
    And row 1 has id="row-1"
    And row 2 has id="row-2"

  Scenario: Table uses override ID when provided
    Given a TableConfig with table_id_override="custom-holdings"
    And table data with 1 row
    When the table is rendered
    Then the table element has id="custom-holdings"

  Scenario: Table uses default ID when no override
    Given a TableConfig with table_name="holdings" and no table_id_override
    And table data with 1 row
    When the table is rendered
    Then the table element has id="tablix-holdings"
