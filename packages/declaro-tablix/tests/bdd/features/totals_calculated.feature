Feature: Totals and Calculated Fields
  As a developer building filter layouts
  I want to display totals and calculated values in the filter section
  So that users see summary statistics alongside filters

  Background:
    Given the filter layout module is available

  Scenario: FilterControlType has TOTAL_ABSOLUTE value
    When I check FilterControlType enum values
    Then TOTAL_ABSOLUTE is a valid FilterControlType

  Scenario: FilterControlType has TOTAL_VISIBLE value
    When I check FilterControlType enum values
    Then TOTAL_VISIBLE is a valid FilterControlType

  Scenario: FilterControlType has CALCULATED_FIELD value
    When I check FilterControlType enum values
    Then CALCULATED_FIELD is a valid FilterControlType

  Scenario: FilterControlConfig accepts source_field and format
    Given a FilterControlConfig with type "total_absolute"
    And source_field is "market_value"
    And format is "currency"
    And label is "Total AUM"
    Then the config is valid
    And config.source_field equals "market_value"
    And config.format equals "currency"

  Scenario: FilterControlConfig accepts formula field
    Given a FilterControlConfig with type "calculated_field"
    And formula is "{reviewed_count} / {total_count} * 100"
    And format is "percentage"
    And label is "Progress"
    Then the config is valid
    And config.formula equals "{reviewed_count} / {total_count} * 100"

  Scenario: FilterControlConfig accepts badge_thresholds
    Given a FilterControlConfig with type "calculated_field"
    And formula is "{value}"
    And badge_thresholds are defined
    Then the config is valid
    And config.badge_thresholds has 3 entries

  Scenario: Render absolute total with currency format
    Given a FilterControlConfig with type "total_absolute"
    And source_field is "market_value"
    And format is "currency"
    And label is "Total AUM"
    And total value is 96000000
    When the total field is rendered
    Then the output contains "$96,000,000"
    And the output contains "Total AUM"

  Scenario: Render absolute total with number format
    Given a FilterControlConfig with type "total_absolute"
    And source_field is "quantity"
    And format is "number"
    And label is "Total Shares"
    And total value is 150000
    When the total field is rendered
    Then the output contains "150,000"
    And the output contains "Total Shares"

  Scenario: Render calculated field as percentage
    Given a FilterControlConfig with type "calculated_field"
    And formula is "{reviewed_count} / {total_count} * 100"
    And format is "percentage"
    And label is "Review Progress"
    And calculation context has reviewed_count=74 and total_count=100
    When the calculated field is rendered
    Then the output contains "74%"
    And the output contains "Review Progress"

  Scenario: Calculated field with badge styling based on value
    Given a FilterControlConfig with type "calculated_field"
    And formula is "{value}"
    And format is "percentage"
    And badge_thresholds define danger below 50, warning 50-80, success above 80
    And calculation context has value=74
    When the calculated field is rendered
    Then the output contains badge with class "bg-warning"
    And the output contains "74%"
