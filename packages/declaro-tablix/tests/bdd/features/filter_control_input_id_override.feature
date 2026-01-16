Feature: Filter Control Input ID Override
  As a developer building filter controls
  I want to override input IDs for form elements
  So that I can have predictable IDs for testing and accessibility

  Background:
    Given the filter layout module is available

  Scenario: FilterControlConfig accepts input_id_override field
    Given a FilterControlConfig with type "search_input"
    When I set input_id_override to "my-custom-input"
    Then the config is valid
    And config.input_id_override equals "my-custom-input"

  Scenario: input_id_override defaults to None
    Given a FilterControlConfig with type "search_input"
    Then config.input_id_override is None

  Scenario: Search input uses input_id_override when provided
    Given a FilterControlConfig with type "search_input" and id "test-search"
    And input_id_override is "custom-search"
    When the filter control is rendered
    Then the rendered HTML contains id="custom-search"

  Scenario: Search input uses default ID when no override
    Given a FilterControlConfig with type "search_input" and id "test-search"
    When the filter control is rendered
    Then the rendered HTML contains id="filter-test-search"

  Scenario: Single select uses input_id_override when provided
    Given a FilterControlConfig with type "single_select" and id "test-select"
    And input_id_override is "custom-select"
    When the filter control is rendered
    Then the rendered HTML contains id="custom-select"

  Scenario: Single select uses default ID when no override
    Given a FilterControlConfig with type "single_select" and id "test-select"
    When the filter control is rendered
    Then the rendered HTML contains id="filter-test-select"
