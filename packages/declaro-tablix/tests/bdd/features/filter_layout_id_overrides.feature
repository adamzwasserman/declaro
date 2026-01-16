Feature: Filter Layout ID Overrides
  As a developer building filter layouts
  I want to override container, form, and controls IDs
  So that I can have predictable IDs for testing and styling

  Background:
    Given the filter layout module is available

  Scenario: FilterLayoutConfig accepts container_id_override field
    Given a FilterLayoutConfig model
    When I set container_id_override to "my-filter-container"
    Then the config is valid
    And config.container_id_override equals "my-filter-container"

  Scenario: FilterLayoutConfig accepts form_id_override field
    Given a FilterLayoutConfig model
    When I set form_id_override to "my-filter-form"
    Then the config is valid
    And config.form_id_override equals "my-filter-form"

  Scenario: FilterLayoutConfig accepts controls_id_override field
    Given a FilterLayoutConfig model
    When I set controls_id_override to "my-filter-controls"
    Then the config is valid
    And config.controls_id_override equals "my-filter-controls"

  Scenario: ID overrides default to None
    Given a FilterLayoutConfig without ID overrides specified
    Then config.container_id_override is None
    And config.form_id_override is None
    And config.controls_id_override is None

  Scenario: Filter layout uses container_id_override when provided
    Given a FilterLayoutConfig with container_id_override="custom-container"
    When the filter layout is rendered
    Then the container element has id="custom-container"

  Scenario: Filter layout uses default container ID when no override
    Given a FilterLayoutConfig with id="test-layout" and no container_id_override
    When the filter layout is rendered
    Then the container element has id="filter-layout-test-layout"

  Scenario: Filter layout uses form_id_override when provided
    Given a FilterLayoutConfig with form_id_override="custom-form"
    When the filter layout is rendered
    Then the form element has id="custom-form"

  Scenario: Filter layout uses default form ID when no override
    Given a FilterLayoutConfig with id="test-layout" and no form_id_override
    When the filter layout is rendered
    Then the form element has id="filter-form-test-layout"

  Scenario: Filter layout uses controls_id_override when provided
    Given a FilterLayoutConfig with controls_id_override="custom-controls"
    When the filter layout is rendered
    Then the controls element has id="custom-controls"

  Scenario: Filter layout uses default controls ID when no override
    Given a FilterLayoutConfig with id="test-layout" and no controls_id_override
    When the filter layout is rendered
    Then the controls element has id="filter-controls-test-layout"
