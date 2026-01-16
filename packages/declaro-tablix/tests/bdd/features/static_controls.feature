Feature: Static Display Controls
  As a developer building filter layouts
  I want to add static text and images to filter sections
  So that I can create rich, branded filter UIs

  Background:
    Given the filter layout module is available

  Scenario: FilterControlType has STATIC_TEXT value
    When I check FilterControlType enum values
    Then STATIC_TEXT is a valid FilterControlType

  Scenario: FilterControlType has STATIC_IMAGE value
    When I check FilterControlType enum values
    Then STATIC_IMAGE is a valid FilterControlType

  Scenario: FilterControlConfig accepts text_content field
    Given a FilterControlConfig with type "static_text"
    And text_content is "Welcome to Holdings"
    Then the config is valid
    And config.text_content equals "Welcome to Holdings"

  Scenario: FilterControlConfig accepts image_url field
    Given a FilterControlConfig with type "static_image"
    And image_url is "/assets/logo.png"
    And image_alt is "Company Logo"
    Then the config is valid
    And config.image_url equals "/assets/logo.png"
    And config.image_alt equals "Company Logo"

  Scenario: Render static text control
    Given a FilterControlConfig with type "static_text"
    And text_content is "Welcome to Holdings"
    And css_class is "text-primary fs-4"
    When the filter control is rendered
    Then the output contains "Welcome to Holdings"
    And the output contains class "text-primary fs-4"

  Scenario: Render static image control
    Given a FilterControlConfig with type "static_image"
    And image_url is "/assets/logo.png"
    And image_alt is "Company Logo"
    And css_class is "rounded shadow"
    When the filter control is rendered
    Then the output contains img element with src "/assets/logo.png"
    And the output contains alt "Company Logo"
    And the output contains class "rounded shadow"

  Scenario: Static controls have no form inputs
    Given a FilterControlConfig with type "static_text"
    And text_content is "Just a label"
    When the filter control is rendered
    Then the output does not contain input element
    And the output does not contain select element
