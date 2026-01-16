Feature: Multiselect Search
  As a user filtering table data
  I want to search within multiselect dropdown options
  So that I can quickly find options in long lists

  Background:
    Given the filter layout module is available

  Scenario: Searchable multiselect has search input
    Given a FilterControlConfig with type "multi_select"
    And searchable is True
    When the filter control is rendered
    Then the output contains a search input element
    And the search input has placeholder "Search..."

  Scenario: Non-searchable multiselect has no search input
    Given a FilterControlConfig with type "multi_select"
    And searchable is False
    When the filter control is rendered
    Then the output does not contain a search input element

  Scenario: Search input filters options client-side
    Given a FilterControlConfig with type "multi_select"
    And searchable is True
    And options include "Technology", "Healthcare", "Finance", "Techno Music"
    When the filter control is rendered
    Then the output contains JavaScript for filtering options
    And filtering by "tech" would show "Technology" and "Techno Music"

  Scenario: FilterControlConfig accepts searchable field
    Given a FilterControlConfig with searchable=True
    Then the config is valid
    And config.searchable equals True

  Scenario: FilterControlConfig searchable defaults to False
    Given a FilterControlConfig without searchable specified
    Then config.searchable equals False
