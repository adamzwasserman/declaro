"""Tests for table service with sorting support.

BDD Scenarios based on Gherkin specification:
  Feature: Table service with sorting support
    As a tablix user
    I want to sort table data by columns
    So that I can view data in my preferred order
"""

import pytest

from declaro_tablix.domain.models import (
    SortDefinition,
    SortDirection,
    TableData,
)
from declaro_tablix.services.table_service import sort_table_data


class TestSortDataAscendingBySingleColumn:
    """Scenario: Sort data ascending by single column."""

    def test_sort_by_name_ascending(self) -> None:
        """Given table data with columns [name, value]
        And rows [(zebra, 1), (apple, 2), (mango, 3)]
        When I call sort_table_data(data, [SortDefinition(column_id=name, direction=asc)])
        Then rows are ordered [(apple, 2), (mango, 3), (zebra, 1)].
        """
        data = TableData(
            rows=[
                {"name": "zebra", "value": 1},
                {"name": "apple", "value": 2},
                {"name": "mango", "value": 3},
            ],
            total_count=3,
        )
        sorts = [SortDefinition(column_id="name", direction=SortDirection.ASC)]
        
        result = sort_table_data(data, sorts)
        
        assert result.rows[0]["name"] == "apple"
        assert result.rows[1]["name"] == "mango"
        assert result.rows[2]["name"] == "zebra"


class TestSortDataDescending:
    """Scenario: Sort data descending."""

    def test_sort_by_value_descending(self) -> None:
        """Given table data with column value
        When I sort by value descending
        Then rows are ordered by value high to low.
        """
        data = TableData(
            rows=[
                {"name": "apple", "value": 2},
                {"name": "mango", "value": 3},
                {"name": "zebra", "value": 1},
            ],
            total_count=3,
        )
        sorts = [SortDefinition(column_id="value", direction=SortDirection.DESC)]
        
        result = sort_table_data(data, sorts)
        
        assert result.rows[0]["value"] == 3
        assert result.rows[1]["value"] == 2
        assert result.rows[2]["value"] == 1


class TestMultiColumnSortWithPriority:
    """Scenario: Multi-column sort with priority."""

    def test_priority_0_is_primary_sort(self) -> None:
        """Given sort definitions with priorities [0, 1]
        When I apply sorting
        Then priority 0 column is primary sort
        And priority 1 column breaks ties.
        """
        data = TableData(
            rows=[
                {"category": "B", "name": "zebra", "value": 1},
                {"category": "A", "name": "apple", "value": 2},
                {"category": "A", "name": "mango", "value": 3},
                {"category": "B", "name": "banana", "value": 4},
            ],
            total_count=4,
        )
        sorts = [
            SortDefinition(column_id="category", direction=SortDirection.ASC, priority=0),
            SortDefinition(column_id="name", direction=SortDirection.ASC, priority=1),
        ]
        
        result = sort_table_data(data, sorts)
        
        # Primary sort by category (A before B)
        assert result.rows[0]["category"] == "A"
        assert result.rows[1]["category"] == "A"
        assert result.rows[2]["category"] == "B"
        assert result.rows[3]["category"] == "B"
        
        # Secondary sort by name within same category
        assert result.rows[0]["name"] == "apple"
        assert result.rows[1]["name"] == "mango"
        assert result.rows[2]["name"] == "banana"
        assert result.rows[3]["name"] == "zebra"


class TestSortPreservesMetadata:
    """Test that sorting preserves existing metadata."""

    def test_metadata_preserved(self) -> None:
        """Metadata should be preserved after sorting."""
        data = TableData(
            rows=[
                {"name": "zebra", "value": 1},
                {"name": "apple", "value": 2},
            ],
            total_count=2,
            metadata={"source": "test"},
        )
        sorts = [SortDefinition(column_id="name", direction=SortDirection.ASC)]
        
        result = sort_table_data(data, sorts)
        
        assert result.metadata is not None
        assert result.metadata.get("source") == "test"
        assert "sorts_applied" in result.metadata


class TestSortWithNoneValues:
    """Test sorting handles None values correctly."""

    def test_none_values_sort_to_end(self) -> None:
        """None values should sort to the end."""
        data = TableData(
            rows=[
                {"name": None, "value": 1},
                {"name": "apple", "value": 2},
                {"name": "zebra", "value": 3},
            ],
            total_count=3,
        )
        sorts = [SortDefinition(column_id="name", direction=SortDirection.ASC)]
        
        result = sort_table_data(data, sorts)
        
        assert result.rows[0]["name"] == "apple"
        assert result.rows[1]["name"] == "zebra"
        assert result.rows[2]["name"] is None


class TestSortEmptyData:
    """Test sorting empty data."""

    def test_empty_rows_returns_empty(self) -> None:
        """Empty data should return empty data."""
        data = TableData(rows=[], total_count=0)
        sorts = [SortDefinition(column_id="name", direction=SortDirection.ASC)]
        
        result = sort_table_data(data, sorts)
        
        assert result.rows == []
        assert result.total_count == 0
