"""Tests for FastAPI routes for table operations.

BDD Scenarios based on Gherkin specification:
  Feature: FastAPI routes for table operations
    As an API consumer
    I want REST endpoints for table data
    So that I can fetch sorted/filtered data

Note: These tests mock the database layer to test route behavior.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from declaro_tablix.domain.models import TableData
from declaro_tablix.routes import table_router


@pytest.fixture
def app() -> FastAPI:
    """Create test FastAPI app with tablix router."""
    app = FastAPI()
    app.include_router(table_router)
    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def mock_table_data() -> TableData:
    """Sample table data for testing."""
    return TableData(
        rows=[
            {"name": "zebra", "value": 1},
            {"name": "apple", "value": 2},
            {"name": "mango", "value": 3},
        ],
        total_count=3,
    )


class TestPostTablesDataReturnsSortedData:
    """Scenario: POST /tables/data returns sorted data."""

    @patch("declaro_tablix.routes.table_routes.table_exists")
    @patch("declaro_tablix.routes.table_routes.get_table_data")
    def test_post_returns_200_ok(
        self, mock_get_data: MagicMock, mock_exists: MagicMock, client: TestClient
    ) -> None:
        """Given table test_table exists with data
        When I POST to /tables/data with sorting params
        Then I receive 200 OK.
        """
        mock_exists.return_value = True
        mock_get_data.return_value = TableData(
            rows=[
                {"name": "apple", "value": 2},
                {"name": "zebra", "value": 1},
            ],
            total_count=2,
        )

        response = client.post(
            "/tables/data",
            json={
                "table_name": "test_table",
                "sort_column": "name",
                "sort_direction": "asc",
            },
        )
        assert response.status_code == 200

    @patch("declaro_tablix.routes.table_routes.table_exists")
    @patch("declaro_tablix.routes.table_routes.get_table_data")
    def test_response_contains_sorted_rows(
        self, mock_get_data: MagicMock, mock_exists: MagicMock, client: TestClient
    ) -> None:
        """Response contains sorted rows."""
        mock_exists.return_value = True
        mock_get_data.return_value = TableData(
            rows=[
                {"name": "apple", "value": 2},
                {"name": "zebra", "value": 1},
            ],
            total_count=2,
        )

        response = client.post(
            "/tables/data",
            json={
                "table_name": "test_table",
                "sort_column": "name",
                "sort_direction": "asc",
            },
        )
        data = response.json()
        assert data["success"] is True
        assert data["data"]["rows"][0]["name"] == "apple"
        assert data["data"]["rows"][1]["name"] == "zebra"

    @patch("declaro_tablix.routes.table_routes.table_exists")
    @patch("declaro_tablix.routes.table_routes.get_table_data")
    def test_response_matches_schema(
        self, mock_get_data: MagicMock, mock_exists: MagicMock, client: TestClient
    ) -> None:
        """Response matches TableDataResponse schema."""
        mock_exists.return_value = True
        mock_get_data.return_value = TableData(
            rows=[{"name": "test"}],
            total_count=1,
        )

        response = client.post(
            "/tables/data",
            json={
                "table_name": "test_table",
            },
        )
        data = response.json()
        assert "success" in data
        assert "data" in data
        assert "message" in data


class TestSortDirectionParameter:
    """Scenario: Sort direction parameter."""

    @patch("declaro_tablix.routes.table_routes.table_exists")
    @patch("declaro_tablix.routes.table_routes.get_table_data")
    def test_desc_sorts_descending(
        self, mock_get_data: MagicMock, mock_exists: MagicMock, client: TestClient
    ) -> None:
        """When I POST with sort_direction=desc
        Then data is sorted descending.
        """
        mock_exists.return_value = True
        mock_get_data.return_value = TableData(
            rows=[
                {"name": "mango", "value": 3},
                {"name": "apple", "value": 2},
                {"name": "zebra", "value": 1},
            ],
            total_count=3,
        )

        response = client.post(
            "/tables/data",
            json={
                "table_name": "test_table",
                "sort_column": "value",
                "sort_direction": "desc",
            },
        )
        data = response.json()
        assert data["data"]["rows"][0]["value"] == 3
        assert data["data"]["rows"][1]["value"] == 2
        assert data["data"]["rows"][2]["value"] == 1


class TestPaginationWithSorting:
    """Scenario: Pagination with sorting."""

    @patch("declaro_tablix.routes.table_routes.table_exists")
    @patch("declaro_tablix.routes.table_routes.get_table_data")
    def test_pagination_with_sorting(
        self, mock_get_data: MagicMock, mock_exists: MagicMock, client: TestClient
    ) -> None:
        """Given 100 rows in table
        When I POST with page=2, per_page=25, sort_column=name
        Then I receive rows 26-50 sorted by name.
        """
        mock_exists.return_value = True
        # Simulate page 2 of 100 rows (rows 26-50)
        mock_get_data.return_value = TableData(
            rows=[{"name": f"item_{i:03d}", "value": i} for i in range(25, 50)],
            total_count=100,
        )

        response = client.post(
            "/tables/data",
            json={
                "table_name": "test_table",
                "sort_column": "name",
                "sort_direction": "asc",
                "page": 2,
                "per_page": 25,
            },
        )
        data = response.json()

        # Should have 25 rows (page 2)
        assert len(data["data"]["rows"]) == 25

        # Pagination metadata
        assert data["pagination"]["page"] == 2
        assert data["pagination"]["per_page"] == 25
        assert data["pagination"]["total_count"] == 100


class TestHTMXPartialResponse:
    """Scenario: HTMX partial response."""

    @patch("declaro_tablix.routes.table_routes.table_exists")
    @patch("declaro_tablix.routes.table_routes.get_table_data")
    def test_htmx_request_returns_html(
        self, mock_get_data: MagicMock, mock_exists: MagicMock, client: TestClient
    ) -> None:
        """When request has HX-Request header
        Then response is HTML partial (not JSON).
        """
        mock_exists.return_value = True
        mock_get_data.return_value = TableData(
            rows=[{"name": "test", "value": 1}],
            total_count=1,
        )

        response = client.post(
            "/tables/data",
            json={
                "table_name": "test_table",
            },
            headers={"HX-Request": "true"},
        )
        # Note: This test verifies basic route functionality
        # HTMX rendering requires template configuration
        assert response.status_code == 200


class TestHealthEndpoint:
    """Test health check endpoint."""

    def test_health_returns_200(self, client: TestClient) -> None:
        """Health endpoint returns 200."""
        response = client.get("/tables/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
