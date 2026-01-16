"""Tests for Persistum-backed table repository.

BDD Scenarios based on Gherkin specification:
  Feature: Persistum-backed table repository
    As a tablix user
    I want table configs stored via persistum
    So that data persists across sessions

    Scenario: Get table config by name
      Given a table config "holdings" exists in the database
      When I call repository.get_config(conn, "holdings")
      Then I receive a TableConfigModel with table_name="holdings"

    Scenario: Save new table config
      Given no table config "new_table" exists
      When I call repository.save_config(conn, config)
      Then the config is persisted to the database
      And I can retrieve it by name

    Scenario: Get user preferences
      Given user "user123" has preferences for table "holdings"
      When I call repository.get_user_preferences(conn, "user123", "holdings_id")
      Then I receive a list of UserFilterPreference objects
"""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from declaro_tablix.models import (
    TableConfigModel,
    UserFilterPreference,
)
from declaro_tablix.repositories.table_repository import (
    get_config,
    get_user_preferences,
    save_config,
)


@pytest.fixture
def mock_connection() -> AsyncMock:
    """Create a mock database connection that simulates persistum behavior."""
    conn = AsyncMock()
    return conn


@pytest.fixture
def sample_table_config() -> TableConfigModel:
    """Create a sample table config for testing."""
    return TableConfigModel(
        id=str(uuid4()),
        table_name="holdings",
        display_name="Holdings Table",
        description="Test holdings table",
        config_json='{"table_name": "holdings", "columns": []}',
        filter_layout_json=None,
        is_active=True,
        created_at="2024-01-01T00:00:00",
        updated_at="2024-01-01T00:00:00",
    )


@pytest.fixture
def sample_user_preference() -> UserFilterPreference:
    """Create a sample user filter preference for testing."""
    return UserFilterPreference(
        id=str(uuid4()),
        user_id="user123",
        table_id="holdings_id",
        preference_name="Default View",
        filter_state_json='{"filters": []}',
        is_default=True,
        created_at="2024-01-01T00:00:00",
        updated_at="2024-01-01T00:00:00",
    )


class TestGetConfigByName:
    """Scenario: Get table config by name."""

    @pytest.mark.asyncio
    async def test_get_existing_config(
        self, mock_connection: AsyncMock, sample_table_config: TableConfigModel
    ) -> None:
        """Given a table config exists, when we get it by name, we receive the config."""
        # Given: a table config "holdings" exists in the database
        mock_connection.find_first = AsyncMock(
            return_value={
                "id": sample_table_config.id,
                "table_name": sample_table_config.table_name,
                "display_name": sample_table_config.display_name,
                "description": sample_table_config.description,
                "config_json": sample_table_config.config_json,
                "filter_layout_json": sample_table_config.filter_layout_json,
                "is_active": sample_table_config.is_active,
                "created_at": sample_table_config.created_at,
                "updated_at": sample_table_config.updated_at,
            }
        )

        # When: I call repository.get_config(conn, "holdings")
        result = await get_config(mock_connection, "holdings")

        # Then: I receive a TableConfigModel with table_name="holdings"
        assert result is not None
        assert isinstance(result, TableConfigModel)
        assert result.table_name == "holdings"

    @pytest.mark.asyncio
    async def test_get_nonexistent_config(self, mock_connection: AsyncMock) -> None:
        """Given no config exists, when we get it by name, we receive None."""
        # Given: no table config exists
        mock_connection.find_first = AsyncMock(return_value=None)

        # When: I call repository.get_config(conn, "nonexistent")
        result = await get_config(mock_connection, "nonexistent")

        # Then: I receive None
        assert result is None


class TestSaveConfig:
    """Scenario: Save new table config."""

    @pytest.mark.asyncio
    async def test_save_new_config(
        self, mock_connection: AsyncMock, sample_table_config: TableConfigModel
    ) -> None:
        """Given no config exists, when we save it, it's persisted."""
        # Given: no table config "new_table" exists
        mock_connection.find_first = AsyncMock(return_value=None)
        mock_connection.create = AsyncMock(
            return_value={
                "id": sample_table_config.id,
                "table_name": sample_table_config.table_name,
                "display_name": sample_table_config.display_name,
                "description": sample_table_config.description,
                "config_json": sample_table_config.config_json,
                "filter_layout_json": sample_table_config.filter_layout_json,
                "is_active": sample_table_config.is_active,
                "created_at": sample_table_config.created_at,
                "updated_at": sample_table_config.updated_at,
            }
        )

        # When: I call repository.save_config(conn, config)
        result = await save_config(mock_connection, sample_table_config)

        # Then: the config is persisted to the database
        assert result is not None
        assert isinstance(result, TableConfigModel)
        mock_connection.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_existing_config(
        self, mock_connection: AsyncMock, sample_table_config: TableConfigModel
    ) -> None:
        """Given config exists, when we save it, it's updated."""
        # Given: config already exists
        mock_connection.find_first = AsyncMock(
            return_value={"id": sample_table_config.id}
        )
        mock_connection.update = AsyncMock(
            return_value={
                "id": sample_table_config.id,
                "table_name": sample_table_config.table_name,
                "display_name": "Updated Name",
                "description": sample_table_config.description,
                "config_json": sample_table_config.config_json,
                "filter_layout_json": sample_table_config.filter_layout_json,
                "is_active": sample_table_config.is_active,
                "created_at": sample_table_config.created_at,
                "updated_at": "2024-01-02T00:00:00",
            }
        )

        # When: I call repository.save_config(conn, config)
        updated_config = TableConfigModel(
            id=sample_table_config.id,
            table_name=sample_table_config.table_name,
            display_name="Updated Name",
            description=sample_table_config.description,
            config_json=sample_table_config.config_json,
        )
        result = await save_config(mock_connection, updated_config)

        # Then: the config is updated
        assert result is not None
        mock_connection.update.assert_called_once()


class TestGetUserPreferences:
    """Scenario: Get user preferences."""

    @pytest.mark.asyncio
    async def test_get_existing_preferences(
        self, mock_connection: AsyncMock, sample_user_preference: UserFilterPreference
    ) -> None:
        """Given user has preferences, when we get them, we receive a list."""
        # Given: user "user123" has preferences for table "holdings"
        mock_connection.find_many = AsyncMock(
            return_value=[
                {
                    "id": sample_user_preference.id,
                    "user_id": sample_user_preference.user_id,
                    "table_id": sample_user_preference.table_id,
                    "preference_name": sample_user_preference.preference_name,
                    "filter_state_json": sample_user_preference.filter_state_json,
                    "is_default": sample_user_preference.is_default,
                    "created_at": sample_user_preference.created_at,
                    "updated_at": sample_user_preference.updated_at,
                }
            ]
        )

        # When: I call repository.get_user_preferences(conn, "user123", "holdings_id")
        result = await get_user_preferences(mock_connection, "user123", "holdings_id")

        # Then: I receive a list of UserFilterPreference objects
        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], UserFilterPreference)
        assert result[0].user_id == "user123"

    @pytest.mark.asyncio
    async def test_get_empty_preferences(self, mock_connection: AsyncMock) -> None:
        """Given user has no preferences, when we get them, we receive empty list."""
        # Given: no preferences exist
        mock_connection.find_many = AsyncMock(return_value=[])

        # When: I call repository.get_user_preferences(conn, "user123", "nonexistent")
        result = await get_user_preferences(mock_connection, "user123", "nonexistent")

        # Then: I receive an empty list
        assert result is not None
        assert isinstance(result, list)
        assert len(result) == 0
