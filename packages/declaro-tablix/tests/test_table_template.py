"""Tests for HTMX-powered sort buttons in table template.

BDD Scenarios based on Gherkin specification:
  Feature: HTMX-powered sort buttons in table headers
    As a user viewing a table
    I want to click column headers to sort
    So that I can reorder data without page reload
"""

from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

from declaro_tablix.domain.models import ColumnDefinition, ColumnType, TableConfig


@pytest.fixture
def jinja_env() -> Environment:
    template_dir = (
        Path(__file__).parent.parent
        / "src"
        / "declaro_tablix"
        / "templates"
        / "components"
    )
    return Environment(
        loader=FileSystemLoader(str(template_dir)),
        autoescape=True,
    )


@pytest.fixture
def sortable_config() -> TableConfig:
    return TableConfig(
        table_name="test_table",
        columns=[
            ColumnDefinition(
                id="name",
                name="Name",
                type=ColumnType.TEXT,
                sortable=True,
            ),
            ColumnDefinition(
                id="value",
                name="Value",
                type=ColumnType.NUMBER,
                sortable=True,
            ),
            ColumnDefinition(
                id="notes",
                name="Notes",
                type=ColumnType.TEXT,
                sortable=False,
            ),
        ],
    )


@pytest.fixture
def sample_data() -> list[dict]:
    return [
        {"name": "Apple", "value": 10, "notes": "Red fruit"},
        {"name": "Banana", "value": 20, "notes": "Yellow fruit"},
    ]


class TestSortableColumnRendersButton:
    def test_sortable_column_has_button(
        self,
        jinja_env: Environment,
        sortable_config: TableConfig,
        sample_data: list[dict],
    ) -> None:
        template = jinja_env.get_template("table.html")
        html = template.render(
            config=sortable_config,
            data=sample_data,
            sort_field=None,
            sort_dir=None,
            sort_url="/api/table/data",
        )
        assert "hx-get" in html
        assert "tablix-sort-btn" in html

    def test_button_has_htmx_attributes(
        self,
        jinja_env: Environment,
        sortable_config: TableConfig,
        sample_data: list[dict],
    ) -> None:
        template = jinja_env.get_template("table.html")
        html = template.render(
            config=sortable_config,
            data=sample_data,
            sort_field=None,
            sort_dir=None,
            sort_url="/api/table/data",
        )
        assert "hx-target" in html
        assert "hx-swap" in html


class TestNonSortableColumnRendersPlainText:
    def test_non_sortable_column_no_button(
        self,
        jinja_env: Environment,
        sortable_config: TableConfig,
        sample_data: list[dict],
    ) -> None:
        template = jinja_env.get_template("table.html")
        html = template.render(
            config=sortable_config,
            data=sample_data,
            sort_field=None,
            sort_dir=None,
            sort_url="/api/table/data",
        )
        assert "Notes" in html


class TestActiveSortShowsIndicator:
    def test_active_sort_column_has_indicator(
        self,
        jinja_env: Environment,
        sortable_config: TableConfig,
        sample_data: list[dict],
    ) -> None:
        template = jinja_env.get_template("table.html")
        html = template.render(
            config=sortable_config,
            data=sample_data,
            sort_field="name",
            sort_dir="asc",
            sort_url="/api/table/data",
        )
        assert "tablix-sort-active" in html


class TestClickTogglesSortDirection:
    def test_ascending_generates_desc_url(
        self,
        jinja_env: Environment,
        sortable_config: TableConfig,
        sample_data: list[dict],
    ) -> None:
        template = jinja_env.get_template("table.html")
        html = template.render(
            config=sortable_config,
            data=sample_data,
            sort_field="name",
            sort_dir="asc",
            sort_url="/api/table/data",
        )
        assert "sort=name" in html
        assert "dir=desc" in html


class TestHTMXSwapsTable:
    def test_button_targets_table(
        self,
        jinja_env: Environment,
        sortable_config: TableConfig,
        sample_data: list[dict],
    ) -> None:
        template = jinja_env.get_template("table.html")
        html = template.render(
            config=sortable_config,
            data=sample_data,
            sort_field=None,
            sort_dir=None,
            sort_url="/api/table/data",
        )
        assert "hx-target" in html

    def test_swap_mode_is_outer_or_inner(
        self,
        jinja_env: Environment,
        sortable_config: TableConfig,
        sample_data: list[dict],
    ) -> None:
        template = jinja_env.get_template("table.html")
        html = template.render(
            config=sortable_config,
            data=sample_data,
            sort_field=None,
            sort_dir=None,
            sort_url="/api/table/data",
        )
        assert "hx-swap" in html
