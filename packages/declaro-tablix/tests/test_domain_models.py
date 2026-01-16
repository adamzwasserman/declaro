"""Tests for domain models."""

import pytest
from pydantic import ValidationError

from declaro_tablix.domain.models import (
    ColumnDefinition,
    ColumnType,
    FilterDefinition,
    FilterOperator,
    PaginationSettings,
    SortDefinition,
    SortDirection,
    TableConfig,
    TableCssConfig,
    TableData,
)
from declaro_tablix.domain.filter_layout import (
    FilterControlConfig,
    FilterControlType,
    FilterLayoutConfig,
    FilterState,
)


class TestColumnDefinition:
    """Tests for ColumnDefinition model."""

    def test_create_basic_column(self):
        col = ColumnDefinition(id="name", name="Name", type=ColumnType.TEXT)
        assert col.id == "name"
        assert col.name == "Name"
        assert col.type == ColumnType.TEXT
        assert col.sortable is True
        assert col.filterable is True

    def test_column_id_cannot_be_empty(self):
        with pytest.raises(ValidationError):
            ColumnDefinition(id="", name="Name", type=ColumnType.TEXT)

    def test_column_name_cannot_be_empty(self):
        with pytest.raises(ValidationError):
            ColumnDefinition(id="id", name="", type=ColumnType.TEXT)

    def test_column_width_bounds(self):
        col = ColumnDefinition(id="id", name="Name", type=ColumnType.TEXT, width=100)
        assert col.width == 100

        with pytest.raises(ValidationError):
            ColumnDefinition(id="id", name="Name", type=ColumnType.TEXT, width=30)

        with pytest.raises(ValidationError):
            ColumnDefinition(id="id", name="Name", type=ColumnType.TEXT, width=3000)


class TestFilterDefinition:
    """Tests for FilterDefinition model."""

    def test_create_equals_filter(self):
        f = FilterDefinition(column_id="status", operator=FilterOperator.EQUALS, value="active")
        assert f.column_id == "status"
        assert f.value == "active"

    def test_in_operator_requires_values(self):
        with pytest.raises(ValidationError):
            FilterDefinition(column_id="status", operator=FilterOperator.IN, value="test")

    def test_in_operator_with_values(self):
        f = FilterDefinition(
            column_id="status",
            operator=FilterOperator.IN,
            values=["active", "pending"],
        )
        assert f.values == ["active", "pending"]
        assert f.value is None

    def test_is_null_operator_no_value(self):
        f = FilterDefinition(column_id="status", operator=FilterOperator.IS_NULL)
        assert f.value is None


class TestPaginationSettings:
    """Tests for PaginationSettings model."""

    def test_default_pagination(self):
        p = PaginationSettings()
        assert p.page == 1
        assert p.page_size == 25
        assert p.offset == 0

    def test_offset_calculation(self):
        p = PaginationSettings(page=3, page_size=10)
        assert p.offset == 20

    def test_total_pages_calculation(self):
        p = PaginationSettings(page=1, page_size=10, total_count=95)
        assert p.total_pages == 10


class TestTableConfig:
    """Tests for TableConfig model."""

    def test_create_table_config(self):
        cols = [
            ColumnDefinition(id="id", name="ID", type=ColumnType.NUMBER),
            ColumnDefinition(id="name", name="Name", type=ColumnType.TEXT),
        ]
        tc = TableConfig(table_name="users", columns=cols)
        assert tc.table_name == "users"
        assert len(tc.columns) == 2

    def test_table_requires_columns(self):
        with pytest.raises(ValidationError):
            TableConfig(table_name="users", columns=[])

    def test_column_ids_must_be_unique(self):
        with pytest.raises(ValidationError):
            TableConfig(
                table_name="users",
                columns=[
                    ColumnDefinition(id="id", name="ID", type=ColumnType.NUMBER),
                    ColumnDefinition(id="id", name="ID 2", type=ColumnType.NUMBER),
                ],
            )


class TestFilterControlConfig:
    """Tests for FilterControlConfig model."""

    def test_create_search_input(self):
        c = FilterControlConfig(
            id="search",
            control_type=FilterControlType.SEARCH_INPUT,
            column_id="name",
            placeholder="Search by name...",
        )
        assert c.control_type == FilterControlType.SEARCH_INPUT
        assert c.debounce_ms == 300

    def test_create_checkbox_group(self):
        c = FilterControlConfig(
            id="status",
            control_type=FilterControlType.CHECKBOX_GROUP,
            column_id="status",
            options_static=["Active", "Pending", "Closed"],
        )
        assert c.options_static == ["Active", "Pending", "Closed"]


class TestFilterLayoutConfig:
    """Tests for FilterLayoutConfig model."""

    def test_create_layout(self):
        layout = FilterLayoutConfig(
            id="main-filters",
            table_id="users",
            grid_template_columns="auto 1fr 1fr auto",
            controls=[
                FilterControlConfig(
                    id="search",
                    control_type=FilterControlType.SEARCH_INPUT,
                    column_id="name",
                ),
            ],
        )
        assert layout.grid_template_columns == "auto 1fr 1fr auto"
        assert len(layout.controls) == 1

    def test_control_ids_must_be_unique(self):
        with pytest.raises(ValidationError):
            FilterLayoutConfig(
                id="layout",
                table_id="users",
                controls=[
                    FilterControlConfig(
                        id="search",
                        control_type=FilterControlType.SEARCH_INPUT,
                        column_id="name",
                    ),
                    FilterControlConfig(
                        id="search",
                        control_type=FilterControlType.SEARCH_INPUT,
                        column_id="email",
                    ),
                ],
            )

    def test_get_control(self):
        layout = FilterLayoutConfig(
            id="layout",
            table_id="users",
            controls=[
                FilterControlConfig(
                    id="search",
                    control_type=FilterControlType.SEARCH_INPUT,
                    column_id="name",
                ),
            ],
        )
        c = layout.get_control("search")
        assert c is not None
        assert c.id == "search"

        assert layout.get_control("nonexistent") is None

    def test_to_css_grid_style(self):
        layout = FilterLayoutConfig(
            id="layout",
            table_id="users",
            grid_template_columns="1fr 2fr",
            grid_gap="20px",
            padding="16px",
        )
        style = layout.to_css_grid_style()
        assert "display: grid" in style
        assert "grid-template-columns: 1fr 2fr" in style
        assert "gap: 20px" in style
        assert "padding: 16px" in style


class TestFilterState:
    """Tests for FilterState model."""

    def test_set_and_get_value(self):
        state = FilterState(layout_id="layout")
        state.set_value("search", "test")
        assert state.get_value("search") == "test"
        assert state.get_value("nonexistent") is None

    def test_clear(self):
        state = FilterState(layout_id="layout", values={"a": 1, "b": 2})
        state.clear()
        assert state.values == {}

    def test_to_query_params(self):
        state = FilterState(layout_id="layout", values={
            "search": "test",
            "status": ["active", "pending"],
        })
        params = state.to_query_params()
        assert params["search"] == "test"
        assert params["status"] == "active,pending"

    def test_from_query_params(self):
        state = FilterState.from_query_params("layout", {
            "search": "test",
            "status": "active,pending",
        })
        assert state.get_value("search") == "test"
        assert state.get_value("status") == ["active", "pending"]


class TestTableCssConfig:
    """Tests for TableCssConfig model."""

    def test_create_default_css_config(self):
        """CSS config should have empty lists by default."""
        css = TableCssConfig()
        assert css.table == []
        assert css.thead == []
        assert css.tbody == []
        assert css.row == []
        assert css.row_alt == []

    def test_create_with_table_classes(self):
        """Should accept list of CSS classes for table element."""
        css = TableCssConfig(table=["holdings-table", "striped"])
        assert css.table == ["holdings-table", "striped"]

    def test_create_with_all_classes(self):
        """Should accept classes for all table elements."""
        css = TableCssConfig(
            table=["data-grid"],
            thead=["sticky-header"],
            tbody=["scrollable"],
            row=["hover-effect"],
            row_alt=["alternate-row"],
        )
        assert css.table == ["data-grid"]
        assert css.thead == ["sticky-header"]
        assert css.tbody == ["scrollable"]
        assert css.row == ["hover-effect"]
        assert css.row_alt == ["alternate-row"]


class TestColumnDefinitionCss:
    """Tests for CSS fields on ColumnDefinition."""

    def test_column_with_css_class(self):
        """Column should accept css_class for header styling."""
        col = ColumnDefinition(
            id="logo",
            name="Logo",
            type=ColumnType.TEXT,
            css_class="holdings-table__cell--logo sortable",
        )
        assert col.css_class == "holdings-table__cell--logo sortable"

    def test_column_with_cell_css_class(self):
        """Column should accept cell_css_class for body cell styling."""
        col = ColumnDefinition(
            id="value",
            name="Value",
            type=ColumnType.NUMBER,
            cell_css_class="holdings-table__cell--number align-right",
        )
        assert col.cell_css_class == "holdings-table__cell--number align-right"

    def test_column_with_both_css_classes(self):
        """Column should accept both header and cell CSS classes."""
        col = ColumnDefinition(
            id="value",
            name="Value",
            type=ColumnType.NUMBER,
            css_class="sortable",
            cell_css_class="text-right",
        )
        assert col.css_class == "sortable"
        assert col.cell_css_class == "text-right"

    def test_column_css_classes_default_none(self):
        """CSS class fields should default to None."""
        col = ColumnDefinition(id="name", name="Name", type=ColumnType.TEXT)
        assert col.css_class is None
        assert col.cell_css_class is None


class TestTableConfigWithCss:
    """Tests for TableConfig with CSS configuration."""

    def test_table_config_has_css_field(self):
        """TableConfig should have a css field."""
        cols = [
            ColumnDefinition(id="id", name="ID", type=ColumnType.NUMBER),
        ]
        tc = TableConfig(table_name="users", columns=cols)
        assert hasattr(tc, "css")
        assert isinstance(tc.css, TableCssConfig)

    def test_table_config_css_default_empty(self):
        """TableConfig.css should default to empty TableCssConfig."""
        cols = [
            ColumnDefinition(id="id", name="ID", type=ColumnType.NUMBER),
        ]
        tc = TableConfig(table_name="users", columns=cols)
        assert tc.css.table == []
        assert tc.css.thead == []

    def test_table_config_with_custom_css(self):
        """TableConfig should accept custom CSS configuration."""
        cols = [
            ColumnDefinition(
                id="id",
                name="ID",
                type=ColumnType.NUMBER,
                css_class="col-id",
                cell_css_class="cell-id",
            ),
        ]
        css = TableCssConfig(
            table=["holdings-table"],
            thead=["sticky-header"],
        )
        tc = TableConfig(table_name="users", columns=cols, css=css)
        assert tc.css.table == ["holdings-table"]
        assert tc.css.thead == ["sticky-header"]
        assert tc.columns[0].css_class == "col-id"
        assert tc.columns[0].cell_css_class == "cell-id"


class TestTableTemplateRendering:
    """Tests for table template rendering with CSS classes."""

    def test_render_table_with_css_classes(self):
        """Table template should apply custom CSS classes."""
        from declaro_tablix.templates import render_table

        cols = [
            ColumnDefinition(
                id="id",
                name="ID",
                type=ColumnType.NUMBER,
                css_class="col-id sortable",
                cell_css_class="cell-id align-right",
            ),
            ColumnDefinition(
                id="name",
                name="Name",
                type=ColumnType.TEXT,
            ),
        ]
        css = TableCssConfig(
            table=["holdings-table", "striped"],
            thead=["sticky-header"],
            tbody=["scrollable-body"],
            row=["hover-row"],
        )
        config = TableConfig(table_name="test", columns=cols, css=css)
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        html = render_table(config, data)

        # Table element has custom classes
        assert 'class="tablix-table holdings-table striped"' in html
        # Thead has custom classes
        assert 'class="tablix-thead sticky-header"' in html
        # Tbody has custom classes
        assert 'class="tablix-tbody scrollable-body"' in html
        # Row has custom classes
        assert 'class="tablix-tr hover-row"' in html
        # Header cell has custom classes
        assert 'class="tablix-th col-id sortable"' in html
        # Body cell has custom classes
        assert 'class="tablix-td cell-id align-right"' in html
        # Header cell without custom class still has base class
        assert 'class="tablix-th"' in html

    def test_render_table_without_custom_css(self):
        """Table template should work without custom CSS classes."""
        from declaro_tablix.templates import render_table

        cols = [
            ColumnDefinition(id="id", name="ID", type=ColumnType.NUMBER),
        ]
        config = TableConfig(table_name="test", columns=cols)
        data = [{"id": 1}]

        html = render_table(config, data)

        assert 'class="tablix-table"' in html
        assert 'class="tablix-thead"' in html
        assert 'class="tablix-tbody"' in html
        assert 'class="tablix-tr"' in html
        assert 'class="tablix-th"' in html
        assert 'class="tablix-td"' in html
