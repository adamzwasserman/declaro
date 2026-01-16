"""Jinja2 templates for declaro-tablix.

This module provides access to the filter templates and a configured
Jinja2 environment for rendering.
"""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

# Path to the templates directory
TEMPLATES_DIR = Path(__file__).parent

# Pre-configured Jinja2 environment
_env: Environment | None = None


def get_jinja_env() -> Environment:
    """Get the Jinja2 environment for rendering filter templates.

    Returns:
        Configured Jinja2 Environment instance.
    """
    global _env
    if _env is None:
        _env = Environment(
            loader=FileSystemLoader(TEMPLATES_DIR),
            autoescape=select_autoescape(["html", "xml"]),
            trim_blocks=True,
            lstrip_blocks=True,
        )
    return _env


def render_filter_layout(
    layout: "FilterLayoutConfig",
    state: "FilterState | None" = None,
    options: dict[str, list] | None = None,
) -> str:
    """Render a filter layout to HTML.

    Args:
        layout: FilterLayoutConfig defining the layout
        state: Optional FilterState with current filter values
        options: Optional dict of options keyed by options_source name

    Returns:
        Rendered HTML string
    """
    env = get_jinja_env()
    template = env.get_template("components/filter_layout.html")
    return template.render(
        layout=layout,
        state=state,
        options=options or {},
    )


def render_table(
    config: "TableConfig",
    data: list[dict],
) -> str:
    """Render a table to HTML with CSS class passthrough.

    Args:
        config: TableConfig with columns, css, and other settings
        data: List of row dictionaries to render

    Returns:
        Rendered HTML string
    """
    env = get_jinja_env()
    template = env.get_template("components/table.html")
    return template.render(
        config=config,
        data=data,
    )


def render_table_ui(
    config: "TableConfig",
    data: list[dict],
    header: "TableHeaderConfig | None" = None,
    filter_layout: "FilterLayoutConfig | None" = None,
    filter_state: "FilterState | None" = None,
    filter_options: dict[str, list] | None = None,
) -> str:
    """Render complete table UI with optional header, filters, and table.

    Convenience function to render a complete table interface with all sections.

    Args:
        config: TableConfig with columns, css, and other settings
        data: List of row dictionaries to render
        header: Optional TableHeaderConfig for header/branding section
        filter_layout: Optional FilterLayoutConfig for filter controls
        filter_state: Optional FilterState with current filter values
        filter_options: Optional dict of options keyed by options_source name

    Returns:
        Rendered HTML string containing all requested sections
    """
    sections = []

    # Render header section if provided
    if header:
        env = get_jinja_env()
        header_template = env.get_template("components/header.html")
        header_html = header_template.render(header=header)
        sections.append(header_html)

    # Render filter section if provided
    if filter_layout:
        filter_html = render_filter_layout(
            layout=filter_layout,
            state=filter_state,
            options=filter_options,
        )
        sections.append(filter_html)

    # Always render table section
    table_html = render_table(config=config, data=data)
    sections.append(table_html)

    # Combine all sections
    return "\n".join(sections)


# Import types for type hints (avoid circular import at runtime)
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from declaro_tablix.domain.filter_layout import FilterLayoutConfig, FilterState
    from declaro_tablix.domain.models import TableConfig, TableHeaderConfig


__all__ = [
    "TEMPLATES_DIR",
    "get_jinja_env",
    "render_filter_layout",
    "render_table",
    "render_table_ui",
]
