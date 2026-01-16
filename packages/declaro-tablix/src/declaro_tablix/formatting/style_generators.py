"""
CSS generation functions for TableV2 formatting.

This module provides function-based CSS generation for table styling,
following the function-based architecture pattern.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from .formatting_strategy import FormattingContext, FormattingStrategy


class TableTheme(Enum):
    """Available table themes."""

    DEFAULT = "default"
    DARK = "dark"
    LIGHT = "light"
    MINIMAL = "minimal"
    PROFESSIONAL = "professional"
    COLORFUL = "colorful"


class TextAlignment(Enum):
    """Text alignment options."""

    LEFT = "left"
    CENTER = "center"
    RIGHT = "right"
    JUSTIFY = "justify"


@dataclass
class StyleConfig:
    """Configuration for CSS generation."""

    theme: TableTheme = TableTheme.DEFAULT
    responsive: bool = True
    compact: bool = False
    zebra_stripes: bool = True
    hover_effects: bool = True
    border_style: str = "1px solid #e0e0e0"
    font_family: str = "system-ui, -apple-system, sans-serif"
    font_size: str = "14px"
    custom_classes: Dict[str, str] = None

    def __post_init__(self):
        if self.custom_classes is None:
            self.custom_classes = {}


# Global CSS class registry
_CSS_CLASS_REGISTRY: Dict[str, str] = {}


def register_css_class(class_name: str, css_definition: str) -> None:
    """
    Register a CSS class definition.

    Args:
        class_name: The CSS class name
        css_definition: The CSS definition string
    """
    _CSS_CLASS_REGISTRY[class_name] = css_definition


def get_css_class(class_name: str) -> Optional[str]:
    """
    Get a registered CSS class definition.

    Args:
        class_name: The CSS class name

    Returns:
        The CSS definition or None if not found
    """
    return _CSS_CLASS_REGISTRY.get(class_name)


def generate_table_css(table_id: str, config: StyleConfig, context: FormattingContext) -> str:
    """
    Generate CSS for the main table element.

    Args:
        table_id: Unique identifier for the table
        config: Style configuration
        context: Formatting context

    Returns:
        CSS string for the table
    """
    theme_colors = get_theme_colors(config.theme)

    css_parts = [
        f"#{table_id} {{",
        f"  width: 100%;",
        f"  border-collapse: collapse;",
        f"  font-family: {config.font_family};",
        f"  font-size: {config.font_size};",
        f"  background-color: {theme_colors['background']};",
        f"  color: {theme_colors['text']};",
        f"  border: {config.border_style};",
        f"}}",
    ]

    # Add responsive styling
    if config.responsive:
        css_parts.extend([f"@media (max-width: 768px) {{", f"  #{table_id} {{", f"    font-size: 12px;", f"  }}", f"}}"])

    # Add compact styling
    if config.compact or context.strategy == FormattingStrategy.COMPACT:
        css_parts.extend([f"#{table_id} td, #{table_id} th {{", f"  padding: 4px 8px;", f"}}"])
    else:
        css_parts.extend([f"#{table_id} td, #{table_id} th {{", f"  padding: 8px 12px;", f"}}"])

    return "\n".join(css_parts)


def generate_header_css(table_id: str, config: StyleConfig, context: FormattingContext) -> str:
    """
    Generate CSS for table headers.

    Args:
        table_id: Unique identifier for the table
        config: Style configuration
        context: Formatting context

    Returns:
        CSS string for table headers
    """
    theme_colors = get_theme_colors(config.theme)

    css_parts = [
        f"#{table_id} thead th {{",
        f"  background-color: {theme_colors['header_bg']};",
        f"  color: {theme_colors['header_text']};",
        f"  font-weight: 600;",
        f"  text-align: left;",
        f"  border-bottom: 2px solid {theme_colors['border']};",
        f"  position: sticky;",
        f"  top: 0;",
        f"  z-index: 10;",
        f"}}",
    ]

    # Add sortable header styling
    css_parts.extend(
        [
            f"#{table_id} thead th.sortable {{",
            f"  cursor: pointer;",
            f"  user-select: none;",
            f"}}",
            f"#{table_id} thead th.sortable:hover {{",
            f"  background-color: {theme_colors['header_hover']};",
            f"}}",
        ]
    )

    return "\n".join(css_parts)


def generate_row_css(table_id: str, config: StyleConfig, context: FormattingContext) -> str:
    """
    Generate CSS for table rows.

    Args:
        table_id: Unique identifier for the table
        config: Style configuration
        context: Formatting context

    Returns:
        CSS string for table rows
    """
    theme_colors = get_theme_colors(config.theme)

    css_parts = [f"#{table_id} tbody tr {{", f"  border-bottom: 1px solid {theme_colors['border']};", f"}}"]

    # Add zebra stripes
    if config.zebra_stripes:
        css_parts.extend(
            [f"#{table_id} tbody tr:nth-child(even) {{", f"  background-color: {theme_colors['row_alternate']};", f"}}"]
        )

    # Add hover effects
    if config.hover_effects:
        css_parts.extend(
            [
                f"#{table_id} tbody tr:hover {{",
                f"  background-color: {theme_colors['row_hover']};",
                f"  cursor: pointer;",
                f"}}",
            ]
        )

    return "\n".join(css_parts)


def generate_cell_css(table_id: str, column_type: str, config: StyleConfig, context: FormattingContext) -> str:
    """
    Generate CSS for table cells based on column type.

    Args:
        table_id: Unique identifier for the table
        column_type: The column type (TEXT, NUMBER, etc.)
        config: Style configuration
        context: Formatting context

    Returns:
        CSS string for table cells
    """
    theme_colors = get_theme_colors(config.theme)
    alignment = get_default_alignment_for_type(column_type)

    css_parts = [
        f"#{table_id} td.{column_type.lower()}-cell {{",
        f"  text-align: {alignment.value};",
        f"  vertical-align: middle;",
        f"  border: none;",
        f"}}",
    ]

    # Add type-specific styling
    if column_type == "NUMBER" or column_type == "CURRENCY":
        css_parts.extend(
            [
                f"#{table_id} td.{column_type.lower()}-cell {{",
                f"  font-family: 'SF Mono', Consolas, monospace;",
                f"  font-variant-numeric: tabular-nums;",
                f"}}",
            ]
        )
    elif column_type == "DATE" or column_type == "DATETIME":
        css_parts.extend([f"#{table_id} td.{column_type.lower()}-cell {{", f"  white-space: nowrap;", f"}}"])
    elif column_type == "BOOLEAN":
        css_parts.extend([f"#{table_id} td.{column_type.lower()}-cell {{", f"  font-weight: 500;", f"}}"])
    elif column_type == "URL":
        css_parts.extend(
            [
                f"#{table_id} td.{column_type.lower()}-cell a {{",
                f"  color: {theme_colors['link']};",
                f"  text-decoration: none;",
                f"}}",
                f"#{table_id} td.{column_type.lower()}-cell a:hover {{",
                f"  text-decoration: underline;",
                f"}}",
            ]
        )
    elif column_type == "EMAIL":
        css_parts.extend(
            [
                f"#{table_id} td.{column_type.lower()}-cell a {{",
                f"  color: {theme_colors['link']};",
                f"  text-decoration: none;",
                f"}}",
                f"#{table_id} td.{column_type.lower()}-cell a:hover {{",
                f"  text-decoration: underline;",
                f"}}",
            ]
        )

    return "\n".join(css_parts)


def generate_conditional_css(table_id: str, conditions: List[Dict[str, Any]], config: StyleConfig) -> str:
    """
    Generate CSS for conditional formatting rules.

    Args:
        table_id: Unique identifier for the table
        conditions: List of conditional formatting rules
        config: Style configuration

    Returns:
        CSS string for conditional formatting
    """
    css_parts = []

    for i, condition in enumerate(conditions):
        class_name = f"condition-{i}"

        css_parts.extend(
            [
                f"#{table_id} td.{class_name} {{",
                f"  background-color: {condition.get('background_color', 'transparent')};",
                f"  color: {condition.get('text_color', 'inherit')};",
                f"  font-weight: {condition.get('font_weight', 'normal')};",
                f"  border-left: {condition.get('border_left', 'none')};",
                f"}}",
            ]
        )

    return "\n".join(css_parts)


def generate_responsive_css(table_id: str, config: StyleConfig) -> str:
    """
    Generate responsive CSS for different screen sizes.

    Args:
        table_id: Unique identifier for the table
        config: Style configuration

    Returns:
        CSS string for responsive design
    """
    css_parts = []

    # Mobile breakpoint
    css_parts.extend(
        [
            f"@media (max-width: 768px) {{",
            f"  #{table_id} {{",
            f"    font-size: 12px;",
            f"  }}",
            f"  #{table_id} th, #{table_id} td {{",
            f"    padding: 6px 8px;",
            f"  }}",
            f"  #{table_id} .hide-mobile {{",
            f"    display: none;",
            f"  }}",
            f"}}",
        ]
    )

    # Tablet breakpoint
    css_parts.extend(
        [
            f"@media (min-width: 769px) and (max-width: 1024px) {{",
            f"  #{table_id} {{",
            f"    font-size: 13px;",
            f"  }}",
            f"  #{table_id} .hide-tablet {{",
            f"    display: none;",
            f"  }}",
            f"}}",
        ]
    )

    # Desktop breakpoint
    css_parts.extend(
        [f"@media (min-width: 1025px) {{", f"  #{table_id} .hide-desktop {{", f"    display: none;", f"  }}", f"}}"]
    )

    return "\n".join(css_parts)


def generate_theme_css(table_id: str, theme: TableTheme, config: StyleConfig) -> str:
    """
    Generate theme-specific CSS.

    Args:
        table_id: Unique identifier for the table
        theme: The theme to apply
        config: Style configuration

    Returns:
        CSS string for the theme
    """
    theme_colors = get_theme_colors(theme)

    css_parts = [
        f"#{table_id}.theme-{theme.value} {{",
        f"  background-color: {theme_colors['background']};",
        f"  color: {theme_colors['text']};",
        f"}}",
        f"#{table_id}.theme-{theme.value} thead th {{",
        f"  background-color: {theme_colors['header_bg']};",
        f"  color: {theme_colors['header_text']};",
        f"}}",
        f"#{table_id}.theme-{theme.value} tbody tr:nth-child(even) {{",
        f"  background-color: {theme_colors['row_alternate']};",
        f"}}",
        f"#{table_id}.theme-{theme.value} tbody tr:hover {{",
        f"  background-color: {theme_colors['row_hover']};",
        f"}}",
    ]

    return "\n".join(css_parts)


def get_theme_colors(theme: TableTheme) -> Dict[str, str]:
    """
    Get color palette for a specific theme.

    Args:
        theme: The theme to get colors for

    Returns:
        Dictionary of color values
    """
    themes = {
        TableTheme.DEFAULT: {
            "background": "#ffffff",
            "text": "#333333",
            "header_bg": "#f8f9fa",
            "header_text": "#212529",
            "header_hover": "#e9ecef",
            "border": "#e0e0e0",
            "row_alternate": "#f8f9fa",
            "row_hover": "#e8f4f8",
            "link": "#0066cc",
        },
        TableTheme.DARK: {
            "background": "#2d3748",
            "text": "#e2e8f0",
            "header_bg": "#4a5568",
            "header_text": "#f7fafc",
            "header_hover": "#718096",
            "border": "#4a5568",
            "row_alternate": "#374151",
            "row_hover": "#4a5568",
            "link": "#63b3ed",
        },
        TableTheme.LIGHT: {
            "background": "#f7fafc",
            "text": "#2d3748",
            "header_bg": "#edf2f7",
            "header_text": "#1a202c",
            "header_hover": "#e2e8f0",
            "border": "#e2e8f0",
            "row_alternate": "#ffffff",
            "row_hover": "#edf2f7",
            "link": "#3182ce",
        },
        TableTheme.MINIMAL: {
            "background": "#ffffff",
            "text": "#333333",
            "header_bg": "#ffffff",
            "header_text": "#333333",
            "header_hover": "#f5f5f5",
            "border": "#e0e0e0",
            "row_alternate": "transparent",
            "row_hover": "#f5f5f5",
            "link": "#0066cc",
        },
        TableTheme.PROFESSIONAL: {
            "background": "#ffffff",
            "text": "#2c3e50",
            "header_bg": "#34495e",
            "header_text": "#ffffff",
            "header_hover": "#2c3e50",
            "border": "#bdc3c7",
            "row_alternate": "#f8f9fa",
            "row_hover": "#e8f4f8",
            "link": "#2980b9",
        },
        TableTheme.COLORFUL: {
            "background": "#ffffff",
            "text": "#333333",
            "header_bg": "#667eea",
            "header_text": "#ffffff",
            "header_hover": "#5a67d8",
            "border": "#e0e0e0",
            "row_alternate": "#f0f4f8",
            "row_hover": "#e8f4f8",
            "link": "#0066cc",
        },
    }

    return themes.get(theme, themes[TableTheme.DEFAULT])


def get_default_alignment_for_type(column_type: str) -> TextAlignment:
    """
    Get the default text alignment for a column type.

    Args:
        column_type: The column type

    Returns:
        Default text alignment
    """
    alignment_map = {
        "TEXT": TextAlignment.LEFT,
        "NUMBER": TextAlignment.RIGHT,
        "CURRENCY": TextAlignment.RIGHT,
        "PERCENTAGE": TextAlignment.RIGHT,
        "DATE": TextAlignment.CENTER,
        "DATETIME": TextAlignment.CENTER,
        "BOOLEAN": TextAlignment.CENTER,
        "EMAIL": TextAlignment.LEFT,
        "URL": TextAlignment.LEFT,
        "PHONE": TextAlignment.LEFT,
        "JSON": TextAlignment.LEFT,
    }

    return alignment_map.get(column_type, TextAlignment.LEFT)


def build_css_class_map(
    table_id: str, column_types: List[str], config: StyleConfig, context: FormattingContext
) -> Dict[str, str]:
    """
    Build a complete CSS class mapping for a table.

    Args:
        table_id: Unique identifier for the table
        column_types: List of column types in the table
        config: Style configuration
        context: Formatting context

    Returns:
        Dictionary mapping CSS classes to their definitions
    """
    css_map = {}

    # Generate base table CSS
    css_map["table"] = generate_table_css(table_id, config, context)
    css_map["header"] = generate_header_css(table_id, config, context)
    css_map["row"] = generate_row_css(table_id, config, context)
    css_map["theme"] = generate_theme_css(table_id, config.theme, config)

    # Generate column-specific CSS
    for column_type in column_types:
        css_map[f"cell_{column_type.lower()}"] = generate_cell_css(table_id, column_type, config, context)

    # Add responsive CSS
    if config.responsive:
        css_map["responsive"] = generate_responsive_css(table_id, config)

    return css_map


def optimize_css_output(css_map: Dict[str, str]) -> str:
    """
    Optimize and combine CSS output.

    Args:
        css_map: Dictionary of CSS definitions

    Returns:
        Optimized CSS string
    """
    # Combine all CSS parts
    css_parts = []
    for key, css_definition in css_map.items():
        if css_definition.strip():
            css_parts.append(css_definition)

    # Join and optimize
    combined_css = "\n\n".join(css_parts)

    # Remove extra whitespace and empty lines
    optimized_lines = []
    for line in combined_css.split("\n"):
        line = line.strip()
        if line:
            optimized_lines.append(line)

    return "\n".join(optimized_lines)


def generate_print_css(table_id: str, config: StyleConfig) -> str:
    """
    Generate CSS optimized for printing.

    Args:
        table_id: Unique identifier for the table
        config: Style configuration

    Returns:
        CSS string for print media
    """
    css_parts = [
        f"@media print {{",
        f"  #{table_id} {{",
        f"    background: white !important;",
        f"    color: black !important;",
        f"    border: 1px solid black;",
        f"  }}",
        f"  #{table_id} thead th {{",
        f"    background: white !important;",
        f"    color: black !important;",
        f"    border-bottom: 2px solid black;",
        f"  }}",
        f"  #{table_id} tbody tr {{",
        f"    border-bottom: 1px solid black;",
        f"  }}",
        f"  #{table_id} tbody tr:nth-child(even) {{",
        f"    background: #f5f5f5 !important;",
        f"  }}",
        f"  #{table_id} .hide-print {{",
        f"    display: none !important;",
        f"  }}",
        f"}}",
    ]

    return "\n".join(css_parts)


def create_style_config(
    theme: TableTheme = TableTheme.DEFAULT, responsive: bool = True, compact: bool = False, **kwargs
) -> StyleConfig:
    """
    Create a StyleConfig with the specified options.

    Args:
        theme: The table theme
        responsive: Whether to include responsive CSS
        compact: Whether to use compact styling
        **kwargs: Additional configuration options

    Returns:
        StyleConfig object
    """
    return StyleConfig(theme=theme, responsive=responsive, compact=compact, **kwargs)
