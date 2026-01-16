"""CSS styling for declaro-tablix.

This module contains CSS files for filter layouts:
- filter_variables.css: CSS variables with viewport scaling
- filter_components.css: Component styles
"""

from pathlib import Path

# Path to the styling directory for easy access
STYLING_DIR = Path(__file__).parent

# CSS file paths
FILTER_VARIABLES_CSS = STYLING_DIR / "filter_variables.css"
FILTER_COMPONENTS_CSS = STYLING_DIR / "filter_components.css"


def get_filter_css() -> str:
    """Get the complete CSS for filter layouts.

    Returns:
        Combined CSS string with variables and component styles.
    """
    css_parts = []

    if FILTER_VARIABLES_CSS.exists():
        css_parts.append(FILTER_VARIABLES_CSS.read_text())

    if FILTER_COMPONENTS_CSS.exists():
        css_parts.append(FILTER_COMPONENTS_CSS.read_text())

    return "\n\n".join(css_parts)


__all__ = [
    "STYLING_DIR",
    "FILTER_VARIABLES_CSS",
    "FILTER_COMPONENTS_CSS",
    "get_filter_css",
]
