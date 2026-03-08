# declaro-tablix

Declarative table and filter system with CSS Grid layouts.

## Features

- **Domain Models**: Pydantic models for table configuration, filtering, sorting, and pagination
- **CSS Grid Layouts**: Viewport-scaling filter layouts that maintain pixel-perfect proportions
- **Jinja2 Templates**: HTMX-powered filter control templates
- **Database Integration**: declaro-persistum models for storing configurations
- **HTMX Sorting**: Click-to-sort column headers with visual indicators
- **FastAPI Integration**: Ready-to-mount router for table operations
- **Redis Caching**: Per-user sort state caching

## Installation

```bash
pip install declaro-tablix
```

## Quick Start

```python
from declaro_tablix import (
    FilterControlConfig,
    FilterControlType,
    FilterLayoutConfig,
    TableConfig,
    ColumnDefinition,
)
from declaro_tablix.templates import render_table_ui

# Define table configuration
table_config = TableConfig(
    table_name="users",
    columns=[
        ColumnDefinition(id="name", name="Name", type="text", sortable=True),
        ColumnDefinition(id="status", name="Status", type="text", filterable=True),
    ],
)

# Define filter layout
filter_layout = FilterLayoutConfig(
    id="user-filters",
    table_id="users",
    grid_template_columns="auto 1fr 1fr auto",
    controls=[
        FilterControlConfig(
            id="search",
            control_type=FilterControlType.SEARCH_INPUT,
            column_id="name",
            placeholder="Search users...",
        ),
        FilterControlConfig(
            id="status",
            control_type=FilterControlType.CHECKBOX_GROUP,
            column_id="status",
            options_static=["Active", "Pending", "Inactive"],
        ),
    ],
)

# Render complete UI with filters and table
html = render_table_ui(
    config=table_config,
    data=[{"name": "Alice", "status": "Active"}],
    filter_layout=filter_layout,
)
```

## Sorting

tablix provides click-to-sort column headers with HTMX integration.

### Enabling Sorting on Columns

By default, columns are sortable. You can disable sorting per-column:

```python
from declaro_tablix.domain.models import ColumnConfig

columns = [
    ColumnConfig(id="name", name="Name", sortable=True),  # Default: sortable
    ColumnConfig(id="actions", name="Actions", sortable=False),  # Disabled
]
```

### How Sort Buttons Work

The table template renders sortable columns with HTMX attributes:

```html
<button class="tablix-sort-btn"
        hx-get="/tables/data?sort=name&dir=asc"
        hx-target="closest table"
        hx-swap="outerHTML">
    Name <span class="tablix-sort-indicator">&#9650;</span>
</button>
```

Clicking toggles between ascending/descending. The indicator (▰▬↑) shows current direction.

### Per-User Sort State Caching

tablix caches sort state per-user in Redis:

```python
from declaro_tablix.caching import CacheService, generate_cache_key, DEFAULT_TTL

cache = CacheService(redis_url="redis://localhost:6379/0")

# Generate a cache key
key = generate_cache_key(
    table_name="users",
    user_id="user123",
    sort_column="name",
    sort_direction="asc"
)

# Cache table data (5 min TTL by default)
cache.cache_table_data(key, {"rows": [...]})

# Retrieve cached data
data = cache.get_cached_table_data(key)
```

## Filter Control Types

### Interactive Filters
- `search_input`: Text search with debounce
- `checkbox_group`: Inline checkboxes for multi-select
- `multi_select`: Dropdown with multiple selection (supports `searchable=True`)
- `number_range`: Min/max number inputs
- `date_range`: From/to date inputs
- `single_select`: Standard dropdown
- `tab_filter`: Horizontal tabs with optional counts (e.g., "All (100) | To Review (33)")
- `action_button`: Action buttons (clear search, reset filters)

### Static Display Controls
- `static_text`: Display static text content (for labels, instructions)
- `static_image`: Display static images (for logos, branding)

### Calculated Display Controls
- `total_absolute`: Show aggregate totals from all data
- `total_visible`: Show totals from filtered/visible data
- `calculated_field`: Display calculated values with optional badge styling based on thresholds

### Tab Filter Example

```python
from declaro_tablix.domain.filter_layout import (
    FilterControlConfig, FilterControlType, FilterLayoutConfig, FilterOption
)

# Define filter layout with tab filter
filter_layout = FilterLayoutConfig(
    id="orders-filters",
    table_id="orders",
    controls=[
        FilterControlConfig(
            id="status-tabs",
            control_type=FilterControlType.TAB_FILTER,
            column_id="status",
            options_source="statuses",
            grid_column="1 / -1",  # Span full width
        ),
    ],
)

# Provide options with counts
options = {
    "statuses": [
        FilterOption(value="all", label="All", count=150),
        FilterOption(value="pending", label="Pending", count=42),
        FilterOption(value="completed", label="Completed", count=108),
    ]
}
```

## FastAPI Integration

tablix provides a ready-to-mount FastAPI router:

```python
from fastapi import FastAPI
from declaro_tablix.routes import table_router

app = FastAPI()
app.include_router(table_router, prefix="/tables", tags=["tables"])
```

### Available Endpoints

| Endpoint | Method | Description |
|---------|--------|-------------|
| `/tables/health` | GET | Health check |
| `/tables/data` | POST | Get table data with sorting/pagination |

## API Reference

### Modules

- **`domain/`**: Pydantic models for tables, columns, filters, sorting
- **`templates/`**: Jinja2 templates and macros for HTML rendering
- **`routes/`**: FastAPI router and Pydantic request/response models
- **`services/`**: Table service functions (sorting, filtering)
- **`caching/`**: Redis cache layer for per-user state
- **`repositories/`**: Persistum-backed data access

### Key Classes

```python
from declaro_tablix.domain.models import (
    TableConfig,
    ColumnConfig,
    SortDirection,
    SortDefinition,
    FilterControlConfig,
    FilterLayoutConfig,
)

from declaro_tablix.services import sort_table_data
from declaro_tablix.caching import CacheService, generate_cache_key
from declaro_tablix.routes import table_router
```

## CSS Variables

The package includes CSS variables for viewport-based scaling:

```css
:root {
  --filter-base-width: 1920;
  --filter-scale: calc(100vw / var(--filter-base-width) * 1px);
  --filter-gap: clamp(8px, calc(16 * var(--filter-scale)), 24px);
  /* ... */
}
```

## License

MIT
