"""Tablix Demo App - Interactive filter configuration playground with SQLite backend."""

import sys
sys.path.insert(0, "/Users/adam/dev/declaro/packages/declaro-persistum/src")

from contextlib import asynccontextmanager
from pathlib import Path
import importlib.util

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse

from declaro_persistum.pool import ConnectionPool
from declaro_persistum.query import execute, raw

# Direct file imports to avoid main package __init__.py import chain
# (which has dependencies on declaro_advise that may not be installed)
TABLIX_SRC = "/Users/adam/dev/declaro/packages/declaro-tablix/src/declaro_tablix"


def _load_module(name: str, path: str):
    """Load a Python module directly from file path."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # Register in sys.modules for subsequent imports
    spec.loader.exec_module(module)
    return module


# Load domain models module
_models = _load_module("declaro_tablix.domain.models", f"{TABLIX_SRC}/domain/models.py")
TableConfig = _models.TableConfig
ColumnDefinition = _models.ColumnDefinition
ColumnType = _models.ColumnType

# Load filter_layout module
_filter_layout = _load_module("declaro_tablix.domain.filter_layout", f"{TABLIX_SRC}/domain/filter_layout.py")
FilterLayoutConfig = _filter_layout.FilterLayoutConfig
FilterControlConfig = _filter_layout.FilterControlConfig
FilterControlGroup = _filter_layout.FilterControlGroup
FilterControlType = _filter_layout.FilterControlType
FilterPosition = _filter_layout.FilterPosition
FilterOption = _filter_layout.FilterOption

# Load templates module
_templates = _load_module("declaro_tablix.templates", f"{TABLIX_SRC}/templates/__init__.py")
get_jinja_env = _templates.get_jinja_env
get_fmtx_script_tag = _templates.get_fmtx_script_tag

# Database path
DB_PATH = Path(__file__).parent / "demo.db"

# Global pool
pool = None


async def init_db():
    """Initialize SQLite database with holdings table."""
    global pool
    pool = await ConnectionPool.sqlite(str(DB_PATH))

    async with pool.acquire() as conn:
        # Create holdings table with review_status for tab filter demo
        await execute(raw("""
            CREATE TABLE IF NOT EXISTS holdings (
                id INTEGER PRIMARY KEY,
                ticker TEXT NOT NULL,
                name TEXT NOT NULL,
                sector TEXT NOT NULL,
                value REAL NOT NULL,
                change REAL NOT NULL,
                review_status TEXT NOT NULL DEFAULT 'to_review'
            )
        """), conn)

        # Check if data exists
        result = await execute(raw("SELECT COUNT(*) as cnt FROM holdings"), conn)
        if result[0]["cnt"] == 0:
            # Seed data with review_status (id, ticker, name, sector, value, change, review_status)
            holdings = [
                (1, "AAPL", "Apple Inc.", "Technology", 150000, 2.5, "reviewed"),
                (2, "GOOGL", "Alphabet Inc.", "Technology", 200000, -1.2, "reviewed"),
                (3, "JPM", "JPMorgan Chase", "Financials", 180000, 0.8, "to_review"),
                (4, "BAC", "Bank of America", "Financials", 120000, -0.5, "to_review"),
                (5, "XOM", "Exxon Mobil", "Energy", 95000, 3.1, "reviewed"),
                (6, "CVX", "Chevron", "Energy", 88000, 2.8, "to_review"),
                (7, "JNJ", "Johnson & Johnson", "Healthcare", 165000, 0.3, "reviewed"),
                (8, "PFE", "Pfizer", "Healthcare", 72000, -2.1, "to_review"),
                (9, "PG", "Procter & Gamble", "Consumer", 140000, 1.1, "reviewed"),
                (10, "KO", "Coca-Cola", "Consumer", 110000, 0.6, "reviewed"),
            ]
            for h in holdings:
                await execute(raw(
                    "INSERT INTO holdings (id, ticker, name, sector, value, change, review_status) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    params=h
                ), conn)
            await conn.commit()


async def close_db():
    """Close database pool."""
    global pool
    if pool:
        await pool.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown."""
    await init_db()
    yield
    await close_db()


app = FastAPI(title="Tablix Demo", lifespan=lifespan)


def get_table_config() -> TableConfig:
    """Build table configuration."""
    return TableConfig(
        table_name="holdings",
        table_id_override="demo-table",
        row_id_template="holding-row-{row_idx}",
        columns=[
            ColumnDefinition(id="ticker", name="Ticker", type=ColumnType.TEXT, cell_id_template="ticker-{row_idx}"),
            ColumnDefinition(id="name", name="Company", type=ColumnType.TEXT),
            ColumnDefinition(id="sector", name="Sector", type=ColumnType.TEXT),
            ColumnDefinition(
                id="value",
                name="Value",
                type=ColumnType.CURRENCY,
                format_options={"currency": "USD", "decimals": 0},
            ),
            ColumnDefinition(
                id="change",
                name="Change %",
                type=ColumnType.PERCENTAGE,
                format_options={"decimals": 1},
            ),
        ],
    )


def get_filter_config(
    show_search: bool = True,
    show_sector_filter: bool = True,
    sector_searchable: bool = False,
    auto_submit: bool = True,
    show_tab_filter: bool = True,
) -> FilterLayoutConfig:
    """Build filter layout configuration based on options."""
    controls = []

    # Tab filter spans full width on row 1
    if show_tab_filter:
        controls.append(
            FilterControlConfig(
                id="review-status",
                control_type=FilterControlType.TAB_FILTER,
                column_id="review_status",
                hx_get="/table",
                hx_target="#table-section",
                auto_submit=auto_submit,
                options_source="review_statuses",
                grid_column="1 / -1",  # Span all columns
                grid_row="1",
            )
        )

    # Group search and sector filter together in the left column (row 2)
    left_controls = []

    if show_search:
        left_controls.append(
            FilterControlConfig(
                id="search",
                control_type=FilterControlType.SEARCH_INPUT,
                column_id="name",
                label="Search",
                placeholder="Search holdings...",
                hx_get="/table",
                hx_target="#table-section",
                hx_trigger="keyup changed delay:300ms",
                auto_submit=auto_submit,
                debounce_ms=300,
            )
        )

    if show_sector_filter:
        left_controls.append(
            FilterControlConfig(
                id="sector",
                control_type=FilterControlType.MULTI_SELECT,
                column_id="sector",
                label="Sector",
                placeholder="All Sectors",
                hx_get="/table",
                hx_target="#table-section",
                auto_submit=auto_submit,
                searchable=sector_searchable,
                options_source="sectors",
            )
        )

    if left_controls:
        controls.append(
            FilterControlGroup(
                id="left-filters",
                position=FilterPosition.LEFT,
                controls=left_controls,
                gap="1rem",
                grid_row="2",
            )
        )

    return FilterLayoutConfig(
        id="holdings-filters",
        table_id="holdings",
        container_id_override="filter-container",
        controls=controls,
    )


@app.get("/", response_class=HTMLResponse)
async def demo_page():
    """Render the demo page."""
    fmtx_script = get_fmtx_script_tag()
    return """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Tablix Demo</title>
    <script src="https://unpkg.com/htmx.org@1.9.10"></script>
    """ + fmtx_script + """
    <script src="https://unpkg.com/alpinejs@3.13.3/dist/cdn.min.js" defer></script>
    <style>
        * { box-sizing: border-box; }
        body { font-family: system-ui, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .demo-container { max-width: 1200px; margin: 0 auto; }
        h1 { margin-bottom: 20px; }

        .config-panel {
            background: #fff;
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
        }
        .config-panel h2 { margin: 0 0 15px 0; font-size: 16px; color: #666; }
        .config-row { display: flex; gap: 20px; flex-wrap: wrap; }
        .config-item { display: flex; align-items: center; gap: 8px; }
        .config-item label { cursor: pointer; user-select: none; }

        .table-container {
            background: #fff;
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 20px;
        }

        #filter-section { margin-bottom: 20px; }

        table { width: 100%; border-collapse: collapse; }
        th, td { padding: 12px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f9f9f9; font-weight: 600; }
        tr:hover { background: #f5f5f5; }

        .tablix-sort-btn {
            background: none;
            border: none;
            cursor: pointer;
            font-weight: 600;
            font-size: inherit;
            padding: 0;
            display: flex;
            align-items: center;
            gap: 4px;
        }
        .tablix-sort-btn:hover { color: #0066cc; }
        .tablix-sort-active { color: #0066cc; }
        .tablix-sort-indicator { font-size: 10px; }

        .filter-multi-select { position: relative; min-width: 200px; }
        .filter-multi-select-trigger {
            width: 100%;
            padding: 8px 12px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background: #fff;
            cursor: pointer;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .filter-multi-select-dropdown {
            position: absolute;
            top: 100%;
            left: 0;
            right: 0;
            background: #fff;
            border: 1px solid #ddd;
            border-radius: 4px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            z-index: 100;
            margin-top: 4px;
        }
        .filter-multi-select-option {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 8px 12px;
            cursor: pointer;
        }
        .filter-multi-select-option:hover { background: #f5f5f5; }
        .filter-multi-select-checkbox { width: 16px; height: 16px; accent-color: #0066cc; }
        .filter-multi-select-actions {
            display: flex;
            justify-content: flex-end;
            gap: 8px;
            padding: 8px 12px;
            border-top: 1px solid #eee;
        }
        .filter-multi-select-clear { padding: 6px 12px; border: none; background: none; cursor: pointer; color: #666; }
        .filter-multi-select-search { padding: 8px; border-bottom: 1px solid #eee; }
        .filter-multi-select-search input { width: 100%; padding: 6px; border: 1px solid #ddd; border-radius: 4px; }

        .filter-search-wrapper { position: relative; max-width: 300px; }
        .filter-search-field {
            width: 100%;
            padding: 8px 12px 8px 36px;
            border: 1px solid #ddd;
            border-radius: 4px;
        }
        .filter-search-icon {
            position: absolute;
            left: 10px;
            top: 50%;
            transform: translateY(-50%);
            width: 16px;
            height: 16px;
            color: #999;
        }

        .filter-label { display: block; margin-bottom: 4px; font-size: 14px; color: #666; }

        [x-cloak] { display: none !important; }

        .htmx-indicator { display: none; }
        .htmx-request .htmx-indicator { display: inline; }

        /* Tab Filter Styles */
        .tab-filter {
            display: flex;
            align-items: center;
            gap: 0;
            border-bottom: 1px solid #ddd;
            margin-bottom: 16px;
        }
        .tab-filter__radio { position: absolute; opacity: 0; width: 0; height: 0; }
        .tab-filter__tab {
            display: inline-flex;
            align-items: center;
            gap: 4px;
            padding: 8px 16px;
            font-size: 14px;
            color: #666;
            background: transparent;
            border: none;
            border-bottom: 2px solid transparent;
            margin-bottom: -1px;
            cursor: pointer;
            transition: all 0.15s ease;
        }
        .tab-filter__tab:hover { color: #333; background: #f5f5f5; }
        .tab-filter__tab--active {
            color: #0066cc;
            border-bottom-color: #0066cc;
            font-weight: 600;
        }
        .tab-filter__count { opacity: 0.8; }
        .sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0,0,0,0); border: 0; }

        .status { padding: 10px; background: #e8f5e9; border-radius: 4px; margin-top: 10px; font-size: 14px; }

        .db-info { font-size: 12px; color: #888; margin-top: 10px; }
    </style>
</head>
<body>
    <div class="demo-container">
        <h1>Tablix Demo (SQLite Backend)</h1>

        <div class="config-panel" x-data="configPanel()">
            <h2>Configuration</h2>
            <div class="config-row">
                <div class="config-item">
                    <input type="checkbox" id="show-search" x-model="showSearch" @change="updateFilters()">
                    <label for="show-search">Show Search</label>
                </div>
                <div class="config-item">
                    <input type="checkbox" id="show-sector" x-model="showSectorFilter" @change="updateFilters()">
                    <label for="show-sector">Show Sector Filter</label>
                </div>
                <div class="config-item">
                    <input type="checkbox" id="sector-searchable" x-model="sectorSearchable" @change="updateFilters()">
                    <label for="sector-searchable">Searchable Dropdown</label>
                </div>
                <div class="config-item">
                    <input type="checkbox" id="auto-submit" x-model="autoSubmit" @change="updateFilters()">
                    <label for="auto-submit">Auto-submit on Change</label>
                </div>
                <div class="config-item">
                    <input type="checkbox" id="show-tabs" x-model="showTabFilter" @change="updateFilters()">
                    <label for="show-tabs">Show Tab Filter</label>
                </div>
            </div>
            <div class="status" x-show="statusMessage" x-text="statusMessage"></div>
        </div>

        <div class="table-container">
            <div id="filter-section" hx-get="/filters" hx-trigger="load" hx-swap="innerHTML">
                Loading filters...
            </div>

            <div id="table-section" hx-get="/table" hx-trigger="load" hx-swap="innerHTML">
                Loading table...
            </div>

            <div class="db-info">Database: demo.db (SQLite)</div>
        </div>
    </div>

    <script>
        function configPanel() {
            return {
                showSearch: true,
                showSectorFilter: true,
                sectorSearchable: false,
                autoSubmit: true,
                showTabFilter: true,
                statusMessage: '',

                updateFilters() {
                    const params = new URLSearchParams({
                        show_search: this.showSearch,
                        show_sector: this.showSectorFilter,
                        sector_searchable: this.sectorSearchable,
                        auto_submit: this.autoSubmit,
                        show_tab_filter: this.showTabFilter,
                    });

                    this.statusMessage = 'Updating...';

                    htmx.ajax('GET', '/filters?' + params.toString(), {
                        target: '#filter-section',
                        swap: 'innerHTML'
                    }).then(() => {
                        this.statusMessage = 'Config updated';
                        setTimeout(() => this.statusMessage = '', 2000);
                    });
                }
            };
        }
    </script>
</body>
</html>"""


@app.get("/filters", response_class=HTMLResponse)
async def get_filters(
    show_search: bool = True,
    show_sector: bool = True,
    sector_searchable: bool = False,
    auto_submit: bool = True,
    show_tab_filter: bool = True,
):
    """Render filter section based on config."""
    config = get_filter_config(
        show_search=show_search,
        show_sector_filter=show_sector,
        sector_searchable=sector_searchable,
        auto_submit=auto_submit,
        show_tab_filter=show_tab_filter,
    )

    async with pool.acquire() as conn:
        # Get sectors from database
        result = await execute(raw("SELECT DISTINCT sector FROM holdings ORDER BY sector"), conn)
        sectors = [row["sector"] for row in result]

        # Get counts for tab filter
        total_count = (await execute(raw("SELECT COUNT(*) as cnt FROM holdings"), conn))[0]["cnt"]
        to_review_count = (await execute(raw("SELECT COUNT(*) as cnt FROM holdings WHERE review_status = 'to_review'"), conn))[0]["cnt"]
        reviewed_count = (await execute(raw("SELECT COUNT(*) as cnt FROM holdings WHERE review_status = 'reviewed'"), conn))[0]["cnt"]

    sector_options = [FilterOption(value=s, label=s) for s in sectors]

    # Tab filter options with counts (like "All (100) | To Review (33) | Reviewed (67)")
    review_status_options = [
        FilterOption(value="all", label="All", count=total_count),
        FilterOption(value="to_review", label="To Review", count=to_review_count),
        FilterOption(value="reviewed", label="Reviewed", count=reviewed_count),
    ]

    env = get_jinja_env()
    template = env.get_template("components/filter_layout.html")

    options = {
        "sectors": sector_options,
        "review_statuses": review_status_options,
    }

    html = template.render(
        layout=config,
        state=None,
        options=options,
    )

    return html


@app.get("/table", response_class=HTMLResponse)
async def get_table(
    request: Request,
    name: str | None = None,
    sector: str | None = None,
    review_status: str | None = None,
    sort: str | None = None,
    direction: str = "asc",
):
    """Render full table using tablix with data from SQLite."""
    # Parse sector param
    sectors = None
    all_sectors = request.query_params.getlist("sector")
    if all_sectors:
        sectors = [s for s in all_sectors if s]
    elif sector:
        sectors = [s.strip() for s in sector.split(",") if s.strip()]

    # Build SQL query
    sql = "SELECT id, ticker, name, sector, value, change FROM holdings WHERE 1=1"
    params = []

    if name:
        sql += " AND (name LIKE ? OR ticker LIKE ?)"
        params.extend([f"%{name}%", f"%{name}%"])

    if sectors:
        placeholders = ",".join("?" * len(sectors))
        sql += f" AND sector IN ({placeholders})"
        params.extend(sectors)

    # Filter by review status (tab filter)
    if review_status and review_status != "all":
        sql += " AND review_status = ?"
        params.append(review_status)

    # Sort
    valid_cols = {"ticker", "name", "sector", "value", "change"}
    if sort and sort in valid_cols:
        dir_sql = "DESC" if direction == "desc" else "ASC"
        sql += f" ORDER BY {sort} {dir_sql}"

    # Execute query
    async with pool.acquire() as conn:
        result = await execute(raw(sql, params=tuple(params)), conn)
        data = [dict(row) for row in result]

    # Render using tablix
    config = get_table_config()
    env = get_jinja_env()
    template = env.get_template("components/table.html")
    return template.render(
        config=config,
        data=data,
        sort_url="/table",
        sort_field=sort,
        sort_dir=direction,
    )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8888)
