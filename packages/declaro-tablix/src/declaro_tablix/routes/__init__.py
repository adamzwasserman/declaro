"""Routes for declaro-tablix.

Provides FastAPI router for table operations.
"""

from declaro_tablix.routes.table_routes import router as table_router

__all__ = ["table_router"]
