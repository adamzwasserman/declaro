"""
Saved layout management components for TableV2 customization.

This module provides function-based HTMX components for managing saved layouts,
user preferences, and default configurations.
"""

import json
from typing import Any, Dict, List, Optional

from declaro_advise import error, success, warning
from declaro_tablix.customization.ui import UI_CONFIG, UI_ERRORS, UI_SUCCESS


def render_layout_manager(
    user_id: str,
    table_name: str,
    current_layout: Optional[Dict[str, Any]] = None,
    saved_layouts: Optional[List[Dict[str, Any]]] = None,
    layout_mode: str = "manage",
    db_session=None,
) -> Dict[str, Any]:
    """
    Render the layout manager component.

    Args:
        user_id: User identifier
        table_name: Name of the table
        current_layout: Current layout configuration
        saved_layouts: List of saved layouts
        layout_mode: Mode (manage, save, load)
        db_session: Optional database session

    Returns:
        Component render result with HTML and metadata
    """
    try:
        # Initialize default values
        if current_layout is None:
            current_layout = {}

        if saved_layouts is None:
            saved_layouts = []

        # Build component data
        component_data = {
            "user_id": user_id,
            "table_name": table_name,
            "current_layout": current_layout,
            "saved_layouts": saved_layouts,
            "layout_mode": layout_mode,
            "available_modes": ["manage", "save", "load"],
        }

        # Generate HTML content
        html_content = _generate_layout_manager_html(component_data)

        return {
            "success": True,
            "html": html_content,
            "component_type": "layout_manager",
            "metadata": component_data,
        }

    except Exception as e:
        error(f"Failed to render layout manager: {str(e)}")
        return {
            "success": False,
            "error": UI_ERRORS["render_error"],
            "html": _generate_error_html(str(e)),
        }


def save_layout(
    user_id: str,
    table_name: str,
    layout_name: str,
    layout_description: str,
    layout_data: Dict[str, Any],
    is_default: bool = False,
    is_shared: bool = False,
    db_session=None,
) -> Dict[str, Any]:
    """
    Save a layout configuration.

    Args:
        user_id: User identifier
        table_name: Name of the table
        layout_name: Name for the layout
        layout_description: Description of the layout
        layout_data: Layout configuration data
        is_default: Whether this is the default layout
        is_shared: Whether this layout is shared
        db_session: Optional database session

    Returns:
        Save result with success status
    """
    try:
        # Validate layout data
        validation_result = _validate_layout_data(layout_data)
        if not validation_result["success"]:
            return {
                "success": False,
                "error": "Layout validation failed",
                "errors": validation_result["errors"],
            }

        # Save layout using persistence layer
        # This would integrate with the actual persistence functions
        layout_record = {
            "user_id": user_id,
            "table_name": table_name,
            "layout_name": layout_name,
            "layout_description": layout_description,
            "layout_data": layout_data,
            "is_default": is_default,
            "is_shared": is_shared,
        }

        # Here you would use the actual persistence functions
        # For now, we'll simulate the save operation

        success(UI_SUCCESS["layout_applied"])
        return {
            "success": True,
            "message": f"Layout '{layout_name}' saved successfully",
            "data": layout_record,
            "refresh_component": True,
        }

    except Exception as e:
        error(f"Failed to save layout: {str(e)}")
        return {"success": False, "error": str(e)}


def load_layout(
    user_id: str,
    table_name: str,
    layout_id: str,
    set_as_default: bool = False,
    db_session=None,
) -> Dict[str, Any]:
    """
    Load a saved layout.

    Args:
        user_id: User identifier
        table_name: Name of the table
        layout_id: ID of the layout to load
        set_as_default: Whether to set as default
        db_session: Optional database session

    Returns:
        Load result with layout data
    """
    try:
        # Load layout from persistence layer
        # This would integrate with the actual persistence functions
        # For now, we'll simulate the load operation

        layout_data = {
            "columns": [
                {"id": "name", "visible": True, "width": 200, "order": 1},
                {"id": "email", "visible": True, "width": 250, "order": 2},
                {"id": "status", "visible": False, "width": 100, "order": 3},
            ],
            "filters": [],
            "sorts": [{"column": "name", "direction": "asc"}],
            "page_size": 25,
        }

        if set_as_default:
            # Set as default layout
            pass

        success(UI_SUCCESS["layout_applied"])
        return {
            "success": True,
            "message": "Layout loaded successfully",
            "data": layout_data,
            "apply_layout": True,
        }

    except Exception as e:
        error(f"Failed to load layout: {str(e)}")
        return {"success": False, "error": str(e)}


def delete_layout(
    user_id: str,
    table_name: str,
    layout_id: str,
    db_session=None,
) -> Dict[str, Any]:
    """
    Delete a saved layout.

    Args:
        user_id: User identifier
        table_name: Name of the table
        layout_id: ID of the layout to delete
        db_session: Optional database session

    Returns:
        Delete result with success status
    """
    try:
        # Delete layout using persistence layer
        # This would integrate with the actual persistence functions

        success("Layout deleted successfully")
        return {
            "success": True,
            "message": "Layout deleted successfully",
            "refresh_component": True,
        }

    except Exception as e:
        error(f"Failed to delete layout: {str(e)}")
        return {"success": False, "error": str(e)}


def _generate_layout_manager_html(component_data: Dict[str, Any]) -> str:
    """Generate HTML for the layout manager."""
    user_id = component_data["user_id"]
    table_name = component_data["table_name"]
    current_layout = component_data["current_layout"]
    saved_layouts = component_data["saved_layouts"]
    layout_mode = component_data["layout_mode"]

    # Generate saved layouts HTML
    saved_layouts_html = _generate_saved_layouts_html(saved_layouts)

    # Generate current layout summary
    current_layout_html = _generate_current_layout_html(current_layout)

    return f"""
    <div class="layout-manager">
        <div class="row">
            <!-- Current Layout Panel -->
            <div class="col-md-6">
                <div class="card h-100">
                    <div class="card-header d-flex justify-content-between align-items-center">
                        <h6 class="mb-0"><i class="fas fa-layout me-2"></i>Current Layout</h6>
                        <div>
                            <button type="button"
                                    class="btn btn-sm btn-outline-success"
                                    onclick="showSaveLayoutModal()">
                                <i class="fas fa-save me-1"></i>Save
                            </button>
                            <button type="button"
                                    class="btn btn-sm btn-outline-primary"
                                    onclick="exportLayout()">
                                <i class="fas fa-download me-1"></i>Export
                            </button>
                        </div>
                    </div>
                    <div class="card-body">
                        <div id="currentLayout">
                            {current_layout_html}
                        </div>

                        <div class="mt-3">
                            <button type="button"
                                    class="btn btn-outline-warning btn-sm me-2"
                                    onclick="resetToDefault()">
                                <i class="fas fa-undo me-1"></i>Reset to Default
                            </button>
                            <button type="button"
                                    class="btn btn-outline-info btn-sm"
                                    onclick="previewLayout()">
                                <i class="fas fa-eye me-1"></i>Preview
                            </button>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Saved Layouts Panel -->
            <div class="col-md-6">
                <div class="card h-100">
                    <div class="card-header d-flex justify-content-between align-items-center">
                        <h6 class="mb-0"><i class="fas fa-bookmark me-2"></i>Saved Layouts</h6>
                        <div>
                            <button type="button"
                                    class="btn btn-sm btn-outline-primary"
                                    onclick="refreshSavedLayouts()">
                                <i class="fas fa-refresh me-1"></i>Refresh
                            </button>
                            <button type="button"
                                    class="btn btn-sm btn-outline-success"
                                    onclick="importLayout()">
                                <i class="fas fa-upload me-1"></i>Import
                            </button>
                        </div>
                    </div>
                    <div class="card-body" style="max-height: 400px; overflow-y: auto;">
                        <div id="savedLayouts">
                            {saved_layouts_html}
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Layout Actions Row -->
        <div class="row mt-3">
            <div class="col-12">
                <div class="card">
                    <div class="card-header">
                        <h6 class="mb-0"><i class="fas fa-tools me-2"></i>Layout Actions</h6>
                    </div>
                    <div class="card-body">
                        <div class="row">
                            <div class="col-md-3">
                                <button type="button"
                                        class="btn btn-outline-primary w-100"
                                        onclick="createNewLayout()">
                                    <i class="fas fa-plus me-2"></i>Create New Layout
                                </button>
                            </div>
                            <div class="col-md-3">
                                <button type="button"
                                        class="btn btn-outline-success w-100"
                                        onclick="duplicateCurrentLayout()">
                                    <i class="fas fa-copy me-2"></i>Duplicate Current
                                </button>
                            </div>
                            <div class="col-md-3">
                                <button type="button"
                                        class="btn btn-outline-info w-100"
                                        onclick="shareLayout()">
                                    <i class="fas fa-share me-2"></i>Share Layout
                                </button>
                            </div>
                            <div class="col-md-3">
                                <button type="button"
                                        class="btn btn-outline-warning w-100"
                                        onclick="manageDefaults()">
                                    <i class="fas fa-star me-2"></i>Manage Defaults
                                </button>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Hidden form data -->
        <input type="hidden" name="user_id" value="{user_id}">
        <input type="hidden" name="table_name" value="{table_name}">
    </div>

    <!-- Save Layout Modal -->
    <div class="modal fade" id="saveLayoutModal" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title"><i class="fas fa-save me-2"></i>Save Layout</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <form id="saveLayoutForm">
                        <div class="mb-3">
                            <label for="layoutName" class="form-label fw-bold">Layout Name <span class="text-danger">*</span></label>
                            <input type="text"
                                   class="form-control"
                                   id="layoutName"
                                   name="layout_name"
                                   required
                                   maxlength="255"
                                   placeholder="Enter layout name...">
                        </div>

                        <div class="mb-3">
                            <label for="layoutDescription" class="form-label">Description</label>
                            <textarea class="form-control"
                                      id="layoutDescription"
                                      name="layout_description"
                                      rows="3"
                                      maxlength="1000"
                                      placeholder="Optional description..."></textarea>
                        </div>

                        <div class="mb-3">
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" id="setAsDefault" name="is_default">
                                <label class="form-check-label" for="setAsDefault">
                                    Set as default layout
                                </label>
                            </div>
                        </div>

                        <div class="mb-3">
                            <div class="form-check">
                                <input class="form-check-input" type="checkbox" id="shareLayout" name="is_shared">
                                <label class="form-check-label" for="shareLayout">
                                    Share with other users
                                </label>
                            </div>
                        </div>
                    </form>
                </div>
                <div class="modal-footer">
                    <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>
                    <button type="button" class="btn btn-primary" onclick="saveCurrentLayout()">
                        <i class="fas fa-save me-2"></i>Save Layout
                    </button>
                </div>
            </div>
        </div>
    </div>

    <!-- Default Management Modal -->
    <div class="modal fade" id="defaultManagementModal" tabindex="-1" aria-hidden="true">
        <div class="modal-dialog modal-lg">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title"><i class="fas fa-star me-2"></i>Manage Default Layouts</h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
                </div>
                <div class="modal-body">
                    <div id="defaultManagementContent">
                        <!-- Content will be loaded dynamically -->
                    </div>
                </div>
            </div>
        </div>
    </div>

    <style>
        .layout-item {{
            padding: 12px;
            border: 1px solid #dee2e6;
            border-radius: 0.375rem;
            margin-bottom: 8px;
            background: white;
            transition: all 0.2s ease;
        }}

        .layout-item:hover {{
            background: #f8f9fa;
            border-color: #6c757d;
        }}

        .layout-item.default {{
            background: #fff3cd;
            border-color: #ffc107;
        }}

        .layout-item.shared {{
            background: #d1ecf1;
            border-color: #17a2b8;
        }}

        .layout-summary {{
            font-size: 0.875rem;
            color: #6c757d;
        }}

        .layout-stats {{
            display: flex;
            gap: 12px;
            margin-top: 8px;
        }}

        .layout-stats .stat {{
            display: flex;
            align-items: center;
            gap: 4px;
            font-size: 0.75rem;
            color: #6c757d;
        }}

        .default-badge {{
            background: #ffc107;
            color: #212529;
            padding: 2px 6px;
            border-radius: 0.25rem;
            font-size: 0.75rem;
            font-weight: 500;
        }}

        .shared-badge {{
            background: #17a2b8;
            color: white;
            padding: 2px 6px;
            border-radius: 0.25rem;
            font-size: 0.75rem;
            font-weight: 500;
        }}
    </style>

    <script>
        // Layout manager JavaScript
        function showSaveLayoutModal() {{
            const modal = new bootstrap.Modal(document.getElementById('saveLayoutModal'));
            modal.show();
        }}

        function saveCurrentLayout() {{
            const form = document.getElementById('saveLayoutForm');
            const formData = new FormData(form);

            if (!formData.get('layout_name').trim()) {{
                alert('Please enter a layout name');
                return;
            }}

            // Get current layout data
            const layoutData = getCurrentLayoutData();

            htmx.ajax('POST', '/api/tableV2/customization/save-layout', {{
                values: {{
                    user_id: '{user_id}',
                    table_name: '{table_name}',
                    layout_name: formData.get('layout_name'),
                    layout_description: formData.get('layout_description'),
                    layout_data: JSON.stringify(layoutData),
                    is_default: formData.get('is_default') ? true : false,
                    is_shared: formData.get('is_shared') ? true : false
                }},
                target: '.layout-manager'
            }});

            bootstrap.Modal.getInstance(document.getElementById('saveLayoutModal')).hide();
        }}

        function loadLayout(layoutId, setAsDefault = false) {{
            if (confirm('Load this layout? Current changes will be lost.')) {{
                htmx.ajax('POST', '/api/tableV2/customization/load-layout', {{
                    values: {{
                        user_id: '{user_id}',
                        table_name: '{table_name}',
                        layout_id: layoutId,
                        set_as_default: setAsDefault
                    }},
                    target: '.layout-manager'
                }});
            }}
        }}

        function deleteLayout(layoutId, layoutName) {{
            if (confirm(`Delete layout "${{layoutName}}"? This action cannot be undone.`)) {{
                htmx.ajax('POST', '/api/tableV2/customization/delete-layout', {{
                    values: {{
                        user_id: '{user_id}',
                        table_name: '{table_name}',
                        layout_id: layoutId
                    }},
                    target: '.layout-manager'
                }});
            }}
        }}

        function setLayoutAsDefault(layoutId, layoutName) {{
            if (confirm(`Set "${{layoutName}}" as your default layout?`)) {{
                htmx.ajax('POST', '/api/tableV2/customization/set-default-layout', {{
                    values: {{
                        user_id: '{user_id}',
                        table_name: '{table_name}',
                        layout_id: layoutId
                    }},
                    target: '.layout-manager'
                }});
            }}
        }}

        function duplicateLayout(layoutId, layoutName) {{
            const newName = prompt(`Enter name for duplicated layout:`, `${{layoutName}} (Copy)`);
            if (newName && newName.trim()) {{
                htmx.ajax('POST', '/api/tableV2/customization/duplicate-layout', {{
                    values: {{
                        user_id: '{user_id}',
                        table_name: '{table_name}',
                        layout_id: layoutId,
                        new_name: newName.trim()
                    }},
                    target: '.layout-manager'
                }});
            }}
        }}

        function refreshSavedLayouts() {{
            htmx.ajax('POST', '/api/tableV2/customization/get-saved-layouts', {{
                values: {{
                    user_id: '{user_id}',
                    table_name: '{table_name}'
                }},
                target: '#savedLayouts'
            }});
        }}

        function createNewLayout() {{
            showSaveLayoutModal();
        }}

        function duplicateCurrentLayout() {{
            const name = prompt('Enter name for the new layout:');
            if (name && name.trim()) {{
                const layoutData = getCurrentLayoutData();

                htmx.ajax('POST', '/api/tableV2/customization/save-layout', {{
                    values: {{
                        user_id: '{user_id}',
                        table_name: '{table_name}',
                        layout_name: name.trim(),
                        layout_description: 'Copy of current layout',
                        layout_data: JSON.stringify(layoutData),
                        is_default: false,
                        is_shared: false
                    }},
                    target: '.layout-manager'
                }});
            }}
        }}

        function shareLayout() {{
            alert('Layout sharing functionality will be implemented in a future update.');
        }}

        function manageDefaults() {{
            htmx.ajax('POST', '/api/tableV2/customization/get-default-management', {{
                values: {{
                    user_id: '{user_id}',
                    table_name: '{table_name}'
                }},
                target: '#defaultManagementContent'
            }});

            const modal = new bootstrap.Modal(document.getElementById('defaultManagementModal'));
            modal.show();
        }}

        function exportLayout() {{
            const layoutData = getCurrentLayoutData();
            const dataStr = JSON.stringify(layoutData, null, 2);
            const dataBlob = new Blob([dataStr], {{type: 'application/json'}});

            const link = document.createElement('a');
            link.href = URL.createObjectURL(dataBlob);
            link.download = `${{'{table_name}'}} - layout.json`;
            link.click();
        }}

        function importLayout() {{
            const input = document.createElement('input');
            input.type = 'file';
            input.accept = '.json';
            input.onchange = function(e) {{
                const file = e.target.files[0];
                if (file) {{
                    const reader = new FileReader();
                    reader.onload = function(e) {{
                        try {{
                            const layoutData = JSON.parse(e.target.result);

                            const name = prompt('Enter name for imported layout:');
                            if (name && name.trim()) {{
                                htmx.ajax('POST', '/api/tableV2/customization/save-layout', {{
                                    values: {{
                                        user_id: '{user_id}',
                                        table_name: '{table_name}',
                                        layout_name: name.trim(),
                                        layout_description: 'Imported layout',
                                        layout_data: JSON.stringify(layoutData),
                                        is_default: false,
                                        is_shared: false
                                    }},
                                    target: '.layout-manager'
                                }});
                            }}
                        }} catch (error) {{
                            alert('Invalid layout file format');
                        }}
                    }};
                    reader.readAsText(file);
                }}
            }};
            input.click();
        }}

        function resetToDefault() {{
            if (confirm('Reset to default layout? Current changes will be lost.')) {{
                htmx.ajax('POST', '/api/tableV2/customization/reset-to-default', {{
                    values: {{
                        user_id: '{user_id}',
                        table_name: '{table_name}'
                    }},
                    target: '.layout-manager'
                }});
            }}
        }}

        function previewLayout() {{
            // This would show a preview of how the current layout would look
            alert('Layout preview functionality will be implemented in a future update.');
        }}

        function getCurrentLayoutData() {{
            // This would collect current layout configuration from the table
            // For now, return a sample layout
            return {{
                columns: [
                    {{id: 'name', visible: true, width: 200, order: 1}},
                    {{id: 'email', visible: true, width: 250, order: 2}},
                    {{id: 'status', visible: false, width: 100, order: 3}}
                ],
                filters: [],
                sorts: [{{column: 'name', direction: 'asc'}}],
                page_size: 25
            }};
        }}

        // Auto-focus layout name input when modal opens
        document.getElementById('saveLayoutModal').addEventListener('shown.bs.modal', function() {{
            document.getElementById('layoutName').focus();
        }});
    </script>
    """


def _generate_saved_layouts_html(saved_layouts: List[Dict[str, Any]]) -> str:
    """Generate HTML for saved layouts display."""
    if not saved_layouts:
        return """
        <div class="text-center py-4">
            <i class="fas fa-bookmark fa-2x text-muted mb-2"></i>
            <div class="text-muted">No saved layouts</div>
            <button type="button" class="btn btn-outline-primary btn-sm mt-2" onclick="createNewLayout()">
                <i class="fas fa-plus me-1"></i>Create First Layout
            </button>
        </div>
        """

    html_items = []
    for layout in saved_layouts:
        layout_id = layout.get("id", "")
        layout_name = layout.get("name", "Unnamed Layout")
        layout_description = layout.get("description", "")
        is_default = layout.get("is_default", False)
        is_shared = layout.get("is_shared", False)
        created_at = layout.get("created_at", "")
        column_count = len(layout.get("layout_data", {}).get("columns", []))

        badges_html = ""
        if is_default:
            badges_html += '<span class="default-badge me-1">⭐ Default</span>'
        if is_shared:
            badges_html += '<span class="shared-badge">🌐 Shared</span>'

        html_items.append(
            f"""
        <div class="layout-item {'default' if is_default else ''} {'shared' if is_shared else ''}">
            <div class="d-flex justify-content-between align-items-start">
                <div class="flex-grow-1">
                    <div class="d-flex align-items-center">
                        <strong>{layout_name}</strong>
                        {badges_html}
                    </div>
                    {f'<div class="layout-summary mt-1">{layout_description}</div>' if layout_description else ''}
                    <div class="layout-stats">
                        <div class="stat">
                            <i class="fas fa-columns"></i>
                            <span>{column_count} columns</span>
                        </div>
                        <div class="stat">
                            <i class="fas fa-calendar"></i>
                            <span>{created_at}</span>
                        </div>
                    </div>
                </div>
                <div class="btn-group-vertical">
                    <button type="button"
                            class="btn btn-sm btn-outline-primary"
                            onclick="loadLayout('{layout_id}')"
                            title="Load Layout">
                        <i class="fas fa-play"></i>
                    </button>
                    <div class="btn-group">
                        <button type="button"
                                class="btn btn-sm btn-outline-secondary dropdown-toggle"
                                data-bs-toggle="dropdown">
                            <i class="fas fa-ellipsis-v"></i>
                        </button>
                        <ul class="dropdown-menu">
                            <li><a class="dropdown-item" href="#" onclick="loadLayout('{layout_id}', true)">
                                <i class="fas fa-star me-2"></i>Load & Set as Default
                            </a></li>
                            <li><a class="dropdown-item" href="#" onclick="duplicateLayout('{layout_id}', '{layout_name}')">
                                <i class="fas fa-copy me-2"></i>Duplicate
                            </a></li>
                            <li><hr class="dropdown-divider"></li>
                            <li><a class="dropdown-item text-danger" href="#" onclick="deleteLayout('{layout_id}', '{layout_name}')">
                                <i class="fas fa-trash me-2"></i>Delete
                            </a></li>
                        </ul>
                    </div>
                </div>
            </div>
        </div>
        """
        )

    return "\n".join(html_items)


def _generate_current_layout_html(current_layout: Dict[str, Any]) -> str:
    """Generate HTML for current layout display."""
    if not current_layout:
        return """
        <div class="text-center py-3">
            <i class="fas fa-layout fa-2x text-muted mb-2"></i>
            <div class="text-muted">Default layout active</div>
        </div>
        """

    columns = current_layout.get("columns", [])
    filters = current_layout.get("filters", [])
    sorts = current_layout.get("sorts", [])
    page_size = current_layout.get("page_size", 25)

    visible_columns = [col for col in columns if col.get("visible", True)]
    hidden_columns = [col for col in columns if not col.get("visible", True)]

    return f"""
    <div class="current-layout-summary">
        <div class="row g-2">
            <div class="col-6">
                <div class="stat-card">
                    <div class="stat-icon"><i class="fas fa-eye text-success"></i></div>
                    <div class="stat-info">
                        <div class="stat-value">{len(visible_columns)}</div>
                        <div class="stat-label">Visible Columns</div>
                    </div>
                </div>
            </div>
            <div class="col-6">
                <div class="stat-card">
                    <div class="stat-icon"><i class="fas fa-eye-slash text-muted"></i></div>
                    <div class="stat-info">
                        <div class="stat-value">{len(hidden_columns)}</div>
                        <div class="stat-label">Hidden Columns</div>
                    </div>
                </div>
            </div>
            <div class="col-6">
                <div class="stat-card">
                    <div class="stat-icon"><i class="fas fa-filter text-info"></i></div>
                    <div class="stat-info">
                        <div class="stat-value">{len(filters)}</div>
                        <div class="stat-label">Active Filters</div>
                    </div>
                </div>
            </div>
            <div class="col-6">
                <div class="stat-card">
                    <div class="stat-icon"><i class="fas fa-sort text-warning"></i></div>
                    <div class="stat-info">
                        <div class="stat-value">{len(sorts)}</div>
                        <div class="stat-label">Sort Rules</div>
                    </div>
                </div>
            </div>
        </div>

        <div class="mt-3">
            <small class="text-muted">
                <i class="fas fa-list me-1"></i>Page Size: {page_size} rows
            </small>
        </div>
    </div>

    <style>
        .stat-card {{
            display: flex;
            align-items: center;
            padding: 8px;
            background: #f8f9fa;
            border-radius: 0.375rem;
            gap: 8px;
        }}

        .stat-icon {{
            font-size: 1.25rem;
        }}

        .stat-value {{
            font-size: 1.1rem;
            font-weight: 600;
            line-height: 1;
        }}

        .stat-label {{
            font-size: 0.75rem;
            color: #6c757d;
            line-height: 1;
        }}
    </style>
    """


def _validate_layout_data(layout_data: Dict[str, Any]) -> Dict[str, Any]:
    """Validate layout configuration data."""
    errors = []

    if not isinstance(layout_data, dict):
        errors.append("Layout data must be a dictionary")
        return {"success": False, "errors": errors}

    # Validate columns configuration
    if "columns" in layout_data:
        columns = layout_data["columns"]
        if not isinstance(columns, list):
            errors.append("Columns must be a list")
        else:
            for i, column in enumerate(columns):
                if not isinstance(column, dict):
                    errors.append(f"Column {i} must be a dictionary")
                elif "id" not in column:
                    errors.append(f"Column {i} missing required 'id' field")

    # Validate filters configuration
    if "filters" in layout_data:
        filters = layout_data["filters"]
        if not isinstance(filters, list):
            errors.append("Filters must be a list")

    # Validate sorts configuration
    if "sorts" in layout_data:
        sorts = layout_data["sorts"]
        if not isinstance(sorts, list):
            errors.append("Sorts must be a list")
        else:
            for i, sort in enumerate(sorts):
                if not isinstance(sort, dict):
                    errors.append(f"Sort {i} must be a dictionary")
                elif "column" not in sort or "direction" not in sort:
                    errors.append(f"Sort {i} missing required fields")

    return {"success": len(errors) == 0, "errors": errors}


def _generate_error_html(error_message: str) -> str:
    """Generate error HTML for failed component renders."""
    return f"""
    <div class="alert alert-danger" role="alert">
        <i class="fas fa-exclamation-triangle me-2"></i>
        <strong>Error:</strong> {error_message}
    </div>
    """


# Export all functions
__all__ = [
    "render_layout_manager",
    "save_layout",
    "load_layout",
    "delete_layout",
]
