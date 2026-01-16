"""
Column rename dialog component for TableV2 customization.

This module provides function-based HTMX component for renaming columns,
including validation, preview, and history management.
"""

from typing import Any, Dict, Optional

from declaro_advise import error, success, warning
from declaro_tablix.customization.persistence import (
    create_column_customization,
    get_column_customizations,
    update_column_customization,
)
from declaro_tablix.customization.ui import UI_CONFIG, UI_ERRORS, UI_SUCCESS
from declaro_tablix.customization.validators import validate_column_customization_data


def render_column_rename_dialog(
    user_id: str,
    table_name: str,
    column_id: str,
    current_alias: Optional[str] = None,
    show_history: bool = False,
    db_session=None,
) -> Dict[str, Any]:
    """
    Render the column rename dialog component.

    Args:
        user_id: User identifier
        table_name: Name of the table
        column_id: Column identifier
        current_alias: Current column alias
        show_history: Whether to show rename history
        db_session: Optional database session

    Returns:
        Component render result with HTML and metadata
    """
    try:
        # Get existing customization
        existing_result = get_column_customizations(
            user_id=user_id,
            table_name=table_name,
            column_id=column_id,
            db_session=db_session,
        )

        existing_customization = None
        if existing_result["success"] and existing_result["data"]:
            existing_customization = existing_result["data"][0]
            current_alias = existing_customization.get("alias") or current_alias

        # Build component data
        component_data = {
            "user_id": user_id,
            "table_name": table_name,
            "column_id": column_id,
            "current_alias": current_alias or "",
            "original_column_name": column_id,
            "show_history": show_history,
            "existing_customization": existing_customization,
        }

        # Generate HTML content
        html_content = _generate_rename_dialog_html(component_data)

        return {
            "success": True,
            "html": html_content,
            "component_type": "column_rename_dialog",
            "metadata": component_data,
        }

    except Exception as e:
        error(f"Failed to render column rename dialog: {str(e)}")
        return {
            "success": False,
            "error": UI_ERRORS["render_error"],
            "html": _generate_error_html(str(e)),
        }


def process_column_rename(
    user_id: str,
    table_name: str,
    column_id: str,
    new_alias: str,
    db_session=None,
) -> Dict[str, Any]:
    """
    Process column rename request.

    Args:
        user_id: User identifier
        table_name: Name of the table
        column_id: Column identifier
        new_alias: New column alias
        db_session: Optional database session

    Returns:
        Processing result with success status
    """
    try:
        # Validate input
        if not new_alias or not new_alias.strip():
            warning("Column alias cannot be empty")
            return {"success": False, "error": "Alias cannot be empty"}

        # Clean the alias
        clean_alias = new_alias.strip()

        # Validate customization data
        customization_data = {
            "user_id": user_id,
            "table_name": table_name,
            "column_id": column_id,
            "alias": clean_alias,
            "is_visible": True,
        }

        validation_result = validate_column_customization_data(customization_data)
        if not validation_result["success"]:
            error("Column rename validation failed")
            return {
                "success": False,
                "error": "Validation failed",
                "errors": validation_result["errors"],
            }

        # Check if customization exists
        existing_result = get_column_customizations(
            user_id=user_id,
            table_name=table_name,
            column_id=column_id,
            db_session=db_session,
        )

        if existing_result["success"] and existing_result["data"]:
            # Update existing customization
            result = update_column_customization(
                user_id=user_id,
                table_name=table_name,
                column_id=column_id,
                customization_data={"alias": clean_alias},
                db_session=db_session,
            )
        else:
            # Create new customization
            result = create_column_customization(
                user_id=user_id,
                table_name=table_name,
                column_id=column_id,
                customization_data={"alias": clean_alias},
                db_session=db_session,
            )

        if result["success"]:
            success(UI_SUCCESS["customization_saved"])
            return {
                "success": True,
                "message": f"Column renamed to '{clean_alias}'",
                "data": result["data"],
                "refresh_component": True,
            }
        else:
            error(f"Failed to save column rename: {result.get('error', 'Unknown error')}")
            return {"success": False, "error": result.get("error", "Failed to save rename")}

    except Exception as e:
        error(f"Failed to process column rename: {str(e)}")
        return {"success": False, "error": str(e)}


def clear_column_alias(
    user_id: str,
    table_name: str,
    column_id: str,
    db_session=None,
) -> Dict[str, Any]:
    """
    Clear column alias (reset to original name).

    Args:
        user_id: User identifier
        table_name: Name of the table
        column_id: Column identifier
        db_session: Optional database session

    Returns:
        Clear result with success status
    """
    try:
        # Update customization to remove alias
        result = update_column_customization(
            user_id=user_id,
            table_name=table_name,
            column_id=column_id,
            customization_data={"alias": None},
            db_session=db_session,
        )

        if result["success"]:
            success("Column alias cleared")
            return {
                "success": True,
                "message": f"Column '{column_id}' reset to original name",
                "refresh_component": True,
            }
        else:
            warning("Column customization not found")
            return {"success": False, "error": "No customization found to clear"}

    except Exception as e:
        error(f"Failed to clear column alias: {str(e)}")
        return {"success": False, "error": str(e)}


def _generate_rename_dialog_html(component_data: Dict[str, Any]) -> str:
    """Generate HTML for the column rename dialog."""
    user_id = component_data["user_id"]
    table_name = component_data["table_name"]
    column_id = component_data["column_id"]
    current_alias = component_data["current_alias"]
    original_name = component_data["original_column_name"]
    existing = component_data["existing_customization"]

    return f"""
    <div class="modal fade" id="columnRenameModal" tabindex="-1" aria-labelledby="columnRenameModalLabel" aria-hidden="true">
        <div class="modal-dialog modal-lg">
            <div class="modal-content">
                <div class="modal-header">
                    <h5 class="modal-title" id="columnRenameModalLabel">
                        <i class="fas fa-edit me-2"></i>Rename Column
                    </h5>
                    <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                </div>

                <div class="modal-body">
                    <form id="columnRenameForm"
                          hx-post="/api/tableV2/customization/rename-column"
                          hx-target="#renameResult"
                          hx-indicator="#renameSpinner">

                        <input type="hidden" name="user_id" value="{user_id}">
                        <input type="hidden" name="table_name" value="{table_name}">
                        <input type="hidden" name="column_id" value="{column_id}">

                        <!-- Current column info -->
                        <div class="row mb-3">
                            <div class="col-md-6">
                                <label class="form-label fw-bold">Original Column Name</label>
                                <div class="form-control-plaintext bg-light p-2 rounded">
                                    <code>{original_name}</code>
                                </div>
                            </div>
                            <div class="col-md-6">
                                <label class="form-label fw-bold">Current Display Name</label>
                                <div class="form-control-plaintext bg-light p-2 rounded">
                                    {current_alias if current_alias else f'<em class="text-muted">{original_name} (original)</em>'}
                                </div>
                            </div>
                        </div>

                        <!-- New alias input -->
                        <div class="mb-3">
                            <label for="newAlias" class="form-label fw-bold">
                                New Display Name <span class="text-danger">*</span>
                            </label>
                            <input type="text"
                                   class="form-control"
                                   id="newAlias"
                                   name="new_alias"
                                   value="{current_alias}"
                                   placeholder="Enter new display name..."
                                   maxlength="255"
                                   required
                                   hx-trigger="keyup changed delay:500ms"
                                   hx-post="/api/tableV2/customization/validate-alias"
                                   hx-target="#aliasValidation"
                                   hx-indicator="#aliasSpinner">
                            <div class="form-text">
                                This will change how the column appears in the table header.
                            </div>
                            <div id="aliasValidation" class="mt-1"></div>
                            <div id="aliasSpinner" class="htmx-indicator mt-1">
                                <i class="fas fa-spinner fa-spin"></i> Validating...
                            </div>
                        </div>

                        <!-- Preview section -->
                        <div class="mb-3">
                            <label class="form-label fw-bold">Preview</label>
                            <div class="card">
                                <div class="card-body p-2">
                                    <div class="d-flex align-items-center">
                                        <span class="badge bg-primary me-2">Before:</span>
                                        <code>{original_name}</code>
                                    </div>
                                    <div class="d-flex align-items-center mt-1">
                                        <span class="badge bg-success me-2">After:</span>
                                        <span id="previewAlias">{current_alias if current_alias else original_name}</span>
                                    </div>
                                </div>
                            </div>
                        </div>

                        <!-- Action buttons -->
                        <div class="d-flex gap-2">
                            <button type="submit" class="btn btn-primary">
                                <i class="fas fa-save me-1"></i>Save Rename
                            </button>

                            {'<button type="button" class="btn btn-outline-secondary" onclick="clearColumnAlias()"><i class="fas fa-undo me-1"></i>Reset to Original</button>' if existing else ''}

                            <button type="button" class="btn btn-secondary" data-bs-dismiss="modal">
                                Cancel
                            </button>
                        </div>

                        <div id="renameSpinner" class="htmx-indicator mt-2">
                            <i class="fas fa-spinner fa-spin"></i> Saving...
                        </div>
                    </form>

                    <!-- Result area -->
                    <div id="renameResult" class="mt-3"></div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Update preview as user types
        document.getElementById('newAlias').addEventListener('input', function(e) {{
            const preview = document.getElementById('previewAlias');
            const value = e.target.value.trim();
            preview.textContent = value || '{original_name}';

            // Update preview styling
            if (value && value !== '{original_name}') {{
                preview.className = 'fw-bold text-primary';
            }} else {{
                preview.className = 'text-muted';
            }}
        }});

        // Clear alias function
        function clearColumnAlias() {{
            if (confirm('Reset column to original name?')) {{
                htmx.ajax('POST', '/api/tableV2/customization/clear-alias', {{
                    values: {{
                        user_id: '{user_id}',
                        table_name: '{table_name}',
                        column_id: '{column_id}'
                    }},
                    target: '#renameResult'
                }});
            }}
        }}

        // Auto-focus the input when modal opens
        document.getElementById('columnRenameModal').addEventListener('shown.bs.modal', function() {{
            document.getElementById('newAlias').focus();
            document.getElementById('newAlias').select();
        }});
    </script>
    """


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
    "render_column_rename_dialog",
    "process_column_rename",
    "clear_column_alias",
]
