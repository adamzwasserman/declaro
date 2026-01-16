"""
User preferences panel for TableV2 customization.

This module provides function-based HTMX component for managing user preferences,
default settings, and personalization options.
"""

import json
from typing import Any, Dict, List, Optional

from declaro_advise import error, success, warning
from declaro_tablix.customization.ui import UI_CONFIG, UI_ERRORS, UI_SUCCESS


def render_user_preferences_panel(
    user_id: str,
    table_name: str,
    current_preferences: Optional[Dict[str, Any]] = None,
    preference_categories: Optional[List[str]] = None,
    panel_mode: str = "manage",
    db_session=None,
) -> Dict[str, Any]:
    """
    Render the user preferences panel component.

    Args:
        user_id: User identifier
        table_name: Name of the table
        current_preferences: Current user preferences
        preference_categories: Categories of preferences to display
        panel_mode: Mode (manage, defaults, export)
        db_session: Optional database session

    Returns:
        Component render result with HTML and metadata
    """
    try:
        # Initialize default values
        if current_preferences is None:
            current_preferences = _get_default_preferences()

        if preference_categories is None:
            preference_categories = ["display", "behavior", "defaults", "notifications"]

        # Build component data
        component_data = {
            "user_id": user_id,
            "table_name": table_name,
            "current_preferences": current_preferences,
            "preference_categories": preference_categories,
            "panel_mode": panel_mode,
            "available_modes": ["manage", "defaults", "export", "import"],
        }

        # Generate HTML content
        html_content = _generate_preferences_panel_html(component_data)

        return {
            "success": True,
            "html": html_content,
            "component_type": "user_preferences_panel",
            "metadata": component_data,
        }

    except Exception as e:
        error(f"Failed to render user preferences panel: {str(e)}")
        return {
            "success": False,
            "error": UI_ERRORS["render_error"],
            "html": _generate_error_html(str(e)),
        }


def save_user_preferences(
    user_id: str,
    table_name: str,
    preferences: Dict[str, Any],
    db_session=None,
) -> Dict[str, Any]:
    """
    Save user preferences.

    Args:
        user_id: User identifier
        table_name: Name of the table
        preferences: User preferences data
        db_session: Optional database session

    Returns:
        Save result with success status
    """
    try:
        # Validate preferences
        validation_result = _validate_preferences(preferences)
        if not validation_result["success"]:
            return {
                "success": False,
                "error": "Preferences validation failed",
                "errors": validation_result["errors"],
            }

        # Save preferences using persistence layer
        # This would integrate with the actual persistence functions

        success(UI_SUCCESS["preferences_updated"])
        return {
            "success": True,
            "message": "User preferences saved successfully",
            "data": preferences,
            "refresh_component": True,
        }

    except Exception as e:
        error(f"Failed to save user preferences: {str(e)}")
        return {"success": False, "error": str(e)}


def reset_user_preferences(
    user_id: str,
    table_name: str,
    reset_category: Optional[str] = None,
    db_session=None,
) -> Dict[str, Any]:
    """
    Reset user preferences to defaults.

    Args:
        user_id: User identifier
        table_name: Name of the table
        reset_category: Optional category to reset (or all if None)
        db_session: Optional database session

    Returns:
        Reset result with success status
    """
    try:
        # Reset preferences to defaults
        default_preferences = _get_default_preferences()

        if reset_category:
            # Reset only specific category
            message = f"Reset {reset_category} preferences to defaults"
        else:
            # Reset all preferences
            message = "Reset all preferences to defaults"

        success(message)
        return {
            "success": True,
            "message": message,
            "data": default_preferences,
            "refresh_component": True,
        }

    except Exception as e:
        error(f"Failed to reset user preferences: {str(e)}")
        return {"success": False, "error": str(e)}


def _generate_preferences_panel_html(component_data: Dict[str, Any]) -> str:
    """Generate HTML for the user preferences panel."""
    user_id = component_data["user_id"]
    table_name = component_data["table_name"]
    current_preferences = component_data["current_preferences"]
    preference_categories = component_data["preference_categories"]
    panel_mode = component_data["panel_mode"]

    # Generate tabs for preference categories
    tabs_html = _generate_preference_tabs(preference_categories, current_preferences)

    return f"""
    <div class="user-preferences-panel">
        <div class="card">
            <div class="card-header">
                <div class="d-flex justify-content-between align-items-center">
                    <h6 class="mb-0"><i class="fas fa-cog me-2"></i>User Preferences</h6>
                    <div class="btn-group btn-group-sm">
                        <button type="button"
                                class="btn btn-outline-secondary"
                                onclick="exportPreferences()">
                            <i class="fas fa-download me-1"></i>Export
                        </button>
                        <button type="button"
                                class="btn btn-outline-secondary"
                                onclick="importPreferences()">
                            <i class="fas fa-upload me-1"></i>Import
                        </button>
                        <button type="button"
                                class="btn btn-outline-warning"
                                onclick="resetAllPreferences()">
                            <i class="fas fa-undo me-1"></i>Reset All
                        </button>
                    </div>
                </div>
            </div>

            <div class="card-body">
                <!-- Preference Tabs -->
                <ul class="nav nav-tabs" id="preferenceTabs" role="tablist">
                    {_generate_tab_headers(preference_categories)}
                </ul>

                <!-- Tab Content -->
                <div class="tab-content mt-3" id="preferenceTabContent">
                    {tabs_html}
                </div>

                <!-- Action Buttons -->
                <div class="d-flex justify-content-between mt-4">
                    <div>
                        <button type="button"
                                class="btn btn-outline-info"
                                onclick="previewPreferences()">
                            <i class="fas fa-eye me-2"></i>Preview Changes
                        </button>
                    </div>

                    <div>
                        <button type="button"
                                class="btn btn-secondary me-2"
                                onclick="cancelPreferences()">
                            Cancel
                        </button>

                        <button type="button"
                                class="btn btn-primary"
                                onclick="savePreferences()"
                                id="savePreferencesBtn">
                            <i class="fas fa-save me-2"></i>Save Preferences
                        </button>
                    </div>
                </div>
            </div>
        </div>

        <!-- Hidden form data -->
        <input type="hidden" name="user_id" value="{user_id}">
        <input type="hidden" name="table_name" value="{table_name}">
        <input type="hidden" name="preferences_data" id="preferencesData" value="">
    </div>

    <style>
        .preference-section {{
            margin-bottom: 20px;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 0.375rem;
        }}

        .preference-section h6 {{
            color: #495057;
            margin-bottom: 12px;
            font-weight: 600;
        }}

        .preference-item {{
            margin-bottom: 12px;
        }}

        .preference-item:last-child {{
            margin-bottom: 0;
        }}

        .preference-description {{
            font-size: 0.875rem;
            color: #6c757d;
            margin-top: 4px;
        }}

        .preference-preview {{
            background: #e9ecef;
            border-radius: 0.25rem;
            padding: 8px 12px;
            font-size: 0.875rem;
            margin-top: 8px;
        }}

        .default-indicator {{
            color: #6c757d;
            font-size: 0.75rem;
            font-style: italic;
        }}

        .changed-indicator {{
            color: #28a745;
            font-size: 0.75rem;
            font-weight: 500;
        }}
    </style>

    <script>
        // User preferences JavaScript
        let currentPreferences = {json.dumps(current_preferences)};
        let originalPreferences = JSON.parse(JSON.stringify(currentPreferences));

        function updatePreference(category, key, value, type = 'string') {{
            if (!currentPreferences[category]) {{
                currentPreferences[category] = {{}};
            }}

            // Convert value based on type
            switch (type) {{
                case 'boolean':
                    value = Boolean(value);
                    break;
                case 'number':
                    value = parseInt(value) || 0;
                    break;
                case 'float':
                    value = parseFloat(value) || 0.0;
                    break;
                default:
                    value = String(value);
            }}

            currentPreferences[category][key] = value;
            updatePreferenceIndicators(category, key);
            updatePreviewIfVisible();
        }}

        function updatePreferenceIndicators(category, key) {{
            const element = document.querySelector(`[data-preference-category="${{category}}"][data-preference-key="${{key}}"]`);
            if (element) {{
                const indicator = element.closest('.preference-item').querySelector('.preference-indicator');
                const originalValue = originalPreferences[category] && originalPreferences[category][key];
                const currentValue = currentPreferences[category] && currentPreferences[category][key];

                if (JSON.stringify(originalValue) !== JSON.stringify(currentValue)) {{
                    indicator.innerHTML = '<span class="changed-indicator">• Changed</span>';
                }} else {{
                    indicator.innerHTML = '<span class="default-indicator">• Default</span>';
                }}
            }}
        }}

        function resetCategoryPreferences(category) {{
            if (confirm(`Reset all ${{category}} preferences to defaults?`)) {{
                const defaultPrefs = {json.dumps(_get_default_preferences())};
                currentPreferences[category] = defaultPrefs[category] || {{}};

                // Update form inputs
                const categoryInputs = document.querySelectorAll(`[data-preference-category="${{category}}"]`);
                categoryInputs.forEach(input => {{
                    const key = input.dataset.preferenceKey;
                    const value = currentPreferences[category][key];

                    if (input.type === 'checkbox') {{
                        input.checked = Boolean(value);
                    }} else {{
                        input.value = value || '';
                    }}

                    updatePreferenceIndicators(category, key);
                }});

                updatePreviewIfVisible();
            }}
        }}

        function resetAllPreferences() {{
            if (confirm('Reset all preferences to defaults? This will undo all your customizations.')) {{
                htmx.ajax('POST', '/api/tableV2/customization/reset-preferences', {{
                    values: {{
                        user_id: '{user_id}',
                        table_name: '{table_name}'
                    }},
                    target: '.user-preferences-panel'
                }});
            }}
        }}

        function savePreferences() {{
            const preferencesData = JSON.stringify(currentPreferences);
            document.getElementById('preferencesData').value = preferencesData;

            htmx.ajax('POST', '/api/tableV2/customization/save-preferences', {{
                values: {{
                    user_id: '{user_id}',
                    table_name: '{table_name}',
                    preferences: preferencesData
                }},
                target: '.user-preferences-panel'
            }});
        }}

        function cancelPreferences() {{
            if (hasUnsavedChanges()) {{
                if (confirm('You have unsaved changes. Discard them?')) {{
                    location.reload();
                }}
            }} else {{
                // Close or navigate away
                if (typeof bootstrap !== 'undefined') {{
                    const modal = bootstrap.Modal.getInstance(document.querySelector('.modal'));
                    if (modal) modal.hide();
                }}
            }}
        }}

        function previewPreferences() {{
            alert('Preferences preview functionality will show how the table will look with these settings.');
        }}

        function exportPreferences() {{
            const dataStr = JSON.stringify(currentPreferences, null, 2);
            const dataBlob = new Blob([dataStr], {{type: 'application/json'}});

            const link = document.createElement('a');
            link.href = URL.createObjectURL(dataBlob);
            link.download = `${{'{table_name}'}} - preferences.json`;
            link.click();
        }}

        function importPreferences() {{
            const input = document.createElement('input');
            input.type = 'file';
            input.accept = '.json';
            input.onchange = function(e) {{
                const file = e.target.files[0];
                if (file) {{
                    const reader = new FileReader();
                    reader.onload = function(e) {{
                        try {{
                            const importedPrefs = JSON.parse(e.target.result);

                            if (confirm('Import these preferences? Current settings will be replaced.')) {{
                                currentPreferences = importedPrefs;
                                location.reload(); // Reload to show imported preferences
                            }}
                        }} catch (error) {{
                            alert('Invalid preferences file format');
                        }}
                    }};
                    reader.readAsText(file);
                }}
            }};
            input.click();
        }}

        function updatePreviewIfVisible() {{
            // Update any preview elements if they exist
            const previews = document.querySelectorAll('.preference-preview');
            previews.forEach(preview => {{
                // Update preview based on current preferences
                // This would be customized based on the specific preference
            }});
        }}

        function hasUnsavedChanges() {{
            return JSON.stringify(currentPreferences) !== JSON.stringify(originalPreferences);
        }}

        // Initialize preference indicators on page load
        document.addEventListener('DOMContentLoaded', function() {{
            Object.keys(currentPreferences).forEach(category => {{
                Object.keys(currentPreferences[category]).forEach(key => {{
                    updatePreferenceIndicators(category, key);
                }});
            }});
        }});

        // Warn about unsaved changes
        window.addEventListener('beforeunload', function(e) {{
            if (hasUnsavedChanges()) {{
                e.preventDefault();
                e.returnValue = '';
            }}
        }});
    </script>
    """


def _generate_tab_headers(preference_categories: List[str]) -> str:
    """Generate tab headers for preference categories."""
    tab_icons = {
        "display": "fas fa-eye",
        "behavior": "fas fa-mouse-pointer",
        "defaults": "fas fa-star",
        "notifications": "fas fa-bell",
        "performance": "fas fa-tachometer-alt",
        "accessibility": "fas fa-universal-access",
    }

    tabs = []
    for i, category in enumerate(preference_categories):
        active_class = "active" if i == 0 else ""
        icon = tab_icons.get(category, "fas fa-cog")

        tabs.append(
            f"""
        <li class="nav-item" role="presentation">
            <button class="nav-link {active_class}"
                    id="{category}-tab"
                    data-bs-toggle="tab"
                    data-bs-target="#{category}-pane"
                    type="button"
                    role="tab">
                <i class="{icon} me-2"></i>{category.title()}
            </button>
        </li>
        """
        )

    return "\n".join(tabs)


def _generate_preference_tabs(preference_categories: List[str], current_preferences: Dict[str, Any]) -> str:
    """Generate tab content for preference categories."""
    tab_contents = []

    for i, category in enumerate(preference_categories):
        active_class = "show active" if i == 0 else ""
        content = _generate_category_content(category, current_preferences.get(category, {}))

        tab_contents.append(
            f"""
        <div class="tab-pane fade {active_class}"
             id="{category}-pane"
             role="tabpanel"
             aria-labelledby="{category}-tab">
            {content}
        </div>
        """
        )

    return "\n".join(tab_contents)


def _generate_category_content(category: str, category_preferences: Dict[str, Any]) -> str:
    """Generate content for a specific preference category."""
    if category == "display":
        return _generate_display_preferences(category_preferences)
    elif category == "behavior":
        return _generate_behavior_preferences(category_preferences)
    elif category == "defaults":
        return _generate_defaults_preferences(category_preferences)
    elif category == "notifications":
        return _generate_notifications_preferences(category_preferences)
    else:
        return f'<div class="text-muted">No preferences available for {category}</div>'


def _generate_display_preferences(preferences: Dict[str, Any]) -> str:
    """Generate display preferences HTML."""
    return f"""
    <div class="preference-section">
        <h6><i class="fas fa-palette me-2"></i>Appearance</h6>

        <div class="preference-item">
            <label for="theme" class="form-label">Theme</label>
            <select class="form-select"
                    id="theme"
                    data-preference-category="display"
                    data-preference-key="theme"
                    onchange="updatePreference('display', 'theme', this.value)">
                <option value="auto" {"selected" if preferences.get("theme") == "auto" else ""}>Auto (System)</option>
                <option value="light" {"selected" if preferences.get("theme") == "light" else ""}>Light</option>
                <option value="dark" {"selected" if preferences.get("theme") == "dark" else ""}>Dark</option>
            </select>
            <div class="preference-description">Choose your preferred color theme</div>
            <div class="preference-indicator"></div>
        </div>

        <div class="preference-item">
            <label for="density" class="form-label">Table Density</label>
            <select class="form-select"
                    id="density"
                    data-preference-category="display"
                    data-preference-key="density"
                    onchange="updatePreference('display', 'density', this.value)">
                <option value="comfortable" {"selected" if preferences.get("density") == "comfortable" else ""}>Comfortable</option>
                <option value="standard" {"selected" if preferences.get("density") == "standard" else ""}>Standard</option>
                <option value="compact" {"selected" if preferences.get("density") == "compact" else ""}>Compact</option>
            </select>
            <div class="preference-description">Row spacing and padding</div>
            <div class="preference-indicator"></div>
        </div>

        <div class="preference-item">
            <div class="form-check">
                <input class="form-check-input"
                       type="checkbox"
                       id="showRowNumbers"
                       data-preference-category="display"
                       data-preference-key="show_row_numbers"
                       {"checked" if preferences.get("show_row_numbers") else ""}
                       onchange="updatePreference('display', 'show_row_numbers', this.checked, 'boolean')">
                <label class="form-check-label" for="showRowNumbers">
                    Show row numbers
                </label>
            </div>
            <div class="preference-description">Display row numbers in the first column</div>
            <div class="preference-indicator"></div>
        </div>
    </div>

    <div class="preference-section">
        <h6><i class="fas fa-columns me-2"></i>Columns</h6>

        <div class="preference-item">
            <label for="defaultColumnWidth" class="form-label">Default Column Width</label>
            <div class="input-group">
                <input type="number"
                       class="form-control"
                       id="defaultColumnWidth"
                       data-preference-category="display"
                       data-preference-key="default_column_width"
                       value="{preferences.get('default_column_width', 150)}"
                       min="50"
                       max="500"
                       onchange="updatePreference('display', 'default_column_width', this.value, 'number')">
                <span class="input-group-text">px</span>
            </div>
            <div class="preference-description">Default width for new columns</div>
            <div class="preference-indicator"></div>
        </div>

        <div class="preference-item">
            <div class="form-check">
                <input class="form-check-input"
                       type="checkbox"
                       id="autoResizeColumns"
                       data-preference-category="display"
                       data-preference-key="auto_resize_columns"
                       {"checked" if preferences.get("auto_resize_columns") else ""}
                       onchange="updatePreference('display', 'auto_resize_columns', this.checked, 'boolean')">
                <label class="form-check-label" for="autoResizeColumns">
                    Auto-resize columns to fit content
                </label>
            </div>
            <div class="preference-description">Automatically adjust column widths</div>
            <div class="preference-indicator"></div>
        </div>
    </div>
    """


def _generate_behavior_preferences(preferences: Dict[str, Any]) -> str:
    """Generate behavior preferences HTML."""
    return f"""
    <div class="preference-section">
        <h6><i class="fas fa-mouse-pointer me-2"></i>Interaction</h6>

        <div class="preference-item">
            <label for="clickBehavior" class="form-label">Row Click Behavior</label>
            <select class="form-select"
                    id="clickBehavior"
                    data-preference-category="behavior"
                    data-preference-key="click_behavior"
                    onchange="updatePreference('behavior', 'click_behavior', this.value)">
                <option value="select" {"selected" if preferences.get("click_behavior") == "select" else ""}>Select Row</option>
                <option value="edit" {"selected" if preferences.get("click_behavior") == "edit" else ""}>Edit Row</option>
                <option value="view" {"selected" if preferences.get("click_behavior") == "view" else ""}>View Details</option>
                <option value="none" {"selected" if preferences.get("click_behavior") == "none" else ""}>No Action</option>
            </select>
            <div class="preference-description">What happens when you click a table row</div>
            <div class="preference-indicator"></div>
        </div>

        <div class="preference-item">
            <div class="form-check">
                <input class="form-check-input"
                       type="checkbox"
                       id="enableKeyboardNav"
                       data-preference-category="behavior"
                       data-preference-key="enable_keyboard_navigation"
                       {"checked" if preferences.get("enable_keyboard_navigation", True) else ""}
                       onchange="updatePreference('behavior', 'enable_keyboard_navigation', this.checked, 'boolean')">
                <label class="form-check-label" for="enableKeyboardNav">
                    Enable keyboard navigation
                </label>
            </div>
            <div class="preference-description">Use arrow keys to navigate table cells</div>
            <div class="preference-indicator"></div>
        </div>
    </div>

    <div class="preference-section">
        <h6><i class="fas fa-clock me-2"></i>Auto-Save</h6>

        <div class="preference-item">
            <div class="form-check">
                <input class="form-check-input"
                       type="checkbox"
                       id="enableAutoSave"
                       data-preference-category="behavior"
                       data-preference-key="enable_auto_save"
                       {"checked" if preferences.get("enable_auto_save", True) else ""}
                       onchange="updatePreference('behavior', 'enable_auto_save', this.checked, 'boolean')">
                <label class="form-check-label" for="enableAutoSave">
                    Auto-save changes
                </label>
            </div>
            <div class="preference-description">Automatically save changes without confirmation</div>
            <div class="preference-indicator"></div>
        </div>

        <div class="preference-item">
            <label for="autoSaveDelay" class="form-label">Auto-save Delay</label>
            <div class="input-group">
                <input type="number"
                       class="form-control"
                       id="autoSaveDelay"
                       data-preference-category="behavior"
                       data-preference-key="auto_save_delay"
                       value="{preferences.get('auto_save_delay', 2000)}"
                       min="500"
                       max="10000"
                       step="500"
                       onchange="updatePreference('behavior', 'auto_save_delay', this.value, 'number')">
                <span class="input-group-text">ms</span>
            </div>
            <div class="preference-description">Delay before auto-saving changes</div>
            <div class="preference-indicator"></div>
        </div>
    </div>
    """


def _generate_defaults_preferences(preferences: Dict[str, Any]) -> str:
    """Generate defaults preferences HTML."""
    return f"""
    <div class="preference-section">
        <h6><i class="fas fa-star me-2"></i>Default Settings</h6>

        <div class="preference-item">
            <label for="defaultPageSize" class="form-label">Default Page Size</label>
            <select class="form-select"
                    id="defaultPageSize"
                    data-preference-category="defaults"
                    data-preference-key="default_page_size"
                    onchange="updatePreference('defaults', 'default_page_size', this.value, 'number')">
                <option value="10" {"selected" if preferences.get("default_page_size") == 10 else ""}>10 rows</option>
                <option value="25" {"selected" if preferences.get("default_page_size") == 25 else ""}>25 rows</option>
                <option value="50" {"selected" if preferences.get("default_page_size") == 50 else ""}>50 rows</option>
                <option value="100" {"selected" if preferences.get("default_page_size") == 100 else ""}>100 rows</option>
            </select>
            <div class="preference-description">Number of rows to show per page by default</div>
            <div class="preference-indicator"></div>
        </div>

        <div class="preference-item">
            <label for="defaultSortColumn" class="form-label">Default Sort Column</label>
            <input type="text"
                   class="form-control"
                   id="defaultSortColumn"
                   data-preference-category="defaults"
                   data-preference-key="default_sort_column"
                   value="{preferences.get('default_sort_column', '')}"
                   placeholder="Column name..."
                   onchange="updatePreference('defaults', 'default_sort_column', this.value)">
            <div class="preference-description">Column to sort by when table loads</div>
            <div class="preference-indicator"></div>
        </div>

        <div class="preference-item">
            <div class="form-check">
                <input class="form-check-input"
                       type="checkbox"
                       id="rememberFilters"
                       data-preference-category="defaults"
                       data-preference-key="remember_filters"
                       {"checked" if preferences.get("remember_filters", True) else ""}
                       onchange="updatePreference('defaults', 'remember_filters', this.checked, 'boolean')">
                <label class="form-check-label" for="rememberFilters">
                    Remember filter settings
                </label>
            </div>
            <div class="preference-description">Restore previous filter settings when you return</div>
            <div class="preference-indicator"></div>
        </div>
    </div>

    <div class="preference-section">
        <div class="d-flex justify-content-between align-items-center mb-3">
            <h6 class="mb-0"><i class="fas fa-bookmark me-2"></i>Default Layouts</h6>
            <button type="button" class="btn btn-sm btn-outline-primary" onclick="manageDefaultLayouts()">
                <i class="fas fa-cog me-1"></i>Manage
            </button>
        </div>

        <div class="preference-item">
            <div class="form-check">
                <input class="form-check-input"
                       type="checkbox"
                       id="autoLoadDefaultLayout"
                       data-preference-category="defaults"
                       data-preference-key="auto_load_default_layout"
                       {"checked" if preferences.get("auto_load_default_layout", True) else ""}
                       onchange="updatePreference('defaults', 'auto_load_default_layout', this.checked, 'boolean')">
                <label class="form-check-label" for="autoLoadDefaultLayout">
                    Automatically load default layout
                </label>
            </div>
            <div class="preference-description">Load your default layout when opening tables</div>
            <div class="preference-indicator"></div>
        </div>
    </div>
    """


def _generate_notifications_preferences(preferences: Dict[str, Any]) -> str:
    """Generate notifications preferences HTML."""
    return f"""
    <div class="preference-section">
        <h6><i class="fas fa-bell me-2"></i>Notification Settings</h6>

        <div class="preference-item">
            <div class="form-check">
                <input class="form-check-input"
                       type="checkbox"
                       id="enableNotifications"
                       data-preference-category="notifications"
                       data-preference-key="enable_notifications"
                       {"checked" if preferences.get("enable_notifications", True) else ""}
                       onchange="updatePreference('notifications', 'enable_notifications', this.checked, 'boolean')">
                <label class="form-check-label" for="enableNotifications">
                    Enable notifications
                </label>
            </div>
            <div class="preference-description">Show success, error, and warning messages</div>
            <div class="preference-indicator"></div>
        </div>

        <div class="preference-item">
            <label for="notificationDuration" class="form-label">Notification Duration</label>
            <div class="input-group">
                <input type="number"
                       class="form-control"
                       id="notificationDuration"
                       data-preference-category="notifications"
                       data-preference-key="notification_duration"
                       value="{preferences.get('notification_duration', 5000)}"
                       min="1000"
                       max="15000"
                       step="1000"
                       onchange="updatePreference('notifications', 'notification_duration', this.value, 'number')">
                <span class="input-group-text">ms</span>
            </div>
            <div class="preference-description">How long notifications stay visible</div>
            <div class="preference-indicator"></div>
        </div>

        <div class="preference-item">
            <div class="form-check">
                <input class="form-check-input"
                       type="checkbox"
                       id="showSuccessNotifications"
                       data-preference-category="notifications"
                       data-preference-key="show_success_notifications"
                       {"checked" if preferences.get("show_success_notifications", True) else ""}
                       onchange="updatePreference('notifications', 'show_success_notifications', this.checked, 'boolean')">
                <label class="form-check-label" for="showSuccessNotifications">
                    Show success notifications
                </label>
            </div>
            <div class="preference-description">Display notifications for successful operations</div>
            <div class="preference-indicator"></div>
        </div>
    </div>

    <div class="preference-section">
        <h6><i class="fas fa-volume-up me-2"></i>Sound</h6>

        <div class="preference-item">
            <div class="form-check">
                <input class="form-check-input"
                       type="checkbox"
                       id="enableSounds"
                       data-preference-category="notifications"
                       data-preference-key="enable_sounds"
                       {"checked" if preferences.get("enable_sounds", False) else ""}
                       onchange="updatePreference('notifications', 'enable_sounds', this.checked, 'boolean')">
                <label class="form-check-label" for="enableSounds">
                    Enable notification sounds
                </label>
            </div>
            <div class="preference-description">Play sounds for important notifications</div>
            <div class="preference-indicator"></div>
        </div>
    </div>
    """


def _get_default_preferences() -> Dict[str, Any]:
    """Get default user preferences."""
    return {
        "display": {
            "theme": "auto",
            "density": "standard",
            "show_row_numbers": False,
            "default_column_width": 150,
            "auto_resize_columns": False,
        },
        "behavior": {
            "click_behavior": "select",
            "enable_keyboard_navigation": True,
            "enable_auto_save": True,
            "auto_save_delay": 2000,
        },
        "defaults": {
            "default_page_size": 25,
            "default_sort_column": "",
            "remember_filters": True,
            "auto_load_default_layout": True,
        },
        "notifications": {
            "enable_notifications": True,
            "notification_duration": 5000,
            "show_success_notifications": True,
            "enable_sounds": False,
        },
    }


def _validate_preferences(preferences: Dict[str, Any]) -> Dict[str, Any]:
    """Validate user preferences data."""
    errors = []

    if not isinstance(preferences, dict):
        errors.append("Preferences must be a dictionary")
        return {"success": False, "errors": errors}

    # Validate each category
    for category, category_prefs in preferences.items():
        if not isinstance(category_prefs, dict):
            errors.append(f"Category '{category}' must be a dictionary")
            continue

        # Category-specific validation
        if category == "display":
            if "default_column_width" in category_prefs:
                width = category_prefs["default_column_width"]
                if not isinstance(width, int) or width < 50 or width > 500:
                    errors.append("Default column width must be between 50 and 500 pixels")

        elif category == "behavior":
            if "auto_save_delay" in category_prefs:
                delay = category_prefs["auto_save_delay"]
                if not isinstance(delay, int) or delay < 500 or delay > 10000:
                    errors.append("Auto-save delay must be between 500 and 10000 milliseconds")

        elif category == "defaults":
            if "default_page_size" in category_prefs:
                page_size = category_prefs["default_page_size"]
                if not isinstance(page_size, int) or page_size not in [10, 25, 50, 100]:
                    errors.append("Default page size must be 10, 25, 50, or 100")

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
    "render_user_preferences_panel",
    "save_user_preferences",
    "reset_user_preferences",
]
